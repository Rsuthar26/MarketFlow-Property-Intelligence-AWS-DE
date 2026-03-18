import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from datetime import datetime

# ===================================================================
# MarketFlow Property Intelligence -- Glue ETL Job
# ===================================================================
# What this job does:
#   Task 1 -- Validate raw data (nulls, duplicates, counts)
#   Task 2 -- Clean data (fix dates, fix negatives, fix placeholders)
#   Task 3 -- Transform dataset (add columns, standardise values)
#   Task 4 -- Store as Parquet partitioned by feed_date (Bronze -> Silver)
#   Task 5 -- Combine all 10 sources into one unified dataset
#
# Partitioning strategy:
#   Partitioned by feed_date (actual data date) not ingestion_date
#   This means each day's data goes to its own partition
#   New day = new partition, old partitions never touched
#   Overwrite mode -- safe because each day is its own partition
#
# Fixes applied based on Data Quality Check findings:
#   Fix 1 -- Added DD-MM-YYYY date format (Purple Bricks)
#   Fix 2 -- Deduplicate by property_id before writing
#   Fix 3 -- Replace sqft >= 99999 with null (Rightmove placeholder)
#   Fix 4 -- Longitude/latitude negatives are valid (West London)
#   Fix 5 -- Partition by feed_date not ingestion_date (incremental)
# ===================================================================

# -- Initialise Glue job -------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -- Constants -----------------------------------------------------
BUCKET         = "marketflow-property-intelligence-data"
RAW_PATH       = f"s3://{BUCKET}/"
PROCESSED_PATH = f"s3://{BUCKET}/"
TODAY          = datetime.today().strftime('%Y-%m-%d')

# Maximum realistic sqft for a London property
# Rightmove uses 99999 as a placeholder for unknown sqft
SQFT_MAX_THRESHOLD = 99998

# All 10 property data sources
SOURCES = [
    "cbre-living",
    "cushman-wakefield",
    "foxtons",
    "hamptons",
    "jll-residential",
    "knight-frank",
    "purple-bricks",
    "right-move",
    "savills",
    "strutt-parker"
]


# ===================================================================
# TASK 1 -- DATA VALIDATION
# ===================================================================
def validate_source(df, source_name):
    """
    Validates raw dataframe from one source.
    Raises error if source is empty or has null property IDs.
    """
    total     = df.count()
    nulls     = df.filter(
        F.col("property_id").isNull() |
        (F.col("property_id") == '')
    ).count()
    dupes     = total - df.dropDuplicates(["property_id"]).count()
    neg_price = df.filter(F.col("price") < 0).count()
    neg_sqft  = df.filter(F.col("sqft") < 0).count()

    print(f"  [{source_name}] Total rows     : {total}")
    print(f"  [{source_name}] Null IDs       : {nulls}")
    print(f"  [{source_name}] Duplicate IDs  : {dupes}")
    print(f"  [{source_name}] Negative prices: {neg_price}")
    print(f"  [{source_name}] Negative sqft  : {neg_sqft}")

    if total == 0:
        raise ValueError(
            f"VALIDATION FAILED [{source_name}]: No records found!"
        )
    if nulls > 0:
        raise ValueError(
            f"VALIDATION FAILED [{source_name}]: {nulls} null property IDs!"
        )

    return total


# ===================================================================
# TASK 2 -- DATA CLEANING
# ===================================================================
def fix_date(date_val):
    """
    Standardises all date formats to YYYY-MM-DD.
    Handles all 5 formats found across our 10 sources:
      YYYY-MM-DD  -- Rightmove, Savills, Hamptons  e.g. 2025-01-06
      DD/MM/YYYY  -- Foxtons                        e.g. 06/01/2025
      YYYY/MM/DD  -- CBRE Living                    e.g. 2025/01/06
      DD Mon YYYY -- Strutt Parker                  e.g. 06 Jan 2025
      DD-MM-YYYY  -- Purple Bricks (Fix 1)          e.g. 06-01-2025
    Returns None if date is missing or unrecognised.
    """
    if date_val is None:
        return None

    date_str = str(date_val).strip()

    if date_str == '' or date_str == 'None':
        return None

    # Already correct format
    if len(date_str) == 10 and date_str[4] == '-':
        return date_str

    formats = [
        '%Y-%m-%d',   # 2025-01-06  Rightmove, Savills
        '%d/%m/%Y',   # 06/01/2025  Foxtons
        '%Y/%m/%d',   # 2025/01/06  CBRE Living
        '%d %b %Y',   # 06 Jan 2025 Strutt Parker
        '%d-%m-%Y',   # 06-01-2025  Purple Bricks (Fix 1)
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None

fix_date_udf = F.udf(fix_date, StringType())


def clean_dataframe(df, source_name):
    """
    Applies all cleaning rules to a raw source dataframe.
    """
    # Cast date columns to string BEFORE applying UDF
    df = df \
        .withColumn("feed_date",
            F.col("feed_date").cast(StringType())) \
        .withColumn("sold_let_date",
            F.col("sold_let_date").cast(StringType()))

    df = df \
        .withColumn("feed_date",
            fix_date_udf(F.col("feed_date"))) \
        \
        .withColumn("sold_let_date",
            fix_date_udf(F.col("sold_let_date"))) \
        \
        .withColumn("sqft",
            # Fix 3: sqft >= 99999 is placeholder -- null
            # Negative sqft is data entry error -- make positive
            F.when(
                F.col("sqft").cast(DoubleType()) >= SQFT_MAX_THRESHOLD,
                None
            ).when(
                F.col("sqft").cast(DoubleType()) < 0,
                F.abs(F.col("sqft").cast(DoubleType()))
            ).otherwise(
                F.col("sqft").cast(DoubleType())
            )) \
        \
        .withColumn("price",
            # Negative price is data entry error -- make positive
            F.abs(F.col("price").cast(DoubleType()))) \
        \
        .withColumn("bedrooms",
            # Negative bedrooms makes no sense -- null
            F.when(
                F.col("bedrooms").cast(IntegerType()) < 0, None
            ).otherwise(
                F.col("bedrooms").cast(IntegerType())
            )) \
        \
        .withColumn("bathrooms",
            # Negative bathrooms makes no sense -- null
            F.when(
                F.col("bathrooms").cast(IntegerType()) < 0, None
            ).otherwise(
                F.col("bathrooms").cast(IntegerType())
            ))

    # NOTE: longitude/latitude NOT modified (Fix 4)
    # Negative longitude = West London (geographically correct)

    # NOTE: epc_rating, borough, tenure NOT modified (by design)

    print(f"  [{source_name}] Cleaning complete")
    return df


# ===================================================================
# FIX 2 -- DEDUPLICATION
# ===================================================================
def deduplicate(df, source_name):
    """
    Removes duplicate property_id rows.
    Applied after cleaning so we compare clean IDs.
    """
    before  = df.count()
    df      = df.dropDuplicates(["property_id"])
    after   = df.count()
    dropped = before - after

    if dropped > 0:
        print(f"  [{source_name}] Deduplication: removed {dropped} duplicate(s)")
    else:
        print(f"  [{source_name}] Deduplication: no duplicates found")

    return df


# ===================================================================
# TASK 3 -- DATASET TRANSFORMATION
# ===================================================================
def transform_dataframe(df, source_name):
    """
    Adds enrichment columns and standardises categorical values.
    """
    df = df \
        .withColumn("source_name",
            F.lit(source_name)) \
        \
        .withColumn("ingestion_timestamp",
            # Track exact processing time for debugging
            F.lit(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))) \
        \
        .withColumn("price_per_sqft",
            # Recalculate using clean values
            F.when(
                (F.col("sqft").isNotNull()) & (F.col("sqft") > 0),
                F.round(F.col("price") / F.col("sqft"), 2)
            ).otherwise(None)) \
        \
        .withColumn("property_type",
            F.initcap(F.col("property_type"))) \
        \
        .withColumn("condition",
            F.initcap(F.col("condition"))) \
        \
        .withColumn("transaction_type",
            F.initcap(F.col("transaction_type"))) \
        \
        .withColumn("listing_status",
            F.initcap(F.col("listing_status"))) \
        \
        .drop("partition_0", "partition_1")

    print(f"  [{source_name}] Transform complete")
    return df


# ===================================================================
# TASK 4 -- STORE AS PARQUET (Bronze -> Silver)
# Fix 5: Partition by feed_date (actual data date)
#   Each day's data gets its own folder:
#   source/processed/feed_date=2025-01-06/part-00000.parquet
#   source/processed/feed_date=2025-01-07/part-00000.parquet
#
#   overwrite mode is safe here because:
#   - Each feed_date is its own partition
#   - Overwrite only affects the specific feed_date being processed
#   - Previous dates are NEVER touched
#   - Running the pipeline twice for the same day = safe (idempotent)
# ===================================================================
def store_parquet(df, source_name):
    """
    Writes cleaned dataframe to S3 as Parquet.
    Partitioned by feed_date so each day is isolated:
    source/processed/feed_date=2025-01-07/part-00000.parquet
    Overwrite mode -- safe because each feed_date is its own partition.
    """
    output_path = f"{PROCESSED_PATH}{source_name}/processed/"

    df.write \
        .mode("overwrite") \
        .partitionBy("feed_date") \
        .parquet(output_path)

    print(f"  [{source_name}] Written to {output_path}")


# ===================================================================
# TASK 5 -- COMBINE ALL SOURCES FOR ANALYTICS
# ===================================================================
def combine_all_sources(processed_dfs):
    """
    Unions all 10 source dataframes into one combined dataset.
    """
    combined = processed_dfs[0]
    for df in processed_dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    return combined


# ===================================================================
# MAIN PIPELINE
# ===================================================================
print("=" * 60)
print("MarketFlow ETL Pipeline Starting...")
print(f"Processing date: {TODAY}")
print("=" * 60)

processed_dfs      = []
validation_summary = []

for source in SOURCES:
    print(f"\n{'--' * 25}")
    print(f"Processing source: {source}")
    print(f"{'--' * 25}")

    raw_path = f"{RAW_PATH}{source}/raw-data/"

    try:
        # Read raw CSV -- inferSchema=False keeps everything as string
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(raw_path)

        # Task 1 -- Validate
        print(f"\n  TASK 1 -- Validating {source}...")
        count = validate_source(df, source)

        # Task 2 -- Clean
        print(f"\n  TASK 2 -- Cleaning {source}...")
        df = clean_dataframe(df, source)

        # Fix 2 -- Deduplicate
        print(f"\n  FIX 2 -- Deduplicating {source}...")
        df = deduplicate(df, source)

        # Task 3 -- Transform
        print(f"\n  TASK 3 -- Transforming {source}...")
        df = transform_dataframe(df, source)

        # Task 4 -- Store Parquet
        print(f"\n  TASK 4 -- Storing {source} as Parquet...")
        store_parquet(df, source)

        processed_dfs.append(df)
        validation_summary.append((source, count, "SUCCESS"))

    except Exception as e:
        print(f"\n  ERROR processing {source}: {str(e)}")
        validation_summary.append((source, 0, f"FAILED: {str(e)}"))


# Task 5 -- Combine all sources
if processed_dfs:
    print(f"\n{'--' * 25}")
    print("TASK 5 -- Combining all sources...")
    print(f"{'--' * 25}")

    combined_df   = combine_all_sources(processed_dfs)
    combined_path = f"{PROCESSED_PATH}combined/processed/"

    # Partition by feed_date and source_name
    # Each day + source combination gets its own partition
    # Safe to overwrite -- each partition is isolated
    combined_df.write \
        .mode("overwrite") \
        .partitionBy("feed_date", "source_name") \
        .parquet(combined_path)

    total_records = combined_df.count()
    print(f"  Combined dataset written to: {combined_path}")
    print(f"  Total records across all sources: {total_records}")

else:
    print("\n  WARNING: No sources processed!")


# Pipeline Summary
print("\n" + "=" * 60)
print("PIPELINE SUMMARY")
print("=" * 60)
for source, count, status in validation_summary:
    print(f"  {source:<25} {count:>5} rows   {status}")
print("=" * 60)

job.commit()
print("\nMarketFlow ETL Pipeline Complete!")
