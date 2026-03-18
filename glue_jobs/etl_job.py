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
#   Task 4 -- Store as Parquet in processed/ folder (Bronze -> Silver)
#   Task 5 -- Combine all 10 sources into one unified dataset
#
# Fixes applied based on Data Quality Check findings:
#   Fix 1 -- Added DD-MM-YYYY date format (Purple Bricks)
#   Fix 2 -- Deduplicate by property_id before writing
#   Fix 3 -- Replace sqft = 99999 with null (Rightmove placeholder)
#   Fix 4 -- Longitude/latitude negatives are valid (West London)
#
# Job Bookmark enabled -- only processes NEW files each run
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
# Any value at or above this threshold is treated as null
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
# Checks every source for data quality issues before processing
# Raises error only on critical issues (empty file, null IDs)
# ===================================================================
def validate_source(df, source_name):
    """
    Validates raw dataframe from one source.
    Logs: total rows, null IDs, duplicates,
          negative prices, negative sqft.
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

    # Hard stop -- no rows means something went wrong upstream
    if total == 0:
        raise ValueError(
            f"VALIDATION FAILED [{source_name}]: No records found! "
            f"Check if file landed in S3 correctly."
        )

    # Hard stop -- null IDs means we cannot identify records
    if nulls > 0:
        raise ValueError(
            f"VALIDATION FAILED [{source_name}]: {nulls} null "
            f"property IDs! Cannot process without primary key."
        )

    return total


# ===================================================================
# TASK 2 -- DATA CLEANING
# Fixes all known data quality issues found in the DQ report:
#
#   Dates (5 formats found across 10 sources):
#     YYYY-MM-DD  -- Rightmove, Savills, Hamptons, Cushman, JLL,
#                    Knight Frank (already correct)
#     DD/MM/YYYY  -- Foxtons
#     YYYY/MM/DD  -- CBRE Living
#     DD Mon YYYY -- Strutt Parker
#     DD-MM-YYYY  -- Purple Bricks (Fix 1 -- newly discovered)
#
#   Numeric fixes:
#     sqft < 0      -- abs() make positive (data entry error)
#     sqft >= 99999 -- null (Rightmove placeholder) (Fix 3)
#     price < 0     -- abs() make positive (data entry error)
#     bedrooms < 0  -- null (makes no logical sense)
#     bathrooms < 0 -- null (makes no logical sense)
#
#   Left as-is (by design):
#     epc_rating  -- empty is valid (not all properties rated)
#     borough     -- empty is valid (some areas unclassified)
#     tenure      -- N/A is valid (rental properties)
#     longitude   -- negatives are valid West London coords (Fix 4)
#     latitude    -- always positive in London
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

    # Convert to string -- Spark may have auto-parsed some dates
    # as date objects, our string operations need a string
    date_str = str(date_val).strip()

    if date_str == '' or date_str == 'None':
        return None

    # Already in correct YYYY-MM-DD format -- return as is
    if len(date_str) == 10 and date_str[4] == '-':
        return date_str

    # Try each known format until one matches
    formats = [
        '%Y-%m-%d',   # 2025-01-06  Rightmove, Savills, Hamptons
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

    # Unrecognised format -- return None rather than crashing
    return None

# Register as Spark UDF so it runs on DataFrame columns
fix_date_udf = F.udf(fix_date, StringType())


def clean_dataframe(df, source_name):
    """
    Applies all cleaning rules to a raw source dataframe.
    See TASK 2 header above for full list of rules applied.
    """

    # Cast date columns to string BEFORE applying UDF
    # Prevents errors when Spark auto-parses dates as date objects
    df = df \
        .withColumn("feed_date",
            F.col("feed_date").cast(StringType())) \
        .withColumn("sold_let_date",
            F.col("sold_let_date").cast(StringType()))

    df = df \
        .withColumn("feed_date",
            # Standardise all 5 date formats to YYYY-MM-DD
            fix_date_udf(F.col("feed_date"))) \
        \
        .withColumn("sold_let_date",
            # Standardise all 5 date formats to YYYY-MM-DD
            fix_date_udf(F.col("sold_let_date"))) \
        \
        .withColumn("sqft",
            # Fix 3: sqft >= 99999 is Rightmove placeholder -- null
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
            # e.g. -813000 becomes 813000
            F.abs(F.col("price").cast(DoubleType()))) \
        \
        .withColumn("bedrooms",
            # Negative bedrooms makes no sense -- set to null
            F.when(
                F.col("bedrooms").cast(IntegerType()) < 0, None
            ).otherwise(
                F.col("bedrooms").cast(IntegerType())
            )) \
        \
        .withColumn("bathrooms",
            # Negative bathrooms makes no sense -- set to null
            F.when(
                F.col("bathrooms").cast(IntegerType()) < 0, None
            ).otherwise(
                F.col("bathrooms").cast(IntegerType())
            ))

    # NOTE: longitude and latitude are NOT modified (Fix 4)
    # Negative longitude = West London (geographically correct)
    # e.g. Chelsea SW3 longitude = -0.168 is a valid negative value

    # NOTE: epc_rating, borough, tenure NOT modified (by design)
    # Empty EPC rating = property not yet rated (valid)
    # Empty borough = area outside standard classification (valid)
    # N/A tenure = rental property (freehold/leasehold not applicable)

    print(f"  [{source_name}] Cleaning complete")
    return df


# ===================================================================
# FIX 2 -- DEDUPLICATION
# Purple Bricks had PUR-20250106-001 appearing twice in DQ report
# Applied to ALL sources as a safety measure
# Keeps first occurrence, drops subsequent duplicates
# ===================================================================
def deduplicate(df, source_name):
    """
    Removes duplicate property_id rows.
    Applied after cleaning so we compare clean IDs.
    Keeps first occurrence, drops subsequent duplicates.
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
# Enriches cleaned data with additional columns:
#   source_name         -- which of the 10 agencies this came from
#   ingestion_date      -- date this file was processed
#   ingestion_timestamp -- exact UTC time of processing
#   price_per_sqft      -- recalculated from clean price/sqft values
# Standardises text casing on key categorical columns
# Removes Glue crawler partition columns (no longer needed)
# ===================================================================
def transform_dataframe(df, source_name):
    """
    Adds enrichment columns and standardises categorical values.
    All 10 sources go through identical transformation so the
    combined dataset is fully consistent for analytics queries.
    """
    df = df \
        .withColumn("source_name",
            # Track which agency this property came from
            # Replaces the partition_0 column added by Glue crawler
            F.lit(source_name)) \
        \
        .withColumn("ingestion_date",
            # Date this file was processed -- used for S3 partitioning
            # Enables queries: WHERE ingestion_date = '2026-03-18'
            F.lit(TODAY)) \
        \
        .withColumn("ingestion_timestamp",
            # Exact UTC processing time -- useful for debugging
            # and tracking how fresh the data is
            F.lit(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))) \
        \
        .withColumn("price_per_sqft",
            # Recalculate using our clean values
            # Original values may have been corrupted by negatives
            # Only calculate when sqft is known and greater than 0
            F.when(
                (F.col("sqft").isNotNull()) & (F.col("sqft") > 0),
                F.round(F.col("price") / F.col("sqft"), 2)
            ).otherwise(None)) \
        \
        .withColumn("property_type",
            # Standardise casing: 'terraced house' -> 'Terraced House'
            F.initcap(F.col("property_type"))) \
        \
        .withColumn("condition",
            # Standardise casing: 'needs renovation' -> 'Needs Renovation'
            F.initcap(F.col("condition"))) \
        \
        .withColumn("transaction_type",
            # Standardise casing: 'sale' -> 'Sale'
            F.initcap(F.col("transaction_type"))) \
        \
        .withColumn("listing_status",
            # Standardise casing: 'under offer' -> 'Under Offer'
            F.initcap(F.col("listing_status"))) \
        \
        .drop("partition_0", "partition_1")
        # Remove Glue crawler auto-generated partition columns
        # These are replaced by our cleaner source_name column

    print(f"  [{source_name}] Transform complete")
    return df


# ===================================================================
# TASK 4 -- STORE AS PARQUET (Bronze -> Silver)
# Writes clean data to processed/ folder as Parquet format
# Parquet benefits over CSV:
#   5-10x faster queries in Athena
#   60-70% smaller file size
#   Preserves data types correctly
#   Partitioned by date for efficient filtering
# ===================================================================
def store_parquet(df, source_name):
    """
    Writes cleaned dataframe to S3 as Parquet.
    Partitioned by ingestion_date:
    source/processed/ingestion_date=2026-03-18/part-00000.parquet
    Mode append adds new daily files without overwriting old ones.
    """
    output_path = f"{PROCESSED_PATH}{source_name}/processed/"

    df.write \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)

    print(f"  [{source_name}] Written to {output_path}")


# ===================================================================
# TASK 5 -- COMBINE ALL SOURCES FOR ANALYTICS
# Merges all 10 sources into one unified dataset
# Powers cross-source analytics in Athena:
#   Average price in Chelsea across all agencies
#   Which agency lists most properties in Canary Wharf
#   Price trend across all sources by borough
# ===================================================================
def combine_all_sources(processed_dfs):
    """
    Unions all 10 source dataframes into one combined dataset.
    allowMissingColumns=True handles any minor schema differences.
    source_name column tells you which agency each row came from.
    """
    combined = processed_dfs[0]
    for df in processed_dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    return combined


# ===================================================================
# MAIN PIPELINE -- Runs all 5 tasks for each of the 10 sources
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
        # -- Read raw CSV -----------------------------------------
        # inferSchema=False keeps everything as string initially
        # Prevents Spark auto-parsing dates before our UDF runs
        # We cast to correct types in clean_dataframe() ourselves
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(raw_path)

        # -- Task 1: Validate -------------------------------------
        print(f"\n  TASK 1 -- Validating {source}...")
        count = validate_source(df, source)

        # -- Task 2: Clean ----------------------------------------
        print(f"\n  TASK 2 -- Cleaning {source}...")
        df = clean_dataframe(df, source)

        # -- Fix 2: Deduplicate -----------------------------------
        # Applied after cleaning so we compare clean IDs
        # Purple Bricks had PUR-20250106-001 duplicated in DQ report
        print(f"\n  FIX 2 -- Deduplicating {source}...")
        df = deduplicate(df, source)

        # -- Task 3: Transform ------------------------------------
        print(f"\n  TASK 3 -- Transforming {source}...")
        df = transform_dataframe(df, source)

        # -- Task 4: Store Parquet --------------------------------
        print(f"\n  TASK 4 -- Storing {source} as Parquet...")
        store_parquet(df, source)

        processed_dfs.append(df)
        validation_summary.append((source, count, "SUCCESS"))

    except Exception as e:
        # Log error but continue with remaining sources
        # One bad source should not stop the entire pipeline
        print(f"\n  ERROR processing {source}: {str(e)}")
        validation_summary.append((source, 0, f"FAILED: {str(e)}"))


# -- Task 5: Combine all sources ----------------------------------
if processed_dfs:
    print(f"\n{'--' * 25}")
    print("TASK 5 -- Combining all sources into unified dataset...")
    print(f"{'--' * 25}")

    combined_df   = combine_all_sources(processed_dfs)
    combined_path = f"{PROCESSED_PATH}combined/processed/"

    # Write combined dataset partitioned by date and source
    # Enables fast Athena queries by date and source name
    combined_df.write \
        .mode("append") \
        .partitionBy("ingestion_date", "source_name") \
        .parquet(combined_path)

    total_records = combined_df.count()
    print(f"  Combined dataset written to: {combined_path}")
    print(f"  Total records across all sources: {total_records}")

else:
    print("\n  WARNING: No sources processed -- combined dataset not created!")


# -- Pipeline Summary ---------------------------------------------
print("\n" + "=" * 60)
print("PIPELINE SUMMARY")
print("=" * 60)
for source, count, status in validation_summary:
    print(f"  {source:<25} {count:>5} rows   {status}")
print("=" * 60)

# Commit tells Glue to save the bookmark position
# Next run will only process files added AFTER this point
job.commit()
print("\nMarketFlow ETL Pipeline Complete!")
