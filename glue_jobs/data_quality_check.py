import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════
# MarketFlow Property Intelligence — Data Quality Check Job
# ═══════════════════════════════════════════════════════════════════
# Purpose:
#   Runs BEFORE the ETL job as Step 1 in the pipeline.
#   Profiles every raw CSV across all 10 sources and produces
#   a clear readable report showing exactly what's in the data.
#
# This report is what we used to decide the cleaning rules in
#   the ETL job. Run this whenever new data arrives to check
#   if anything unexpected has changed.
#
# Job Bookmark: DISABLED — always checks all files every run
# ═══════════════════════════════════════════════════════════════════

# ── Initialise Glue job ───────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Constants ─────────────────────────────────────────────────────
BUCKET  = "marketflow-property-intelligence-data"
TODAY   = datetime.today().strftime('%Y-%m-%d')

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

# Columns we expect in every source file
# If any of these are missing the report will flag it
EXPECTED_COLUMNS = [
    "property_id", "source_agency", "feed_date", "sold_let_date",
    "transaction_type", "listing_status", "address", "area",
    "borough", "postcode", "property_type", "tenure",
    "bedrooms", "bathrooms", "sqft", "price", "price_per_sqft",
    "condition", "epc_rating", "heating_type", "garden",
    "parking", "floor_level", "days_on_market", "agent_id",
    "is_chain_free", "longitude", "latitude"
]

# Numeric columns — we check these for negatives and outliers
NUMERIC_COLUMNS = [
    "bedrooms", "bathrooms", "sqft", "price",
    "price_per_sqft", "days_on_market", "longitude", "latitude"
]

# Date columns — we check these for format consistency
DATE_COLUMNS = ["feed_date", "sold_let_date"]

# Categorical columns — we check unique values
CATEGORICAL_COLUMNS = [
    "transaction_type", "listing_status", "property_type",
    "tenure", "condition", "epc_rating", "heating_type",
    "garden", "parking", "is_chain_free"
]


# ═══════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def detect_date_format(date_str):
    """
    Detects which date format a string is using.
    Returns a human readable label for the report.
    """
    if date_str is None or date_str.strip() == '':
        return 'empty'
    date_str = date_str.strip()
    from datetime import datetime
    formats = {
        '%Y-%m-%d': 'YYYY-MM-DD',
        '%d/%m/%Y': 'DD/MM/YYYY',
        '%Y/%m/%d': 'YYYY/MM/DD',
        '%d %b %Y': 'DD Mon YYYY',
    }
    for fmt, label in formats.items():
        try:
            datetime.strptime(date_str, fmt)
            return label
        except ValueError:
            continue
    return f'UNKNOWN ({date_str})'

detect_date_format_udf = F.udf(detect_date_format)


def print_divider(char='═', width=60):
    print(char * width)

def print_section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

def print_field(label, value, indent=4, width=25):
    spaces = ' ' * indent
    print(f"{spaces}{label:<{width}}: {value}")


# ═══════════════════════════════════════════════════════════════════
# MAIN PROFILING FUNCTION
# Runs all quality checks on one source and prints the report
# ═══════════════════════════════════════════════════════════════════

def profile_source(source_name):
    """
    Profiles all raw CSV files for one source.
    Checks and reports:
      1. Basic stats (row count, column count)
      2. Schema check (expected vs actual columns)
      3. Primary key quality (nulls, duplicates)
      4. Numeric column analysis (negatives, zeros, min, max, avg)
      5. Date format detection per column
      6. Null and empty value counts per column
      7. Categorical column unique values
      8. Overall data quality score
    """
    raw_path = f"s3://{BUCKET}/{source_name}/raw-data/"

    try:
        # Read raw CSV — keep everything as string
        # so we can detect formats and issues accurately
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(raw_path)

        total_rows = df.count()
        actual_cols = df.columns

        # ── 1. BASIC STATS ────────────────────────────────────────
        print_section(f"SOURCE: {source_name.upper()}")
        print_field("Total rows",    total_rows)
        print_field("Total columns", len(actual_cols))
        print_field("File location", raw_path)

        # ── 2. SCHEMA CHECK ───────────────────────────────────────
        # Compare expected columns to actual columns in the file
        print(f"\n    SCHEMA CHECK:")
        missing_cols = [c for c in EXPECTED_COLUMNS if c not in actual_cols]
        extra_cols   = [c for c in actual_cols
                        if c not in EXPECTED_COLUMNS
                        and c not in ['partition_0', 'partition_1']]

        if not missing_cols and not extra_cols:
            print(f"      ✓ All {len(EXPECTED_COLUMNS)} expected columns present")
        if missing_cols:
            print(f"      ✗ MISSING columns  : {', '.join(missing_cols)}")
        if extra_cols:
            print(f"      ⚠ EXTRA columns    : {', '.join(extra_cols)}")

        # ── 3. PRIMARY KEY QUALITY ────────────────────────────────
        # property_id must be unique and non-null for every record
        print(f"\n    PRIMARY KEY (property_id):")
        null_ids  = df.filter(
            F.col("property_id").isNull() |
            (F.col("property_id") == '')
        ).count()
        dupe_ids  = total_rows - df.dropDuplicates(["property_id"]).count()

        print_field("Null IDs",      f"{null_ids}  {'✓' if null_ids == 0 else '✗ ACTION NEEDED'}", indent=6)
        print_field("Duplicate IDs", f"{dupe_ids}  {'✓' if dupe_ids == 0 else '⚠ CHECK NEEDED'}", indent=6)

        # Show sample of duplicate IDs if any exist
        if dupe_ids > 0:
            dupes_df = df.groupBy("property_id") \
                         .count() \
                         .filter(F.col("count") > 1) \
                         .limit(5)
            print(f"      Sample duplicate IDs:")
            for row in dupes_df.collect():
                print(f"        {row['property_id']} appears {row['count']} times")

        # ── 4. NUMERIC COLUMN ANALYSIS ────────────────────────────
        # Check each numeric column for negatives, zeros, min, max
        print(f"\n    NUMERIC COLUMNS:")
        for col in NUMERIC_COLUMNS:
            if col not in actual_cols:
                continue

            # Cast to double for numeric operations
            num_df = df.withColumn(
                f"{col}_num",
                F.col(col).cast(DoubleType())
            )

            nulls     = num_df.filter(F.col(f"{col}_num").isNull()).count()
            negatives = num_df.filter(F.col(f"{col}_num") < 0).count()
            zeros     = num_df.filter(F.col(f"{col}_num") == 0).count()

            # Get min, max, avg for non-null values
            stats = num_df.filter(
                F.col(f"{col}_num").isNotNull()
            ).agg(
                F.min(f"{col}_num").alias("min"),
                F.max(f"{col}_num").alias("max"),
                F.round(F.avg(f"{col}_num"), 2).alias("avg")
            ).collect()[0]

            # Build status indicators
            neg_flag  = f"⚠ {negatives} negatives" if negatives > 0 else "✓"
            zero_flag = f"⚠ {zeros} zeros"         if zeros > 0     else ""
            null_flag = f"⚠ {nulls} nulls"          if nulls > 0     else "✓ no nulls"

            print(f"      {col:<20} "
                  f"min:{str(stats['min']):<10} "
                  f"max:{str(stats['max']):<12} "
                  f"avg:{str(stats['avg']):<10} "
                  f"nulls:{nulls:<4} "
                  f"{neg_flag}")

        # ── 5. DATE FORMAT DETECTION ──────────────────────────────
        # Detect what date format each source uses
        # This is how we discovered the 4 different formats
        print(f"\n    DATE COLUMNS:")
        for col in DATE_COLUMNS:
            if col not in actual_cols:
                continue

            # Get sample of non-empty dates
            sample_dates = df.filter(
                F.col(col).isNotNull() & (F.col(col) != '')
            ).select(col).limit(10).collect()

            nulls = df.filter(
                F.col(col).isNull() | (F.col(col) == '')
            ).count()

            # Detect formats from sample
            formats_found = set()
            for row in sample_dates:
                fmt = detect_date_format(row[col])
                formats_found.add(fmt)

            # Show sample date values
            samples = [row[col] for row in sample_dates[:3]]

            print(f"      {col:<20} "
                  f"format: {', '.join(formats_found):<15} "
                  f"nulls/empty: {nulls:<4} "
                  f"samples: {samples}")

        # ── 6. NULL AND EMPTY VALUE COUNTS ───────────────────────
        # Count nulls and empty strings for every column
        print(f"\n    NULL / EMPTY VALUES PER COLUMN:")
        has_issues = False
        for col in actual_cols:
            if col in ['partition_0', 'partition_1']:
                continue
            null_count = df.filter(
                F.col(col).isNull() | (F.col(col) == '')
            ).count()
            if null_count > 0:
                pct = round((null_count / total_rows) * 100, 1)
                flag = "⚠" if pct > 10 else "ℹ"
                print(f"      {flag} {col:<22} {null_count:>4} empty/null "
                      f"({pct}% of rows)")
                has_issues = True
        if not has_issues:
            print(f"      ✓ No null or empty values found")

        # ── 7. CATEGORICAL COLUMN UNIQUE VALUES ───────────────────
        # Show all unique values for key categorical columns
        # Helps spot inconsistent casing or unexpected values
        print(f"\n    CATEGORICAL COLUMNS (unique values):")
        for col in CATEGORICAL_COLUMNS:
            if col not in actual_cols:
                continue
            unique_vals = df.select(col) \
                            .distinct() \
                            .orderBy(col) \
                            .collect()
            vals = [str(r[col]) for r in unique_vals if r[col]]
            print(f"      {col:<20} {vals}")

        # ── 8. DATA QUALITY SCORE ─────────────────────────────────
        # Simple scoring: start at 100, deduct for issues found
        score = 100
        issues = []

        if null_ids > 0:
            score -= 30
            issues.append(f"Null property IDs (-30)")
        if dupe_ids > 0:
            score -= 20
            issues.append(f"Duplicate IDs (-20)")
        if missing_cols:
            score -= 20
            issues.append(f"Missing columns (-20)")

        # Check for negatives in price and sqft
        for col in ['price', 'sqft']:
            if col in actual_cols:
                neg_count = df.withColumn(
                    f"{col}_num", F.col(col).cast(DoubleType())
                ).filter(F.col(f"{col}_num") < 0).count()
                if neg_count > 0:
                    score -= 5
                    issues.append(f"Negative {col} (-5)")

        score = max(0, score)
        grade = (
            "EXCELLENT" if score >= 90 else
            "GOOD"      if score >= 75 else
            "FAIR"      if score >= 60 else
            "POOR"
        )

        print(f"\n    DATA QUALITY SCORE:")
        print(f"      Score : {score}/100  [{grade}]")
        if issues:
            print(f"      Issues found:")
            for issue in issues:
                print(f"        - {issue}")
        else:
            print(f"      ✓ No critical issues found")

        return {
            'source': source_name,
            'rows': total_rows,
            'score': score,
            'grade': grade,
            'null_ids': null_ids,
            'dupe_ids': dupe_ids,
            'missing_cols': missing_cols,
            'status': 'PROFILED'
        }

    except Exception as e:
        print(f"\n  ✗ ERROR profiling {source_name}: {str(e)}")
        return {
            'source': source_name,
            'rows': 0,
            'score': 0,
            'grade': 'ERROR',
            'status': f'FAILED: {str(e)}'
        }


# ═══════════════════════════════════════════════════════════════════
# RUN PROFILING FOR ALL 10 SOURCES
# ═══════════════════════════════════════════════════════════════════
print_divider()
print(f"  MarketFlow — Data Quality Report")
print(f"  Generated : {TODAY}")
print(f"  Sources   : {len(SOURCES)}")
print(f"  Bucket    : s3://{BUCKET}/")
print_divider()

# Run profiling for each source and collect results
results = []
for source in SOURCES:
    result = profile_source(source)
    results.append(result)


# ═══════════════════════════════════════════════════════════════════
# FINAL SUMMARY — All sources at a glance
# ═══════════════════════════════════════════════════════════════════
print(f"\n\n")
print_divider()
print(f"  OVERALL PIPELINE SUMMARY")
print_divider()
print(f"  {'Source':<25} {'Rows':>6}  {'Score':>6}  {'Grade':<10}  Status")
print(f"  {'─'*25} {'─'*6}  {'─'*6}  {'─'*10}  {'─'*20}")

total_rows_all = 0
scores         = []

for r in results:
    total_rows_all += r.get('rows', 0)
    scores.append(r.get('score', 0))
    grade = r.get('grade', 'N/A')
    grade_icon = (
        "✓" if grade in ['EXCELLENT', 'GOOD'] else
        "⚠" if grade == 'FAIR' else
        "✗"
    )
    print(f"  {r['source']:<25} "
          f"{r.get('rows', 0):>6}  "
          f"{r.get('score', 0):>5}%  "
          f"{grade:<10}  "
          f"{grade_icon} {r.get('status', '')}")

# Overall pipeline score
avg_score    = round(sum(scores) / len(scores), 1) if scores else 0
overall_grade = (
    "EXCELLENT" if avg_score >= 90 else
    "GOOD"      if avg_score >= 75 else
    "FAIR"      if avg_score >= 60 else
    "POOR"
)

print(f"\n  {'─'*60}")
print(f"  Total records across all sources : {total_rows_all}")
print(f"  Average data quality score       : {avg_score}/100")
print(f"  Overall pipeline grade           : {overall_grade}")
print_divider()

# ── Pipeline recommendation ───────────────────────────────────────
print(f"\n  RECOMMENDATION:")
if avg_score >= 75:
    print(f"  ✓ Data quality is acceptable — safe to run ETL job")
    print(f"  → Run: marketflow-glue-etl-job")
elif avg_score >= 60:
    print(f"  ⚠ Data quality has some issues — review before ETL")
    print(f"  → Check flagged sources above before running ETL job")
else:
    print(f"  ✗ Data quality is poor — DO NOT run ETL job")
    print(f"  → Fix source data issues before proceeding")

print_divider()

job.commit()
print("\nData Quality Check Complete!")
