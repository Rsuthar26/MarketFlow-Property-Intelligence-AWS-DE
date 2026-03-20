# MarketFlow Property Intelligence
## AWS Data Engineering Pipeline

---

## Overview

MarketFlow receives daily property data from **10 real estate agencies** across London. This pipeline automatically ingests, validates, cleans, transforms and serves that data — from a GitHub commit all the way to an Athena SQL query.

```
GitHub push
    ↓
GitHub Actions (CI/CD)
    ↓
S3 raw-data/ (Bronze)
    ↓
Step Functions DAG
    ↓
Glue DQ Check → Glue ETL
    ↓
S3 processed/ Parquet (Silver)
    ↓
Glue Crawler → Athena
```

---

## Architecture

| Layer | AWS Service | Purpose |
|---|---|---|
| Source control | GitHub + GitHub Actions | Version control and CI/CD trigger |
| Storage (Bronze) | Amazon S3 `raw-data/` | Raw CSVs as received — never modified |
| Validation | AWS Glue DQ Check Job | Profile and validate all 10 sources |
| Processing (Silver) | AWS Glue ETL Job | Clean, transform, write Parquet |
| Orchestration | AWS Step Functions | Automate the full pipeline DAG |
| Analytics | Amazon Athena | SQL queries on clean Parquet data |
| Monitoring | AWS CloudWatch | Alerts for failures and missing data |

---

## Repository Structure

```
MarketFlow-Property-Intelligence-AWS-DE/
├── .github/workflows/deploy.yml   # CI/CD pipeline
├── data/                          # Raw CSV files (10 sources)
│   ├── cbre-living/
│   ├── cushman-wakefield/
│   ├── foxtons/
│   ├── hamptons/
│   ├── jll-residential/
│   ├── knight-frank/
│   ├── purple-bricks/
│   ├── right-move/
│   ├── savills/
│   └── strutt-parker/
├── glue_jobs/
│   ├── etl_job.py                 # ETL transformation script
│   └── data_quality_check.py     # Data profiling script
└── README.md
```

---

## Data Sources

| Source | Agency | Date Format |
|---|---|---|
| `cbre-living` | CBRE Living | YYYY/MM/DD |
| `cushman-wakefield` | Cushman & Wakefield | YYYY-MM-DD |
| `foxtons` | Foxtons | DD/MM/YYYY |
| `hamptons` | Hamptons | YYYY-MM-DD |
| `jll-residential` | JLL Residential | YYYY-MM-DD |
| `knight-frank` | Knight Frank | YYYY-MM-DD |
| `purple-bricks` | Purplebricks | DD-MM-YYYY |
| `right-move` | Rightmove Partners | YYYY-MM-DD |
| `savills` | Savills | YYYY-MM-DD |
| `strutt-parker` | Strutt & Parker | DD Mon YYYY |

All 10 sources share the same **28-column schema** — enabling unified cross-agency analytics.

---

## Pipeline DAG (Step Functions)

```
Start
  ↓
task_1_validate_data      → Glue: marketflow-data-quality-check
  ↓
task_2_clean_data         → Glue: marketflow-glue-etl-job
  ↓
task_3_transform_dataset  → Pass (handled within ETL job)
  ↓
task_4_store_parquet      → Pass (confirmed within ETL job)
  ↓
task_5_trigger_analytics  → Glue: StartCrawler
  ↓
pipeline_succeeded ✅

Any failure → pipeline_failed ❌ (with error details captured)
```

---

## ETL Cleaning Rules

| Column | Rule | Reason |
|---|---|---|
| `feed_date` / `sold_let_date` | Standardise to YYYY-MM-DD | 5 different formats across sources |
| `sqft` | `abs()` if negative, `null` if ≥ 99999 | Data entry errors + Rightmove placeholder |
| `price` | `abs()` if negative | Data entry error |
| `bedrooms` / `bathrooms` | `null` if negative | Logically impossible |
| `epc_rating` | Leave as-is | Empty = not yet rated (valid) |
| `borough` | Leave as-is | Empty = unclassified area (valid) |
| `tenure` | Leave as-is | N/A = rental property (valid) |
| `longitude` / `latitude` | Leave as-is | Negatives = West London (geographically correct) |

---

## S3 Storage Structure

```
marketflow-property-intelligence-data/
├── {source}/raw-data/      ← Bronze: original CSVs (never modified)
├── {source}/processed/     ← Silver: Parquet partitioned by feed_date
│   ├── feed_date=2025-01-06/part-00000.parquet
│   └── feed_date=2025-01-07/part-00000.parquet
├── combined/processed/     ← All 10 sources unified for Athena
├── glue-scripts/           ← Scripts auto-deployed by GitHub Actions
├── athena-results/         ← Athena query output
└── temp/                   ← Glue temp files
```

**Partitioned by `feed_date`** (actual data date, not pipeline run date). Each day gets its own isolated partition — new uploads add new partitions without touching existing ones.

---

## Data Quality Check Report

**Overall Score: 94.5/100 — EXCELLENT**

| Source | Score | Key Finding |
|---|---|---|
| cbre-living | 95% | 1 negative price |
| cushman-wakefield | 100% | Clean ✓ |
| foxtons | 95% | 1 negative price |
| hamptons | 90% | 1 negative price + 1 negative sqft |
| jll-residential | 100% | Clean ✓ |
| knight-frank | 95% | 1 negative sqft |
| purple-bricks | 80% | 1 duplicate property ID |
| right-move | 95% | sqft=99999 placeholder + 1 negative sqft |
| savills | 100% | Clean ✓ |
| strutt-parker | 95% | 2 negative prices |

---

## Athena Results — Verified Clean Data

```sql
SELECT source_name, feed_date, COUNT(*) as rows
FROM processed_processed
GROUP BY source_name, feed_date
ORDER BY source_name, feed_date
```

| source_name | 2025-01-06 | 2025-01-07 |
|---|---|---|
| cbre-living | 13 | 19 |
| cushman-wakefield | 20 | 21 |
| foxtons | 19 | 13 |
| hamptons | 16 | 12 |
| jll-residential | 15 | 12 |
| knight-frank | 18 | 13 |
| purple-bricks | 19 | 15 |
| right-move | 14 | 12 |
| savills | 18 | 12 |
| strutt-parker | 22 | 20 |

**Total: 350 rows | 20 partitions | 0 duplicates**

---

## CloudWatch Monitoring

| Alarm | Triggers when | Notifies |
|---|---|---|
| `marketflow-pipeline-failure-alert` | Step Functions execution fails | Email via SNS |
| `marketflow-glue-job-failed-tasks` | Glue ETL has failed Spark tasks | Email via SNS |
| `marketflow-pipeline-no-execution` | No pipeline run detected | Email via SNS |

---

## CI/CD — GitHub Actions Workflow

Every push to `main` automatically:

1. ✅ Validates Python syntax of both Glue scripts
2. ✅ Syncs CSV data files to S3 `raw-data/` folders
3. ✅ Uploads Glue scripts to S3 `glue-scripts/`
4. ✅ Updates both Glue jobs (locked to GlueVersion 5.0)
5. ✅ Triggers Step Functions pipeline execution

---

## AWS Resources

| Resource | Name / ARN |
|---|---|
| S3 Bucket | `marketflow-property-intelligence-data` |
| Glue DQ Job | `marketflow-data-quality-check` |
| Glue ETL Job | `marketflow-glue-etl-job` |
| Raw Crawler | `marketflow-raw-crawler` |
| Processed Crawler | `marketflow-processed-crawler` |
| Step Functions | `marketflow-pipeline-dag` |
| Glue Database (raw) | `marketflow_raw` |
| Glue Database (clean) | `marketflow_processed` |
| Athena Table | `processed_processed` |
| SNS Topic | `marketflow-pipeline-alerts` |
| Region | `eu-north-1` |

---
