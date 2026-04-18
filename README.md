# Olist AWS → Snowflake Data Pipeline

> End-to-end production data engineering pipeline — from raw e-commerce CSVs to a live analytics dashboard.

[![dbt CI](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/dbt_ci.yml)
[![Deploy Glue](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_glue.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_glue.yml)
[![Deploy Dashboard](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_dashboard.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_dashboard.yml)

---

## Overview

This project implements a fully automated, production-grade data pipeline using the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The pipeline ingests raw CSV data, transforms it into a star-schema data warehouse, and serves a live interactive dashboard — with end-to-end orchestration, monitoring, CI/CD, and security hardening.

**Live dashboard:** [Streamlit Cloud](https://share.streamlit.io)  
**Pipeline runtime:** ~8 minutes end-to-end  
**Data volume:** 112,650 order-item rows across 2016–2018

---

## Architecture

```
EventBridge (daily 02:00 UTC)
        │
        ▼
Step Functions state machine
  ├── ValidateRawData   → Lambda: 28 S3 quality checks
  ├── StartCrawler      → AWS Glue Crawler (9 tables)
  ├── WaitForCrawler    → polls until READY
  ├── StartETL          → AWS Glue ETL (Spark, incremental)
  ├── WaitForETL        → polls until SUCCEEDED
  ├── ValidateProcessed → Lambda: confirms 1 019 Parquet files
  └── SUCCEEDED / FAILED → SNS email alert on failure
        │
        ▼
S3 /processed/ (Parquet, snappy, partitioned by month)
        │
        ▼ (COPY INTO)
Snowflake RAW schema (4 tables, 112k rows)
        │
        ▼ (dbt run)
Snowflake MARTS schema (dim × 3, fct × 2, aggregates)
        │
        ▼
Streamlit Dashboard (public URL)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cloud | AWS (us-east-1) |
| Storage | S3 (data lake: raw + processed) |
| Processing | AWS Glue 4.0 (PySpark 3.3) |
| Orchestration | AWS Step Functions + Lambda |
| Data Warehouse | Snowflake (X-Small WH) |
| Transformation | dbt Core 1.11 + Snowflake adapter |
| Dashboard | Streamlit 1.45, Streamlit Cloud |
| IaC | Terraform 1.5 |
| CI/CD | GitHub Actions |
| Containers | Docker + AWS ECR |
| Security | AWS Secrets Manager, IAM least-privilege |
| Monitoring | CloudWatch, SNS, Dead Letter Queue |

---

## Project Structure

```
olist-aws-snowflake-pipeline/
├── terraform/                  # All AWS infrastructure
│   ├── main.tf                 # Glue, EventBridge, state machine
│   ├── iam.tf                  # Least-privilege IAM roles
│   ├── s3.tf                   # Bucket, encryption, lifecycle
│   ├── lambda.tf               # Lambda functions
│   ├── monitoring.tf           # CloudWatch, SNS, DLQ
│   └── stepfunctions_definition.json
├── lambda_function/
│   ├── lambda_handler.py       # S3 validation + Glue trigger
│   ├── pipeline_orchestrator.py # Step Functions action handler
│   └── data_validator.py       # Multi-stage quality checks
├── glue_jobs/
│   ├── olist_etl_script.py     # PySpark star schema ETL
│   └── schema_validator.py     # CSV schema fingerprinting
├── dbt_olist/
│   ├── models/
│   │   ├── staging/            # 4 thin views over RAW
│   │   └── marts/              # 5 enriched tables
│   ├── tests/                  # Custom row count + anomaly tests
│   ├── seeds/brazil_states.csv # Brazilian state reference data
│   └── snapshots/              # SCD Type 2 on dim_customers
├── dashboard/
│   ├── streamlit_app.py        # Analytics dashboard
│   └── Dockerfile              # Container image
├── sql_scripts/                # Snowflake setup (run once)
│   ├── 01_create_storage_integration.sql
│   ├── 02_create_external_stage.sql
│   ├── 03_create_snowpipe.sql
│   ├── 04_create_marts_schema.sql
│   └── 05_resource_monitor.sql
├── scripts/
│   └── get_secret.py           # Fetch credentials from Secrets Manager
└── .github/workflows/          # CI/CD pipelines
    ├── dbt_ci.yml
    ├── deploy_glue.yml
    └── deploy_dashboard.yml
```

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| AWS CLI | v2 | Deploy and manage AWS resources |
| Terraform | ≥ 1.5 | Provision infrastructure |
| Python | 3.12 | Lambda, dbt, Streamlit |
| dbt-snowflake | 1.11.4 | Data transformations |
| Snowflake account | Any | Data warehouse |

---

## Setup Instructions

### 1. Clone and configure

```bash
git clone https://github.com/noran-salm/olist-aws-snowflake-pipeline.git
cd olist-aws-snowflake-pipeline
cp .env.example .env
# Fill in AWS and Snowflake credentials
```

### 2. Deploy AWS infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### 3. Upload the dataset

```bash
# Download from Kaggle and sync to S3
kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p /tmp/olist/
aws s3 sync /tmp/olist/ s3://olist-lake-<ACCOUNT_ID>/raw/ --sse AES256
```

### 4. Configure Snowflake

```sql
-- Run each script in order in a Snowflake worksheet
-- sql_scripts/01_create_storage_integration.sql
-- sql_scripts/02_create_external_stage.sql
-- sql_scripts/03_create_snowpipe.sql
-- sql_scripts/04_create_marts_schema.sql
```

### 5. Store credentials in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name "olist/snowflake/credentials" \
  --secret-string '{
    "account":"YOUR_ACCOUNT",
    "user":"YOUR_USER",
    "password":"YOUR_PASSWORD",
    "role":"SYSADMIN",
    "database":"OLIST_DW",
    "warehouse":"OLIST_WH",
    "schema":"MARTS"
  }'
```

### 6. Set up dbt

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install dbt-snowflake==1.11.4
eval $(python3 scripts/get_secret.py)
cd dbt_olist && dbt debug
```

### 7. Add GitHub Secrets

In your GitHub repo → Settings → Secrets → Actions, add:

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION          = us-east-1
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
```

---

## Running the Pipeline

### Manual trigger

```bash
SF_ARN=$(aws stepfunctions list-state-machines \
  --query 'stateMachines[?name==`olist-data-pipeline`].stateMachineArn' \
  --output text)

aws stepfunctions start-execution \
  --state-machine-arn $SF_ARN \
  --name "manual-$(date +%Y%m%d-%H%M%S)" \
  --input '{"triggered_by":"manual","run_id":"manual-001"}'
```

### Scheduled trigger

EventBridge fires automatically at **02:00 UTC daily**. No action needed.

### Monitor execution

```bash
# Check latest run status
aws stepfunctions list-executions \
  --state-machine-arn $SF_ARN \
  --query 'executions[0].{Status:status,Start:startDate}' \
  --output table

# Live Glue logs
aws logs tail /aws-glue/jobs/output --follow
```

### Run dbt manually

```bash
source .venv/bin/activate
eval $(python3 scripts/get_secret.py)
cd dbt_olist
dbt run           # run all models
dbt test          # run 43 data quality tests
dbt docs serve    # browse lineage at localhost:8080
```

---

## Data Quality Framework

Quality checks run at three stages:

| Stage | Check type | Tool | Failure action |
|-------|-----------|------|----------------|
| S3 raw | File existence, freshness, row counts, schema | data_validator.py | Block pipeline, SNS alert |
| Post-ETL | Parquet file existence, count > 0 | pipeline_orchestrator.py | Fail Step Functions |
| Snowflake | Nulls, uniqueness, ranges, accepted values | dbt tests | Block dbt run |

**Custom dbt tests:**
- `assert_row_count_in_range.sql` — fct_orders must have 100k–200k rows
- `assert_no_null_year_month.sql` — all orders must have a valid year-month
- `assert_revenue_not_zero.sql` — delivered orders must have non-zero revenue
- `assert_monthly_revenue_completeness.sql` — no month below R$1,000

---

## Monitoring & Debugging

### CloudWatch Dashboard

```
https://us-east-1.console.aws.amazon.com/cloudwatch/home#dashboards:name=OlistPipelineV2
```

Shows: ETL duration, success/failure counts, processed file counts, alarm states.

### Active alarms

| Alarm | Threshold | Action |
|-------|-----------|--------|
| olist-lambda-errors | ≥ 2 errors / 5 min | SNS email |
| olist-lambda-duration | > 240 s | SNS email |
| olist-glue-etl-failed | ≥ 1 failed task | SNS email |
| olist-glue-etl-slow | > 15 min avg | SNS email |
| olist-lambda-dlq-messages | ≥ 1 message | SNS email |

### Debug a failed execution

```bash
# Get the exact failed step
EXEC_ARN="arn:aws:states:us-east-1:..."

aws stepfunctions get-execution-history \
  --execution-arn $EXEC_ARN \
  --output json | python3 -c "
import json,sys
for e in json.load(sys.stdin)['events']:
    if 'Failed' in e.get('type',''):
        d = (e.get('lambdaFunctionFailedEventDetails') or
             e.get('executionFailedEventDetails') or {})
        print(e['type'], '-', d.get('error',''))
        print(d.get('cause','')[:300])
"
```

### Verify data freshness

```sql
SELECT 'MARTS.fct_orders', MAX(updated_at), COUNT(*)
FROM OLIST_DW.MARTS.fct_orders;
```

---

## CI/CD

| Workflow | Trigger | What it does |
|----------|---------|-------------|
| `dbt_ci.yml` | Push / PR touching `dbt_olist/` | Runs `dbt compile + dbt test`. Blocks merge on failure. |
| `deploy_glue.yml` | Push to main touching `glue_jobs/` | Validates Python syntax, uploads ETL script to S3, updates Lambda. |
| `deploy_dashboard.yml` | Push to main touching `dashboard/` | Builds Docker image, pushes to AWS ECR. |

**Zero-downtime deploys:** Glue script is uploaded to S3 before the next run — existing runs complete unaffected. Lambda uses `update-function-code` which is atomic.

---

## Security

- **No plaintext credentials** anywhere in the codebase
- All secrets stored in AWS Secrets Manager under `olist/snowflake/credentials`
- IAM roles follow least-privilege: Lambda can only write to `raw/`, Glue can only write to `processed/`
- S3 bucket enforces TLS (`aws:SecureTransport` deny policy)
- S3 versioning + AES-256 SSE enabled
- Dead letter queue catches silent Lambda failures
- `.gitignore` protects `.env`, `secrets.toml`, and Terraform state files

---

## Cost Optimisation

| Optimisation | Estimated saving |
|-------------|-----------------|
| Glue G.1X × 2 workers (right-sized) | ~60% vs G.2X × 4 |
| Snowflake X-Small, auto-suspend 60s | ~$2–5/month |
| Snowflake resource monitor (20 credits cap) | Hard ceiling on spend |
| S3 Intelligent Tiering (90 days → archive) | ~40% on processed/ |
| dbt `fct_monthly_revenue` pre-aggregation | Reduces dashboard query cost |
| Lambda reserved concurrency: none (pay-per-use) | Near $0 for daily runs |

**Estimated monthly cost (dev usage):** ~$7–15 USD

---

## Snowflake Schema

```
OLIST_DW
├── RAW              ← COPY INTO from S3
│   ├── fct_orders          (112,650 rows)
│   ├── dim_customers       (99,441 rows)
│   ├── dim_products        (32,951 rows)
│   └── dim_sellers         (3,095 rows)
├── STAGING          ← dbt views (thin cleaning layer)
│   ├── stg_orders
│   ├── stg_customers
│   ├── stg_products
│   └── stg_sellers
├── MARTS            ← dbt tables (enriched, analytics-ready)
│   ├── fct_orders          (denormalized, incremental)
│   ├── fct_monthly_revenue (pre-aggregated, incremental)
│   ├── dim_customers       (LTV, segment, region)
│   ├── dim_products        (revenue tier)
│   └── dim_sellers         (GMV tier, late delivery %)
├── SNAPSHOTS        ← SCD Type 2
│   └── customers_snapshot
└── AUDIT            ← Pipeline observability
    └── pipeline_validation_log
```

---

## Future Improvements

| Priority | Improvement | Why |
|----------|-------------|-----|
| High | Add dbt run as Step Functions step | Mart tables currently require manual `dbt run` after ETL |
| High | Enable Snowpipe AUTO_INGEST | Replace manual COPY INTO with event-driven loading |
| High | Configure S3 event notifications → Snowpipe SQS | Completes the Snowpipe AUTO_INGEST setup |
| Medium | Airflow or Prefect orchestration | Better task visibility, retries, and scheduling than Step Functions for dbt |
| Medium | Data contracts on RAW tables | Prevent schema drift from silently corrupting COPY INTO |
| Medium | Row-level freshness SLA | Alert if `max(order_purchase_timestamp)` is stale by > 48h |
| Low | Streaming ingestion (Kinesis) | Enable near-real-time updates if Olist exposes a live API |
| Low | Great Expectations integration | Richer data quality profiling with HTML reports |
| Low | Terraform remote state (S3 backend) | Safer state management for team environments |

---

## License

MIT — free for personal and educational use.

---

## Author

Built as a Data Zoomcamp final project.  
Pipeline designed and implemented from scratch using AWS, Snowflake, dbt, and Streamlit.