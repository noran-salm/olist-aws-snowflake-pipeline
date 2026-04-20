# Olist E-Commerce Data Pipeline


[![dbt CI](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/dbt_ci.yml)
[![Deploy Glue](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_glue.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_glue.yml)
[![Deploy Dashboard](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_dashboard.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_dashboard.yml)

A production-grade, end-to-end data engineering pipeline built on AWS and Snowflake. It transforms raw Brazilian e-commerce data into a live analytics dashboard through a fully automated, validated, monitored, and tested infrastructure — provisioned entirely with Terraform.

**Live Dashboard:** [https://olist-aws-app-pipeline-nagr2optdeyqzxnrqzxhv3.streamlit.app](https://olist-aws-app-pipeline-nagr2optdeyqzxnrqzxhv3.streamlit.app)

**Dataset:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 112,650 order-item records · 2016–2018

---

## Architecture Overview

The pipeline follows a layered medallion architecture across five logical zones:

| Layer | Purpose | Tools |
|---|---|---|
| Ingestion | Land raw CSVs to S3, catalog schema | S3, Glue Crawler, Glue ETL |
| Orchestration | Sequence all steps, handle retries and failures | Step Functions, Lambda, EventBridge |
| Warehouse | Store raw, staged, and mart-level data | Snowflake (RAW → STAGING → MARTS) |
| Transformation | Apply business logic, validate quality | dbt Core (11 models, 51 tests) |
| Visualization | Serve analytics to end users | Streamlit, Plotly |

A cross-cutting **Infrastructure** layer (Terraform) provisions and manages all AWS resources. A **Monitoring** layer (CloudWatch + SNS + DLQ) ensures operational visibility. A **CI/CD** layer (GitHub Actions) automates testing and deployment on every push.

---

## Tech Stack

| Category | Technology |
|---|---|
| Orchestration | AWS Step Functions, AWS EventBridge |
| Compute | AWS Lambda (Python 3.12), AWS Glue 4.0 (PySpark 3.3) |
| Storage | Amazon S3 (raw + processed zones) |
| Data Warehouse | Snowflake (X-Small WH, auto-suspend 60 s) |
| Transformation | dbt Core 1.11, dbt-snowflake adapter |
| Visualization | Streamlit 1.45, Plotly 5.24 |
| Containerization | Docker, Amazon ECR |
| CI/CD | GitHub Actions (3 workflows) |
| Monitoring | AWS CloudWatch, AWS SNS, AWS SQS (DLQ) |
| Security | AWS Secrets Manager, IAM least-privilege |
| Infrastructure as Code | Terraform 1.5 |

---

## Project Structure

```
olist-aws-snowflake-pipeline/
│
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml
│       ├── deploy_glue.yml
│       └── deploy_dashboard.yml
│
├── terraform/                     
│   ├── main.tf                   
│   ├── s3.tf
│   ├── iam.tf
│   ├── lambda.tf
│   ├── glue.tf
│   ├── monitoring.tf
│   ├── outputs.tf
│   ├── variables.tf
│   └── stepfunctions_definition.json
│
├── ingestion/                      
│   ├── lambda_handler.py
│   ├── pipeline_orchestrator.py
│   ├── data_validator.py
│   ├── dbt_runner.py
│   └── health_check.py
│
├── processing/                      
│   ├── olist_etl_script.py
│   ├── schema_validator.py
│   ├── data_quality_check.py
│   └── glue_job_args.json
│
├── transformation/                  
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   ├── macros/
│   ├── seeds/
│   └── snapshots/
│
├── dashboard/
│   ├── streamlit_app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .streamlit/config.toml
│   └── .streamlit/config.toml
│
├── docs/
│
├── warehouse/                       
│   ├── 01_create_storage_integration.sql
│   ├── 02_create_external_stage.sql
│   ├── 03_create_snowpipe.sql
│   ├── 04_create_marts_schema.sql
│   ├── 05_data_check.sql
│   ├── 06_resource_monitor.sql
│   └── 07_pipeline_validation.sql
│
├── scripts/
│   └── get_secret.py
│
├── buildspec_dbt.yml
├── .env.example                
├── .gitignore                     
└── README.md
```

---

## Infrastructure (Terraform)

All AWS resources are defined as code in `terraform/`. The state file is stored locally by default — replace with an S3 backend for team environments.

### What Terraform provisions

| Resource | File | Description |
|---|---|---|
| S3 bucket | `s3.tf` | Versioning, AES-256 SSE, lifecycle tiering, TLS policy |
| IAM roles (6) | `iam.tf` | Lambda, Glue, Step Functions, EventBridge, App Runner, CodeBuild |
| AWS Glue Crawler | `glue.tf` | Crawls `raw/` prefix, writes to `olist_raw_db` |
| AWS Glue ETL Job | `glue.tf` | G.1X × 2 workers, job bookmarks enabled |
| Lambda functions | `lambda.tf` | Ingestion, orchestrator, validator — with DLQ |
| Step Functions | `main.tf` | 8-state standard workflow |
| EventBridge rule | `main.tf` | Daily schedule at 02:00 UTC |
| CloudWatch alarms | `monitoring.tf` | 5 metric alarms |
| SNS topic | `monitoring.tf` | Email subscription for pipeline alerts |
| SQS DLQ | `monitoring.tf` | Catches silent Lambda failures |

### Deploy

```bash
cd terraform

# First time
terraform init

# Preview changes
terraform plan \
  -var="snowflake_aws_role_arn=arn:aws:iam::ACCOUNT:user/snow-s3-..." \
  -var="alert_email=your@email.com"

# Apply
terraform apply \
  -var="snowflake_aws_role_arn=arn:aws:iam::ACCOUNT:user/snow-s3-..." \
  -var="alert_email=your@email.com"
```

### Remote state (recommended for teams)

```hcl
# Add to terraform/main.tf
terraform {
  backend "s3" {
    bucket         = "olist-terraform-state"
    key            = "pipeline/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "olist-terraform-locks"
    encrypt        = true
  }
}
```

---

## Pipeline Flow

The pipeline runs automatically at 02:00 UTC daily. Total runtime: approximately 8 minutes.

**1. EventBridge trigger**
The CloudWatch Events rule fires and starts the Step Functions state machine. The execution input is `{}` — the state machine generates a unique `run_id` internally from the execution name.

**2. InitRun (Pass state)**
Step Functions derives `run_id` using `States.Format('run-{}', $$.Execution.Name)`. No external input required, making the pipeline safe to trigger from the AWS console, CLI, or EventBridge with any input including empty JSON.

**3. Data validation — 28 checks**
A Lambda validator runs before any compute job. It checks all nine CSV files for existence, minimum row counts, required column presence, and file freshness (30-day threshold). Zero failures are allowed. Any critical check failure sends an SNS alert and terminates the execution.

**4. Glue Crawler**
The orchestrator Lambda starts the `olist-raw-crawler`. Step Functions polls every 30 seconds until the crawler state returns `READY`. The crawler writes table metadata to the `olist_raw_db` Glue catalog database.

**5. Glue ETL job**
A PySpark job reads the raw CSVs directly from S3 (`spark.read.csv(header=True)`), builds a star schema, derives `order_year_month` from purchase timestamps, casts payment types, and writes snappy Parquet to `processed/`. Job bookmarks ensure idempotent incremental processing.

**6. Post-ETL S3 validation**
The orchestrator verifies all four output prefixes contain at least one Parquet file. This prevents silent ETL failures from propagating to Snowflake.

**7. Snowflake load**
`COPY INTO` loads Parquet files from the external stage into four RAW schema tables. The external stage authenticates via IAM role. Credentials are resolved at runtime from AWS Secrets Manager.

**8. dbt run — 11 models**
dbt executes models in DAG order: four staging views → three dimension tables → two incremental fact tables (merge strategy on composite keys) → one monthly revenue rollup. Incremental models process only new records on repeated runs.

**9. dbt test — 51 tests**
Tests validate every primary key (not-null + unique), every categorical column (accepted-values), revenue column ranges, and four custom SQL assertions. Any failure exits non-zero and triggers an SNS alert.

**10. Dashboard serves updated data**
The Streamlit dashboard on Streamlit Cloud connects to Snowflake MARTS via `st.secrets`. Query results cache for 10 minutes per session.

---

## Data Quality

Quality is enforced at three independent stages:

**Stage 1 — Pre-ETL (Lambda validator):**
28 checks across 9 CSV files: file existence (8), file freshness against 30-day threshold (9), minimum row count (8), required column presence (3). Failures block the pipeline before any compute runs.

**Stage 2 — Post-ETL (S3 file check):**
Confirms all four Parquet output directories are non-empty before Snowflake load. Prevents loading partial or missing data.

**Stage 3 — Post-dbt (dbt tests):**
51 tests across all 11 models. Includes not-null/unique on all PKs, accepted-values on status and tier columns, and four custom tests: row count bounds (100k–200k for fct_orders), NULL year-month detection, zero-revenue on delivered orders, and monthly revenue completeness for 2017–2018.

---

## dbt Models

| Model | Materialization | Description |
|---|---|---|
| `stg_orders` | View | Type casting, derived columns from RAW fct_orders |
| `stg_customers` | View | Deduplication, state normalization |
| `stg_products` | View | Category standardization |
| `stg_sellers` | View | State-level aggregation prep |
| `dim_customers` | Table | LTV computation, segment (high/mid/low), region join |
| `dim_products` | Table | Revenue tier classification (top/mid/tail) |
| `dim_sellers` | Table | GMV tier (platinum/gold/silver/bronze), late delivery rate |
| `fct_orders` | Incremental | Order-item fact, merge on `(order_id, order_item_id)` |
| `fct_monthly_revenue` | Incremental | Pre-aggregated monthly rollup, merge on `order_year_month` |
| `brazil_states` | Seed | 15-row state reference table |
| `customers_snapshot` | Snapshot | SCD Type 2 history on `dim_customers` |

**Testing strategy:**
Standard tests (not-null, unique, accepted-values) cover all primary and foreign keys. Custom SQL tests (`tests/`) guard against known failure modes: the BOOLEAN SUM bug from Snowflake, NULL partition columns from Spark, and revenue anomalies introduced by bad ETL runs.

---

## Monitoring and Reliability

| Alarm | Metric | Threshold | Action |
|---|---|---|---|
| `olist-lambda-errors` | Lambda Errors | ≥ 2 in 5 min | SNS email |
| `olist-lambda-duration` | Lambda Duration | > 240 s max | SNS email |
| `olist-glue-etl-failed` | Glue failed tasks | ≥ 1 | SNS email |
| `olist-glue-etl-slow` | Glue execution time | avg > 15 min | SNS email |
| `olist-lambda-dlq-messages` | SQS messages visible | ≥ 1 | SNS email |

**Reliability mechanisms:**
- Every Step Functions state has a `Retry` block with exponential backoff and `Catch` routing failures to `NotifyAndFail → SNS → PipelineFailed`
- The DLQ catches Lambda invocations that exhaust all retries without raising an exception
- Glue job bookmarks make ETL reruns safe — already-processed files are skipped
- dbt incremental merge strategy ensures repeated runs do not produce duplicates

---

## CI/CD

| Workflow | Trigger | Action |
|---|---|---|
| `dbt_ci.yml` | Push or PR touching `dbt_olist/` | Install dbt, write `profiles.yml` from secrets, run `dbt compile` + `dbt test`. Blocks merge on failure. |
| `deploy_glue.yml` | Push to `main` touching `glue_jobs/` or `lambda_function/pipeline_orchestrator.py` | Validate Python syntax, upload ETL script to S3, package + deploy Lambda, run smoke test against `validate_s3`. |
| `deploy_dashboard.yml` | Push to `main` touching `dashboard/` | Build Docker image, tag with commit SHA, push to ECR. Streamlit Cloud auto-redeploys from GitHub. |

All workflows use `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`. Secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`) are configured in repository settings — never in workflow files.

---

## Dashboard

**Live:** [https://olist-aws-app-pipeline-nagr2optdeyqzxnrqzxhv3.streamlit.app](https://olist-aws-app-pipeline-nagr2optdeyqzxnrqzxhv3.streamlit.app)

The dashboard presents five analytical views across 8 KPI cards:

| Tab | Content |
|---|---|
| Revenue | Monthly GMV + order volume (dual-axis), order status donut |
| Categories | Top N categories by revenue, colored by average review score |
| Geography | Revenue by Brazilian region + state breakdown table |
| Sellers | Top 20 sellers ranked by GMV, filterable by tier |
| Delivery | Late delivery rate + average delivery days by state, traffic-light coloring |

Performance: `@st.cache_resource` on the Snowflake connection (session-scoped), `@st.cache_data(ttl=600)` on all queries. Each tab loads independently. All datasets are exportable as CSV.

---

## Setup

### Prerequisites

AWS CLI v2, Terraform 1.5, Python 3.12, Snowflake account, Docker.

### 1. Clone

```bash
git clone https://github.com/noran-salm/olist-aws-snowflake-pipeline.git
cd olist-aws-snowflake-pipeline
cp .env.example .env   # fill in your values
```

### 2. Provision AWS infrastructure

```bash
cd terraform
terraform init
terraform apply -var="alert_email=your@email.com"
```

### 3. Configure Snowflake

Run `sql_scripts/01_create_storage_integration.sql` through `07_pipeline_validation.sql` in order in a Snowflake worksheet.

After running `01_create_storage_integration.sql`, run `DESC INTEGRATION olist_s3_integration;` and copy the IAM user ARN and external ID into your `.env` as `TF_VAR_snowflake_aws_role_arn` and `TF_VAR_snowflake_external_id`, then re-run `terraform apply`.

### 4. Store Snowflake credentials

```bash
aws secretsmanager create-secret \
  --name "olist/snowflake/credentials" \
  --secret-string '{
    "account": "NZFSGYT-PU98877",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "role": "SYSADMIN",
    "database": "OLIST_DW",
    "warehouse": "OLIST_WH",
    "schema": "MARTS"
  }'
```

### 5. Upload dataset

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p /tmp/olist/
aws s3 sync /tmp/olist/ s3://olist-lake-$(aws sts get-caller-identity \
  --query Account --output text)/raw/ --sse AES256
```

### 6. Add GitHub Secrets

In repository Settings → Secrets → Actions, add: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.

### 7. Trigger the pipeline

**From CLI:**
```bash
SF_ARN=$(aws stepfunctions list-state-machines \
  --query 'stateMachines[?name==`olist-data-pipeline`].stateMachineArn' \
  --output text)
aws stepfunctions start-execution --state-machine-arn $SF_ARN --input '{}'
```

**From AWS Console:**
Step Functions → State machines → `olist-data-pipeline` → Start execution → leave input as `{}` → Start.

### 8. Run dbt manually

```bash
source .venv/bin/activate
eval $(python3 scripts/get_secret.py)
cd dbt_olist && dbt run && dbt test
```

---

## Cost Estimate

| Service | Configuration | Estimated monthly cost |
|---|---|---|
| AWS Glue | G.1X × 2 workers, ~2 min/day | ~$3 |
| Lambda | 3 functions, daily invocations | ~$0.01 |
| Step Functions | Standard, ~30 state transitions/day | ~$0.01 |
| S3 | ~700 MB total, Intelligent-Tiering after 90 days | ~$0.05 |
| CloudWatch | 5 alarms, log ingestion | ~$1 |
| Snowflake | X-Small WH, auto-suspend 60 s | ~$5 |
| ECR | 1 image (~1 GB) | ~$0.10 |
| **Total** | | **~$9–15 / month** |

---

## Future Improvements

**1. Automate dbt in Step Functions** — `dbt_runner.py` and `buildspec_dbt.yml` are present but not wired into the state machine. Adding a final state that triggers the CodeBuild project would make the pipeline fully hands-off.

**2. Snowpipe AUTO_INGEST** — Replace the batch `COPY INTO` with event-driven loading via S3 event notifications to the Snowpipe SQS endpoint. Reduces load latency from hours to seconds.

**3. Terraform remote state** — Move `terraform.tfstate` to an S3 backend with DynamoDB locking. Required for any multi-person or multi-environment setup.

**4. Schema contract enforcement** — Use the existing `schema_validator.py` as a blocking Step Functions state before the ETL job. Prevents corrupt loads when upstream CSV schemas drift.

**5. Row-level freshness SLA** — Alert if `MAX(order_purchase_timestamp)` in `MARTS.fct_orders` is stale by more than 48 hours, rather than relying on S3 file modification timestamps.

**6. Multi-environment Terraform workspaces** — Add `dev` and `staging` workspaces with separate S3 buckets and Snowflake databases to support feature branch testing without touching production.

**7. Airflow on Amazon MWAA** — Replace Step Functions with an Airflow DAG for native dbt operator support, lineage visualization, and per-task retry granularity.

**8. Great Expectations** — Replace the custom `data_validator.py` with a Great Expectations suite for richer profiling, HTML reports, and expectation version control.

**9. Streaming ingestion** — Replace batch CSV ingestion with Kinesis Data Firehose → S3 → Snowpipe for near-real-time dashboard updates.

**10. AWS Cost Tags policy** — Enforce `Project`, `Environment`, and `CostCenter` tags on all resources via AWS Tag Policies to enable accurate per-pipeline attribution in Cost Explorer.

---

## License

MIT. Built as a Data Engineering Zoomcamp final project.