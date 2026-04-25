# Olist E-Commerce Data Pipeline

[![dbt CI](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/dbt_ci.yml)
[![Deploy Glue](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_glue.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_glue.yml)
[![Deploy Dashboard](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_dashboard.yml/badge.svg)](https://github.com/noran-salm/olist-aws-snowflake-pipeline/actions/workflows/deploy_dashboard.yml)
[![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white&style=flat)](https://snowflake.com)
[![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white&style=flat)](https://getdbt.com)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?logo=terraform&logoColor=white&style=flat)](https://terraform.io)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white&style=flat)](https://streamlit.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade, end-to-end data engineering pipeline that ingests raw Brazilian e-commerce data from the [Olist public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), applies medallion architecture transformations across a Snowflake data warehouse, enforces multi-stage data quality validation, and delivers business insights through a live Streamlit analytics dashboard — fully automated, monitored, tested, and provisioned with Terraform.

**Live Dashboard →** [olist-aws-app-pipeline.streamlit.app](https://olist-aws-app-pipeline-nagr2optdeyqzxnrqzxhv3.streamlit.app)

---

## Table of Contents

- [Architecture](#architecture)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Pipeline Flow](#pipeline-flow)
- [Data Quality](#data-quality)
- [dbt Models](#dbt-models)
- [Monitoring & Reliability](#monitoring--reliability)
- [CI/CD](#cicd)
- [Infrastructure (Terraform)](#infrastructure-terraform)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Dashboard](#dashboard)
- [Cost Estimate](#cost-estimate)
- [Scalability & Production Considerations](#scalability--production-considerations)
- [Future Improvements](#future-improvements)

---

## Architecture

```

![Architecture Diagram](docs/olist_reference_style_arch.jpg)

EventBridge (02:00 UTC)
        │
        ▼
AWS Step Functions ──────────────────────────────────────────────┐
  │ InitRun → ValidateRawData → StartCrawler → StartETL          │
  │ → CheckETL → ValidateProcessed → [SUCCEEDED]                 │ On Failure
  │                                                               ▼
  │                                                         SNS Alert + DLQ
  ▼
S3 /raw/ (9 CSVs) → Glue Crawler → Glue ETL (PySpark) → S3 /processed/ (Parquet)
                                                                  │
                                                          COPY INTO (IAM + Secrets)
                                                                  │
                                                       Snowflake OLIST_DW
                                                     ┌────────────┴──────────────┐
                                                   RAW         STAGING         MARTS
                                                 (4 tables)   (4 views)    (5 tables)
                                                                  │
                                                            dbt run + dbt test (51)
                                                                  │
                                                        Streamlit Cloud Dashboard
```

The pipeline implements a **medallion architecture** (RAW → STAGING → MARTS) with three independent data quality gates, full orchestration via AWS Step Functions, and Infrastructure as Code via Terraform.

---

## Key Features

- **End-to-end automation** — EventBridge triggers a full pipeline run daily at 02:00 UTC with zero manual intervention
- **Resilient orchestration** — Step Functions with exponential backoff, Catch blocks on every state, and SNS alerting on failure
- **Multi-stage data quality** — 28 pre-ETL checks + post-ETL S3 validation + 51 dbt tests across all models
- **Idempotent processing** — Glue job bookmarks and dbt incremental merge strategy make every run safe to rerun
- **Medallion architecture** — Clean RAW → STAGING → MARTS separation enforced by dbt models with SCD Type 2 history
- **Zero plaintext credentials** — All secrets injected at runtime from AWS Secrets Manager
- **CI/CD gates** — Three GitHub Actions workflows block merges on test failure and auto-deploy on push
- **Production monitoring** — Five CloudWatch alarms with email notifications; Dead Letter Queue for silent Lambda failures
- **Infrastructure as Code** — All 10+ AWS resources provisioned and version-controlled in Terraform

---

## Tech Stack

| Category | Technology |
|---|---|
| **Orchestration** | AWS Step Functions, AWS EventBridge |
| **Compute** | AWS Lambda (Python 3.12), AWS Glue 4.0 (PySpark 3.3) |
| **Storage** | Amazon S3 (raw + processed zones, Intelligent-Tiering) |
| **Data Warehouse** | Snowflake (X-Small WH, auto-suspend 60 s) |
| **Transformation** | dbt Core 1.11 + dbt-snowflake adapter |
| **Visualization** | Streamlit 1.45, Plotly 5.24 |
| **Containerization** | Docker, Amazon ECR |
| **CI/CD** | GitHub Actions (3 workflows) |
| **Monitoring** | AWS CloudWatch, Amazon SNS, Amazon SQS (DLQ) |
| **Security** | AWS Secrets Manager, IAM least-privilege roles |
| **Infrastructure as Code** | Terraform 1.5 |

---

## Pipeline Flow

The pipeline runs automatically at **02:00 UTC daily** and completes in approximately **8 minutes**.

| Step | Component | Description |
|------|-----------|-------------|
| 1 | EventBridge | Triggers Step Functions with empty input `{}` |
| 2 | Step Functions — InitRun | Generates a unique `run_id` from execution name |
| 3 | Lambda — Data Validator | Runs 28 checks across 9 CSV files. Blocks on any failure |
| 4 | Glue Crawler | Catalogs schema into `olist_raw_db`; polled until `READY` |
| 5 | Glue ETL (PySpark) | Builds star schema; writes 1,019 Snappy Parquet files to `processed/` |
| 6 | Lambda — ValidateProcessed | Confirms all 4 Parquet output prefixes are non-empty |
| 7 | Snowflake COPY INTO | Loads Parquet from external stage via IAM role |
| 8 | dbt run | Executes 11 models: staging views → dimensions → incremental facts |
| 9 | dbt test | Runs 51 tests; any failure triggers SNS alert |
| 10 | Streamlit Dashboard | Serves fresh MARTS data to business users |

---

## Data Quality

Quality is enforced at three independent stages:

| Stage | Mechanism | Checks |
|-------|-----------|--------|
| Pre-ETL | Lambda validator | 28 checks: file existence, freshness (30-day threshold), minimum row counts, required column presence |
| Post-ETL | Lambda post-validator | All 4 Parquet output prefixes must contain at least one file |
| Post-dbt | dbt test suite | 51 tests: not-null, unique, accepted-values on all PKs and categoricals; 4 custom SQL assertions (row count bounds, NULL year-month, revenue integrity, monthly completeness) |

A failure at any gate terminates the pipeline and sends an SNS email alert before any downstream step runs.

---

## dbt Models

| Model | Materialization | Description |
|---|---|---|
| `stg_orders` | View | Type casting, derived `order_year_month` |
| `stg_customers` | View | Deduplication, state normalization |
| `stg_products` | View | Category standardization |
| `stg_sellers` | View | State-level aggregation prep |
| `dim_customers` | Table | LTV, segment (high/mid/low), region join |
| `dim_products` | Table | Revenue tier (top/mid/tail) |
| `dim_sellers` | Table | GMV tier, late delivery rate |
| `fct_orders` | Incremental | Order-item fact; merge on `(order_id, order_item_id)` |
| `fct_monthly_revenue` | Incremental | Pre-aggregated monthly rollup |
| `brazil_states` | Seed | 15-row Brazilian state reference table |
| `customers_snapshot` | Snapshot | SCD Type 2 history on `dim_customers` |

---

## Monitoring & Reliability

| Alarm | Metric | Threshold | Action |
|---|---|---|---|
| `olist-lambda-errors` | Lambda Errors | ≥ 2 in 5 min | SNS email |
| `olist-lambda-duration` | Lambda Duration | > 240 s | SNS email |
| `olist-glue-etl-failed` | Glue failed tasks | ≥ 1 | SNS email |
| `olist-glue-etl-slow` | Glue execution time | avg > 15 min | SNS email |
| `olist-lambda-dlq-messages` | SQS visible messages | ≥ 1 | SNS email |

**Reliability mechanisms:**
- Every Step Functions state has `Retry` (exponential backoff) and `Catch` (routes to SNS before failing)
- SQS Dead Letter Queue catches silent Lambda failures that exhaust all retries
- Glue job bookmarks prevent reprocessing already-committed files
- dbt incremental merge ensures repeated runs produce no duplicates

---

## CI/CD

| Workflow | Trigger | Action |
|---|---|---|
| `dbt_ci.yml` | Push / PR touching `transformation/` | `dbt compile` + `dbt test` — blocks merge on failure |
| `deploy_glue.yml` | Push to `main` touching `processing/` or `ingestion/pipeline_orchestrator.py` | Syntax check → upload ETL to S3 → deploy Lambda → smoke test |
| `deploy_dashboard.yml` | Push to `main` touching `dashboard/` | Docker build → push image to Amazon ECR |

All workflows use `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` and GitHub repository secrets — no credentials in workflow files.

---

## Infrastructure (Terraform)

All AWS resources are defined as code in `terraform/`. State is stored locally by default.

| Resource | File | Description |
|---|---|---|
| S3 bucket | `s3.tf` | Versioning, AES-256 SSE, Intelligent-Tiering lifecycle, TLS enforcement |
| IAM roles (6) | `iam.tf` | Least-privilege roles for Lambda, Glue, Step Functions, EventBridge, ECR, CodeBuild |
| Glue Crawler | `glue.tf` | Crawls `raw/` → populates `olist_raw_db` catalog |
| Glue ETL Job | `glue.tf` | G.1X × 2 workers, job bookmarks for idempotency |
| Lambda functions | `lambda.tf` | Ingestion, orchestrator, validator + DLQ attachment |
| Step Functions | `main.tf` | 8-state standard workflow, retry + catch on every state |
| EventBridge rule | `main.tf` | `cron(0 2 * * ? *)` — daily at 02:00 UTC |
| CloudWatch alarms | `monitoring.tf` | 5 metric alarms |
| SNS topic | `monitoring.tf` | Email subscription for pipeline alerts |
| SQS DLQ | `monitoring.tf` | Catches silent Lambda failures |

```bash
cd terraform
terraform init
terraform plan -var="alert_email=your@email.com"
terraform apply -var="alert_email=your@email.com"
```

**Remote state (recommended for teams):**

```hcl
# terraform/main.tf
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

## Project Structure

```
olist-aws-snowflake-pipeline/
├── .github/workflows/          # CI/CD pipelines (dbt, Glue, dashboard)
├── terraform/                  # All AWS infrastructure as code
│   ├── main.tf                 # Step Functions, EventBridge
│   ├── s3.tf                   # S3 bucket + lifecycle
│   ├── iam.tf                  # IAM roles (6, least-privilege)
│   ├── lambda.tf               # Lambda functions + DLQ
│   ├── glue.tf                 # Glue Crawler + ETL job
│   ├── monitoring.tf           # CloudWatch, SNS, SQS DLQ
│   └── stepfunctions_definition.json
├── ingestion/                  # Lambda source code
│   ├── pipeline_orchestrator.py  # Step Functions action router
│   ├── data_validator.py         # 28-check pre-ETL validator
│   └── health_check.py
├── processing/                 # Glue PySpark ETL
│   ├── olist_etl_script.py     # Main star-schema ETL
│   └── schema_validator.py     # CSV schema fingerprinting
├── transformation/             # dbt project root
│   ├── models/
│   │   ├── staging/            # 4 cleaning views
│   │   └── marts/              # 5 analytics-ready tables
│   ├── tests/                  # 4 custom SQL quality tests
│   ├── seeds/                  # brazil_states.csv
│   └── snapshots/              # SCD Type 2 on dim_customers
├── dashboard/                  # Streamlit analytics app
│   ├── streamlit_app.py
│   └── Dockerfile
├── warehouse/                  # Snowflake setup scripts (run once, in order)
│   ├── 01_create_storage_integration.sql
│   └── ... (07 total)
├── scripts/
│   └── get_secret.py           # Runtime credential fetch from Secrets Manager
├── buildspec_dbt.yml           # CodeBuild spec for automated dbt runs
├── .env.example                # Environment variable template (commit this)
└── README.md
```

---

## How to Run

### Prerequisites

AWS CLI v2 · Terraform 1.5 · Python 3.12 · Snowflake account · Docker · Kaggle CLI

### 1. Clone and configure

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

Run `warehouse/01_create_storage_integration.sql` through `07_pipeline_validation.sql` **in order** inside a Snowflake worksheet.

After running `01`, execute `DESC INTEGRATION olist_s3_integration;` and copy the IAM user ARN and external ID into `.env` as `TF_VAR_snowflake_aws_role_arn` and `TF_VAR_snowflake_external_id`, then re-run `terraform apply`.

### 4. Store Snowflake credentials

```bash
aws secretsmanager create-secret \
  --name "olist/snowflake/credentials" \
  --secret-string '{
    "account":"NZFSGYT-PU98877",
    "user":"YOUR_USER",
    "password":"YOUR_PASSWORD",
    "role":"SYSADMIN",
    "database":"OLIST_DW",
    "warehouse":"OLIST_WH",
    "schema":"MARTS"
  }'
```

### 5. Upload the dataset

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p /tmp/olist/
aws s3 sync /tmp/olist/ \
  s3://olist-lake-$(aws sts get-caller-identity --query Account --output text)/raw/ \
  --sse AES256
```

### 6. Add GitHub Secrets

Repository Settings → Secrets → Actions:

`AWS_ACCESS_KEY_ID` · `AWS_SECRET_ACCESS_KEY` · `AWS_REGION` · `SNOWFLAKE_ACCOUNT` · `SNOWFLAKE_USER` · `SNOWFLAKE_PASSWORD`

### 7. Trigger the pipeline

**AWS Console:** Step Functions → `olist-data-pipeline` → Start execution → input `{}` → Start

**CLI:**
```bash
SF_ARN=$(aws stepfunctions list-state-machines \
  --query 'stateMachines[?name==`olist-data-pipeline`].stateMachineArn' \
  --output text)
aws stepfunctions start-execution --state-machine-arn $SF_ARN --input '{}'
```

### 8. Run dbt manually

```bash
source .venv/bin/activate
eval $(python3 scripts/get_secret.py)
cd transformation && dbt run && dbt test
```

---

## Dashboard

**Live:** [olist-aws-app-pipeline.streamlit.app](https://olist-aws-app-pipeline-nagr2optdeyqzxnrqzxhv3.streamlit.app)

| Tab | Content |
|---|---|
| Revenue | Monthly GMV + order volume (dual-axis bar/line) + order status donut |
| Categories | Top-N products by revenue, colored by average review score |
| Geography | Revenue by Brazilian region + state breakdown table |
| Sellers | Top 20 by GMV, filterable by tier (Platinum / Gold / Silver / Bronze) |
| Delivery | Late delivery rate + avg days by state, traffic-light coloring |

All queries cached with `@st.cache_data(ttl=600)`. Snowflake connection cached with `@st.cache_resource`. All tabs export CSV.

---

## Cost Estimate

| Service | Configuration | Monthly |
|---|---|---|
| AWS Glue | G.1X × 2 workers, ~2 min/day | ~$3.00 |
| Lambda | 3 functions, daily invocations | ~$0.01 |
| Step Functions | ~30 state transitions/day | ~$0.01 |
| Amazon S3 | ~700 MB, Intelligent-Tiering | ~$0.05 |
| CloudWatch | 5 alarms + log ingestion | ~$1.00 |
| Snowflake | X-Small WH, auto-suspend 60 s | ~$5.00 |
| Amazon ECR | 1 image (~1 GB) | ~$0.10 |
| **Total** | | **~$9–15** |

---

## Scalability & Production Considerations

- **Schema evolution** — `schema_validator.py` fingerprints CSV schemas and detects breaking vs additive changes before any ETL runs
- **Incremental processing** — Glue bookmarks + dbt merge strategy handle growing datasets without full reprocesses
- **Idempotency** — Every pipeline step is safe to rerun; no duplicate data is produced
- **Credential rotation** — Secrets Manager enables zero-downtime credential rotation without code changes
- **Multi-environment** — Terraform workspaces can deploy isolated `dev` / `staging` / `prod` environments from the same codebase
- **Cost ceiling** — Snowflake resource monitor caps credit usage at 20 credits/month; Glue right-sized to G.1X
- **Team-ready** — Remote Terraform state on S3 + DynamoDB locking prevents concurrent apply conflicts

---

## Future Improvements

| Priority | Improvement |
|---|---|
| High | Wire `dbt_runner.py` + CodeBuild as the final Step Functions state to fully automate dbt |
| High | Enable Snowpipe `AUTO_INGEST` via S3 event notifications for near-real-time loading |
| High | Migrate Terraform state to S3 backend + DynamoDB locking for team environments |
| Medium | Add schema contract enforcement as a blocking Step Functions state |
| Medium | Row-level freshness SLA: alert if `MAX(order_purchase_timestamp)` is stale > 48 h |
| Medium | Multi-environment Terraform workspaces (`dev` / `staging` / `prod`) |
| Low | Migrate orchestration to Amazon MWAA (Airflow) for native dbt operator support |
| Low | Replace custom `data_validator.py` with Great Expectations for richer profiling |
| Low | Streaming ingestion via Kinesis Data Firehose → Snowpipe |
| Low | Enforce AWS Tag Policies for per-pipeline cost attribution |

---

## License

MIT — see [LICENSE](LICENSE) for details.

Built as the final project for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by DataTalks.Club.

---

<p align="center">
  <a href="https://www.linkedin.com/in/noran-salm">
    <img src="https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin&logoColor=white"/>
  </a>
  &nbsp;
  <a href="https://github.com/noran-salm">
    <img src="https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white"/>
  </a>
</p>
