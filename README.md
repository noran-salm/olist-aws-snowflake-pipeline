# ════════════════════════════════════════════════════════════════════════════
#  olist-aws-snowflake-pipeline — Environment Variables
#  Copy this file to .env and fill in your values.
#  NEVER commit .env to git (it is listed in .gitignore).
# ════════════════════════════════════════════════════════════════════════════

# ── AWS ─────────────────────────────────────────────────────────────────────
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=516671521715
# Use IAM role or long-term credentials (prefer role-based auth)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...

# ── Snowflake ────────────────────────────────────────────────────────────────
# Account format: <org_name>-<account_name>  (find in Snowflake UI → Admin → Accounts)
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=dbt_user
SNOWFLAKE_PASSWORD=super_secret_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_DATABASE=OLIST_DW
SNOWFLAKE_WAREHOUSE=OLIST_WH
SNOWFLAKE_SCHEMA=MARTS

# ── Terraform ────────────────────────────────────────────────────────────────
# These are populated AFTER running sql_scripts/01_create_storage_integration.sql
# and running DESC INTEGRATION olist_s3_integration in Snowflake.
TF_VAR_snowflake_aws_role_arn=arn:aws:iam::111111111111:user/snow-s3-…
TF_VAR_snowflake_external_id=ABC12345_SFCRole=2_abc…

# ── Lambda / Ingestion ───────────────────────────────────────────────────────
DATASET_URL=https://raw.githubusercontent.com/olist/work-at-olist-data-science/main/olist_public_dataset.zip

# ── dbt ──────────────────────────────────────────────────────────────────────
DBT_TARGET=dev \\\\\\\\\Account Details
Account
Config File
Connectors/Drivers
SQL Commands
Sorted by descending
Name
Value
Account identifier
NZFSGYT-PU98877
Data sharing account identifier
NZFSGYT.PU98877
Organization name
NZFSGYT
Account name
PU98877
Account/Server URL
NZFSGYT-PU98877.snowflakecomputing.com
Login name
NORANSALM15
Role
ACCOUNTADMIN
Account locator
ZT89182
Cloud platform
AWS
Edition
Enterprise
Table data loaded










# 🛒 Olist AWS → Snowflake Data Pipeline

End-to-end data engineering project using the [Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

```
EventBridge (daily)
       │
       ▼
  AWS Lambda          ← downloads & unzips Olist CSVs
       │  upload CSV
       ▼
  S3 /raw/            ← data lake landing zone
       │
  Glue Crawler        ← auto-discovers schema → Glue Data Catalog
       │
  Glue ETL Job        ← PySpark: cleans, builds star schema
       │  write Parquet
       ▼
  S3 /processed/      ← analytics-ready Parquet files
       │
  Snowpipe            ← AUTO_INGEST via S3 event notification
       │  COPY INTO
       ▼
  Snowflake RAW       ← landed tables
       │
  dbt Core            ← staging views + mart tables
       │
  Snowflake MARTS     ← dim_customers, dim_products, dim_sellers, fct_orders
       │
  Streamlit           ← interactive dashboard
```

---

## 📋 Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | ≥ 3.12 | [python.org](https://python.org) |
| AWS CLI | ≥ 2.15 | `brew install awscli` or [docs](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) |
| Terraform | ≥ 1.5 | `brew install terraform` or [docs](https://developer.hashicorp.com/terraform/downloads) |
| dbt Core | ≥ 1.8 | `pip install dbt-snowflake` |
| Streamlit | ≥ 1.35 | `pip install streamlit` |
| Snowflake account | — | [trial.snowflake.com](https://trial.snowflake.com) |

---

## 🗂️ Project Structure

```
olist-aws-snowflake-pipeline/
├── terraform/                   # IaC — spins up all AWS resources
│   ├── main.tf                  # Provider, EventBridge, Glue Crawler & Job
│   ├── variables.tf             # Input variables with defaults
│   ├── outputs.tf               # Useful ARNs and names
│   ├── iam.tf                   # Least-privilege IAM roles & policies
│   ├── glue.tf                  # CloudWatch logs, alarms, security config
│   ├── lambda.tf                # Lambda function + Glue trigger
│   └── s3.tf                    # S3 bucket, versioning, encryption, lifecycle
├── lambda_function/
│   ├── lambda_handler.py        # Ingestion: download → unzip → S3 upload
│   └── requirements.txt
├── glue_jobs/
│   ├── olist_etl_script.py      # PySpark star schema ETL
│   └── glue_job_args.json       # Reference job arguments
├── dbt_olist/
│   ├── models/
│   │   ├── staging/             # Thin views over RAW tables
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_sellers.sql
│   │   ├── marts/               # Final analytics tables
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_products.sql
│   │   │   ├── dim_sellers.sql
│   │   │   └── fct_orders.sql
│   │   └── schema.yml           # Sources, models, tests
│   ├── macros/macros.sql        # generate_schema_name, safe_divide, date_spine
│   ├── dbt_project.yml
│   └── profiles.yml
├── dashboard/
│   ├── streamlit_app.py         # Interactive Streamlit dashboard
│   └── requirements.txt
├── sql_scripts/
│   ├── 01_create_storage_integration.sql
│   ├── 02_create_external_stage.sql
│   └── 03_create_snowpipe.sql
├── .env.example                 # Template — copy to .env
├── .gitignore
└── README.md
```

---

## 🚀 Step-by-Step Setup

### Step 1 — Clone & Configure Environment

```bash
# Clone the project
git clone https://github.com/your-username/olist-aws-snowflake-pipeline.git
cd olist-aws-snowflake-pipeline

# Copy env template and fill in your values
cp .env.example .env
# Open .env in VS Code and fill in AWS + Snowflake credentials
code .env
```

### Step 2 — Configure AWS CLI

```bash
# Configure with your IAM user credentials
aws configure
# AWS Access Key ID:     <your key>
# AWS Secret Access Key: <your secret>
# Default region:        us-east-1
# Default output format: json

# Verify it works
aws sts get-caller-identity
# Should print your Account ID, UserId, and ARN
```

### Step 3 — Package the Lambda Function

The Lambda ZIP must be created before Terraform can deploy the function.

```bash
cd lambda_function

# Create a clean package directory
mkdir -p package
pip install -r requirements.txt --target ./package

# Bundle everything into a ZIP
cp lambda_handler.py package/
cd package
zip -r ../lambda_package.zip .
cd ..

# Verify the ZIP was created
ls -lh lambda_package.zip
# Should be < 5 MB

cd ..   # back to project root
```

### Step 4 — Terraform: Deploy AWS Infrastructure

```bash
cd terraform

# Initialise providers and modules
terraform init

# Preview everything that will be created
terraform plan

# Deploy! This creates:
#   - S3 bucket (olist-lake-<account_id>)
#   - IAM roles (Lambda, Glue, Snowflake)
#   - Lambda function
#   - EventBridge schedule rule
#   - Glue Crawler + ETL Job
#   - CloudWatch log groups & alarms
terraform apply
# Type "yes" when prompted.

# Save important outputs
terraform output
# Note the value of: snowflake_s3_role_arn
# You will need it in the next step.

cd ..
```

> ⏱️ Terraform apply takes ~2 minutes.

### Step 5 — Upload Glue ETL Script to S3

Terraform uploads the Glue script automatically via `aws_s3_object`. Verify it landed:

```bash
# Replace <account_id> with your AWS account ID
aws s3 ls s3://olist-lake-<account_id>/scripts/
# Expected: olist_etl_script.py
```

If it's missing:
```bash
aws s3 cp glue_jobs/olist_etl_script.py \
  s3://olist-lake-<account_id>/scripts/olist_etl_script.py
```

### Step 6 — Snowflake: Create Storage Integration

Open a Snowflake worksheet (as ACCOUNTADMIN) and run:

```bash
# From the sql_scripts/ folder, copy-paste each file into Snowflake worksheet.
# Or use SnowSQL CLI:
snowsql -a <your_account> -u <your_user> \
  -f sql_scripts/01_create_storage_integration.sql
```

After running `DESC INTEGRATION olist_s3_integration;`, copy:
- `STORAGE_AWS_IAM_USER_ARN` → this is the Snowflake principal
- `STORAGE_AWS_EXTERNAL_ID`  → the STS external ID

Update your `.env`:
```
TF_VAR_snowflake_aws_role_arn=arn:aws:iam::111111111111:user/snow-s3-…
TF_VAR_snowflake_external_id=ABC12345_SFCRole=2_abc…
```

Re-run Terraform to update the IAM trust policy:
```bash
cd terraform
terraform apply   # only the IAM trust policy changes
cd ..
```

### Step 7 — Snowflake: Create External Stage & Tables

```sql
-- In Snowflake worksheet:
-- 1. Edit the account ID placeholder in the file first:
--    Replace  <YOUR_ACCOUNT_ID>  with your actual AWS account ID.

-- Then run:
```
```bash
snowsql -a <your_account> -u <your_user> \
  -f sql_scripts/02_create_external_stage.sql
```

Verify the stage can see files (run after Step 8):
```sql
LIST @OLIST_DW.RAW.olist_processed_stage;
```

### Step 8 — Run the Full Pipeline (First Time)

#### 8a. Trigger Lambda manually

```bash
aws lambda invoke \
  --function-name olist-ingestion \
  --payload '{"skip_crawler": false, "wait_for_crawler": false}' \
  --cli-binary-format raw-in-base64-out \
  response.json

cat response.json
# Should show: {"status": "success", "file_count": 9, ...}
```

#### 8b. Monitor the Glue Crawler

```bash
# Check crawler status
aws glue get-crawler --name olist-raw-crawler \
  --query 'Crawler.{State:State, LastCrawl:LastCrawl}'

# Or watch CloudWatch logs
aws logs tail /aws-glue/crawlers --follow
```

#### 8c. Run the Glue ETL Job

The Glue trigger fires automatically after the crawler succeeds (if `enable_glue_trigger=true`). To run manually:

```bash
aws glue start-job-run --job-name olist-etl-job \
  --arguments '{
    "--SOURCE_BUCKET":"olist-lake-<account_id>",
    "--SOURCE_PREFIX":"raw/",
    "--TARGET_PREFIX":"processed/",
    "--DATABASE_NAME":"olist_raw_db"
  }'

# Get the job run ID from the output, then monitor:
aws glue get-job-run \
  --job-name olist-etl-job \
  --run-id <job_run_id> \
  --query 'JobRun.{State:JobRunState,Message:ErrorMessage}'
```

> ⏱️ Glue ETL takes ~5–8 minutes on G.1X (2 workers).

#### 8d. Verify processed files in S3

```bash
aws s3 ls s3://olist-lake-<account_id>/processed/ --recursive | head -20
# Should show Parquet files under:
#   processed/dim_customers/
#   processed/dim_products/
#   processed/dim_sellers/
#   processed/fct_orders/order_year_month=2017-01/...
```

### Step 9 — Snowflake: Create Snowpipe

```bash
snowsql -a <your_account> -u <your_user> \
  -f sql_scripts/03_create_snowpipe.sql
```

After running, get the SQS ARN from each pipe:
```sql
SHOW PIPES IN SCHEMA OLIST_DW.RAW;
-- Copy the "notification_channel" column value (it's an SQS ARN)
```

Configure S3 event notifications in `terraform/s3.tf`:
1. Open `s3.tf`
2. Uncomment the `queue {}` block inside `aws_s3_bucket_notification`
3. Paste the SQS ARN from Snowflake
4. Run `terraform apply`

Verify Snowpipe is receiving data:
```sql
SELECT SYSTEM$PIPE_STATUS('OLIST_DW.RAW.pipe_fct_orders');
-- {"executionState":"RUNNING","pendingFileCount":0,...}

SELECT COUNT(*) FROM OLIST_DW.RAW.fct_orders;
-- Should show rows after a few minutes
```

### Step 10 — dbt: Install and Run Transformations

```bash
# Install dbt with Snowflake adapter
pip install dbt-snowflake

# Copy profiles to the correct location
# (or set DBT_PROFILES_DIR env var to point to dbt_olist/)
mkdir -p ~/.dbt
cp dbt_olist/profiles.yml ~/.dbt/profiles.yml

# Set environment variables (or source .env)
export SNOWFLAKE_ACCOUNT=myorg-myaccount
export SNOWFLAKE_USER=dbt_user
export SNOWFLAKE_PASSWORD=your_password

# Navigate to the dbt project
cd dbt_olist

# Test the connection
dbt debug
# Should print: "All checks passed!"

# Install dbt packages (if any in packages.yml)
dbt deps

# Run all models
dbt run
# Expected output:
#   Running 8 models...
#   OK  stg_orders             [view in 1.2s]
#   OK  stg_customers          [view in 0.8s]
#   OK  stg_products           [view in 0.9s]
#   OK  stg_sellers            [view in 0.7s]
#   OK  dim_customers          [table in 5.1s]
#   OK  dim_products           [table in 4.3s]
#   OK  dim_sellers            [table in 3.8s]
#   OK  fct_orders             [table in 12.4s]

# Run tests
dbt test
# Should show all tests passing

# Generate and serve documentation
dbt docs generate
dbt docs serve   # opens http://localhost:8080

cd ..
```

### Step 11 — Streamlit Dashboard

```bash
cd dashboard

pip install -r requirements.txt

# Option A: Use .env file (for local dev)
# Make sure .env is set up with SNOWFLAKE_* variables.

# Option B: Use Streamlit secrets (recommended for sharing)
mkdir -p ~/.streamlit
cat > ~/.streamlit/secrets.toml << 'EOF'
[snowflake]
account   = "myorg-myaccount"
user      = "dbt_user"
password  = "your_password"
role      = "SYSADMIN"
database  = "OLIST_DW"
schema    = "MARTS"
warehouse = "OLIST_WH"
EOF

# Run the dashboard
streamlit run streamlit_app.py
# Opens http://localhost:8501
```

---

## 🔄 Daily Automation

Once fully set up, the pipeline runs automatically:

| Time (UTC) | Event |
|-----------|-------|
| 02:00 | EventBridge triggers Lambda |
| ~02:02 | Lambda uploads fresh CSVs to S3 `/raw/` |
| 02:30 | Glue Crawler runs, updates Data Catalog |
| ~02:35 | Glue Trigger fires ETL job |
| ~02:45 | Parquet files land in S3 `/processed/` |
| ~02:50 | Snowpipe auto-ingests new files into Snowflake RAW |
| Manual | Run `dbt run` to refresh mart tables (or schedule with Airflow/Step Functions) |

---

## 🛠️ Useful Commands

```bash
# ── AWS ─────────────────────────────────────────────────────

# Manually invoke Lambda
aws lambda invoke --function-name olist-ingestion \
  --payload '{}' --cli-binary-format raw-in-base64-out /tmp/resp.json

# Check Glue job run history
aws glue get-job-runs --job-name olist-etl-job \
  --query 'JobRuns[0:5].{State:JobRunState,Start:StartedOn,Duration:ExecutionTime}'

# List processed files
aws s3 ls s3://olist-lake-<account_id>/processed/ --recursive --human-readable

# Tail Lambda logs
aws logs tail /aws/lambda/olist-ingestion --follow

# ── Terraform ────────────────────────────────────────────────

terraform plan -out=tfplan     # save plan
terraform apply tfplan          # apply saved plan
terraform destroy               # tear down everything (careful in prod!)
terraform output -json          # all outputs as JSON

# ── dbt ──────────────────────────────────────────────────────

dbt run --select staging        # run only staging models
dbt run --select marts          # run only mart models
dbt run --select fct_orders+    # run fct_orders and all downstream
dbt test --select dim_customers # test a single model
dbt compile                     # compile SQL without running
dbt run --target prod           # run against production Snowflake

# ── Streamlit ────────────────────────────────────────────────

streamlit run dashboard/streamlit_app.py --server.port 8502
```

---

## 🏗️ Architecture Decisions

| Decision | Rationale |
|---------|-----------|
| **Lambda for ingestion** | Lightweight, serverless, zero idle cost; Olist dataset is small (~45 MB) |
| **Glue Crawler** | Auto-discovers CSV schema changes without manual DDL |
| **Glue ETL (Spark)** | Handles larger datasets if Olist scales; native AWS integration |
| **Parquet + Snappy** | ~75% storage reduction vs CSV; columnar = fast analytical queries |
| **Snowpipe AUTO_INGEST** | Near-real-time loading; decoupled from Glue job completion |
| **dbt Core** | Version-controlled transformations; lineage DAG; built-in tests |
| **Streamlit** | Rapid Python-native dashboarding; no BI license required |

---

## 💰 Cost Estimate (us-east-1)

| Service | Usage | Est. Monthly Cost |
|---------|-------|-------------------|
| Lambda | 30 invocations × 5 min | < $0.01 |
| S3 | ~500 MB storage | ~$0.01 |
| Glue Crawler | 30 runs × 1 min | ~$0.15 |
| Glue ETL | 30 runs × 10 min × 2 DPU | ~$4.40 |
| EventBridge | 30 events | < $0.01 |
| Snowflake | X-Small WH, 60s auto-suspend | ~$2–5 |
| **Total** | | **~$7–10/month** |

> Costs assume dev/learning usage. Production workloads will vary.

---

## 🐛 Troubleshooting

### Lambda times out downloading the dataset
```
# Increase timeout in terraform/variables.tf:
lambda_timeout = 600  # 10 minutes
# Then: terraform apply
```

### Glue Crawler finds no tables
```bash
# Check that Lambda successfully uploaded files:
aws s3 ls s3://olist-lake-<account_id>/raw/
# If empty, check Lambda CloudWatch logs:
aws logs tail /aws/lambda/olist-ingestion --since 1h
```

### Glue ETL Job fails with "Table not found"
```
# The Glue Crawler must finish BEFORE the ETL job runs.
# Check the Glue trigger is enabled:
aws glue get-trigger --name olist-trigger-etl-after-crawler
```

### Snowpipe not ingesting files
```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('OLIST_DW.RAW.pipe_fct_orders');

-- Refresh pipe manually (useful after initial setup)
ALTER PIPE OLIST_DW.RAW.pipe_fct_orders REFRESH;

-- Check copy history for errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'OLIST_DW.RAW.fct_orders',
  START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP())
));
```

### dbt "relation does not exist"
```bash
# Ensure Snowpipe has ingested data first:
# SELECT COUNT(*) FROM OLIST_DW.RAW.fct_orders;  -- must be > 0

# Check dbt is pointed at the right schema:
dbt debug
```

### Streamlit "cannot connect to Snowflake"
```bash
# Verify env vars are set:
echo $SNOWFLAKE_ACCOUNT $SNOWFLAKE_USER

# Test connection directly:
python -c "
import os, snowflake.connector
conn = snowflake.connector.connect(
  account=os.environ['SNOWFLAKE_ACCOUNT'],
  user=os.environ['SNOWFLAKE_USER'],
  password=os.environ['SNOWFLAKE_PASSWORD'],
)
print(conn.cursor().execute('SELECT CURRENT_VERSION()').fetchone())
"
```

---

## 🔒 Security Checklist

- [x] S3 bucket blocks all public access
- [x] S3 enforces TLS (`aws:SecureTransport` bucket policy)
- [x] Lambda role has write-only access to `/raw/` prefix
- [x] Glue role has no access to `/processed/` raw S3 path beyond what it needs
- [x] Snowflake role can only read `/processed/` via STS assume-role + ExternalId
- [x] Secrets stored in `.env` (gitignored) or Streamlit secrets
- [x] CloudWatch log retention set (not infinite)
- [x] S3 versioning enabled for data lineage
- [ ] Enable AWS CloudTrail for audit logging (recommended for prod)
- [ ] Rotate IAM access keys regularly or use IAM Identity Center

---

## 📚 References

- [Olist Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [Snowflake Snowpipe Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
- [dbt Core Documentation](https://docs.getdbt.com/docs/core/installation)
- [Streamlit Snowflake Connection](https://docs.streamlit.io/develop/api-reference/connections/st.connection)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
