terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "olist-aws-snowflake-pipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# ─── Data Sources ─────────────────────────────────────────────

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  bucket_name = "olist-lake-${local.account_id}"
  common_tags = {
    Project     = "olist-aws-snowflake-pipeline"
    Environment = var.environment
  }
}

# ─── EventBridge Schedule (triggers Lambda from lambda.tf) ─────

resource "aws_cloudwatch_event_rule" "olist_ingestion_schedule" {
  name                = "olist-ingestion-daily"
  description         = "Triggers Olist Lambda ingestion every day at 02:00 UTC"
  schedule_expression = "cron(0 2 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.olist_ingestion_schedule.name
  target_id = "OlistLambdaIngestion"
  arn       = aws_lambda_function.olist_ingestion.arn   # from lambda.tf
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.olist_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.olist_ingestion_schedule.arn
}

# ─── Glue Database ─────────────────────────────────────────────

resource "aws_glue_catalog_database" "olist_raw_db" {
  name        = "olist_raw_db"
  description = "Glue Data Catalog database for raw Olist data"
}

# ─── Glue ETL Job ─────────────────────────────────────────────

resource "aws_glue_job" "olist_etl" {
  name         = "olist-etl-job"
  role_arn     = aws_iam_role.glue_role.arn   # from iam.tf
  description  = "Spark ETL: cleans raw data and writes star schema Parquet to processed/"
  glue_version = "4.0"
  max_retries  = 1

  command {
    name            = "glueetl"
    script_location = "s3://${local.bucket_name}/scripts/olist_etl_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = ""
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.bucket_name}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${local.bucket_name}/glue-temp/"
    "--SOURCE_BUCKET"                    = local.bucket_name
    "--SOURCE_PREFIX"                    = "raw/"
    "--TARGET_PREFIX"                    = "processed/"
    "--DATABASE_NAME"                    = aws_glue_catalog_database.olist_raw_db.name
  }

  execution_property {
    max_concurrent_runs = 1
  }

  number_of_workers = 2
  worker_type       = "G.1X"
}

# Upload Glue script to S3 (bucket defined in s3.tf)
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.olist_lake.id   # from s3.tf
  key    = "scripts/olist_etl_script.py"
  source = "${path.module}/../glue_jobs/olist_etl_script.py"
  etag   = filemd5("${path.module}/../glue_jobs/olist_etl_script.py")
}