# ─────────────────────────────────────────────────────────────────────────────
#  Lambda: Olist Ingestion Function
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_lambda_function" "olist_ingestion" {
  function_name = "olist-ingestion"
  description   = "Downloads Olist dataset ZIP, extracts CSVs, and uploads to S3 raw/"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_handler.handler"
  runtime       = "python3.12"
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_mb

  # Lambda Layer for requests library (or package in deployment zip)
  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)

  environment {
    variables = {
      S3_BUCKET      = local.bucket_name
      S3_RAW_PREFIX  = "raw/"
      GLUE_CRAWLER   = aws_glue_crawler.olist_raw_crawler.name
      GLUE_ETL_JOB   = aws_glue_job.olist_etl.name
      LOG_LEVEL      = "INFO"
      # DATASET_URL removed — no longer downloading from GitHub
    }
  }

  # Ephemeral /tmp storage for ZIP extraction (Olist dataset ~45MB unzipped)
  ephemeral_storage {
    size = 1024 # MB
  }

  tracing_config {
    mode = "Active" # Enable X-Ray tracing
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_logs,
    aws_iam_role_policy.lambda_s3,
    aws_s3_bucket.olist_lake
  ]
}

# CloudWatch Log Group with retention
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.olist_ingestion.function_name}"
  retention_in_days = 14
}

# ─────────────────────────────────────────────────────────────────────────────
#  Lambda: Glue Trigger (optional - chain Lambda → Glue after ingestion)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_trigger" "etl_after_crawler" {
  count = var.enable_glue_trigger ? 1 : 0
  name  = "olist-trigger-etl-after-crawler"
  type  = "CONDITIONAL"

  actions {
    job_name = aws_glue_job.olist_etl.name
  }

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.olist_raw_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }
}
