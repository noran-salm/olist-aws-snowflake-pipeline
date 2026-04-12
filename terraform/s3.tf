# ─────────────────────────────────────────────────────────────────────────────
#  S3: Olist Data Lake Bucket
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "olist_lake" {
  bucket        = local.bucket_name
  force_destroy = var.environment != "prod" # Protect prod data

  lifecycle {
    prevent_destroy = false
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "olist_lake" {
  bucket = aws_s3_bucket.olist_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning for data lineage
resource "aws_s3_bucket_versioning" "olist_lake" {
  bucket = aws_s3_bucket.olist_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption with AWS managed keys
resource "aws_s3_bucket_server_side_encryption_configuration" "olist_lake" {
  bucket = aws_s3_bucket.olist_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Lifecycle rules: move processed data to Intelligent-Tiering after 30 days
resource "aws_s3_bucket_lifecycle_configuration" "olist_lake" {
  bucket = aws_s3_bucket.olist_lake.id

  rule {
    id     = "archive-processed-data"
    status = "Enabled"

    filter {
      prefix = "processed/"
    }

    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  rule {
    id     = "cleanup-glue-temp"
    status = "Enabled"

    filter {
      prefix = "glue-temp/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "cleanup-spark-logs"
    status = "Enabled"

    filter {
      prefix = "spark-logs/"
    }

    expiration {
      days = 14
    }
  }
}

# Enable S3 Event Notifications for Snowpipe (AUTO_INGEST)
resource "aws_s3_bucket_notification" "snowpipe_notification" {
  bucket = aws_s3_bucket.olist_lake.id

  # This SQS queue is created by Snowflake after the External Stage is configured.
  # The ARN is provided in the Snowpipe setup output.
  # Uncomment after completing sql_scripts/03_create_snowpipe.sql:
  #
  # queue {
  #   queue_arn     = "<snowflake_sqs_arn_from_show_external_stages>"
  #   events        = ["s3:ObjectCreated:*"]
  #   filter_prefix = "processed/"
  #   filter_suffix = ".parquet"
  # }
}

# ─────────────────────────────────────────────────────────────────────────────
#  S3: Bucket Policy
# ─────────────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "olist_lake_policy" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.olist_lake.arn,
      "${aws_s3_bucket.olist_lake.arn}/*"
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid    = "AllowGlueRole"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.olist_lake.arn,
      "${aws_s3_bucket.olist_lake.arn}/*"
    ]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.glue_role.arn]
    }
  }

  statement {
    sid    = "AllowSnowflakeRole"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.olist_lake.arn,
      "${aws_s3_bucket.olist_lake.arn}/processed/*"
    ]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.snowflake_s3_role.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "olist_lake" {
  bucket = aws_s3_bucket.olist_lake.id
  policy = data.aws_iam_policy_document.olist_lake_policy.json

  depends_on = [aws_s3_bucket_public_access_block.olist_lake]
}

# ─────────────────────────────────────────────────────────────────────────────
#  S3: Placeholder Objects (create folder structure)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.olist_lake.id
  key     = "raw/.keep"
  content = ""
}

resource "aws_s3_object" "processed_prefix" {
  bucket  = aws_s3_bucket.olist_lake.id
  key     = "processed/.keep"
  content = ""
}
