# ─────────────────────────────────────────────────────────────────────────────
#  IAM: Lambda Execution Role
# ─────────────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    sid     = "LambdaAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name               = "olist-lambda-ingestion-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  description        = "Execution role for the Olist ingestion Lambda function"
}

data "aws_iam_policy_document" "lambda_s3_policy" {
  statement {
    sid    = "WriteRawData"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl",
      "s3:GetObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.olist_lake.arn}/raw/*",
      "${aws_s3_bucket.olist_lake.arn}/scripts/*"
    ]
  }

  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [aws_s3_bucket.olist_lake.arn]
  }
}

resource "aws_iam_role_policy" "lambda_s3" {
  name   = "olist-lambda-s3-policy"
  role   = aws_iam_role.lambda_role.id
  policy = data.aws_iam_policy_document.lambda_s3_policy.json
}

# Attach AWS managed policy for CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ─────────────────────────────────────────────────────────────────────────────
#  IAM: Glue Service Role
# ─────────────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "GlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "olist-glue-service-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  description        = "Service role for Glue Crawler and ETL jobs"
}

data "aws_iam_policy_document" "glue_s3_policy" {
  statement {
    sid    = "ReadRawWriteProcessed"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload"
    ]
    resources = [
      "${aws_s3_bucket.olist_lake.arn}/raw/*",
      "${aws_s3_bucket.olist_lake.arn}/processed/*",
      "${aws_s3_bucket.olist_lake.arn}/scripts/*",
      "${aws_s3_bucket.olist_lake.arn}/glue-temp/*",
      "${aws_s3_bucket.olist_lake.arn}/spark-logs/*"
    ]
  }

  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [aws_s3_bucket.olist_lake.arn]
  }

  statement {
    sid    = "GlueDataCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition"
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${aws_glue_catalog_database.olist_raw_db.name}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${aws_glue_catalog_database.olist_raw_db.name}/*"
    ]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "olist-glue-s3-catalog-policy"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.glue_s3_policy.json
}
# ─────────────────────────────────────────────────────────────
#  IAM: Lambda Glue Crawler Permission
# ─────────────────────────────────────────────────────────────

resource "aws_iam_role_policy" "lambda_glue_crawler" {
  name = "lambda-glue-crawler-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler"
        ]
        Resource = aws_glue_crawler.olist_raw_crawler.arn   # from glue.tf
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
#  IAM: Snowflake S3 Access Role
#  Snowflake assumes this role via cross-account STS to read processed/ data
# ─────────────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    sid     = "SnowflakeAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      # This is Snowflake's AWS account that will assume the role.
      # The actual value comes from Snowflake after creating a Storage Integration.
      # Replace with the STORAGE_AWS_IAM_USER_ARN from Snowflake.
      identifiers = [
        var.snowflake_aws_role_arn != "" ? var.snowflake_aws_role_arn : "arn:aws:iam::${local.account_id}:root"
      ]
    }

    dynamic "condition" {
      for_each = var.snowflake_external_id != "" ? [1] : []
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [var.snowflake_external_id]
      }
    }
  }
}

resource "aws_iam_role" "snowflake_s3_role" {
  name               = "olist-snowflake-s3-role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
  description        = "IAM role that Snowflake assumes to read processed data from S3"
}

data "aws_iam_policy_document" "snowflake_s3_policy" {
  statement {
    sid    = "SnowflakeReadProcessed"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion"
    ]
    resources = ["${aws_s3_bucket.olist_lake.arn}/processed/*"]
  }

  statement {
    sid    = "SnowflakeListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources  = [aws_s3_bucket.olist_lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["processed/*"]
    }
  }
}

resource "aws_iam_role_policy" "snowflake_s3" {
  name   = "olist-snowflake-s3-read-policy"
  role   = aws_iam_role.snowflake_s3_role.id
  policy = data.aws_iam_policy_document.snowflake_s3_policy.json
}

# Patch: Add processed/ write access that was missing from original policy
resource "aws_iam_role_policy" "glue_s3_processed" {
  name = "olist-glue-processed-write"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "WriteProcessed"
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObject"
      ]
      Resource = [
        "arn:aws:s3:::olist-lake-516671521715/processed/*",
        "arn:aws:s3:::olist-lake-516671521715/processed_*"
      ]
    }]
  })
}
