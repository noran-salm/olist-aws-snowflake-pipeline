output "s3_bucket_name" {
  description = "Name of the Olist data lake S3 bucket"
  value       = aws_s3_bucket.olist_lake.id
}

output "s3_bucket_arn" {
  description = "ARN of the Olist data lake S3 bucket"
  value       = aws_s3_bucket.olist_lake.arn
}

output "lambda_function_name" {
  description = "Name of the ingestion Lambda function"
  value       = aws_lambda_function.olist_ingestion.function_name
}

output "lambda_function_arn" {
  description = "ARN of the ingestion Lambda function"
  value       = aws_lambda_function.olist_ingestion.arn
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.olist_raw_db.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler"
  value       = aws_glue_crawler.olist_raw_crawler.name
}

output "glue_etl_job_name" {
  description = "Name of the Glue ETL job"
  value       = aws_glue_job.olist_etl.name
}

output "glue_role_arn" {
  description = "ARN of the Glue IAM role"
  value       = aws_iam_role.glue_role.arn
}

output "snowflake_s3_role_arn" {
  description = "ARN of the IAM role for Snowflake to assume (use in Snowflake Storage Integration)"
  value       = aws_iam_role.snowflake_s3_role.arn
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge schedule rule"
  value       = aws_cloudwatch_event_rule.olist_ingestion_schedule.arn
}

output "raw_s3_path" {
  description = "S3 path for raw data"
  value       = "s3://${aws_s3_bucket.olist_lake.id}/raw/"
}

output "processed_s3_path" {
  description = "S3 path for processed Parquet data (Snowflake External Stage points here)"
  value       = "s3://${aws_s3_bucket.olist_lake.id}/processed/"
}

output "account_id" {
  description = "AWS Account ID"
  value       = local.account_id
}
