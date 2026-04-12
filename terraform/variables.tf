variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "lambda_zip_path" {
  description = "Local path to the zipped Lambda deployment package"
  type        = string
  default     = "../lambda_function/lambda_package.zip"
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_mb" {
  description = "Lambda function memory allocation in MB"
  type        = number
  default     = 512
}

variable "olist_dataset_url" {
  description = "Public URL of the Olist dataset ZIP file"
  type        = string
  default     = "https://raw.githubusercontent.com/olist/work-at-olist-data-science/main/olist_public_dataset.zip"
}

variable "snowflake_aws_role_arn" {
  description = "AWS IAM Role ARN that Snowflake is allowed to assume (filled after Snowflake storage integration is created)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "snowflake_external_id" {
  description = "External ID provided by Snowflake storage integration (for trust policy)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "enable_glue_trigger" {
  description = "Automatically trigger Glue ETL job after crawler completes"
  type        = bool
  default     = true
}
