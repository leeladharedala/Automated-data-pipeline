variable "aws_region" {
  description = "AWS region where resources will be deployed."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, staging, prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "bucket_name" {
  description = "Name of the S3 bucket used for raw data, transformed data, and Glue scripts."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 prefix (folder) for raw JSONL input data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 prefix (folder) for transformed Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 prefix (folder) for Glue PySpark scripts."
  type        = string
  default     = "scripts/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "energy-etl-transform"
}

variable "glue_version" {
  description = "AWS Glue version."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Worker type for the Glue job."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers to allocate for the Glue job."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum number of times to retry the Glue job on failure."
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Job timeout in minutes."
  type        = number
  default     = 60
}

variable "enable_glue_job_bookmark" {
  description = "Enable Glue job bookmarks."
  type        = bool
  default     = false
}

variable "enable_continuous_cloudwatch_log" {
  description = "Enable continuous CloudWatch logging."
  type        = bool
  default     = true
}