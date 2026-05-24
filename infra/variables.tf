variable "aws_region" {
  description = "AWS region to deploy resources into"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data"
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 prefix for raw JSONL input data"
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 prefix for transformed Parquet output data"
  type        = string
  default     = "transformed_data/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job"
  type        = string
  default     = "energy-etl-glue-job"
}

variable "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database"
  type        = string
  default     = "energy_etl_database"
}

variable "glue_crawler_name" {
  description = "Name of the AWS Glue crawler"
  type        = string
  default     = "energy-etl-crawler"
}

variable "glue_version" {
  description = "AWS Glue version (determines Spark and Python versions)"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Worker type for the Glue job (G.1X, G.2X, G.4X, etc.)"
  type        = string
  default     = "G.1X"

  validation {
    condition     = contains(["G.1X", "G.2X", "G.4X", "G.8X", "G.12X", "G.16X", "R.1X", "R.2X", "R.4X", "R.8X"], var.glue_worker_type)
    error_message = "Worker type must be one of: G.1X, G.2X, G.4X, G.8X, G.12X, G.16X, R.1X, R.2X, R.4X, R.8X."
  }
}

variable "glue_number_of_workers" {
  description = "Number of workers to allocate for the Glue job (minimum 2)"
  type        = number
  default     = 2

  validation {
    condition     = var.glue_number_of_workers >= 2
    error_message = "Number of workers must be at least 2."
  }
}

variable "glue_max_retries" {
  description = "Maximum number of times to retry the Glue job on failure"
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Timeout in minutes for the Glue job"
  type        = number
  default     = 60
}

variable "scripts_prefix" {
  description = "S3 prefix where Glue job scripts are stored"
  type        = string
  default     = "scripts/"
}