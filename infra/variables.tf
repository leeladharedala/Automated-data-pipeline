variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name of the project, used for resource naming and tagging."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 prefix for raw JSONL input data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 prefix for transformed Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "glue_scripts_prefix" {
  description = "S3 prefix where Glue PySpark scripts are stored."
  type        = string
  default     = "glue_scripts/"
}

variable "glue_temp_prefix" {
  description = "S3 prefix for Glue temporary files."
  type        = string
  default     = "glue_temp/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "energy-etl-job"
}

variable "glue_version" {
  description = "AWS Glue version. 4.0 = Spark 3.3/Python 3.10; 5.0 = Spark 3.5/Python 3.11."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type. G.1X is recommended for standard ETL workloads."
  type        = string
  default     = "G.1X"

  validation {
    condition     = contains(["G.025X", "G.1X", "G.2X", "G.4X", "G.8X", "G.12X", "G.16X", "R.1X", "R.2X", "R.4X", "R.8X"], var.glue_worker_type)
    error_message = "glue_worker_type must be a valid AWS Glue worker type (e.g. G.1X, G.2X)."
  }
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers to allocate. Minimum is 2."
  type        = number
  default     = 2

  validation {
    condition     = var.glue_number_of_workers >= 2
    error_message = "glue_number_of_workers must be at least 2."
  }
}

variable "glue_max_retries" {
  description = "Maximum number of times to retry the Glue job if it fails."
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Timeout in minutes for the Glue job run."
  type        = number
  default     = 60
}