# ============================================================
# variables.tf — Input variable declarations
# ============================================================

variable "aws_region" {
  description = "AWS region to deploy all resources into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project identifier used in resource names and tags."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "data_bucket_name" {
  description = "Name of the S3 bucket that holds raw and transformed energy data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 prefix (folder) for raw JSONL input data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 prefix (folder) for Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "energy-etl-glue-job"
}

variable "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database."
  type        = string
  default     = "energy_etl_db"
}

variable "glue_version" {
  description = "AWS Glue version. 4.0 = Spark 3.3 / Python 3.10."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum retries for the Glue job."
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Job timeout in minutes."
  type        = number
  default     = 60
}

variable "glue_log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 30
}