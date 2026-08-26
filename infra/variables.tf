variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used for tagging and resource naming."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 key prefix for raw JSONL input data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix for transformed Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix where the Glue PySpark script is stored (uploaded out-of-band by CI/CD)."
  type        = string
  default     = "scripts/"
}

variable "temp_prefix" {
  description = "S3 key prefix used by Glue for temporary staging files."
  type        = string
  default     = "tmp/"
}

variable "glue_script_filename" {
  description = "Filename of the PySpark ETL script within the scripts prefix."
  type        = string
  default     = "transform.py"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue job."
  type        = string
  default     = "multi-agent-pipeline-dev-transform-job"
}

variable "glue_version" {
  description = "AWS Glue version to use for the job."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Worker type for the Glue job (e.g. G.1X, G.2X)."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers allocated to the Glue job."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum number of retries for the Glue job."
  type        = number
  default     = 0
}

variable "glue_timeout_minutes" {
  description = "Timeout for the Glue job, in minutes."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database for the raw data table."
  type        = string
  default     = "multi_agent_pipeline_dev_db"
}

variable "glue_catalog_table_name" {
  description = "Name of the Glue Data Catalog table for the raw JSONL data."
  type        = string
  default     = "raw_data"
}

variable "log_retention_in_days" {
  description = "Retention period, in days, for the Glue job CloudWatch log group."
  type        = number
  default     = 14
}