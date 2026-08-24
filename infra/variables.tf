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

variable "raw_data_bucket_name" {
  description = "Name of the existing S3 bucket containing raw_data/ and transformed_data/ prefixes."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 prefix (within the bucket) where raw JSONL source data lives."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 prefix (within the bucket) where transformed Parquet output is written."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 prefix (within the bucket) where the Glue PySpark script is stored (uploaded out-of-band by CI/CD)."
  type        = string
  default     = "scripts/"
}

variable "temp_dir_prefix" {
  description = "S3 prefix (within the bucket) used as the Glue job's temporary directory."
  type        = string
  default     = "tmp/"
}

variable "spark_event_logs_prefix" {
  description = "S3 prefix (within the bucket) used for Glue Spark UI event logs."
  type        = string
  default     = "sparkHistoryLogs/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "multi-agent-pipeline-transform-job"
}

variable "glue_script_filename" {
  description = "Filename of the PySpark transform script within the scripts prefix (uploaded out-of-band)."
  type        = string
  default     = "transform.py"
}

variable "glue_version" {
  description = "AWS Glue version to use for the ETL job."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Worker type for the Glue job (Standard, G.1X, G.2X, G.025X, G.4X, G.8X)."
  type        = string
  default     = "G.1X"

  validation {
    condition     = contains(["Standard", "G.1X", "G.2X", "G.025X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "glue_worker_type must be one of: Standard, G.1X, G.2X, G.025X, G.4X, G.8X."
  }
}

variable "glue_number_of_workers" {
  description = "Number of workers allocated to the Glue job."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum number of automatic retries for the Glue job."
  type        = number
  default     = 0
}

variable "glue_timeout_minutes" {
  description = "Timeout (in minutes) for the Glue job."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "log_retention_in_days" {
  description = "Retention period (days) for the Glue job's CloudWatch log group."
  type        = number
  default     = 14
}

variable "enable_glue_catalog" {
  description = "Whether to provision a Glue Catalog database and crawler for the transformed_data prefix."
  type        = bool
  default     = true
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database used to catalog transformed data."
  type        = string
  default     = "multi_agent_pipeline_dev"
}

variable "glue_crawler_name" {
  description = "Name of the Glue Crawler used to catalog the transformed_data prefix."
  type        = string
  default     = "multi-agent-pipeline-transformed-data-crawler"
}

variable "glue_crawler_schedule" {
  description = "Cron schedule expression for the Glue crawler. Empty string disables scheduling (on-demand only)."
  type        = string
  default     = ""
}