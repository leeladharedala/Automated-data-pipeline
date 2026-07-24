variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "Environment name (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project/application name used for resource naming and tagging."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "raw_data_bucket_name" {
  description = "Name of the existing S3 bucket that holds raw data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 key prefix under the bucket where raw JSONL data lives."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix under the bucket where transformed Parquet data is written."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix under the bucket where Glue ETL scripts are stored."
  type        = string
  default     = "scripts/"
}

variable "temp_prefix" {
  description = "S3 key prefix under the bucket used for Glue job temporary data."
  type        = string
  default     = "temp/"
}

variable "spark_logs_prefix" {
  description = "S3 key prefix under the bucket used for Glue Spark UI event logs."
  type        = string
  default     = "sparkHistoryLogs/"
}

variable "glue_script_object_key" {
  description = "S3 object key (relative to bucket root, includes scripts_prefix) of the PySpark ETL script. Uploaded out-of-band by CI/CD."
  type        = string
  default     = "scripts/transform.py"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "multi-agent-pipeline-energy-etl"
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
  description = "Number of workers to allocate to the Glue job."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum number of retries for the Glue job."
  type        = number
  default     = 0
}

variable "glue_timeout_minutes" {
  description = "Timeout for the Glue job in minutes."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database used for downstream querying (e.g. Athena)."
  type        = string
  default     = "multi_agent_pipeline_dev_db"
}

variable "enable_glue_crawler" {
  description = "Whether to provision a Glue Crawler over the transformed_data/ prefix."
  type        = bool
  default     = true
}

variable "glue_crawler_name" {
  description = "Name of the Glue Crawler for the transformed data prefix."
  type        = string
  default     = "multi-agent-pipeline-transformed-data-crawler"
}

variable "glue_crawler_schedule" {
  description = "Cron schedule expression for the Glue Crawler. Empty string disables scheduling (on-demand only)."
  type        = string
  default     = ""
}

variable "log_retention_in_days" {
  description = "CloudWatch Logs retention period in days for Glue job logs."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Additional tags to apply to all resources."
  type        = map(string)
  default     = {}
}