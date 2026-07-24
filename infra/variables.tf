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
  description = "Name of the S3 bucket used as both source and sink for the data pipeline."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 key prefix for raw JSONL source data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix for transformed Parquet sink data."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix under which the Glue PySpark script(s) are stored."
  type        = string
  default     = "scripts/"
}

variable "temp_prefix" {
  description = "S3 key prefix used as the Glue job TempDir for shuffle/staging data."
  type        = string
  default     = "tmp/"
}

variable "glue_script_object_key" {
  description = "Full S3 key (including prefix) of the PySpark transform script, uploaded out-of-band by CI/CD."
  type        = string
  default     = "scripts/transform.py"
}

variable "force_destroy_bucket" {
  description = "Whether to allow destruction of the S3 bucket even if it contains objects."
  type        = bool
  default     = true
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "multi-agent-pipeline-dev-transform-job"
}

variable "glue_version" {
  description = "AWS Glue version to use for the ETL job."
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
  description = "Timeout, in minutes, for the Glue job run."
  type        = number
  default     = 10
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database used to hold crawled table metadata."
  type        = string
  default     = "multi_agent_pipeline_dev"
}

variable "glue_crawler_name" {
  description = "Name of the AWS Glue crawler used to catalog the transformed_data/ Parquet output."
  type        = string
  default     = "multi-agent-pipeline-dev-transformed-data-crawler"
}

variable "glue_crawler_schedule" {
  description = "Cron schedule expression for the Glue crawler. Leave empty string for on-demand only."
  type        = string
  default     = ""
}

variable "log_retention_in_days" {
  description = "Retention period, in days, for CloudWatch Log Groups created for the Glue job."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Additional tags to apply to all resources."
  type        = map(string)
  default     = {}
}