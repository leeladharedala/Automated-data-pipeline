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
  description = "Project name used for resource naming and tagging."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "raw_bucket_name" {
  description = "Name of the existing S3 bucket that holds raw JSONL data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "sink_bucket_name" {
  description = "Name of the S3 bucket used for transformed (Parquet) output. Defaults to the raw bucket."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "scripts_bucket_name" {
  description = "Name of the S3 bucket used to store the Glue ETL PySpark script. Defaults to the raw bucket."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_prefix" {
  description = "S3 key prefix for raw JSONL source data."
  type        = string
  default     = "raw_data/"
}

variable "sink_prefix" {
  description = "S3 key prefix for transformed Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix under which the Glue PySpark ETL script is stored (uploaded out-of-band by CI/CD)."
  type        = string
  default     = "scripts/"
}

variable "glue_script_filename" {
  description = "Filename of the PySpark ETL script object within the scripts prefix."
  type        = string
  default     = "transform.py"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "multi-agent-pipeline-dev-etl-job"
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
  description = "Timeout for the Glue job, in minutes."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "log_retention_in_days" {
  description = "Retention period in days for CloudWatch log groups."
  type        = number
  default     = 14
}

variable "create_glue_catalog" {
  description = "Whether to provision a Glue Catalog database for the pipeline."
  type        = bool
  default     = true
}

variable "enable_crawlers" {
  description = "Whether to provision Glue Crawlers for the raw and transformed data prefixes."
  type        = bool
  default     = true
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database."
  type        = string
  default     = "multi_agent_pipeline_dev"
}

variable "crawler_schedule" {
  description = "Optional cron schedule expression for the Glue crawlers. Leave empty for on-demand only."
  type        = string
  default     = ""
}