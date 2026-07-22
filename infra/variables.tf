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

variable "raw_data_prefix" {
  description = "S3 key prefix under the raw bucket where source JSONL data lives."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix under the sink bucket where transformed Parquet data is written."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix where Glue ETL scripts are stored (uploaded out-of-band by CI/CD)."
  type        = string
  default     = "scripts/"
}

variable "temp_dir_prefix" {
  description = "S3 key prefix used by Glue as a scratch/temp directory."
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
  description = "Timeout in minutes for the Glue job."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database for transformed data."
  type        = string
  default     = "multi_agent_pipeline_dev"
}

variable "glue_catalog_table_name" {
  description = "Name of the Glue Data Catalog table for transformed data."
  type        = string
  default     = "transformed_data"
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