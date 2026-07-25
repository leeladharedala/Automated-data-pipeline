variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for naming and tagging resources."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "Name of the existing S3 bucket containing raw JSONL data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 key prefix under the raw bucket where source JSONL objects live."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix under the sink bucket where transformed Parquet objects are written."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix in the sink bucket where the Glue PySpark script is stored (uploaded out-of-band by CI/CD)."
  type        = string
  default     = "scripts/"
}

variable "glue_script_filename" {
  description = "Filename of the PySpark ETL script expected at the scripts_prefix location in the sink bucket."
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
  description = "Number of workers for the Glue job."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum number of retries for the Glue job."
  type        = number
  default     = 0
}

variable "glue_timeout_minutes" {
  description = "Timeout, in minutes, for the Glue job."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "log_retention_in_days" {
  description = "Retention, in days, for CloudWatch log groups created for this pipeline."
  type        = number
  default     = 14
}

variable "enable_glue_crawler" {
  description = "Whether to provision a Glue Catalog database and crawler over the transformed data for schema discovery."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default     = {}
}