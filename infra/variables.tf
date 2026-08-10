variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming and tagging."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "Name of the existing S3 bucket used as the raw data source. Must already exist; it is referenced via a data source, not managed by this Terraform config."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 key prefix under the raw bucket where source JSONL data lives."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix under the sink bucket where transformed Parquet output is written."
  type        = string
  default     = "transformed_data/"
}

variable "use_dedicated_sink_bucket" {
  description = "If true, provision a dedicated processed-data bucket for transformed output. If false, reuse the raw bucket with the transformed_data_prefix."
  type        = bool
  default     = false
}

variable "sink_bucket_name" {
  description = "Name of the dedicated sink bucket to create when use_dedicated_sink_bucket is true."
  type        = string
  default     = "multi-agent-pipeline-dev-processed-data"
}

variable "glue_script_bucket" {
  description = "Name of the S3 bucket where the CI/CD pipeline uploads the Glue PySpark script. Defaults to the raw data bucket."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "glue_script_key" {
  description = "S3 object key (within glue_script_bucket) for the Glue PySpark ETL script, packaged from src/transformations/transform.py."
  type        = string
  default     = "scripts/glue_job_script.py"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "multi-agent-pipeline-dev-etl-job"
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

variable "glue_timeout" {
  description = "Timeout (in minutes) for the Glue job run."
  type        = number
  default     = 60
}

variable "glue_max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for the Glue job."
  type        = number
  default     = 1
}

variable "log_retention_in_days" {
  description = "Retention period, in days, for the Glue job's CloudWatch log group."
  type        = number
  default     = 14
}