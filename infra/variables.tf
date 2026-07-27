variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for tagging and naming resources."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "create_bucket" {
  description = "Whether Terraform should create the S3 bucket (set to false to reference an existing bucket)."
  type        = bool
  default     = true
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
  description = "S3 key prefix where Glue PySpark scripts are stored (uploaded out-of-band by CI/CD)."
  type        = string
  default     = "scripts/"
}

variable "glue_script_filename" {
  description = "Filename of the Glue PySpark transform script within the scripts prefix."
  type        = string
  default     = "transform_job.py"
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

variable "glue_timeout" {
  description = "Timeout in minutes for the Glue job."
  type        = number
  default     = 10
}

variable "glue_database_name" {
  description = "Name of the Glue Data Catalog database."
  type        = string
  default     = "multi_agent_pipeline_dev"
}

variable "log_retention_in_days" {
  description = "Retention period in days for CloudWatch log groups."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Additional tags to apply to all resources."
  type        = map(string)
  default     = {}
}