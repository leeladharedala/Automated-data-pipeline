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
  description = "Short name used as a prefix for resources created by this stack."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "raw_data_bucket_name" {
  description = "Name of the existing S3 bucket containing raw_data/ and transformed_data/ prefixes."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "create_bucket" {
  description = "Whether Terraform should create/manage the S3 bucket. Set to false to reference an existing bucket by name via data source."
  type        = bool
  default     = false
}

variable "raw_data_prefix" {
  description = "S3 key prefix for the raw JSONL source data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix for the transformed Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix where the Glue PySpark script is uploaded out-of-band by CI/CD."
  type        = string
  default     = "scripts/"
}

variable "glue_script_key" {
  description = "S3 object key (within scripts_prefix) of the PySpark ETL script. Uploaded out-of-band by CI/CD, not managed by Terraform."
  type        = string
  default     = "transform.py"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "transform-job"
}

variable "glue_version" {
  description = "AWS Glue version to use for the job (controls Spark/Python runtime)."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Worker type for the Glue job (e.g. G.1X, G.2X, Standard)."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers to allocate to the Glue job."
  type        = number
  default     = 2
}

variable "glue_timeout" {
  description = "Job timeout in minutes."
  type        = number
  default     = 30
}

variable "glue_max_retries" {
  description = "Maximum number of automatic retries after a job failure."
  type        = number
  default     = 0
}

variable "glue_python_version" {
  description = "Python version used by the Glue PySpark job."
  type        = string
  default     = "3"
}

variable "log_retention_in_days" {
  description = "Retention period (days) for the Glue job CloudWatch log group."
  type        = number
  default     = 14
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database used to register the transformed dataset."
  type        = string
  default     = "multi_agent_pipeline_dev"
}

variable "glue_catalog_table_name" {
  description = "Name of the Glue Catalog table for the transformed Parquet data."
  type        = string
  default     = "transformed_data"
}

variable "tags" {
  description = "Common tags applied to all taggable resources."
  type        = map(string)
  default = {
    Project   = "multi-agent-pipeline"
    ManagedBy = "terraform"
  }
}