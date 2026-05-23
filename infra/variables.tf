variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project name used for tagging and resource naming."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "bucket_name" {
  description = "Name of the S3 bucket used for raw data, transformed data, and Glue scripts."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "raw_data_prefix" {
  description = "S3 prefix (folder) for raw JSONL input data."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 prefix (folder) for transformed Parquet output data."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 prefix (folder) for Glue PySpark scripts."
  type        = string
  default     = "scripts/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "energy-etl-job"
}

variable "glue_version" {
  description = "AWS Glue version. 4.0 provides Spark 3.3.0 and Python 3.10."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type. G.1X = 1 DPU (4 vCPU, 16 GB) — cost-effective for standard ETL."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers to allocate for the job."
  type        = number
  default     = 2
}

variable "glue_job_timeout" {
  description = "Maximum job run time in minutes before Glue forcefully stops it."
  type        = number
  default     = 60
}

variable "glue_max_retries" {
  description = "Number of times Glue retries the job if it fails."
  type        = number
  default     = 1
}