variable "aws_region" {
  description = "AWS region to deploy resources into"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "raw_data_bucket_name" {
  description = "Name of the existing S3 bucket that holds raw JSONL data"
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "transformed_data_bucket_name" {
  description = "Name of the S3 bucket where Glue writes Parquet output"
  type        = string
  default     = "multi-agent-pipeline-dev-transformed-data"
}

variable "raw_data_prefix" {
  description = "S3 key prefix for raw input data"
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix for transformed Parquet output"
  type        = string
  default     = "transformed_data/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job"
  type        = string
  default     = "energy-etl-glue-job"
}

variable "glue_script_location" {
  description = "S3 URI of the PySpark ETL script"
  type        = string
  default     = "s3://multi-agent-pipeline-dev-raw-data/scripts/transform.py"
}

variable "glue_version" {
  description = "AWS Glue version (4.0 = Spark 3.3.0 / Python 3.10)"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X recommended for standard ETL)"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers to allocate for the job"
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Maximum number of retries on job failure"
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Job timeout in minutes (max 10080 / 7 days)"
  type        = number
  default     = 60
}

variable "glue_temp_dir_prefix" {
  description = "S3 key prefix used by Glue for temporary files"
  type        = string
  default     = "glue-temp/"
}