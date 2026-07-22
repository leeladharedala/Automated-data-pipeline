variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used as a prefix for resource naming."
  type        = string
  default     = "multi-agent-pipeline"
}

variable "environment" {
  description = "Deployment environment name (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data. Must match the existing bucket if create_bucket is false."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "create_bucket" {
  description = "Whether Terraform should create the S3 bucket. Set to false to reference an existing bucket instead."
  type        = bool
  default     = true
}

variable "raw_data_prefix" {
  description = "S3 key prefix where raw JSONL data is stored."
  type        = string
  default     = "raw_data/"
}

variable "transformed_data_prefix" {
  description = "S3 key prefix where transformed Parquet data is written."
  type        = string
  default     = "transformed_data/"
}

variable "scripts_prefix" {
  description = "S3 key prefix where the Glue PySpark ETL script is stored. The script itself is uploaded out-of-band by the CI/CD pipeline, not by Terraform."
  type        = string
  default     = "glue_scripts/"
}

variable "glue_script_filename" {
  description = "Filename of the Glue PySpark ETL script within the scripts prefix."
  type        = string
  default     = "transform_job.py"
}

variable "glue_temp_prefix" {
  description = "S3 key prefix used by Glue for temporary files."
  type        = string
  default     = "glue_temp/"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "multi-agent-pipeline-dev-transform-job"
}

variable "glue_version" {
  description = "AWS Glue version to run the job on."
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
  description = "Timeout in minutes for the Glue job."
  type        = number
  default     = 60
}

variable "create_glue_crawler" {
  description = "Whether to create a Glue Crawler and Catalog Database for schema discovery."
  type        = bool
  default     = true
}

variable "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database used for schema discovery."
  type        = string
  default     = "multi_agent_pipeline_dev_db"
}

variable "glue_crawler_name" {
  description = "Name of the Glue Crawler that discovers schema in the transformed_data prefix."
  type        = string
  default     = "multi-agent-pipeline-dev-transformed-data-crawler"
}

variable "glue_crawler_schedule" {
  description = "Cron schedule expression (without the cron() wrapper) for the Glue Crawler. Leave empty to run on-demand only."
  type        = string
  default     = ""
}

variable "log_retention_in_days" {
  description = "Retention period in days for Glue CloudWatch log groups managed by this stack."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default = {
    Project     = "multi-agent-pipeline"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}