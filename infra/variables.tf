variable "region" {
  description = "AWS region to deploy resources into."
  type        = string
  default     = "us-east-1"
}

variable "env" {
  description = "Deployment environment (e.g., dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project name used for tagging and resource naming."
  type        = string
  default     = "energy-etl"
}

variable "raw_data_bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "scripts_bucket_name" {
  description = "Name of the S3 bucket used to store Glue PySpark scripts."
  type        = string
  default     = "multi-agent-pipeline-dev-scripts"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  type        = string
  default     = "energy-etl-glue-job"
}

variable "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database."
  type        = string
  default     = "energy_etl_db"
}

variable "glue_iam_role_name" {
  description = "Name of the IAM role assumed by AWS Glue."
  type        = string
  default     = "energy-etl-glue-role"
}

variable "glue_version" {
  description = "AWS Glue version (determines Spark and Python versions)."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type (e.g., G.1X, G.2X)."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers to allocate for the job."
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "Number of days to retain CloudWatch log events."
  type        = number
  default     = 30
}