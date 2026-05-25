variable "aws_region" {
  description = "AWS region to deploy resources into"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used as a prefix for resource naming and tagging"
  type        = string
  default     = "multi-agent-pipeline-dev"
}

variable "raw_data_bucket_name" {
  description = "Name of the S3 bucket that holds raw and transformed data"
  type        = string
  default     = "multi-agent-pipeline-dev-raw-data"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue ETL job"
  type        = string
  default     = "energy-etl-glue-job"
}

variable "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database"
  type        = string
  default     = "energy_etl_database"
}

variable "glue_crawler_name" {
  description = "Name of the AWS Glue crawler"
  type        = string
  default     = "energy-etl-crawler"
}

variable "glue_version" {
  description = "AWS Glue version (determines Spark and Python versions)"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type: G.1X (4 vCPU / 16 GB) or G.2X (8 vCPU / 32 GB)"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers allocated to the job"
  type        = number
  default     = 2
}

variable "job_timeout_minutes" {
  description = "Maximum job run duration in minutes before it is terminated"
  type        = number
  default     = 60
}

variable "max_retries" {
  description = "Maximum number of retries on job failure"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default = {
    Project     = "multi-agent-pipeline"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}