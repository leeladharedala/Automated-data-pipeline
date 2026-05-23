output "raw_data_bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed energy data."
  value       = aws_s3_bucket.raw_data.bucket
}

output "raw_data_bucket_arn" {
  description = "ARN of the raw data S3 bucket."
  value       = aws_s3_bucket.raw_data.arn
}

output "scripts_bucket_name" {
  description = "Name of the S3 bucket used to store Glue PySpark scripts."
  value       = aws_s3_bucket.scripts.bucket
}

output "scripts_bucket_arn" {
  description = "ARN of the scripts S3 bucket."
  value       = aws_s3_bucket.scripts.arn
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.energy_etl.arn
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = aws_glue_catalog_database.energy_etl.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler for transformed data."
  value       = aws_glue_crawler.transformed_data.name
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by AWS Glue."
  value       = aws_iam_role.glue.arn
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by AWS Glue."
  value       = aws_iam_role.glue.name
}

output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch Log Group for Glue job logs."
  value       = aws_cloudwatch_log_group.glue_job.name
}

output "script_s3_uri" {
  description = "Expected S3 URI for the Glue PySpark transform script."
  value       = "s3://${var.scripts_bucket_name}/src/transformations/transform.py"
}