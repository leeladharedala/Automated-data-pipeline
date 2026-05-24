output "data_bucket_name" {
  description = "Name of the S3 bucket holding raw and transformed energy data"
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "ARN of the S3 data bucket"
  value       = aws_s3_bucket.data.arn
}

output "raw_data_s3_uri" {
  description = "S3 URI for the raw JSONL input prefix"
  value       = "s3://${aws_s3_bucket.data.bucket}/${var.raw_data_prefix}"
}

output "transformed_data_s3_uri" {
  description = "S3 URI for the Parquet output prefix"
  value       = "s3://${aws_s3_bucket.data.bucket}/${var.transformed_data_prefix}"
}

output "glue_scripts_bucket_name" {
  description = "Name of the S3 bucket holding Glue scripts and temporary files"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "glue_scripts_bucket_arn" {
  description = "ARN of the Glue scripts S3 bucket"
  value       = aws_s3_bucket.glue_scripts.arn
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by AWS Glue"
  value       = aws_iam_role.glue.name
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by AWS Glue"
  value       = aws_iam_role.glue.arn
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.energy.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler"
  value       = aws_glue_crawler.energy.name
}

output "glue_job_name" {
  description = "Name of the Glue ETL job"
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "ARN of the Glue ETL job"
  value       = aws_glue_job.energy_etl.arn
}

output "glue_log_group_name" {
  description = "CloudWatch Log Group name for Glue job continuous logs"
  value       = aws_cloudwatch_log_group.glue_job.name
}