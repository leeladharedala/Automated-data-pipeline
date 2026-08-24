output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.transform_job.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.transform_job.arn
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_job_role.arn
}

output "raw_data_bucket_name" {
  description = "Name of the S3 bucket used for source and sink data."
  value       = data.aws_s3_bucket.raw_data.id
}

output "source_data_s3_uri" {
  description = "S3 URI of the raw JSONL source data prefix."
  value       = "s3://${data.aws_s3_bucket.raw_data.id}/${var.raw_data_prefix}"
}

output "transformed_data_s3_uri" {
  description = "S3 URI of the transformed Parquet output prefix."
  value       = "s3://${data.aws_s3_bucket.raw_data.id}/${var.transformed_data_prefix}"
}

output "glue_script_s3_uri" {
  description = "S3 URI where the Glue PySpark script is expected to be uploaded out-of-band by CI/CD."
  value       = "s3://${data.aws_s3_bucket.raw_data.id}/${var.scripts_prefix}${var.glue_script_filename}"
}

output "glue_job_log_group_name" {
  description = "CloudWatch Log Group name for the Glue job logs."
  value       = aws_cloudwatch_log_group.glue_job_logs.name
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database for transformed data (if enabled)."
  value       = var.enable_glue_catalog ? aws_glue_catalog_database.transformed_data_db[0].name : null
}

output "glue_crawler_name" {
  description = "Name of the Glue Crawler for transformed data (if enabled)."
  value       = var.enable_glue_catalog ? aws_glue_crawler.transformed_data_crawler[0].name : null
}