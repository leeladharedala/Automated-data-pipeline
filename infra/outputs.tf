output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.etl_job.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.etl_job.arn
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role used by the Glue ETL job."
  value       = aws_iam_role.glue_job_role.arn
}

output "glue_job_role_name" {
  description = "Name of the IAM role used by the Glue ETL job."
  value       = aws_iam_role.glue_job_role.name
}

output "script_s3_location" {
  description = "S3 URI where the Glue PySpark ETL script is expected to be uploaded (out-of-band)."
  value       = local.scripts_s3_uri
}

output "raw_data_s3_uri" {
  description = "S3 URI of the raw JSONL source data prefix."
  value       = local.raw_s3_uri
}

output "transformed_data_s3_uri" {
  description = "S3 URI of the transformed Parquet output data prefix."
  value       = local.sink_s3_uri
}

output "glue_job_temp_dir" {
  description = "S3 URI used as the Glue job TempDir."
  value       = local.temp_s3_uri
}

output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch Log Group for the Glue job."
  value       = aws_cloudwatch_log_group.glue_job.name
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database, if created."
  value       = var.create_glue_catalog ? aws_glue_catalog_database.this[0].name : null
}

output "glue_raw_crawler_name" {
  description = "Name of the raw data Glue crawler, if created."
  value       = var.create_glue_catalog && var.enable_crawlers ? aws_glue_crawler.raw_data[0].name : null
}

output "glue_transformed_crawler_name" {
  description = "Name of the transformed data Glue crawler, if created."
  value       = var.create_glue_catalog && var.enable_crawlers ? aws_glue_crawler.transformed_data[0].name : null
}