output "raw_data_bucket_name" {
  description = "Name of the existing S3 bucket used as the raw data source and transformed data sink."
  value       = data.aws_s3_bucket.raw_data.id
}

output "raw_data_s3_uri" {
  description = "S3 URI of the raw_data/ prefix (JSONL source)."
  value       = local.raw_data_s3_uri
}

output "transformed_data_s3_uri" {
  description = "S3 URI of the transformed_data/ prefix (Parquet sink)."
  value       = local.transformed_data_s3_uri
}

output "glue_script_s3_uri" {
  description = "Expected S3 URI of the PySpark ETL script (uploaded out-of-band by CI/CD)."
  value       = local.script_s3_uri
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.etl.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.etl.arn
}

output "glue_role_arn" {
  description = "ARN of the IAM role used by the Glue job and crawler."
  value       = aws_iam_role.glue_role.arn
}

output "glue_role_name" {
  description = "Name of the IAM role used by the Glue job and crawler."
  value       = aws_iam_role.glue_role.name
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = aws_glue_catalog_database.this.name
}

output "glue_crawler_name" {
  description = "Name of the Glue Crawler over the transformed_data/ prefix (if enabled)."
  value       = var.enable_glue_crawler ? aws_glue_crawler.transformed_data[0].name : null
}

output "glue_job_log_group_name" {
  description = "CloudWatch Log Group name for Glue job logs."
  value       = aws_cloudwatch_log_group.glue_job.name
}