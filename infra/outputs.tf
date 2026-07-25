output "raw_bucket_name" {
  description = "Name of the existing raw data S3 bucket (source)."
  value       = data.aws_s3_bucket.raw_data.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the existing raw data S3 bucket (source)."
  value       = data.aws_s3_bucket.raw_data.arn
}

output "sink_bucket_name" {
  description = "Name of the S3 bucket used for transformed (Parquet) data, scripts, and Glue temp storage."
  value       = aws_s3_bucket.sink.bucket
}

output "sink_bucket_arn" {
  description = "ARN of the S3 bucket used for transformed (Parquet) data."
  value       = aws_s3_bucket.sink.arn
}

output "transformed_data_s3_uri" {
  description = "Full S3 URI where transformed Parquet data is written."
  value       = "s3://${aws_s3_bucket.sink.id}/${var.transformed_data_prefix}"
}

output "glue_script_expected_s3_uri" {
  description = "Expected S3 URI for the PySpark ETL script (uploaded out-of-band by CI/CD)."
  value       = local.script_location
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.transform.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.transform.arn
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role used by the Glue job."
  value       = aws_iam_role.glue_job_role.arn
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database used for schema discovery (if enabled)."
  value       = var.enable_glue_crawler ? aws_glue_catalog_database.this[0].name : null
}

output "glue_crawler_name" {
  description = "Name of the Glue Crawler over the transformed data (if enabled)."
  value       = var.enable_glue_crawler ? aws_glue_crawler.transformed_data[0].name : null
}

output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch Log Group for Glue job logs."
  value       = aws_cloudwatch_log_group.glue_job.name
}