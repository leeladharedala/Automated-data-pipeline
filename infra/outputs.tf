output "s3_bucket_id" {
  description = "Name (ID) of the S3 data bucket"
  value       = aws_s3_bucket.data.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data bucket"
  value       = aws_s3_bucket.data.arn
}

output "s3_raw_data_uri" {
  description = "S3 URI for the raw JSONL input prefix"
  value       = "s3://${aws_s3_bucket.data.id}/${var.raw_data_prefix}"
}

output "s3_transformed_data_uri" {
  description = "S3 URI for the transformed Parquet output prefix"
  value       = "s3://${aws_s3_bucket.data.id}/${var.transformed_data_prefix}"
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job and crawler"
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.energy_etl.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler"
  value       = aws_glue_crawler.raw_data.name
}

output "glue_job_name" {
  description = "Name of the Glue ETL job"
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_log_group_name" {
  description = "CloudWatch Log Group name for Glue job continuous logging"
  value       = aws_cloudwatch_log_group.glue_job.name
}