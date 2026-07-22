output "bucket_name" {
  description = "Name of the S3 bucket used for raw_data/ and transformed_data/."
  value       = local.bucket_id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket used for raw_data/ and transformed_data/."
  value       = local.bucket_arn
}

output "raw_data_s3_path" {
  description = "Full S3 URI of the raw_data/ source prefix."
  value       = local.source_s3_path
}

output "transformed_data_s3_path" {
  description = "Full S3 URI of the transformed_data/ sink prefix."
  value       = local.target_s3_path
}

output "glue_script_s3_path" {
  description = "Expected S3 URI of the Glue ETL script (uploaded out-of-band by CI/CD)."
  value       = local.script_s3_path
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.transform.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.transform.name}"
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by the Glue job/crawler."
  value       = aws_iam_role.glue_service_role.name
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job/crawler."
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database used for schema discovery (null if disabled)."
  value       = var.create_glue_crawler ? aws_glue_catalog_database.this[0].name : null
}

output "glue_crawler_name" {
  description = "Name of the Glue Crawler that discovers schema over transformed_data/ (null if disabled)."
  value       = var.create_glue_crawler ? aws_glue_crawler.transformed_data[0].name : null
}

output "glue_job_log_group_name" {
  description = "Name of the CloudWatch Log Group for the Glue job."
  value       = aws_cloudwatch_log_group.glue_job.name
}