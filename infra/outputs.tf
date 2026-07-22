output "raw_bucket_name" {
  description = "Name of the existing S3 bucket used for raw data source and transformed data sink."
  value       = data.aws_s3_bucket.raw_data.id
}

output "raw_bucket_arn" {
  description = "ARN of the existing S3 bucket used for raw data source and transformed data sink."
  value       = data.aws_s3_bucket.raw_data.arn
}

output "raw_data_s3_uri" {
  description = "Full S3 URI of the raw_data/ source prefix."
  value       = local.raw_s3_uri
}

output "transformed_data_s3_uri" {
  description = "Full S3 URI of the transformed_data/ sink prefix."
  value       = local.sink_s3_uri
}

output "glue_script_s3_uri" {
  description = "Expected S3 URI of the PySpark ETL script (uploaded out-of-band by CI/CD)."
  value       = local.script_s3_uri
}

output "glue_job_name" {
  description = "Name of the AWS Glue job."
  value       = aws_glue_job.transform.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue job."
  value       = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.transform.name}"
}

output "glue_execution_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_execution_role.arn
}

output "glue_execution_role_name" {
  description = "Name of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_execution_role.name
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database for transformed data."
  value       = aws_glue_catalog_database.transformed.name
}

output "glue_catalog_table_name" {
  description = "Name of the Glue Data Catalog table for transformed data."
  value       = aws_glue_catalog_table.transformed.name
}

output "glue_job_log_group_name" {
  description = "CloudWatch Log Group name for Glue job logs."
  value       = aws_cloudwatch_log_group.glue_job.name
}