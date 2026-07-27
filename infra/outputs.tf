output "s3_bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  value       = local.bucket_id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket used for raw and transformed data."
  value       = local.bucket_arn
}

output "raw_data_s3_uri" {
  description = "Full S3 URI for the raw JSONL data prefix."
  value       = local.raw_data_s3_uri
}

output "transformed_data_s3_uri" {
  description = "Full S3 URI for the transformed Parquet data prefix."
  value       = local.transformed_data_s3_uri
}

output "glue_script_s3_uri" {
  description = "Expected S3 URI of the PySpark transform script (uploaded out-of-band by CI/CD)."
  value       = local.script_s3_uri
}

output "glue_job_name" {
  description = "Name of the AWS Glue job."
  value       = aws_glue_job.transform_job.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue job."
  value       = "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.transform_job.name}"
}

output "glue_role_arn" {
  description = "ARN of the IAM role used by the Glue job."
  value       = aws_iam_role.glue_job_role.arn
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = aws_glue_catalog_database.this.name
}

output "glue_catalog_raw_table_name" {
  description = "Name of the Glue Data Catalog table registered for raw JSONL data."
  value       = aws_glue_catalog_table.raw_data.name
}

output "glue_catalog_transformed_table_name" {
  description = "Name of the Glue Data Catalog table registered for transformed Parquet data."
  value       = aws_glue_catalog_table.transformed_data.name
}

output "glue_job_log_group" {
  description = "CloudWatch log group name for the Glue job."
  value       = aws_cloudwatch_log_group.glue_job.name
}