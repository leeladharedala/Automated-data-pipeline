output "bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  value       = aws_s3_bucket.data.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket used for raw and transformed data."
  value       = aws_s3_bucket.data.arn
}

output "raw_data_s3_uri" {
  description = "Full S3 URI of the raw_data/ prefix (JSONL source)."
  value       = local.raw_data_s3_uri
}

output "transformed_data_s3_uri" {
  description = "Full S3 URI of the transformed_data/ prefix (Parquet sink)."
  value       = local.transformed_data_s3_uri
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
  description = "ARN of the AWS Glue job (constructed; aws_glue_job does not expose an arn attribute)."
  value       = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.transform.name}"
}

output "iam_role_arn" {
  description = "ARN of the IAM role used by the Glue job."
  value       = aws_iam_role.glue_job.arn
}

output "iam_role_name" {
  description = "Name of the IAM role used by the Glue job."
  value       = aws_iam_role.glue_job.name
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = aws_glue_catalog_database.this.name
}

output "glue_catalog_table_name" {
  description = "Name of the Glue Data Catalog table for the raw JSONL data."
  value       = aws_glue_catalog_table.raw_data.name
}

output "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group for the Glue job."
  value       = aws_cloudwatch_log_group.glue_job.name
}