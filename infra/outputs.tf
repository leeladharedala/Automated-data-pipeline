# ============================================================
# outputs.tf — Exported values
# ============================================================

output "data_bucket_name" {
  description = "S3 data bucket name."
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "S3 data bucket ARN."
  value       = aws_s3_bucket.data.arn
}

output "scripts_bucket_name" {
  description = "S3 scripts bucket name."
  value       = aws_s3_bucket.scripts.bucket
}

output "glue_scripts_bucket" {
  description = "Alias for scripts bucket name (used by CI/CD)."
  value       = aws_s3_bucket.scripts.bucket
}

output "raw_data_s3_uri" {
  description = "S3 URI for raw JSONL input."
  value       = local.source_s3_uri
}

output "transformed_data_s3_uri" {
  description = "S3 URI for Parquet output."
  value       = local.sink_s3_uri
}

output "glue_script_s3_uri" {
  description = "S3 URI of the PySpark script."
  value       = local.script_s3_uri
}

output "glue_iam_role_arn" {
  description = "ARN of the Glue IAM role."
  value       = aws_iam_role.glue.arn
}

output "glue_database_name" {
  description = "Glue Data Catalog database name."
  value       = aws_glue_catalog_database.energy.name
}

output "glue_job_name" {
  description = "Glue ETL job name."
  value       = aws_glue_job.energy_etl.name
}

output "glue_log_group_name" {
  description = "CloudWatch log group for Glue."
  value       = aws_cloudwatch_log_group.glue.name
}

output "aws_account_id" {
  description = "AWS account ID."
  value       = data.aws_caller_identity.current.account_id
}