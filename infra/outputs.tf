output "bucket_name" {
  description = "S3 bucket name."
  value       = aws_s3_bucket.pipeline.bucket
}

output "bucket_arn" {
  description = "S3 bucket ARN."
  value       = aws_s3_bucket.pipeline.arn
}

output "raw_data_s3_uri" {
  description = "S3 URI for raw JSONL input."
  value       = "s3://${aws_s3_bucket.pipeline.bucket}/${var.raw_data_prefix}"
}

output "transformed_data_s3_uri" {
  description = "S3 URI for transformed Parquet output."
  value       = "s3://${aws_s3_bucket.pipeline.bucket}/${var.transformed_data_prefix}"
}

output "scripts_s3_uri" {
  description = "S3 URI for Glue scripts."
  value       = "s3://${aws_s3_bucket.pipeline.bucket}/${var.scripts_prefix}"
}

output "glue_job_name" {
  description = "Glue job name."
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "Glue job ARN."
  value       = aws_glue_job.energy_etl.arn
}

output "glue_iam_role_arn" {
  description = "IAM role ARN for Glue."
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_iam_role_name" {
  description = "IAM role name for Glue."
  value       = aws_iam_role.glue_service_role.name
}

output "glue_s3_policy_arn" {
  description = "S3 access policy ARN."
  value       = aws_iam_policy.glue_s3_access.arn
}

output "glue_log_group_name" {
  description = "CloudWatch Log Group for Glue."
  value       = aws_cloudwatch_log_group.glue_job.name
}