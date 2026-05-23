output "bucket_name" {
  description = "Name of the S3 bucket used by the pipeline."
  value       = aws_s3_bucket.pipeline.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket used by the pipeline."
  value       = aws_s3_bucket.pipeline.arn
}

output "raw_data_s3_uri" {
  description = "S3 URI of the raw data prefix (JSONL input)."
  value       = "s3://${aws_s3_bucket.pipeline.id}/${var.raw_data_prefix}"
}

output "transformed_data_s3_uri" {
  description = "S3 URI of the transformed data prefix (Parquet output)."
  value       = "s3://${aws_s3_bucket.pipeline.id}/${var.transformed_data_prefix}"
}

output "scripts_s3_uri" {
  description = "S3 URI of the scripts prefix."
  value       = "s3://${aws_s3_bucket.pipeline.id}/${var.scripts_prefix}"
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.energy_etl.arn
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_service_role.name
}