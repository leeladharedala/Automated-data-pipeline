output "raw_bucket_name" {
  description = "Name of the existing S3 bucket used as the raw data source."
  value       = data.aws_s3_bucket.raw_data.id
}

output "raw_bucket_arn" {
  description = "ARN of the existing S3 bucket used as the raw data source."
  value       = data.aws_s3_bucket.raw_data.arn
}

output "raw_data_s3_uri" {
  description = "Full S3 URI of the raw JSONL data prefix consumed by the Glue job."
  value       = local.source_s3_uri
}

output "sink_bucket_name" {
  description = "Name of the S3 bucket used as the transformed data sink."
  value       = local.sink_bucket_name
}

output "transformed_data_s3_uri" {
  description = "Full S3 URI of the transformed_data/ prefix written by the Glue job (Parquet, overwrite mode)."
  value       = local.sink_s3_uri
}

output "glue_script_s3_uri" {
  description = "S3 URI where the CI/CD pipeline must upload the packaged PySpark script (from src/transformations/transform.py) prior to running the Glue job."
  value       = local.script_s3_uri
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.etl_job.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.etl_job.arn
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role used by the Glue ETL job."
  value       = aws_iam_role.glue_job_role.arn
}

output "glue_job_role_name" {
  description = "Name of the IAM role used by the Glue ETL job."
  value       = aws_iam_role.glue_job_role.name
}

output "glue_job_log_group_name" {
  description = "Name of the CloudWatch Log Group associated with the Glue job."
  value       = aws_cloudwatch_log_group.glue_job.name
}