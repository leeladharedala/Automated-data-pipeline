output "raw_data_bucket_name" {
  description = "Name of the S3 bucket containing raw JSONL input data"
  value       = data.aws_s3_bucket.raw_data.bucket
}

output "raw_data_bucket_arn" {
  description = "ARN of the raw-data S3 bucket"
  value       = data.aws_s3_bucket.raw_data.arn
}

output "transformed_data_bucket_name" {
  description = "Name of the S3 bucket where Parquet output is written"
  value       = aws_s3_bucket.transformed_data.bucket
}

output "transformed_data_bucket_arn" {
  description = "ARN of the transformed-data S3 bucket"
  value       = aws_s3_bucket.transformed_data.arn
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job"
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job"
  value       = aws_glue_job.energy_etl.arn
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job"
  value       = aws_iam_role.glue_role.arn
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by the Glue job"
  value       = aws_iam_role.glue_role.name
}

output "glue_log_group_name" {
  description = "CloudWatch Log Group name for Glue job continuous logging"
  value       = aws_cloudwatch_log_group.glue_job_logs.name
}