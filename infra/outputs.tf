output "s3_bucket_name" {
  description = "Name of the S3 bucket used for the ETL pipeline."
  value       = aws_s3_bucket.data_bucket.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket."
  value       = aws_s3_bucket.data_bucket.arn
}

output "s3_raw_data_uri" {
  description = "S3 URI for the raw JSONL input data prefix."
  value       = "s3://${aws_s3_bucket.data_bucket.bucket}/${var.raw_data_prefix}"
}

output "s3_transformed_data_uri" {
  description = "S3 URI for the transformed Parquet output data prefix."
  value       = "s3://${aws_s3_bucket.data_bucket.bucket}/${var.transformed_data_prefix}"
}

output "s3_glue_script_uri" {
  description = "S3 URI of the uploaded PySpark ETL script."
  value       = "s3://${aws_s3_bucket.data_bucket.bucket}/${aws_s3_object.glue_etl_script.key}"
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_service_role.name
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.energy_etl.arn
}

output "glue_job_start_command" {
  description = "AWS CLI command to manually trigger the Glue ETL job."
  value       = "aws glue start-job-run --job-name ${aws_glue_job.energy_etl.name} --region ${var.aws_region}"
}