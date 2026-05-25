output "data_bucket_name" {
  description = "Name of the S3 bucket holding raw and transformed data"
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "ARN of the S3 data bucket"
  value       = aws_s3_bucket.data.arn
}

output "raw_data_s3_uri" {
  description = "S3 URI for the raw_data/ prefix (JSONL input)"
  value       = "s3://${aws_s3_bucket.data.bucket}/raw_data/"
}

output "transformed_data_s3_uri" {
  description = "S3 URI for the transformed_data/ prefix (Parquet output)"
  value       = "s3://${aws_s3_bucket.data.bucket}/transformed_data/"
}

output "glue_scripts_bucket" {
  description = "Name of the S3 bucket holding Glue scripts and temporary files"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "glue_scripts_bucket_arn" {
  description = "ARN of the Glue scripts S3 bucket"
  value       = aws_s3_bucket.glue_scripts.arn
}

output "etl_script_s3_uri" {
  description = "S3 URI of the uploaded PySpark ETL script"
  value       = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.glue_etl_script.key}"
}

output "glue_iam_role_name" {
  description = "Name of the IAM role assumed by Glue jobs and crawlers"
  value       = aws_iam_role.glue.name
}

output "glue_iam_role_arn" {
  description = "ARN of the IAM role assumed by Glue jobs and crawlers"
  value       = aws_iam_role.glue.arn
}

output "glue_job_log_group" {
  description = "CloudWatch Log Group name for Glue continuous logging"
  value       = aws_cloudwatch_log_group.glue_job.name
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.energy.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler for the raw_data/ prefix"
  value       = aws_glue_crawler.raw_data.name
}

output "glue_job_name" {
  description = "Name of the Glue ETL job"
  value       = aws_glue_job.energy_etl.name
}

output "glue_job_arn" {
  description = "ARN of the Glue ETL job"
  value       = aws_glue_job.energy_etl.arn
}