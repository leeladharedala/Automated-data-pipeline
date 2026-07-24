output "bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  value       = aws_s3_bucket.data_bucket.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket used for raw and transformed data."
  value       = aws_s3_bucket.data_bucket.arn
}

output "raw_data_s3_uri" {
  description = "S3 URI of the raw_data/ source prefix."
  value       = "s3://${aws_s3_bucket.data_bucket.id}/${var.raw_data_prefix}"
}

output "transformed_data_s3_uri" {
  description = "S3 URI of the transformed_data/ sink prefix (Parquet, overwrite mode)."
  value       = "s3://${aws_s3_bucket.data_bucket.id}/${var.transformed_data_prefix}"
}

output "glue_script_s3_uri" {
  description = "Expected S3 URI of the PySpark transform script (uploaded out-of-band by CI/CD)."
  value       = "s3://${aws_s3_bucket.data_bucket.id}/${var.glue_script_object_key}"
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.transform.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.transform.arn
}

output "glue_role_arn" {
  description = "ARN of the IAM role used by the Glue job and crawler."
  value       = aws_iam_role.glue_role.arn
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database holding crawled table metadata."
  value       = aws_glue_catalog_database.this.name
}

output "glue_crawler_name" {
  description = "Name of the Glue crawler that catalogs the transformed_data/ Parquet output. NOTE: must be started manually or via automation; Terraform only provisions it."
  value       = aws_glue_crawler.transformed_data.name
}

output "glue_job_log_group" {
  description = "CloudWatch Log Group name for Glue job logs."
  value       = aws_cloudwatch_log_group.glue_job_logs.name
}