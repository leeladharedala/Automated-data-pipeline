output "glue_job_name" {
  description = "Name of the AWS Glue ETL job."
  value       = aws_glue_job.transform_job.name
}

output "glue_job_arn" {
  description = "ARN of the AWS Glue ETL job."
  value       = aws_glue_job.transform_job.arn
}

output "glue_job_role_arn" {
  description = "ARN of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_job_role.arn
}

output "glue_job_role_name" {
  description = "Name of the IAM role assumed by the Glue job."
  value       = aws_iam_role.glue_job_role.name
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Catalog database holding the transformed table."
  value       = aws_glue_catalog_database.transformed.name
}

output "glue_catalog_table_name" {
  description = "Name of the Glue Catalog table for the transformed Parquet data."
  value       = aws_glue_catalog_table.transformed_data.name
}

output "raw_data_bucket_name" {
  description = "Name of the S3 bucket used for raw and transformed data."
  value       = local.bucket_name
}

output "source_s3_uri" {
  description = "S3 URI the Glue job reads JSONL data from."
  value       = local.source_s3_uri
}

output "destination_s3_uri" {
  description = "S3 URI the Glue job writes Parquet data to."
  value       = local.dest_s3_uri
}

output "glue_script_s3_uri" {
  description = "Expected S3 URI of the PySpark ETL script (uploaded out-of-band by CI/CD, not managed by Terraform)."
  value       = local.script_s3_uri
}

output "cloudwatch_log_group" {
  description = "CloudWatch Log Group used for Glue job continuous logging."
  value       = aws_cloudwatch_log_group.glue_job.name
}