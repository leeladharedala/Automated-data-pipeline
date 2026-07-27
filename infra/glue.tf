# ---------------------------------------------------------------------------
# CloudWatch log group for the Glue job
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# AWS Glue Job (PySpark): reads JSONL from raw_data/, writes Parquet
# (overwrite mode) to transformed_data/.
#
# NOTE: The PySpark script itself is produced and uploaded out-of-band by the
# CI/CD pipeline to `local.script_s3_uri`. Terraform does not manage the
# script's content or upload it, only references its expected S3 location.
# ---------------------------------------------------------------------------

resource "aws_glue_job" "transform_job" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = local.script_s3_uri
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.bucket_name}/tmp/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--source_s3_uri"                    = local.raw_data_s3_uri
    "--source_format"                    = "json"
    "--sink_s3_uri"                      = local.transformed_data_s3_uri
    "--sink_format"                      = "parquet"
    "--write_mode"                       = "overwrite"
  }

  tags = merge(local.common_tags, {
    Name = var.glue_job_name
  })

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_cloudwatch_log_group.glue_job,
  ]
}