# ---------------------------------------------------------------------------
# S3 Bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data_bucket" {
  bucket        = var.s3_bucket_name
  force_destroy = true

  tags = {
    Name = var.s3_bucket_name
  }
}

resource "aws_s3_bucket_public_access_block" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_object" "raw_data_prefix" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "${var.raw_data_prefix}.keep"
  content = ""
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "${var.transformed_data_prefix}.keep"
  content = ""
}

resource "aws_s3_object" "glue_scripts_prefix" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "${var.glue_scripts_prefix}.keep"
  content = ""
}

resource "aws_s3_object" "glue_temp_prefix" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "${var.glue_temp_prefix}.keep"
  content = ""
}

resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.data_bucket.id
  key    = "${var.glue_scripts_prefix}energy_etl.py"
  source = "${path.module}/../src/transformations/transform.py"
  etag   = filemd5("${path.module}/../src/transformations/transform.py")
}

# ---------------------------------------------------------------------------
# AWS Glue Job
# ---------------------------------------------------------------------------

resource "aws_glue_job" "energy_etl" {
  name        = var.glue_job_name
  description = "PySpark ETL job: reads JSONL from raw_data/, transforms, writes Parquet to transformed_data/."
  role_arn    = aws_iam_role.glue_service_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/${var.glue_scripts_prefix}energy_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--SOURCE_PATH"                      = "s3://${aws_s3_bucket.data_bucket.bucket}/${var.raw_data_prefix}"
    "--SINK_PATH"                        = "s3://${aws_s3_bucket.data_bucket.bucket}/${var.transformed_data_prefix}"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_bucket.bucket}/${var.glue_temp_prefix}"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--job-language"                     = "python"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_policy,
    aws_iam_role_policy.glue_s3_policy,
    aws_s3_object.glue_etl_script,
  ]

  tags = {
    Name = var.glue_job_name
  }
}