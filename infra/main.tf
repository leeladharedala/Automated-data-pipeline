# ---------------------------------------------------------------------------
# Data source: reference the pre-existing raw-data bucket without managing it.
# If the bucket does not yet exist, comment out this data source and uncomment
# the aws_s3_bucket.raw_data resource below.
# ---------------------------------------------------------------------------
data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_data_bucket_name
}

# ---------------------------------------------------------------------------
# Transformed-data bucket (Parquet output)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "transformed_data" {
  bucket        = var.transformed_data_bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "transformed_data" {
  bucket = aws_s3_bucket.transformed_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "transformed_data" {
  bucket = aws_s3_bucket.transformed_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "transformed_data" {
  bucket = aws_s3_bucket.transformed_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# IAM role for AWS Glue
# ---------------------------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name                  = "AWSGlueServiceRole-energy-etl"
  description           = "IAM role assumed by the energy ETL Glue job"
  force_detach_policies = true

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_policy" {
  name        = "energy-etl-glue-s3-policy"
  description = "Grants the Glue ETL job read access to raw data and write access to transformed data"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RawDataBucketList"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.raw_data_bucket_name}"
        ]
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.raw_data_prefix}*",
              "scripts/*",
              "${var.glue_temp_dir_prefix}*"
            ]
          }
        }
      },
      {
        Sid    = "RawDataObjectRead"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "arn:aws:s3:::${var.raw_data_bucket_name}/${var.raw_data_prefix}*",
          "arn:aws:s3:::${var.raw_data_bucket_name}/scripts/*"
        ]
      },
      {
        Sid    = "GlueTempDirAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.raw_data_bucket_name}/${var.glue_temp_dir_prefix}*"
        ]
      },
      {
        Sid    = "TransformedDataBucketList"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.transformed_data_bucket_name}"
        ]
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.transformed_data_prefix}*"]
          }
        }
      },
      {
        Sid    = "TransformedDataObjectWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.transformed_data_bucket_name}/${var.transformed_data_prefix}*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Glue continuous logging
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 30
}

# ---------------------------------------------------------------------------
# AWS Glue Job
# ---------------------------------------------------------------------------
resource "aws_glue_job" "energy_etl" {
  name        = var.glue_job_name
  description = "PySpark ETL job: reads JSONL from S3, transforms, writes Parquet to S3"
  role_arn    = aws_iam_role.glue_role.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = var.glue_script_location
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = "s3://${var.raw_data_bucket_name}/${var.glue_temp_dir_prefix}"
    "--SOURCE_BUCKET"                    = var.raw_data_bucket_name
    "--SOURCE_PREFIX"                    = var.raw_data_prefix
    "--SINK_BUCKET"                      = var.transformed_data_bucket_name
    "--SINK_PREFIX"                      = var.transformed_data_prefix
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job_logs.name
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.raw_data_bucket_name}/${var.glue_temp_dir_prefix}spark-logs/"
    "--job-language"                     = "python"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_policy,
    aws_cloudwatch_log_group.glue_job_logs,
  ]
}