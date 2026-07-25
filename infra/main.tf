locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags
  )

  sink_bucket_name = "${var.project_name}-${var.environment}-transformed-data"
  glue_role_name   = "${var.project_name}-${var.environment}-glue-role"

  script_location = "s3://${aws_s3_bucket.sink.id}/${var.scripts_prefix}${var.glue_script_filename}"
  temp_dir        = "s3://${aws_s3_bucket.sink.id}/tmp/"
  source_path     = "s3://${var.raw_bucket_name}/${var.raw_data_prefix}"
  dest_path       = "s3://${aws_s3_bucket.sink.id}/${var.transformed_data_prefix}"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ---------------------------------------------------------------------------
# Raw data source bucket (pre-existing, referenced only)
# ---------------------------------------------------------------------------

data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_bucket_name
}

# ---------------------------------------------------------------------------
# Sink bucket for transformed (Parquet) data, scripts, and Glue temp dir
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "sink" {
  bucket        = local.sink_bucket_name
  force_destroy = true

  tags = merge(local.common_tags, {
    Name = local.sink_bucket_name
  })
}

resource "aws_s3_bucket_versioning" "sink" {
  bucket = aws_s3_bucket.sink.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "sink" {
  bucket = aws_s3_bucket.sink.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "sink" {
  bucket = aws_s3_bucket.sink.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "sink" {
  bucket = aws_s3_bucket.sink.id

  rule {
    id     = "expire-temp-objects"
    status = "Enabled"

    filter {
      prefix = "tmp/"
    }

    expiration {
      days = 7
    }
  }
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Glue job logs
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# IAM role & policies for the Glue job
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job_role" {
  name                  = local.glue_role_name
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job_policy" {
  statement {
    sid    = "ReadRawDataBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      data.aws_s3_bucket.raw_data.arn,
      "${data.aws_s3_bucket.raw_data.arn}/${var.raw_data_prefix}*",
    ]
  }

  statement {
    sid    = "ReadWriteSinkBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.sink.arn,
      "${aws_s3_bucket.sink.arn}/${var.transformed_data_prefix}*",
      "${aws_s3_bucket.sink.arn}/${var.scripts_prefix}*",
      "${aws_s3_bucket.sink.arn}/tmp/*",
    ]
  }

  statement {
    sid    = "CloudWatchLogging"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_job_inline_policy" {
  name   = "${var.project_name}-${var.environment}-glue-job-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

# ---------------------------------------------------------------------------
# AWS Glue Job (PySpark, Glue 4.0)
# ---------------------------------------------------------------------------
# NOTE: The PySpark script is produced and uploaded out-of-band (by CI/CD /
# a separate agent) to the sink bucket at var.scripts_prefix. Terraform does
# not manage the script object itself, only references its expected S3 URI.

resource "aws_glue_job" "transform" {
  name         = var.glue_job_name
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = var.glue_version

  command {
    name            = "glueetl"
    script_location = local.script_location
    python_version  = "3"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = local.temp_dir
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--source_path"                      = local.source_path
    "--dest_path"                        = local.dest_path
  }

  tags = merge(local.common_tags, {
    Name = var.glue_job_name
  })

  depends_on = [
    aws_iam_role_policy.glue_job_inline_policy,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_cloudwatch_log_group.glue_job,
  ]
}

# ---------------------------------------------------------------------------
# Glue Catalog Database & Crawler (optional schema discovery over sink data)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  count = var.enable_glue_crawler ? 1 : 0

  name = replace("${var.project_name}_${var.environment}_db", "-", "_")
}

resource "aws_glue_crawler" "transformed_data" {
  count = var.enable_glue_crawler ? 1 : 0

  name          = "${var.project_name}-${var.environment}-transformed-data-crawler"
  database_name = aws_glue_catalog_database.this[0].name
  role          = aws_iam_role.glue_job_role.arn

  s3_target {
    path = local.dest_path
  }

  schema_change_policy {
    delete_behavior  = "LOG"
    update_behavior  = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.glue_job_inline_policy,
    aws_iam_role_policy_attachment.glue_service_role,
  ]
}