data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# S3 - reference existing raw data bucket (do NOT manage its lifecycle here)
# ---------------------------------------------------------------------------
data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_bucket_name
}

# Sink and scripts buckets default to the same bucket as raw data (per task),
# but are modeled as data sources so this stays valid if a different bucket
# name is supplied via variables.
data "aws_s3_bucket" "sink_data" {
  bucket = var.sink_bucket_name
}

data "aws_s3_bucket" "scripts" {
  bucket = var.scripts_bucket_name
}

locals {
  raw_bucket_arn     = data.aws_s3_bucket.raw_data.arn
  sink_bucket_arn    = data.aws_s3_bucket.sink_data.arn
  scripts_bucket_arn = data.aws_s3_bucket.scripts.arn

  raw_s3_uri     = "s3://${data.aws_s3_bucket.raw_data.id}/${var.raw_prefix}"
  sink_s3_uri    = "s3://${data.aws_s3_bucket.sink_data.id}/${var.sink_prefix}"
  scripts_s3_uri = "s3://${data.aws_s3_bucket.scripts.id}/${var.scripts_prefix}${var.glue_script_filename}"
  temp_s3_uri    = "s3://${data.aws_s3_bucket.scripts.id}/${var.scripts_prefix}tmp/"
  sparklogs_uri  = "s3://${data.aws_s3_bucket.scripts.id}/${var.scripts_prefix}spark-logs/"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# IAM - Glue job execution role
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
  name                  = "${var.project_name}-${var.environment}-glue-job-role"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid     = "ListRawBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      local.raw_bucket_arn,
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.raw_prefix}*"]
    }
  }

  statement {
    sid     = "ReadRawData"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${local.raw_bucket_arn}/${var.raw_prefix}*",
    ]
  }

  statement {
    sid     = "ListSinkBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      local.sink_bucket_arn,
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.sink_prefix}*"]
    }
  }

  statement {
    sid    = "WriteSinkData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${local.sink_bucket_arn}/${var.sink_prefix}*",
    ]
  }

  statement {
    sid    = "ScriptAndTempAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      local.scripts_bucket_arn,
      "${local.scripts_bucket_arn}/${var.scripts_prefix}*",
    ]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "${var.project_name}-${var.environment}-glue-s3-access"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Glue job logs (explicit retention for clean teardown)
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Glue ETL Job (PySpark)
# Script is uploaded out-of-band by CI/CD to the scripts_prefix location.
# ---------------------------------------------------------------------------
resource "aws_glue_job" "etl_job" {
  name         = var.glue_job_name
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = var.glue_version

  command {
    name            = "glueetl"
    script_location = local.scripts_s3_uri
    python_version  = "3"
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  max_retries = var.glue_max_retries
  timeout     = var.glue_timeout_minutes

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = local.temp_s3_uri
    "--spark-event-logs-path"            = local.sparklogs_uri
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--source_path"                      = local.raw_s3_uri
    "--dest_path"                        = local.sink_s3_uri
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_cloudwatch_log_group.glue_job,
  ]
}

# ---------------------------------------------------------------------------
# Optional: Glue Catalog database + crawlers
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "this" {
  count = var.create_glue_catalog ? 1 : 0
  name  = var.glue_catalog_database_name
}

resource "aws_glue_crawler" "raw_data" {
  count         = var.create_glue_catalog && var.enable_crawlers ? 1 : 0
  name          = "${var.project_name}-${var.environment}-raw-data-crawler"
  role          = aws_iam_role.glue_job_role.arn
  database_name = aws_glue_catalog_database.this[0].name
  table_prefix  = "raw_"
  schedule      = var.crawler_schedule != "" ? var.crawler_schedule : null

  s3_target {
    path = local.raw_s3_uri
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
  ]
}

resource "aws_glue_crawler" "transformed_data" {
  count         = var.create_glue_catalog && var.enable_crawlers ? 1 : 0
  name          = "${var.project_name}-${var.environment}-transformed-data-crawler"
  role          = aws_iam_role.glue_job_role.arn
  database_name = aws_glue_catalog_database.this[0].name
  table_prefix  = "transformed_"
  schedule      = var.crawler_schedule != "" ? var.crawler_schedule : null

  s3_target {
    path = local.sink_s3_uri
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_glue_job.etl_job,
  ]
}