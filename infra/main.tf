data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------
# S3 bucket: raw_data/ (source) and transformed_data/ (sink) prefixes
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  count = var.create_bucket ? 1 : 0

  bucket        = var.bucket_name
  force_destroy = true

  tags = merge(var.tags, {
    Name = var.bucket_name
  })
}

resource "aws_s3_bucket_versioning" "data" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.data[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.data[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  count = var.create_bucket ? 1 : 0

  bucket                  = aws_s3_bucket.data[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Reference to a pre-existing bucket when create_bucket = false.
data "aws_s3_bucket" "data" {
  count  = var.create_bucket ? 0 : 1
  bucket = var.bucket_name
}

locals {
  bucket_id  = var.create_bucket ? aws_s3_bucket.data[0].id : data.aws_s3_bucket.data[0].id
  bucket_arn = var.create_bucket ? aws_s3_bucket.data[0].arn : data.aws_s3_bucket.data[0].arn

  source_s3_path = "s3://${local.bucket_id}/${var.raw_data_prefix}"
  target_s3_path = "s3://${local.bucket_id}/${var.transformed_data_prefix}"
  script_s3_path = "s3://${local.bucket_id}/${var.scripts_prefix}${var.glue_script_filename}"
  temp_s3_path   = "s3://${local.bucket_id}/${var.glue_temp_prefix}"
}

resource "aws_s3_object" "raw_data_prefix" {
  bucket       = local.bucket_id
  key          = var.raw_data_prefix
  content_type = "application/x-directory"
  content      = ""
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket       = local.bucket_id
  key          = var.transformed_data_prefix
  content_type = "application/x-directory"
  content      = ""
}

resource "aws_s3_object" "scripts_prefix" {
  bucket       = local.bucket_id
  key          = var.scripts_prefix
  content_type = "application/x-directory"
  content      = ""
}

# -----------------------------------------------------------------------------
# IAM role and policies for AWS Glue
# -----------------------------------------------------------------------------

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

resource "aws_iam_role" "glue_service_role" {
  name                  = "${var.project_name}-${var.environment}-glue-service-role"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true
  tags                  = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service_managed" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [local.bucket_arn]
  }

  statement {
    sid    = "ReadRawData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = ["${local.bucket_arn}/${var.raw_data_prefix}*"]
  }

  statement {
    sid    = "ReadWriteTransformedData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${local.bucket_arn}/${var.transformed_data_prefix}*"]
  }

  statement {
    sid    = "ScriptAndTempAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${local.bucket_arn}/${var.scripts_prefix}*",
      "${local.bucket_arn}/${var.glue_temp_prefix}*",
    ]
  }
}

resource "aws_iam_policy" "glue_s3_access" {
  name        = "${var.project_name}-${var.environment}-glue-s3-access"
  description = "Allows the Glue job to read raw_data/ and read/write transformed_data/ in the pipeline bucket."
  policy      = data.aws_iam_policy_document.glue_s3_access.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

data "aws_iam_policy_document" "glue_cloudwatch_logs" {
  statement {
    sid    = "GlueCloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey",
    ]
    resources = [
      "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_policy" "glue_cloudwatch_logs" {
  name        = "${var.project_name}-${var.environment}-glue-cloudwatch-logs"
  description = "Allows the Glue job to write logs to CloudWatch Logs."
  policy      = data.aws_iam_policy_document.glue_cloudwatch_logs.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_logs" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_logs.arn
}

# -----------------------------------------------------------------------------
# CloudWatch Log Group for the Glue job
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days
  tags              = var.tags
}

# -----------------------------------------------------------------------------
# Glue Catalog Database + Crawler (schema discovery over transformed_data/)
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  count = var.create_glue_crawler ? 1 : 0

  name        = var.glue_catalog_database_name
  description = "Catalog database for the multi-agent-pipeline data lake tables."
}

resource "aws_glue_crawler" "transformed_data" {
  count = var.create_glue_crawler ? 1 : 0

  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.this[0].name
  description   = "Crawls transformed_data/ Parquet output to discover and update table schema."

  s3_target {
    path = local.target_s3_path
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  schedule = var.glue_crawler_schedule != "" ? "cron(${var.glue_crawler_schedule})" : null

  tags = var.tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_managed,
    aws_s3_object.transformed_data_prefix,
  ]
}

# -----------------------------------------------------------------------------
# Glue ETL Job (PySpark): raw_data/ (JSONL) -> transformed_data/ (Parquet)
# -----------------------------------------------------------------------------

resource "aws_glue_job" "transform" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes
  description       = "Reads JSONL from raw_data/, transforms, and writes Parquet (overwrite) to transformed_data/."

  command {
    name            = "glueetl"
    script_location = local.script_s3_path
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--SOURCE_S3_PATH"                   = local.source_s3_path
    "--TARGET_S3_PATH"                   = local.target_s3_path
    "--TempDir"                          = local.temp_s3_path
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  tags = var.tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_managed,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_iam_role_policy_attachment.glue_cloudwatch_logs,
    aws_s3_object.scripts_prefix,
    aws_cloudwatch_log_group.glue_job,
  ]
}