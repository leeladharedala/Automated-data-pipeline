locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  sink_bucket_name = var.use_dedicated_sink_bucket ? var.sink_bucket_name : var.raw_bucket_name

  script_s3_uri = "s3://${var.glue_script_bucket}/${var.glue_script_key}"
  source_s3_uri = "s3://${var.raw_bucket_name}/${var.raw_data_prefix}"
  sink_s3_uri   = "s3://${local.sink_bucket_name}/${var.transformed_data_prefix}"
}

data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket" "processed_data" {
  count = var.use_dedicated_sink_bucket ? 1 : 0

  bucket        = var.sink_bucket_name
  force_destroy = true

  tags = merge(local.common_tags, {
    Name = var.sink_bucket_name
  })
}

resource "aws_s3_bucket_versioning" "processed_data" {
  count = var.use_dedicated_sink_bucket ? 1 : 0

  bucket = aws_s3_bucket.processed_data[0].id

  versioning_configuration {
    status = "Suspended"
  }
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  count = var.use_dedicated_sink_bucket ? 1 : 0

  bucket = aws_s3_bucket.processed_data[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-glue-job-logs"
  })
}

resource "aws_iam_role" "glue_job_role" {
  name                  = "${local.name_prefix}-glue-etl-role"
  force_detach_policies = true

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-glue-etl-role"
  })
}

data "aws_iam_policy_document" "glue_job_policy" {
  statement {
    sid    = "ReadRawDataPrefix"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "arn:aws:s3:::${var.raw_bucket_name}/${var.raw_data_prefix}*",
    ]
  }

  statement {
    sid    = "ListBucketsForPrefixes"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = distinct([
      "arn:aws:s3:::${var.raw_bucket_name}",
      "arn:aws:s3:::${local.sink_bucket_name}",
    ])

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.raw_data_prefix}*",
        "${var.transformed_data_prefix}*",
      ]
    }
  }

  statement {
    sid    = "WriteTransformedDataPrefix"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "arn:aws:s3:::${local.sink_bucket_name}/${var.transformed_data_prefix}*",
    ]
  }

  statement {
    sid    = "ReadGlueScript"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "arn:aws:s3:::${var.glue_script_bucket}/${var.glue_script_key}",
    ]
  }

  statement {
    sid    = "GlueCloudWatchLogs"
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

  statement {
    sid    = "GlueJobControl"
    effect = "Allow"
    actions = [
      "glue:GetJob",
      "glue:GetJobs",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:StartJobRun",
      "glue:BatchStopJobRun",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/${var.glue_job_name}",
    ]
  }
}

resource "aws_iam_role_policy" "glue_job_policy" {
  name   = "${local.name_prefix}-glue-etl-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_job_role.arn

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
    "--job-language"                    = "python"
    "--source_bucket"                   = var.raw_bucket_name
    "--source_prefix"                   = var.raw_data_prefix
    "--target_bucket"                   = local.sink_bucket_name
    "--target_prefix"                   = var.transformed_data_prefix
    "--SOURCE_PATH"                     = local.source_s3_uri
    "--DEST_PATH"                       = local.sink_s3_uri
    "--TempDir"                         = "s3://${local.sink_bucket_name}/tmp/${var.glue_job_name}/"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"             = "true"
    "--job-bookmark-option"             = "job-bookmark-disable"
  }

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  tags = merge(local.common_tags, {
    Name = var.glue_job_name
  })

  depends_on = [
    aws_iam_role_policy.glue_job_policy,
    aws_cloudwatch_log_group.glue_job,
  ]
}