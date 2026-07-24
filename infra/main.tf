data "aws_caller_identity" "current" {}

locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags
  )

  bucket_arn              = data.aws_s3_bucket.raw_data.arn
  raw_data_s3_uri         = "s3://${var.raw_data_bucket_name}/${var.raw_data_prefix}"
  transformed_data_s3_uri = "s3://${var.raw_data_bucket_name}/${var.transformed_data_prefix}"
  temp_dir_s3_uri         = "s3://${var.raw_data_bucket_name}/${var.temp_prefix}"
  spark_logs_s3_uri       = "s3://${var.raw_data_bucket_name}/${var.spark_logs_prefix}"
  script_s3_uri           = "s3://${var.raw_data_bucket_name}/${var.glue_script_object_key}"
  glue_log_group_name     = "/aws-glue/jobs/${var.glue_job_name}"
}

data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_data_bucket_name
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = local.glue_log_group_name
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

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

resource "aws_iam_role" "glue_role" {
  name                  = "${var.project_name}-${var.environment}-glue-role"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_custom" {
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [local.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.raw_data_prefix}*",
        "${var.transformed_data_prefix}*",
        "${var.scripts_prefix}*",
        "${var.temp_prefix}*",
        "${var.spark_logs_prefix}*",
      ]
    }
  }

  statement {
    sid    = "ReadRawData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${local.bucket_arn}/${var.raw_data_prefix}*",
    ]
  }

  statement {
    sid    = "ReadWriteTransformedData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${local.bucket_arn}/${var.transformed_data_prefix}*",
    ]
  }

  statement {
    sid    = "ScriptAndTempAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = [
      "${local.bucket_arn}/${var.scripts_prefix}*",
      "${local.bucket_arn}/${var.temp_prefix}*",
      "${local.bucket_arn}/${var.spark_logs_prefix}*",
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

resource "aws_iam_policy" "glue_custom" {
  name        = "${var.project_name}-${var.environment}-glue-s3-policy"
  description = "Scoped S3 and CloudWatch Logs permissions for the Glue ETL job and crawler."
  policy      = data.aws_iam_policy_document.glue_custom.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_custom" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_custom.arn
}

resource "aws_glue_catalog_database" "this" {
  name        = var.glue_catalog_database_name
  description = "Catalog database for ${var.project_name} (${var.environment}) transformed energy data."

  location_uri = local.transformed_data_s3_uri
}

resource "aws_glue_job" "etl" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = var.glue_version
  number_of_workers = var.glue_number_of_workers
  worker_type       = var.glue_worker_type
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = local.script_s3_uri
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = local.temp_dir_s3_uri
    "--spark-event-logs-path"           = local.spark_logs_s3_uri
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"    = "true"
    "--enable-glue-datacatalog"         = "true"
    "--source_path"                     = local.raw_data_s3_uri
    "--dest_path"                       = local.transformed_data_s3_uri
    "--catalog_database"                = aws_glue_catalog_database.this.name
  }

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_custom,
    aws_cloudwatch_log_group.glue_job,
  ]
}

resource "aws_glue_crawler" "transformed_data" {
  count = var.enable_glue_crawler ? 1 : 0

  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.this.name
  schedule      = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null
  table_prefix  = "transformed_"

  s3_target {
    path = local.transformed_data_s3_uri
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_custom,
    aws_glue_job.etl,
  ]
}