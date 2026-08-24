############################################
# Existing S3 bucket (source + sink prefixes)
############################################

data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_data_bucket_name
}

############################################
# IAM Role for Glue Job (least privilege)
############################################

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

  tags = {
    Name = "${var.project_name}-${var.environment}-glue-job-role"
  }
}

data "aws_iam_policy_document" "glue_job_policy" {
  statement {
    sid     = "ListBucketScoped"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      data.aws_s3_bucket.raw_data.arn,
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.raw_data_prefix}*",
        "${var.transformed_data_prefix}*",
        "${var.scripts_prefix}*",
        "${var.temp_dir_prefix}*",
        "${var.spark_event_logs_prefix}*",
      ]
    }
  }

  statement {
    sid     = "ReadRawData"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.raw_data_prefix}*",
    ]
  }

  statement {
    sid     = "ReadScripts"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.scripts_prefix}*",
    ]
  }

  statement {
    sid    = "WriteTransformedData"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObject",
    ]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.transformed_data_prefix}*",
    ]
  }

  statement {
    sid    = "TempAndEventLogs"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.temp_dir_prefix}*",
      "${data.aws_s3_bucket.raw_data.arn}/${var.spark_event_logs_prefix}*",
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
      "arn:aws:logs:*:*:log-group:/aws-glue/*",
    ]
  }

  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:GetJob",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_job_inline_policy" {
  name   = "${var.project_name}-${var.environment}-glue-job-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

############################################
# CloudWatch Log Group for Glue job logs
############################################

resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = {
    Name = "${var.project_name}-${var.environment}-glue-job-logs"
  }
}

############################################
# AWS Glue Job (PySpark ETL)
############################################

resource "aws_glue_job" "transform_job" {
  name         = var.glue_job_name
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = var.glue_version

  number_of_workers = var.glue_number_of_workers
  worker_type       = var.glue_worker_type
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${data.aws_s3_bucket.raw_data.id}/${var.scripts_prefix}${var.glue_script_filename}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--TempDir"                         = "s3://${data.aws_s3_bucket.raw_data.id}/${var.temp_dir_prefix}"
    "--spark-event-logs-path"           = "s3://${data.aws_s3_bucket.raw_data.id}/${var.spark_event_logs_prefix}"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                 = "true"
    "--enable-job-insights"             = "true"
    "--SOURCE_PATH"                     = "s3://${data.aws_s3_bucket.raw_data.id}/${var.raw_data_prefix}"
    "--DEST_PATH"                       = "s3://${data.aws_s3_bucket.raw_data.id}/${var.transformed_data_prefix}"
  }

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  tags = {
    Name = var.glue_job_name
  }

  depends_on = [
    aws_iam_role_policy.glue_job_inline_policy,
    aws_cloudwatch_log_group.glue_job_logs,
  ]
}

############################################
# Optional: Glue Catalog Database + Crawler
############################################

resource "aws_glue_catalog_database" "transformed_data_db" {
  count = var.enable_glue_catalog ? 1 : 0
  name  = var.glue_catalog_database_name
}

resource "aws_glue_crawler" "transformed_data_crawler" {
  count         = var.enable_glue_catalog ? 1 : 0
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_job_role.arn
  database_name = aws_glue_catalog_database.transformed_data_db[0].name
  schedule      = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  s3_target {
    path = "s3://${data.aws_s3_bucket.raw_data.id}/${var.transformed_data_prefix}"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Name = var.glue_crawler_name
  }

  depends_on = [
    aws_iam_role_policy.glue_job_inline_policy,
  ]
}