###############################################################################
# Local values
###############################################################################

locals {
  raw_data_prefix         = "raw_data/"
  transformed_data_prefix = "transformed_data/"
  script_s3_key           = "src/transformations/transform.py"

  source_path = "s3://${var.raw_data_bucket_name}/${local.raw_data_prefix}"
  sink_path   = "s3://${var.raw_data_bucket_name}/${local.transformed_data_prefix}"
  script_path = "s3://${var.scripts_bucket_name}/${local.script_s3_key}"

  common_tags = {
    Project     = var.project
    Environment = var.env
  }
}

###############################################################################
# S3 - Raw Data Bucket
###############################################################################

resource "aws_s3_bucket" "raw_data" {
  bucket        = var.raw_data_bucket_name
  force_destroy = true

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

###############################################################################
# S3 - Scripts Bucket
###############################################################################

resource "aws_s3_bucket" "scripts" {
  bucket        = var.scripts_bucket_name
  force_destroy = true

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

###############################################################################
# IAM - Glue Service Role
###############################################################################

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "GlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name                  = var.glue_iam_role_name
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  description           = "IAM role assumed by AWS Glue for the energy ETL pipeline."
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid    = "ListDataBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.raw_data.arn,
      aws_s3_bucket.scripts.arn,
    ]
  }

  statement {
    sid    = "ReadWriteDataObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.raw_data.arn}/*",
    ]
  }

  statement {
    sid    = "ReadScriptObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${aws_s3_bucket.scripts.arn}/*",
    ]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey",
    ]
    resources = [
      "${aws_cloudwatch_log_group.glue_job.arn}:*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.glue_iam_role_name}-s3-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

###############################################################################
# CloudWatch Log Group - Glue Job Logs
###############################################################################

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws/glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

###############################################################################
# AWS Glue - Data Catalog Database
###############################################################################

resource "aws_glue_catalog_database" "energy_etl" {
  name        = var.glue_database_name
  description = "Glue Data Catalog database for the energy ETL pipeline (transformed Parquet data)."
}

###############################################################################
# AWS Glue - ETL Job
###############################################################################

resource "aws_glue_job" "energy_etl" {
  name        = var.glue_job_name
  role_arn    = aws_iam_role.glue.arn
  description = "PySpark ETL job: reads JSONL from S3 raw_data/, transforms, writes Parquet to transformed_data/."

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  timeout = 60

  command {
    name            = "glueetl"
    script_location = local.script_path
    python_version  = "3"
  }

  default_arguments = {
    "--source_path"                      = local.source_path
    "--sink_path"                        = local.sink_path
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job.name
    "--continuous-log-logStreamPrefix"   = "energy-etl"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.raw_data_bucket_name}/tmp/"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.raw_data_bucket_name}/spark-logs/"
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3,
  ]
}

###############################################################################
# AWS Glue - Crawler
###############################################################################

resource "aws_glue_crawler" "transformed_data" {
  name          = "${var.project}-transformed-data-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.energy_etl.name
  description   = "Crawls the transformed_data/ Parquet prefix and registers tables in the Glue Data Catalog."

  s3_target {
    path = "s3://${var.raw_data_bucket_name}/${local.transformed_data_prefix}"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3,
  ]
}