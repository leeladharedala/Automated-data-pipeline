###############################################################################
# Data sources
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

###############################################################################
# S3 Bucket
###############################################################################

resource "aws_s3_bucket" "data" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "transformed-data-lifecycle"
    status = "Enabled"

    filter {
      prefix = var.transformed_data_prefix
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_object" "raw_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.raw_data_prefix
  content = ""

  lifecycle {
    ignore_changes = [content, etag]
  }
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.transformed_data_prefix
  content = ""

  lifecycle {
    ignore_changes = [content, etag]
  }
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.scripts_prefix
  content = ""

  lifecycle {
    ignore_changes = [content, etag]
  }
}

###############################################################################
# IAM Role for AWS Glue
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

resource "aws_iam_role" "glue_service_role" {
  name                  = "AWSGlueServiceRole-${var.glue_job_name}"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  description           = "IAM role assumed by the ${var.glue_job_name} Glue job and crawler"
  force_detach_policies = true
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
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
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid    = "ReadRawDataAndScripts"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/${var.raw_data_prefix}*",
      "${aws_s3_bucket.data.arn}/${var.scripts_prefix}*",
    ]
  }

  statement {
    sid    = "WriteTransformedData"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/${var.transformed_data_prefix}*",
    ]
  }

  statement {
    sid    = "GlueTempAndSparkLogs"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/tmp/*",
      "${aws_s3_bucket.data.arn}/spark-logs/*",
    ]
  }
}

resource "aws_iam_policy" "glue_s3_access" {
  name        = "${var.glue_job_name}-s3-access"
  description = "Scoped S3 access for the ${var.glue_job_name} Glue job"
  policy      = data.aws_iam_policy_document.glue_s3_access.json
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

###############################################################################
# CloudWatch Log Groups
###############################################################################

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_crawler" {
  name              = "/aws-glue/crawlers/${var.glue_crawler_name}"
  retention_in_days = 30
}

###############################################################################
# AWS Glue Data Catalog Database
###############################################################################

resource "aws_glue_catalog_database" "energy_etl" {
  name        = var.glue_database_name
  catalog_id  = data.aws_caller_identity.current.account_id
  description = "Glue Data Catalog database for the energy ETL pipeline"

  location_uri = "s3://${var.bucket_name}/${var.transformed_data_prefix}"
}

###############################################################################
# AWS Glue Crawler
###############################################################################

resource "aws_glue_crawler" "raw_data" {
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.energy_etl.name
  description   = "Crawls raw JSONL energy data in s3://${var.bucket_name}/${var.raw_data_prefix}"
  table_prefix  = "raw_"

  s3_target {
    path = "s3://${var.bucket_name}/${var.raw_data_prefix}"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      Tables     = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_policy,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_cloudwatch_log_group.glue_crawler,
  ]
}

###############################################################################
# AWS Glue Job
###############################################################################

resource "aws_glue_job" "energy_etl" {
  name        = var.glue_job_name
  role_arn    = aws_iam_role.glue_service_role.arn
  description = "PySpark ETL job: reads JSONL from raw_data/, writes Parquet to transformed_data/"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/${var.scripts_prefix}transform.py"
    python_version  = "3"
  }

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job.name
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.bucket_name}/spark-logs/"
    "--TempDir"                          = "s3://${var.bucket_name}/tmp/"
    "--SOURCE_PATH"                      = "s3://${var.bucket_name}/${var.raw_data_prefix}"
    "--SINK_PATH"                        = "s3://${var.bucket_name}/${var.transformed_data_prefix}"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_policy,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_cloudwatch_log_group.glue_job,
  ]
}