###############################################################################
# Data sources
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

###############################################################################
# Locals
###############################################################################

locals {
  raw_s3_path         = "s3://${var.data_bucket_name}/${var.raw_data_prefix}"
  transformed_s3_path = "s3://${var.data_bucket_name}/${var.transformed_data_prefix}"
}

###############################################################################
# S3 – Data bucket
###############################################################################

resource "aws_s3_bucket" "data" {
  bucket        = var.data_bucket_name
  force_destroy = true

  tags = { Name = var.data_bucket_name }
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "raw_data_prefix" {
  bucket     = aws_s3_bucket.data.id
  key        = var.raw_data_prefix
  content    = ""
  depends_on = [aws_s3_bucket.data]
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket     = aws_s3_bucket.data.id
  key        = var.transformed_data_prefix
  content    = ""
  depends_on = [aws_s3_bucket.data]
}

###############################################################################
# S3 – Glue scripts bucket
###############################################################################

resource "aws_s3_bucket" "glue_scripts" {
  bucket        = "${var.project_name}-${var.environment}-glue-scripts"
  force_destroy = true
  tags          = { Name = "${var.project_name}-${var.environment}-glue-scripts" }
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

###############################################################################
# IAM – Glue service role
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
  name                  = "AWSGlueServiceRole-${var.project_name}-${var.environment}"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true
  tags                  = { Name = "AWSGlueServiceRole-${var.project_name}-${var.environment}" }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid     = "ListDataBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn, aws_s3_bucket.glue_scripts.arn]
  }
  statement {
    sid     = "ReadRawDataAndScripts"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/${var.raw_data_prefix}*",
      "${aws_s3_bucket.glue_scripts.arn}/scripts/*",
    ]
  }
  statement {
    sid     = "WriteTransformedData"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/${var.transformed_data_prefix}*"]
  }
  statement {
    sid     = "GlueTempAndLogs"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.glue_scripts.arn}/tmp/*",
      "${aws_s3_bucket.glue_scripts.arn}/spark-logs/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "GlueS3Access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

###############################################################################
# CloudWatch Log Group
###############################################################################

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 30
  tags              = { Name = "/aws-glue/jobs/${var.glue_job_name}" }
}

###############################################################################
# Glue Data Catalog
###############################################################################

resource "aws_glue_catalog_database" "energy" {
  name        = var.glue_database_name
  description = "AWS Glue Data Catalog database for the energy ETL pipeline"
  catalog_id  = data.aws_caller_identity.current.account_id
}

resource "aws_glue_crawler" "energy" {
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.energy.name
  description   = "Crawls raw JSONL and Parquet output for the energy ETL pipeline"

  s3_target { path = "s3://${var.data_bucket_name}/${var.raw_data_prefix}" }
  s3_target { path = "s3://${var.data_bucket_name}/${var.transformed_data_prefix}" }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      Tables     = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })

  tags = { Name = var.glue_crawler_name }

  depends_on = [
    aws_glue_catalog_database.energy,
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3,
  ]
}

###############################################################################
# Glue Job
###############################################################################

resource "aws_glue_job" "energy_etl" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  description       = "PySpark ETL job: reads JSONL from raw_data/, writes Parquet to transformed_data/"
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/energy_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--source_path"                      = local.raw_s3_path
    "--sink_path"                        = local.transformed_s3_path
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/tmp/"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-logs/"
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = { Name = var.glue_job_name }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3,
    aws_cloudwatch_log_group.glue_job,
  ]
}