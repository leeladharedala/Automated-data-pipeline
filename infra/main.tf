data "aws_caller_identity" "current" {}

locals {
  bucket_arn = aws_s3_bucket.data_bucket.arn

  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags
  )

  script_s3_uri = "s3://${aws_s3_bucket.data_bucket.id}/${var.glue_script_object_key}"
  temp_s3_uri   = "s3://${aws_s3_bucket.data_bucket.id}/${var.temp_prefix}"
  source_s3_uri = "s3://${aws_s3_bucket.data_bucket.id}/${var.raw_data_prefix}"
  dest_s3_uri   = "s3://${aws_s3_bucket.data_bucket.id}/${var.transformed_data_prefix}"
}

# ---------------------------------------------------------------------------
# S3 bucket: source (raw_data/) and sink (transformed_data/) for the pipeline
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data_bucket" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy_bucket

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Zero-byte placeholder objects to make prefixes visible/browsable in console.
resource "aws_s3_object" "raw_data_prefix" {
  bucket       = aws_s3_bucket.data_bucket.id
  key          = var.raw_data_prefix
  content_type = "application/x-directory"

  depends_on = [aws_s3_bucket_public_access_block.data_bucket]
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket       = aws_s3_bucket.data_bucket.id
  key          = var.transformed_data_prefix
  content_type = "application/x-directory"

  depends_on = [aws_s3_bucket_public_access_block.data_bucket]
}

resource "aws_s3_object" "scripts_prefix" {
  bucket       = aws_s3_bucket.data_bucket.id
  key          = var.scripts_prefix
  content_type = "application/x-directory"

  depends_on = [aws_s3_bucket_public_access_block.data_bucket]
}

resource "aws_s3_object" "temp_prefix" {
  bucket       = aws_s3_bucket.data_bucket.id
  key          = var.temp_prefix
  content_type = "application/x-directory"

  depends_on = [aws_s3_bucket_public_access_block.data_bucket]
}

# NOTE: The PySpark transform script itself (transform.py) is produced and
# uploaded out-of-band by a separate CI/CD pipeline/agent to
# s3://${var.bucket_name}/${var.glue_script_object_key}. Terraform does not
# manage that object and does not reference any local file path for it.

# ---------------------------------------------------------------------------
# IAM role & policies for the Glue job and crawler
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

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid    = "ListAndLocateBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [local.bucket_arn]
  }

  statement {
    sid    = "ReadWriteDataPrefixes"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${local.bucket_arn}/${var.raw_data_prefix}*",
      "${local.bucket_arn}/${var.transformed_data_prefix}*",
      "${local.bucket_arn}/${var.scripts_prefix}*",
      "${local.bucket_arn}/${var.temp_prefix}*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "${var.project_name}-${var.environment}-glue-s3-access"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Glue job logs (explicit retention to avoid orphans)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Glue Data Catalog database + crawler (catalogs transformed_data/ output)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  name        = var.glue_catalog_database_name
  description = "Catalog database for ${var.project_name} (${var.environment}) transformed data."
}

resource "aws_glue_crawler" "transformed_data" {
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.this.name
  description   = "Crawls Parquet output under ${local.dest_s3_uri} to populate the Data Catalog for downstream querying (e.g. Athena)."
  table_prefix  = "transformed_"

  s3_target {
    path = local.dest_s3_uri
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
  ]
}

# NOTE: Terraform only provisions the crawler resource; it does not execute
# it. The crawler must be started manually, via the console, or via a
# separate automation step (e.g. `aws glue start-crawler`) after the Glue
# job has produced Parquet output under transformed_data/.

# ---------------------------------------------------------------------------
# Glue ETL Job (PySpark) - reads JSONL from raw_data/, writes Parquet
# (overwrite mode) to transformed_data/
# ---------------------------------------------------------------------------

resource "aws_glue_job" "transform" {
  name         = var.glue_job_name
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = var.glue_version

  command {
    name            = "glueetl"
    script_location = local.script_s3_uri
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
    "--job-language"                    = "python"
    "--TempDir"                         = local.temp_s3_uri
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--source_path"                     = local.source_s3_uri
    "--dest_path"                       = local.dest_s3_uri
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_cloudwatch_log_group.glue_job_logs,
  ]
}