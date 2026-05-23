# ============================================================
# main.tf — Core infrastructure resources
# ============================================================

locals {
  scripts_bucket_name = "${var.project_name}-${var.environment}-glue-scripts-${data.aws_caller_identity.current.account_id}"
  source_s3_uri = "s3://${var.data_bucket_name}/${var.raw_data_prefix}"
  sink_s3_uri   = "s3://${var.data_bucket_name}/${var.transformed_data_prefix}"
  temp_s3_uri   = "s3://${local.scripts_bucket_name}/tmp/"
  script_s3_uri = "s3://${local.scripts_bucket_name}/scripts/energy_etl.py"
  glue_role_name = "AWSGlueServiceRole-${var.project_name}-${var.environment}"
}

# ── S3 Data Bucket ───────────────────────────────────────────

resource "aws_s3_bucket" "data" {
  bucket        = var.data_bucket_name
  force_destroy = true
  tags = { Name = var.data_bucket_name, Purpose = "energy-etl-data" }
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
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
  bucket  = aws_s3_bucket.data.id
  key     = var.raw_data_prefix
  content = ""
  depends_on = [aws_s3_bucket.data]
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.transformed_data_prefix
  content = ""
  depends_on = [aws_s3_bucket.data]
}

# ── S3 Scripts Bucket ────────────────────────────────────────

resource "aws_s3_bucket" "scripts" {
  bucket        = local.scripts_bucket_name
  force_destroy = true
  tags = { Name = local.scripts_bucket_name, Purpose = "energy-etl-glue-scripts" }
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── CloudWatch ───────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.glue_log_retention_days
  tags = { Name = "glue-${var.glue_job_name}-logs" }
}

# ── IAM ──────────────────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name                  = local.glue_role_name
  description           = "IAM role assumed by AWS Glue for the energy ETL pipeline."
  force_detach_policies = true
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Name = local.glue_role_name }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3" {
  name        = "${local.glue_role_name}-s3-policy"
  description = "Least-privilege S3 access for Glue ETL."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListDataBucket"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.data.arn]
      },
      {
        Sid    = "ReadRawData"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.data.arn}/${var.raw_data_prefix}*"]
      },
      {
        Sid    = "WriteTransformedData"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.data.arn}/${var.transformed_data_prefix}*"]
      },
      {
        Sid    = "ListScriptsBucket"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.scripts.arn]
      },
      {
        Sid    = "AccessScriptsAndTemp"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.scripts.arn}/scripts/*",
          "${aws_s3_bucket.scripts.arn}/tmp/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_s3.arn
}

# ── Glue Catalog ─────────────────────────────────────────────

resource "aws_glue_catalog_database" "energy" {
  name       = var.glue_database_name
  catalog_id = data.aws_caller_identity.current.account_id
  description = "Glue Data Catalog database for the energy ETL pipeline."
}

# ── Glue Crawlers ────────────────────────────────────────────

resource "aws_glue_crawler" "raw" {
  name          = "${var.project_name}-${var.environment}-raw-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.energy.name
  description   = "Crawls raw JSONL energy data."
  s3_target { path = local.source_s3_uri }
  table_prefix  = "raw_"
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
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy_attachment.glue_s3]
}

resource "aws_glue_crawler" "transformed" {
  name          = "${var.project_name}-${var.environment}-transformed-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.energy.name
  description   = "Crawls transformed Parquet energy data."
  s3_target { path = local.sink_s3_uri }
  table_prefix  = "transformed_"
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
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy_attachment.glue_s3]
}

# ── Glue ETL Job ─────────────────────────────────────────────

resource "aws_glue_job" "energy_etl" {
  name        = var.glue_job_name
  role_arn    = aws_iam_role.glue.arn
  description = "PySpark ETL: JSONL → Parquet."
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes
  command {
    name            = "glueetl"
    script_location = local.script_s3_uri
    python_version  = "3"
  }
  default_arguments = {
    "--job-bookmark-option"                  = "job-bookmark-disable"
    "--job-language"                         = "python"
    "--source_path"                          = local.source_s3_uri
    "--sink_path"                            = local.sink_s3_uri
    "--TempDir"                              = local.temp_s3_uri
    "--enable-continuous-cloudwatch-log"     = "true"
    "--enable-continuous-log-filter"         = "true"
    "--continuous-log-logGroup"              = aws_cloudwatch_log_group.glue.name
    "--enable-metrics"                       = "true"
    "--enable-observability-metrics"         = "true"
    "--enable-s3-parquet-optimized-committer" = "true"
    "--enable-glue-datacatalog"              = "true"
    "--enable-auto-scaling"                  = "false"
  }
  execution_property { max_concurrent_runs = 1 }
  tags = { Name = var.glue_job_name }
  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy_attachment.glue_s3,
    aws_cloudwatch_log_group.glue
  ]
}