locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  raw_data_s3_uri         = "s3://${var.bucket_name}/${var.raw_data_prefix}"
  transformed_data_s3_uri = "s3://${var.bucket_name}/${var.transformed_data_prefix}"
  temp_dir_s3_uri         = "s3://${var.bucket_name}/${var.temp_prefix}"
  script_s3_uri           = "s3://${var.bucket_name}/${var.scripts_prefix}${var.glue_script_filename}"
}

# ---------------------------------------------------------------------------
# S3 Bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket        = var.bucket_name
  force_destroy = true

  tags = merge(local.common_tags, {
    Name = var.bucket_name
  })
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
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Folder marker objects (zero-byte) for clarity in the console/CI tooling.
resource "aws_s3_object" "raw_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.raw_data_prefix
  content = ""
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.transformed_data_prefix
  content = ""
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.scripts_prefix
  content = ""
}

resource "aws_s3_object" "temp_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = var.temp_prefix
  content = ""
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Glue job logs
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# IAM Role & Policies for Glue Job
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

resource "aws_iam_role" "glue_job" {
  name                  = "${local.name_prefix}-glue-job-role"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid    = "ListAndLocateBucket"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid    = "ReadRawData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/${var.raw_data_prefix}*",
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
      "${aws_s3_bucket.data.arn}/${var.transformed_data_prefix}*",
    ]
  }

  statement {
    sid    = "ReadWriteTempData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/${var.temp_prefix}*",
    ]
  }

  statement {
    sid    = "ReadGlueScript"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/${var.scripts_prefix}*",
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
      "arn:aws:logs:*:*:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "${local.name_prefix}-glue-s3-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

# ---------------------------------------------------------------------------
# Glue Data Catalog Database & Table (raw JSONL source)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  name = var.glue_catalog_database_name
}

resource "aws_glue_catalog_table" "raw_data" {
  name          = var.glue_catalog_table_name
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "json"
  }

  storage_descriptor {
    location      = local.raw_data_s3_uri
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "site_id"
      type = "string"
    }

    columns {
      name = "timestamp"
      type = "string"
    }

    columns {
      name = "energy_generated_kwh"
      type = "double"
    }

    columns {
      name = "energy_consumed_kwh"
      type = "double"
    }
  }
}

# ---------------------------------------------------------------------------
# AWS Glue Job (PySpark ETL: JSONL -> Parquet, overwrite)
# ---------------------------------------------------------------------------
# NOTE: The PySpark script (transform.py) is produced and uploaded to
# ${local.script_s3_uri} out-of-band by the CI/CD pipeline. Terraform does not
# manage the script object's content; it only references the expected S3 URI.

resource "aws_glue_job" "transform" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_job.arn
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
    "--job-language"                    = "python"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                         = local.temp_dir_s3_uri
    "--SOURCE_PATH"                     = local.raw_data_s3_uri
    "--DEST_PATH"                       = local.transformed_data_s3_uri
  }

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  tags = merge(local.common_tags, {
    Name = var.glue_job_name
  })

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_cloudwatch_log_group.glue_job,
  ]
}