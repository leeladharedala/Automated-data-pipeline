locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags
  )

  script_s3_uri = "s3://${data.aws_s3_bucket.raw_data.id}/${var.scripts_prefix}${var.glue_script_filename}"
  raw_s3_uri    = "s3://${data.aws_s3_bucket.raw_data.id}/${var.raw_data_prefix}"
  sink_s3_uri   = "s3://${data.aws_s3_bucket.raw_data.id}/${var.transformed_data_prefix}"
  temp_s3_uri   = "s3://${data.aws_s3_bucket.raw_data.id}/${var.temp_dir_prefix}"

  log_group_name = "/aws-glue/jobs/${var.glue_job_name}"
}

# ---------------------------------------------------------------------------
# Existing raw data bucket (referenced, not managed) - also used as the sink
# bucket under the transformed_data/ prefix.
# ---------------------------------------------------------------------------
data "aws_s3_bucket" "raw_data" {
  bucket = var.raw_bucket_name
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Glue job logging
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "glue_job" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# IAM Role & Policies for Glue Job execution
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

resource "aws_iam_role" "glue_execution_role" {
  name                  = "${var.project_name}-${var.environment}-glue-execution-role"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid    = "ListRawAndSinkBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      data.aws_s3_bucket.raw_data.arn
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.raw_data_prefix}*",
        "${var.transformed_data_prefix}*",
        "${var.scripts_prefix}*",
        "${var.temp_dir_prefix}*"
      ]
    }
  }

  statement {
    sid    = "ReadRawData"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.raw_data_prefix}*"
    ]
  }

  statement {
    sid    = "WriteTransformedData"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.transformed_data_prefix}*"
    ]
  }

  statement {
    sid    = "ReadGlueScripts"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.scripts_prefix}*"
    ]
  }

  statement {
    sid    = "TempDirAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${data.aws_s3_bucket.raw_data.arn}/${var.temp_dir_prefix}*"
    ]
  }
}

resource "aws_iam_policy" "glue_s3_access" {
  name        = "${var.project_name}-${var.environment}-glue-s3-access"
  description = "Allows the Glue job to read raw_data/, write transformed_data/, and access scripts/tmp prefixes."
  policy      = data.aws_iam_policy_document.glue_s3_access.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

data "aws_iam_policy_document" "glue_catalog_access" {
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
      "glue:DeleteTable",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition"
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_catalog_database_name}",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_catalog_database_name}/${var.glue_catalog_table_name}"
    ]
  }

  statement {
    sid    = "GlueCloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
    ]
  }
}

resource "aws_iam_policy" "glue_catalog_access" {
  name        = "${var.project_name}-${var.environment}-glue-catalog-access"
  description = "Allows the Glue job to manage the transformed data catalog database/table and write CloudWatch logs."
  policy      = data.aws_iam_policy_document.glue_catalog_access.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_catalog_access" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

# ---------------------------------------------------------------------------
# Glue Data Catalog: database + table for transformed (Parquet) data
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "transformed" {
  name        = var.glue_catalog_database_name
  description = "Catalog database for transformed Parquet data produced by ${var.glue_job_name}."
}

resource "aws_glue_catalog_table" "transformed" {
  name          = var.glue_catalog_table_name
  database_name = aws_glue_catalog_database.transformed.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = local.sink_s3_uri
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }

    columns {
      name = "data"
      type = "string"
    }

    columns {
      name = "processed_at"
      type = "string"
    }
  }
}

# ---------------------------------------------------------------------------
# AWS Glue Job (PySpark ETL)
# Script is produced and uploaded out-of-band by the CI/CD pipeline to:
#   s3://<raw_bucket>/<scripts_prefix><glue_script_filename>
# Terraform does not manage or upload the script content itself.
# ---------------------------------------------------------------------------
resource "aws_glue_job" "transform" {
  name         = var.glue_job_name
  role_arn     = aws_iam_role.glue_execution_role.arn
  glue_version = var.glue_version

  command {
    name            = "glueetl"
    script_location = local.script_s3_uri
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = local.temp_s3_uri
    "--S3_SOURCE_PATH"                   = local.raw_s3_uri
    "--S3_TARGET_PATH"                   = local.sink_s3_uri
    "--GLUE_CATALOG_DATABASE"            = aws_glue_catalog_database.transformed.name
    "--GLUE_CATALOG_TABLE"               = aws_glue_catalog_table.transformed.name
  }

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_iam_role_policy_attachment.glue_catalog_access,
    aws_cloudwatch_log_group.glue_job
  ]
}