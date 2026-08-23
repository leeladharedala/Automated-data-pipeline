locals {
  common_tags = merge(
    var.tags,
    {
      Environment = var.environment
    }
  )

  name_prefix = "${var.project_name}-${var.environment}"
}

data "aws_s3_bucket" "existing" {
  count  = var.create_bucket ? 0 : 1
  bucket = var.raw_data_bucket_name
}

resource "aws_s3_bucket" "managed" {
  count         = var.create_bucket ? 1 : 0
  bucket        = var.raw_data_bucket_name
  force_destroy = true

  tags = merge(local.common_tags, {
    Name = var.raw_data_bucket_name
  })
}

locals {
  bucket_name = var.create_bucket ? aws_s3_bucket.managed[0].bucket : data.aws_s3_bucket.existing[0].bucket
  bucket_arn  = var.create_bucket ? aws_s3_bucket.managed[0].arn : data.aws_s3_bucket.existing[0].arn

  source_s3_uri  = "s3://${local.bucket_name}/${var.raw_data_prefix}"
  dest_s3_uri    = "s3://${local.bucket_name}/${var.transformed_data_prefix}"
  script_s3_key  = "${var.scripts_prefix}${var.glue_script_key}"
  script_s3_uri  = "s3://${local.bucket_name}/${local.script_s3_key}"
  temp_dir_s3_uri = "s3://${local.bucket_name}/glue-temp/"
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job_role" {
  name                  = "${local.name_prefix}-glue-job-role"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
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
    sid    = "ReadWriteTempDir"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = [
      "${local.bucket_arn}/glue-temp/*",
    ]
  }

  statement {
    sid    = "ReadGlueScript"
    effect = "Allow"

    actions = [
      "s3:GetObject",
    ]

    resources = [
      "${local.bucket_arn}/${var.scripts_prefix}*",
    ]
  }
}

resource "aws_iam_policy" "glue_s3_access" {
  name        = "${local.name_prefix}-glue-s3-access"
  description = "Grants the Glue job read access to raw_data/, read/write access to transformed_data/, and read access to scripts/ in ${var.raw_data_bucket_name}."
  policy      = data.aws_iam_policy_document.glue_s3_access.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_s3_access_attach" {
  role       = aws_iam_role.glue_job_role.name
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
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdateTable",
      "glue:UpdatePartition",
    ]

    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_catalog_database_name}",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_catalog_database_name}/${var.glue_catalog_table_name}",
    ]
  }
}

resource "aws_iam_policy" "glue_catalog_access" {
  name        = "${local.name_prefix}-glue-catalog-access"
  description = "Grants the Glue job access to the Data Catalog database/table for transformed data."
  policy      = data.aws_iam_policy_document.glue_catalog_access.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_catalog_access_attach" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${local.name_prefix}-${var.glue_job_name}"
  retention_in_days = var.log_retention_in_days

  tags = local.common_tags
}

resource "aws_glue_catalog_database" "transformed" {
  name = var.glue_catalog_database_name
}

resource "aws_glue_catalog_table" "transformed_data" {
  name          = var.glue_catalog_table_name
  database_name = aws_glue_catalog_database.transformed.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
    classification         = "parquet"
  }

  storage_descriptor {
    location      = local.dest_s3_uri
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "transformed-data-serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
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
      type = "float"
    }

    columns {
      name = "energy_consumed_kwh"
      type = "float"
    }
  }
}

resource "aws_glue_job" "transform_job" {
  name              = "${local.name_prefix}-${var.glue_job_name}"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout
  max_retries       = var.glue_max_retries

  command {
    name            = "glueetl"
    script_location = local.script_s3_uri
    python_version  = var.glue_python_version
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--SOURCE_PATH"                      = local.source_s3_uri
    "--DEST_PATH"                        = local.dest_s3_uri
    "--DATABASE_NAME"                    = aws_glue_catalog_database.transformed.name
    "--TABLE_NAME"                       = aws_glue_catalog_table.transformed_data.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job.name
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = local.temp_dir_s3_uri
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_access_attach,
    aws_iam_role_policy_attachment.glue_catalog_access_attach,
  ]
}