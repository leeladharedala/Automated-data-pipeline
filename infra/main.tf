locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags
  )

  raw_data_s3_uri         = "s3://${var.bucket_name}/${var.raw_data_prefix}"
  transformed_data_s3_uri = "s3://${var.bucket_name}/${var.transformed_data_prefix}"
  script_s3_uri           = "s3://${var.bucket_name}/${var.scripts_prefix}${var.glue_script_filename}"
}

# ---------------------------------------------------------------------------
# S3 bucket for raw + transformed data
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  count = var.create_bucket ? 1 : 0

  bucket        = var.bucket_name
  force_destroy = true

  tags = merge(local.common_tags, {
    Name = var.bucket_name
  })
}

data "aws_s3_bucket" "data" {
  count  = var.create_bucket ? 0 : 1
  bucket = var.bucket_name
}

locals {
  bucket_id  = var.create_bucket ? aws_s3_bucket.data[0].id : data.aws_s3_bucket.data[0].id
  bucket_arn = var.create_bucket ? aws_s3_bucket.data[0].arn : data.aws_s3_bucket.data[0].arn
}

resource "aws_s3_bucket_versioning" "data" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.data[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.data[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.data[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# "Folder" placeholder objects for raw_data/ and transformed_data/ prefixes.
resource "aws_s3_object" "raw_data_prefix" {
  bucket       = local.bucket_id
  key          = var.raw_data_prefix
  content      = ""
  content_type = "application/x-directory"

  tags = local.common_tags

  depends_on = [aws_s3_bucket.data]
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket       = local.bucket_id
  key          = var.transformed_data_prefix
  content      = ""
  content_type = "application/x-directory"

  tags = local.common_tags

  depends_on = [aws_s3_bucket.data]
}

# ---------------------------------------------------------------------------
# Glue Data Catalog database + tables (source + sink schema registration)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  name = var.glue_database_name
}

resource "aws_glue_catalog_table" "raw_data" {
  name          = "raw_data"
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
      name                  = "json-serde"
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

resource "aws_glue_catalog_table" "transformed_data" {
  name          = "transformed_data"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = local.transformed_data_s3_uri
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
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