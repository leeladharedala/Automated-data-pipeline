###############################################################################
# Data sources
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

###############################################################################
# S3 – Raw / Transformed data bucket
###############################################################################

resource "aws_s3_bucket" "data" {
  bucket        = var.raw_data_bucket_name
  force_destroy = true

  tags = merge(var.tags, {
    Name = var.raw_data_bucket_name
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

resource "aws_s3_object" "raw_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = "raw_data/.keep"
  content = ""

  tags = var.tags
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket  = aws_s3_bucket.data.id
  key     = "transformed_data/.keep"
  content = ""

  tags = var.tags
}

###############################################################################
# S3 – Glue scripts bucket
###############################################################################

resource "aws_s3_bucket" "glue_scripts" {
  bucket        = "${var.project_name}-glue-scripts-${data.aws_caller_identity.current.account_id}"
  force_destroy = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-glue-scripts"
  })
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "glue_etl_script" {
  bucket  = aws_s3_bucket.glue_scripts.id
  key     = "scripts/energy_etl.py"
  content = local.glue_etl_script

  tags = var.tags
}

###############################################################################
# IAM – Glue service role
###############################################################################

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

resource "aws_iam_role" "glue" {
  name                  = "AWSGlueServiceRole-${var.project_name}"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true

  tags = merge(var.tags, {
    Name = "AWSGlueServiceRole-${var.project_name}"
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid    = "ReadRawData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.data.arn,
      "${aws_s3_bucket.data.arn}/raw_data/*",
    ]
  }

  statement {
    sid    = "WriteTransformedData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.data.arn,
      "${aws_s3_bucket.data.arn}/transformed_data/*",
    ]
  }

  statement {
    sid    = "GlueScriptsBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.glue_scripts.arn,
      "${aws_s3_bucket.glue_scripts.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.project_name}-glue-s3-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

###############################################################################
# CloudWatch Log Group – Glue continuous logging
###############################################################################

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 30

  tags = merge(var.tags, {
    Name = "/aws-glue/jobs/${var.glue_job_name}"
  })
}

###############################################################################
# Glue Data Catalog – Database
###############################################################################

resource "aws_glue_catalog_database" "energy" {
  name        = var.glue_database_name
  description = "AWS Glue Data Catalog database for the energy ETL pipeline"

  tags = var.tags
}

###############################################################################
# Glue Crawler – catalogues raw_data/ prefix
###############################################################################

resource "aws_glue_crawler" "raw_data" {
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.energy.name
  description   = "Crawls the raw_data/ prefix and registers JSONL schema in the Data Catalog"

  s3_target {
    path = "s3://${aws_s3_bucket.data.bucket}/raw_data/"
  }

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

  tags = var.tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3,
  ]
}

###############################################################################
# Glue Job – PySpark ETL (S3 JSONL → S3 Parquet)
###############################################################################

resource "aws_glue_job" "energy_etl" {
  name        = var.glue_job_name
  role_arn    = aws_iam_role.glue.arn
  description = "PySpark ETL job: reads JSONL from raw_data/, writes Parquet to transformed_data/"

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.job_timeout_minutes
  max_retries       = var.max_retries

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/energy_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--SOURCE_S3_PATH" = "s3://${aws_s3_bucket.data.bucket}/raw_data/"
    "--SINK_S3_PATH"   = "s3://${aws_s3_bucket.data.bucket}/transformed_data/"
    "--DATABASE_NAME"  = aws_glue_catalog_database.energy.name

    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job.name
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/tmp/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags

  depends_on = [
    aws_s3_object.glue_etl_script,
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3,
    aws_cloudwatch_log_group.glue_job,
  ]
}

###############################################################################
# Local – inline PySpark ETL script
###############################################################################

locals {
  glue_etl_script = <<-PYTHON
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_S3_PATH", "SINK_S3_PATH", "DATABASE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    schema = StructType([
        StructField("site_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("energy_generated_kwh", DoubleType(), nullable=True),
        StructField("energy_consumed_kwh", DoubleType(), nullable=True),
    ])

    raw_df = spark.read.schema(schema).option("multiline", "false").json(args["SOURCE_S3_PATH"])

    transformed_df = (
        raw_df
        .withColumn("net_energy_kwh", F.col("energy_generated_kwh") - F.col("energy_consumed_kwh"))
        .withColumn("negative_energy_flag",
            F.when((F.col("energy_generated_kwh") < 0) | (F.col("energy_consumed_kwh") < 0), F.lit(1)).otherwise(F.lit(0)))
    )

    transformed_df.write.mode("overwrite").option("compression", "snappy").parquet(args["SINK_S3_PATH"])
    job.commit()
  PYTHON
}