data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

resource "aws_s3_bucket" "pipeline" {
  bucket        = var.bucket_name
  force_destroy = true
  tags = { Name = var.bucket_name }
}

resource "aws_s3_bucket_public_access_block" "pipeline" {
  bucket                  = aws_s3_bucket.pipeline.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_object" "raw_data_prefix" {
  bucket                 = aws_s3_bucket.pipeline.id
  key                    = var.raw_data_prefix
  content                = ""
  server_side_encryption = "AES256"
  depends_on             = [aws_s3_bucket_server_side_encryption_configuration.pipeline]
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket                 = aws_s3_bucket.pipeline.id
  key                    = var.transformed_data_prefix
  content                = ""
  server_side_encryption = "AES256"
  depends_on             = [aws_s3_bucket_server_side_encryption_configuration.pipeline]
}

resource "aws_s3_object" "scripts_prefix" {
  bucket                 = aws_s3_bucket.pipeline.id
  key                    = var.scripts_prefix
  content                = ""
  server_side_encryption = "AES256"
  depends_on             = [aws_s3_bucket_server_side_encryption_configuration.pipeline]
}

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
  name                  = "AWSGlueServiceRole-${var.project}-${var.environment}"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true
  tags                  = { Name = "AWSGlueServiceRole-${var.project}-${var.environment}" }
}

resource "aws_iam_role_policy_attachment" "glue_service_managed" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid     = "ListBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.pipeline.arn]
  }
  statement {
    sid     = "ReadRawDataAndScripts"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.pipeline.arn}/${var.raw_data_prefix}*",
      "${aws_s3_bucket.pipeline.arn}/${var.scripts_prefix}*",
    ]
  }
  statement {
    sid     = "WriteTransformedData"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.pipeline.arn}/${var.transformed_data_prefix}*"]
  }
  statement {
    sid     = "GlueTempDirectory"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.pipeline.arn}/tmp/*"]
  }
}

resource "aws_iam_policy" "glue_s3_access" {
  name   = "${var.project}-${var.environment}-glue-s3-policy"
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

data "aws_iam_policy_document" "glue_cloudwatch" {
  statement {
    sid     = "CloudWatchLogs"
    effect  = "Allow"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:AssociateKmsKey"]
    resources = ["arn:${data.aws_partition.current.partition}:logs:*:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_policy" "glue_cloudwatch" {
  name   = "${var.project}-${var.environment}-glue-cloudwatch-policy"
  policy = data.aws_iam_policy_document.glue_cloudwatch.json
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch.arn
}

resource "aws_glue_job" "energy_etl" {
  name              = var.glue_job_name
  description       = "Energy ETL pipeline: reads JSONL from raw_data/, transforms with PySpark, writes Parquet to transformed_data/."
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_job_timeout
  max_retries       = var.glue_max_retries

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/${var.scripts_prefix}transform.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.bucket_name}/tmp/spark-logs/"
    "--enable-metrics"                   = "true"
    "--SOURCE_PATH"                      = "s3://${var.bucket_name}/${var.raw_data_prefix}"
    "--SINK_PATH"                        = "s3://${var.bucket_name}/${var.transformed_data_prefix}"
    "--SOURCE_FORMAT"                    = "json"
    "--SINK_FORMAT"                      = "parquet"
    "--SINK_MODE"                        = "overwrite"
    "--TempDir"                          = "s3://${var.bucket_name}/tmp/"
  }

  tags = { Name = var.glue_job_name }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_managed,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_iam_role_policy_attachment.glue_cloudwatch,
  ]
}