data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

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
  bucket  = aws_s3_bucket.pipeline.id
  key     = var.raw_data_prefix
  content = ""
  lifecycle { ignore_changes = [content, etag] }
}

resource "aws_s3_object" "transformed_data_prefix" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = var.transformed_data_prefix
  content = ""
  lifecycle { ignore_changes = [content, etag] }
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = var.scripts_prefix
  content = ""
  lifecycle { ignore_changes = [content, etag] }
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
  name                  = "AWSGlueServiceRole-${var.glue_job_name}-${var.environment}"
  assume_role_policy    = data.aws_iam_policy_document.glue_assume_role.json
  force_detach_policies = true
  tags = { Name = "AWSGlueServiceRole-${var.glue_job_name}-${var.environment}" }
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    sid     = "ReadRawData"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.pipeline.arn, "${aws_s3_bucket.pipeline.arn}/${var.raw_data_prefix}*"]
  }
  statement {
    sid     = "ReadGlueScripts"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.pipeline.arn, "${aws_s3_bucket.pipeline.arn}/${var.scripts_prefix}*"]
  }
  statement {
    sid     = "WriteTransformedData"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.pipeline.arn, "${aws_s3_bucket.pipeline.arn}/${var.transformed_data_prefix}*"]
  }
  statement {
    sid     = "GlueTempDirectory"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.pipeline.arn, "${aws_s3_bucket.pipeline.arn}/tmp/*"]
  }
  statement {
    sid     = "SparkUILogs"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.pipeline.arn, "${aws_s3_bucket.pipeline.arn}/spark-logs/*"]
  }
}

resource "aws_iam_policy" "glue_s3_access" {
  name   = "${var.glue_job_name}-s3-access-${var.environment}"
  policy = data.aws_iam_policy_document.glue_s3_access.json
  tags   = { Name = "${var.glue_job_name}-s3-access-${var.environment}" }
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

data "aws_iam_policy_document" "glue_cloudwatch_logs" {
  statement {
    sid    = "CloudWatchLogsGlue"
    effect = "Allow"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:AssociateKmsKey"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_policy" "glue_cloudwatch_logs" {
  name   = "${var.glue_job_name}-cloudwatch-logs-${var.environment}"
  policy = data.aws_iam_policy_document.glue_cloudwatch_logs.json
  tags   = { Name = "${var.glue_job_name}-cloudwatch-logs-${var.environment}" }
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_logs" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_logs.arn
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}-${var.environment}"
  retention_in_days = 30
  tags              = { Name = "/aws-glue/jobs/${var.glue_job_name}-${var.environment}" }
}

resource "aws_glue_job" "energy_etl" {
  name        = "${var.glue_job_name}-${var.environment}"
  role_arn    = aws_iam_role.glue_service_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/${var.scripts_prefix}transform.py"
    python_version  = "3"
  }

  default_arguments = {
    "--SOURCE_S3_PATH"                    = "s3://${var.bucket_name}/${var.raw_data_prefix}"
    "--SINK_S3_PATH"                      = "s3://${var.bucket_name}/${var.transformed_data_prefix}"
    "--TempDir"                           = "s3://${var.bucket_name}/tmp/"
    "--job-bookmark-option"               = var.enable_glue_job_bookmark ? "job-bookmark-enable" : "job-bookmark-disable"
    "--enable-spark-ui"                   = "true"
    "--spark-event-logs-path"             = "s3://${var.bucket_name}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = tostring(var.enable_continuous_cloudwatch_log)
    "--enable-continuous-log-filter"      = "true"
    "--enable-metrics"                    = "true"
    "--job-language"                      = "python"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = { Name = "${var.glue_job_name}-${var.environment}" }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_policy,
    aws_iam_role_policy_attachment.glue_s3_access,
    aws_iam_role_policy_attachment.glue_cloudwatch_logs,
    aws_s3_bucket.pipeline,
    aws_cloudwatch_log_group.glue_job,
  ]
}