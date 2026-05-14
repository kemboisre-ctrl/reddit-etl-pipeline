terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# S3 BUCKET (Data Lake)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "transition-raw-to-ia"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# ---------------------------------------------------------------------------
# GLUE DATA CATALOG (Database + Crawler)
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "reddit" {
  name = "reddit_db"
}

resource "aws_glue_crawler" "raw_crawler" {
  name          = "reddit-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.reddit.name
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/raw/"
  }
  schedule = "cron(0 2 * * ? *)"  # Daily at 2 AM UTC
}

# ---------------------------------------------------------------------------
# IAM ROLE FOR GLUE / REDSHIFT
# ---------------------------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "reddit-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ---------------------------------------------------------------------------
# REDSHIFT SERVERLESS (Analytics Warehouse)
# ---------------------------------------------------------------------------
resource "aws_redshiftserverless_namespace" "reddit" {
  namespace_name       = "reddit-namespace"
  db_name              = "dev"
  admin_username       = "awsuser"
  admin_user_password  = var.redshift_password
  default_iam_role_arn = aws_iam_role.glue_role.arn
}

resource "aws_redshiftserverless_workgroup" "reddit" {
  workgroup_name      = "reddit-workgroup"
  namespace_name      = aws_redshiftserverless_namespace.reddit.namespace_name
  base_capacity       = 8  # RPUs (scales to zero when idle)
  publicly_accessible = true
}