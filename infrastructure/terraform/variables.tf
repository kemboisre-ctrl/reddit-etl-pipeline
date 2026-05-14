variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Globally unique S3 bucket name for the data lake"
  type        = string
}

variable "redshift_password" {
  description = "Admin password for Redshift Serverless"
  type        = string
  sensitive   = true
}