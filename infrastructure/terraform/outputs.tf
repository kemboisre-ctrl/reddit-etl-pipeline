output "s3_bucket_name" {
  description = "Name of the created S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "redshift_namespace_id" {
  description = "Redshift Serverless namespace ID"
  value       = aws_redshiftserverless_namespace.reddit.namespace_id
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = aws_redshiftserverless_workgroup.reddit.endpoint
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.reddit.name
}