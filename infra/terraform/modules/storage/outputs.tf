output "cache_bucket_name" {
  description = "Name of the private build-cache S3 bucket."
  value       = aws_s3_bucket.this.bucket
}

output "cache_bucket_arn" {
  description = "ARN of the private build-cache S3 bucket."
  value       = aws_s3_bucket.this.arn
}

output "ca_key_arn" {
  description = "ARN of the KMS ECC_NIST_P256 CA signing key."
  value       = aws_kms_key.ca.arn
}

output "ca_signer_role_arn" {
  description = "ARN of the only role the CA key policy allows to sign (kms:Sign)."
  value       = var.ca_signer_role_arn
}

output "ca_key_alias" {
  description = "Alias name of the KMS CA signing key (for example alias/gang-ca)."
  value       = aws_kms_alias.this.name
}

output "dynamodb_table_name" {
  description = "Name of the shared state table, or null when disabled."
  value       = one(aws_dynamodb_table.this[*].name)
}

output "dynamodb_table_arn" {
  description = "ARN of the shared state table, or null when disabled."
  value       = one(aws_dynamodb_table.this[*].arn)
}

output "postgres_address" {
  description = "PostgreSQL endpoint address, or null when RDS is disabled."
  value       = one(aws_db_instance.postgres[*].address)
}

output "postgres_port" {
  description = "PostgreSQL endpoint port, or null when RDS is disabled."
  value       = one(aws_db_instance.postgres[*].port)
}

output "postgres_database" {
  description = "PostgreSQL database name, or null when RDS is disabled."
  value       = one(aws_db_instance.postgres[*].db_name)
}

output "postgres_user" {
  description = "PostgreSQL database username, or null when RDS is disabled."
  value       = one(aws_db_instance.postgres[*].username)
}

output "postgres_password_parameter_name" {
  description = "SSM SecureString name for the PostgreSQL password, or null when RDS is disabled."
  value       = one(aws_ssm_parameter.postgres_password[*].name)
}
