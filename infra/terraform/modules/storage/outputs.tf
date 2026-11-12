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
