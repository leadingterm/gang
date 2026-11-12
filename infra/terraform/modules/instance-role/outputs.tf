output "role_arn" {
  description = "ARN of the IAM role."
  value       = aws_iam_role.this.arn
}

output "role_name" {
  description = "Name of the IAM role."
  value       = aws_iam_role.this.name
}

output "permissions_boundary_arn" {
  description = "ARN of the permissions boundary of the IAM role. Null when none is set."
  value       = var.iam_permissions_boundary_arn
}

output "instance_profile_name" {
  description = "Name of the instance profile that carries the IAM role."
  value       = aws_iam_instance_profile.this.name
}

output "instance_profile_arn" {
  description = "ARN of the instance profile that carries the IAM role."
  value       = aws_iam_instance_profile.this.arn
}
