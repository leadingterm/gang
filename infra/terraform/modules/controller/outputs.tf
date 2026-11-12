output "instance_id" {
  description = "ID of the controller EC2 instance."
  value       = aws_instance.this.id
}

output "permissions_boundary_arn" {
  description = "Permissions boundary ARN of the controller IAM role; null when none is set."
  value       = module.instance_role.permissions_boundary_arn
}

output "role_arn" {
  description = "ARN of the controller IAM role."
  value       = module.instance_role.role_arn
}

output "role_name" {
  description = "Name of the controller IAM role."
  value       = module.instance_role.role_name
}

output "secret_parameter_arns" {
  description = "ARNs of the SSM SecureString parameters the controller role may read; empty when none are configured."
  value       = local.secret_parameter_arns
}

output "state_volume_id" {
  description = "ID of the encrypted EBS state volume attached to the controller as /dev/sdf."
  value       = aws_ebs_volume.state.id
}
