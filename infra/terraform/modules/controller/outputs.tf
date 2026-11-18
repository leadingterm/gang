output "instance_id" {
  description = "ID of the controller EC2 instance, or null for the group shape."
  value       = local.instance_shape ? aws_instance.this[0].id : null
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
  description = "ID of the encrypted EBS state volume attached as /dev/sdg, or null for the group shape."
  value       = local.instance_shape ? aws_ebs_volume.state[0].id : null
}

output "autoscaling_group_name" {
  description = "Name of the controller Auto Scaling group, or null for the instance shape."
  value       = local.group_shape ? aws_autoscaling_group.controller[0].name : null
}

output "config_digest" {
  description = "Configuration digest supplied to the controller."
  value       = var.config_digest
}
