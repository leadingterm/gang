output "instance_id" {
  description = "ID of the SQLite controller instance, or null for the service shape."
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
  description = "ID of the encrypted SQLite state volume, or null for the service shape."
  value       = local.instance_shape ? aws_ebs_volume.state[0].id : null
}

output "service_name" {
  description = "ECS service name, or null for the instance shape."
  value       = local.service_shape ? aws_ecs_service.controller[0].name : null
}

output "cluster_name" {
  description = "ECS cluster name, or null for the instance shape."
  value       = local.service_shape ? aws_ecs_cluster.controller[0].name : null
}

output "task_definition_arn" {
  description = "Controller task definition ARN, or null for the instance shape."
  value       = local.service_shape ? aws_ecs_task_definition.controller[0].arn : null
}

output "capacity_asg_name" {
  description = "ECS capacity Auto Scaling group name, or null for the instance shape."
  value       = local.service_shape ? aws_autoscaling_group.capacity[0].name : null
}

output "repository_url" {
  description = "Private controller image repository URL, or null for the instance shape."
  value       = local.service_shape ? aws_ecr_repository.controller[0].repository_url : null
}

output "repository_arn" {
  description = "Private controller image repository ARN, or null for the instance shape."
  value       = local.service_shape ? aws_ecr_repository.controller[0].arn : null
}

output "config_digest" {
  description = "Configuration digest supplied to the controller."
  value       = var.config_digest
}
