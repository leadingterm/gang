output "region" {
  description = "AWS region of the deployment."
  value       = var.region
}

output "gang_toml" {
  description = "Rendered gang.toml, the value of the config SSM parameter. Contains no secrets: confidential values live in the SecureString parameters named by controller_secret_parameters and are referenced by file path."
  value       = local.gang_toml
}

output "config_parameter_name" {
  description = "Name of the active immutable configuration generation."
  value       = aws_ssm_parameter.config_generation.name
}

output "config_parameter_reference" {
  description = "Version-pinned active configuration reference used by controllers."
  value       = "${aws_ssm_parameter.config_generation.name}:1"
}

output "endpoint" {
  description = "Public endpoint (Bazel, gang login and workload clients) written to gang.toml."
  value       = local.endpoint
}

output "worker_endpoint" {
  description = "Worker endpoint written to gang.toml: the controller private DNS name for SQLite, or the internal load balancer hostname for PostgreSQL or DynamoDB."
  value       = local.worker_endpoint
}

output "controller_public_ip" {
  description = "Public IP of the controller Elastic IP, or null for a controller service."
  value       = module.network.controller_public_ip
}

output "load_balancer_dns_name" {
  description = "DNS name of the load balancer; point the hostname's CNAME here. Null without load_balancer."
  value       = one(module.load_balancer[*].dns_name)
}

output "controller_target_group_arns" {
  description = "Controller target groups checked after deploy."
  value = var.load_balancer == null ? [] : compact([
    module.load_balancer[0].serve_target_group_arn,
    module.load_balancer[0].internal_serve_target_group_arn,
    module.load_balancer[0].workers_target_group_arn,
  ])
}

output "certificate_validation_records" {
  description = "DNS records that validate the endpoint certificate; publish them where the hostname's zone is served. Empty without load_balancer."
  value       = var.load_balancer == null ? [] : module.load_balancer[0].certificate_validation_records
}

output "controller_public_dns" {
  description = "EC2 public DNS name of the controller Elastic IP, or null for a controller service."
  value       = module.network.controller_public_dns
}

output "issuer_url" {
  description = "Outbound web identity federation issuer URL of the account. Written to gang.toml as the aws-workers OIDC issuer."
  value       = module.worker_fleet.issuer_url
}

output "asg_names" {
  description = "Names of the worker Auto Scaling groups, in fleet order."
  value       = module.worker_fleet.asg_names
}

output "events_queue_url" {
  description = "URL of the AWS events queue, or null when AWS event observation is disabled."
  value       = var.aws_events_enabled ? aws_sqs_queue.events[0].url : null
}

output "events_dead_letter_queue_url" {
  description = "URL of the AWS events dead-letter queue, or null when AWS event observation is disabled."
  value       = var.aws_events_enabled ? aws_sqs_queue.events_dlq[0].url : null
}

output "controller_instance_id" {
  description = "Instance ID of the SQLite controller, or null for a controller service."
  value       = module.controller.instance_id
}

output "controller_shape" {
  description = "Controller resource shape: instance for SQLite or service for PostgreSQL or DynamoDB."
  value       = local.controller_shape
}

output "controller_count" {
  description = "Desired number of controller tasks or standalone instances."
  value       = var.controller_count
}

output "controller_service_name" {
  description = "ECS controller service name, or null for the instance shape."
  value       = module.controller.service_name
}

output "controller_cluster_name" {
  description = "ECS controller cluster name, or null for the instance shape."
  value       = module.controller.cluster_name
}

output "controller_task_definition_arn" {
  description = "ECS controller task definition ARN, or null for the instance shape."
  value       = module.controller.task_definition_arn
}

output "controller_capacity_asg_name" {
  description = "Controller ECS capacity Auto Scaling group name, or null for the instance shape."
  value       = module.controller.capacity_asg_name
}

output "controller_stop_timeout_seconds" {
  description = "Configured controller drain timeout in seconds."
  value       = var.controller_stop_timeout_seconds
}

output "controller_repository_url" {
  description = "Private ECR repository URL for the controller image."
  value       = module.controller.repository_url
}

output "controller_image_digest" {
  description = "Configured immutable controller image digest."
  value       = var.controller_image_digest
}

output "cache_bucket_name" {
  description = "Name of the S3 build cache bucket."
  value       = module.storage.cache_bucket_name
}

output "images" {
  description = "AMI id of each worker fleet and, for SQLite only, the controller."
  value       = local.node_ami_ids
}
