output "region" {
  description = "AWS region of the deployment."
  value       = var.region
}

output "gang_toml" {
  description = "Rendered gang.toml, the value of the config SSM parameter. Contains no secrets: confidential values live in the SecureString parameters named by controller_secret_parameters and are referenced by file path."
  value       = local.gang_toml
}

output "config_parameter_name" {
  description = "Name of the SSM parameter that holds the rendered gang.toml. Instances read it at boot from their user data."
  value       = aws_ssm_parameter.config.name
}

output "endpoint" {
  description = "Public endpoint (Bazel, gang login and workload clients) written to gang.toml."
  value       = local.endpoint
}

output "worker_endpoint" {
  description = "Worker endpoint written to gang.toml. Resolves to the controller private DNS name inside the VPC."
  value       = local.worker_endpoint
}

output "controller_public_ip" {
  description = "Public IP of the controller Elastic IP: point the endpoint DNS record here, or, with load_balancer, the controller's egress address."
  value       = module.network.controller_public_ip
}

output "load_balancer_dns_name" {
  description = "DNS name of the load balancer; point the hostname's CNAME here. Null without load_balancer."
  value       = one(module.load_balancer[*].dns_name)
}

output "certificate_validation_records" {
  description = "DNS records that validate the endpoint certificate; publish them where the hostname's zone is served. Empty without load_balancer."
  value       = var.load_balancer == null ? [] : module.load_balancer[0].certificate_validation_records
}

output "controller_public_dns" {
  description = "EC2 public DNS name of the controller Elastic IP."
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

output "controller_instance_id" {
  description = "Instance id of the controller."
  value       = module.controller.instance_id
}

output "cache_bucket_name" {
  description = "Name of the S3 build cache bucket."
  value       = module.storage.cache_bucket_name
}
