output "asg_names" {
  description = "Names of the worker Auto Scaling groups in fleet order: each fleet's group, then its on-demand twin when it has one."
  value       = [for name in local.group_names : aws_autoscaling_group.this[name].name]
}

output "memory_mib_by_fleet" {
  description = "Smallest nominal EC2 memory size in MiB per fleet, used as a prelaunch capacity estimate."
  value       = { for fleet in var.fleets : fleet.name => min([for type in fleet.instance_types : data.aws_ec2_instance_type.this[type].memory_size]...) }
}

output "permissions_boundary_arn" {
  description = "Permissions boundary ARN of the worker IAM role; null when none is set."
  value       = module.instance_role.permissions_boundary_arn
}

output "role_arn" {
  description = "ARN of the worker IAM role (EC2 instance role, also the role that requests workload identity tokens)."
  value       = module.instance_role.role_arn
}

output "role_name" {
  description = "Name of the worker IAM role."
  value       = module.instance_role.role_name
}

output "issuer_url" {
  description = "Account-wide outbound web identity federation issuer URL; the [identity.providers.aws-workers.oidc] issuer_url."
  value       = one(concat(aws_iam_outbound_web_identity_federation.this[*].issuer_identifier, data.aws_iam_outbound_web_identity_federation.this[*].issuer_identifier))
}
