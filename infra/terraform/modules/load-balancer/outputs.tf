output "dns_name" {
  description = "DNS name of the load balancer. Point the hostname's CNAME record here."
  value       = aws_lb.this.dns_name
}

output "public_static_ips" {
  description = "Allocated public load balancer addresses, keyed by availability zone or controller."
  value       = { for key, eip in aws_eip.public : key => eip.public_ip }
}

output "zone_id" {
  description = "Route 53 hosted zone of the load balancer, for alias records."
  value       = aws_lb.this.zone_id
}

output "certificate_arn" {
  description = "ARN of the validated certificate used by the listener."
  value       = aws_acm_certificate_validation.this.certificate_arn
}

output "certificate_validation_records" {
  description = "DNS records that validate the certificate: publish each where the hostname's zone is served."
  value = [for option in aws_acm_certificate.this.domain_validation_options : {
    name  = option.resource_record_name
    type  = option.resource_record_type
    value = option.resource_record_value
  }]
}

output "security_group_id" {
  description = "Security group ID of the load balancer."
  value       = aws_security_group.this.id
}

output "serve_target_group_arn" {
  description = "ARN of the public serve target group."
  value       = aws_lb_target_group.this.arn
}

output "internal_serve_target_group_arn" {
  description = "ARN of the internal serve target group, or null when disabled."
  value       = var.shared_database ? aws_lb_target_group.internal["serve"].arn : null
}

output "workers_target_group_arn" {
  description = "ARN of the internal worker target group, or null when disabled."
  value       = var.shared_database ? aws_lb_target_group.internal["workers"].arn : null
}

output "target_groups" {
  description = "Listener-associated target groups and container ports for the shared-database controller service."
  # ECS requires each target group to be associated with a load balancer first.
  depends_on = [aws_lb_listener.this, aws_lb_listener.internal]
  value = var.shared_database ? {
    serve          = { arn = aws_lb_target_group.this.arn, port = var.port }
    internal_serve = { arn = aws_lb_target_group.internal["serve"].arn, port = var.port }
    workers        = { arn = aws_lb_target_group.internal["workers"].arn, port = var.workers_port }
  } : {}
}

output "internal_dns_name" {
  description = "DNS name of the internal load balancer, or null when disabled."
  value       = one(aws_lb.internal[*].dns_name)
}

output "health_port" {
  description = "Health listener port checked by the public serve target group."
  value       = tonumber(aws_lb_target_group.this.health_check[0].port)
}

output "proxy_protocol_trusted_cidrs" {
  description = "Exact public and internal NLB interface addresses trusted to send PROXY headers."
  # New controllers require the target groups' framing and health-path updates first.
  depends_on = [aws_lb_target_group.this, aws_lb_target_group.internal]

  value = sort(distinct(flatten([
    for eni in concat(values(data.aws_network_interface.public), values(data.aws_network_interface.internal)) :
    [for ip in eni.private_ips : "${ip}/32"]
  ])))
}
