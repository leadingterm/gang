output "dns_name" {
  description = "DNS name of the load balancer. Point the hostname's CNAME record here."
  value       = aws_lb.this.dns_name
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
