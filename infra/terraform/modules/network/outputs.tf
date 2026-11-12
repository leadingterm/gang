output "vpc_id" {
  description = "VPC ID."
  value       = aws_vpc.this.id
}

output "vpc_cidr_block" {
  description = "IPv4 CIDR block of the VPC."
  value       = aws_vpc.this.cidr_block
}

output "availability_zones" {
  description = "Availability zone names used by the subnets, in subnet index order."
  value       = local.availability_zones
}

output "public_subnet_ids" {
  description = "Public subnet IDs in availability zone order. Element 0 hosts the controller and the NAT gateway in single mode."
  value       = aws_subnet.public[*].id
}

output "public_subnet_cidr_blocks" {
  description = "Public subnet CIDR blocks in availability zone order."
  value       = aws_subnet.public[*].cidr_block
}

output "private_subnet_ids" {
  description = "Private subnet IDs in availability zone order. Worker fleets launch here."
  value       = aws_subnet.private[*].id
}

output "controller_security_group_id" {
  description = "Security group ID for the controller instance."
  value       = aws_security_group.controller.id
}

output "worker_security_group_id" {
  description = "Security group ID for worker instances."
  value       = aws_security_group.worker.id
}

output "controller_eip_allocation_id" {
  description = "Allocation ID of the controller Elastic IP, for the controller's EIP association."
  value       = aws_eip.controller.allocation_id
}

output "controller_public_ip" {
  description = "Public IPv4 address of the controller Elastic IP."
  value       = aws_eip.controller.public_ip
}

output "controller_public_dns" {
  description = "EC2 public DNS name of the controller Elastic IP."
  value = (
    local.region == "us-east-1"
    ? "ec2-${local.controller_public_ip_dashed}.compute-1.amazonaws.com"
    : "ec2-${local.controller_public_ip_dashed}.${local.region}.compute.amazonaws.com"
  )
}

output "controller_private_ip" {
  description = "Deterministic private IPv4 address of the controller: host 10 of public subnet 0."
  value       = local.controller_private_ip
}

output "controller_private_dns" {
  description = "EC2 private DNS name of the controller's fixed private address. Workers reach the controller worker listener through it."
  value = (
    local.region == "us-east-1"
    ? "ip-${local.controller_private_ip_dashed}.ec2.internal"
    : "ip-${local.controller_private_ip_dashed}.${local.region}.compute.internal"
  )
}

output "nat_public_ips" {
  description = "Public IPv4 addresses of the NAT gateways."
  value       = aws_eip.nat[*].public_ip
}

output "public_port" {
  description = "TCP port of the controller's public endpoint listener. Opened to client_ingress_cidrs and to the worker security group."
  value       = local.public_port
}

output "worker_port" {
  description = "TCP port of the controller's worker listener. Opened to the worker security group only."
  value       = local.worker_port
}
