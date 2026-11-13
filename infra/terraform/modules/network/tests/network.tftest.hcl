mock_provider "aws" {
  mock_data "aws_region" {
    defaults = {
      region = "eu-north-1"
    }
  }

  mock_data "aws_availability_zones" {
    defaults = {
      names    = ["eu-north-1a", "eu-north-1b", "eu-north-1c", "eu-north-1d", "eu-north-1e", "eu-north-1f"]
      zone_ids = ["eun1-az1", "eun1-az2", "eun1-az3", "eun1-az4", "eun1-az5", "eun1-az6"]
    }
  }
}

variables {
  name                    = "gang"
  cidr_block              = "10.20.0.0/16"
  availability_zone_count = 3
  nat_mode                = "single"
  client_ingress_cidrs    = ["0.0.0.0/0"]
}

run "subnets_per_az" {
  command = plan

  variables {
    availability_zone_count = 3
  }

  assert {
    condition     = length(aws_subnet.public) == 3 && length(aws_subnet.private) == 3
    error_message = "Expected one public and one private subnet per availability zone."
  }

  assert {
    condition     = aws_subnet.public[*].cidr_block == ["10.20.0.0/20", "10.20.16.0/20", "10.20.32.0/20"] && aws_subnet.private[*].cidr_block == ["10.20.128.0/20", "10.20.144.0/20", "10.20.160.0/20"]
    error_message = "Public subnets must be 10.20.0.0/20, 10.20.16.0/20, 10.20.32.0/20 and private subnets 10.20.128.0/20, 10.20.144.0/20, 10.20.160.0/20 for the first three zones."
  }

  assert {
    condition     = alltrue([for c in concat(aws_subnet.public[*].cidr_block, aws_subnet.private[*].cidr_block) : cidrcontains(var.cidr_block, cidrhost(c, 0)) && cidrcontains(var.cidr_block, cidrhost(c, -1))])
    error_message = "Every public and private subnet CIDR must lie inside the VPC CIDR block."
  }

  assert {
    condition     = alltrue([for i, a in concat(aws_subnet.public[*].cidr_block, aws_subnet.private[*].cidr_block) : alltrue([for j, b in concat(aws_subnet.public[*].cidr_block, aws_subnet.private[*].cidr_block) : i == j || (!cidrcontains(a, cidrhost(b, 0)) && !cidrcontains(b, cidrhost(a, 0)))])])
    error_message = "No two public or private subnets may overlap."
  }

  assert {
    condition     = aws_subnet.public[*].availability_zone == ["eu-north-1a", "eu-north-1b", "eu-north-1c"] && aws_subnet.private[*].availability_zone == ["eu-north-1a", "eu-north-1b", "eu-north-1c"]
    error_message = "Public and private subnets must each cover the first three zones in name order."
  }

  assert {
    condition     = alltrue([for subnet in aws_subnet.public : subnet.map_public_ip_on_launch]) && alltrue([for subnet in aws_subnet.private : !subnet.map_public_ip_on_launch])
    error_message = "Only public subnets may assign public IPs on launch."
  }
}

run "two_zones_two_subnets_each" {
  command = plan

  variables {
    availability_zone_count = 2
  }

  assert {
    condition     = aws_subnet.public[*].cidr_block == ["10.20.0.0/20", "10.20.16.0/20"] && aws_subnet.private[*].cidr_block == ["10.20.128.0/20", "10.20.144.0/20"]
    error_message = "availability_zone_count = 2 must give public subnets 10.20.0.0/20, 10.20.16.0/20 and private subnets 10.20.128.0/20, 10.20.144.0/20."
  }
}

run "nat_single_one_gateway_shared_private_routes" {
  command = plan

  variables {
    nat_mode = "single"
  }

  assert {
    condition     = length(aws_nat_gateway.this) == 1 && length(aws_eip.nat) == 1
    error_message = "single mode must create exactly one NAT gateway with one Elastic IP."
  }

  assert {
    condition     = length(aws_route_table.private) == 1 && length(aws_route.private_default) == 1
    error_message = "single mode must share one private route table that defaults to the NAT gateway."
  }
}

run "nat_per_az_gateway_per_zone" {
  command = plan

  variables {
    nat_mode = "per_az"
  }

  assert {
    condition     = length(aws_nat_gateway.this) == 3 && length(aws_eip.nat) == 3
    error_message = "per_az mode must create one NAT gateway and Elastic IP per zone."
  }

  assert {
    condition     = length(aws_route_table.private) == 3 && length(aws_route.private_default) == 3
    error_message = "per_az mode must give each zone a private route table defaulting to its own NAT gateway."
  }
}

run "s3_gateway_endpoint_on_every_route_table" {
  command = plan

  variables {
    nat_mode = "per_az"
  }

  override_resource {
    target = aws_route_table.public
    values = {
      id = "rtb-public"
    }
  }

  override_resource {
    target = aws_route_table.private
    values = {
      id = "rtb-private"
    }
  }

  assert {
    condition     = aws_vpc_endpoint.this.service_name == "com.amazonaws.eu-north-1.s3" && aws_vpc_endpoint.this.vpc_endpoint_type == "Gateway"
    error_message = "An S3 gateway endpoint for the region must exist."
  }

  assert {
    condition     = length(aws_route_table.private) == 3 && length(aws_vpc_endpoint.this.route_table_ids) == 2 && contains(aws_vpc_endpoint.this.route_table_ids, aws_route_table.public.id) && contains(aws_vpc_endpoint.this.route_table_ids, "rtb-private")
    error_message = "The S3 gateway endpoint must be attached to the public route table and the private route table set."
  }
}

run "controller_worker_port_only_from_worker_sg" {
  command = plan

  assert {
    condition     = aws_vpc_security_group_ingress_rule.controller_worker.from_port == output.worker_port && aws_vpc_security_group_ingress_rule.controller_worker.to_port == output.worker_port && aws_vpc_security_group_ingress_rule.controller_worker.ip_protocol == "tcp"
    error_message = "The worker listener rule must open TCP worker_port only."
  }

  assert {
    condition     = aws_vpc_security_group_ingress_rule.controller_worker.referenced_security_group_id == aws_security_group.worker.id && aws_vpc_security_group_ingress_rule.controller_worker.cidr_ipv4 == null
    error_message = "TCP worker_port must be granted to the worker security group, never to a CIDR."
  }

  assert {
    condition     = alltrue([for rule in values(aws_vpc_security_group_ingress_rule.controller_public) : rule.from_port != output.worker_port && rule.to_port != output.worker_port]) && aws_vpc_security_group_ingress_rule.controller_worker_public.from_port != output.worker_port && aws_vpc_security_group_ingress_rule.controller_worker_public.to_port != output.worker_port
    error_message = "The worker_port must not be opened by any ingress rule other than the worker listener rule."
  }
}

run "public_port_from_worker_sg_and_client_cidrs_only" {
  command = plan

  variables {
    client_ingress_cidrs = ["198.51.100.0/24"]
  }

  assert {
    condition     = aws_vpc_security_group_ingress_rule.controller_worker_public.from_port == output.public_port && aws_vpc_security_group_ingress_rule.controller_worker_public.to_port == output.public_port && aws_vpc_security_group_ingress_rule.controller_worker_public.ip_protocol == "tcp"
    error_message = "The worker public rule must open TCP public_port only."
  }

  assert {
    condition     = aws_vpc_security_group_ingress_rule.controller_worker_public.referenced_security_group_id == aws_security_group.worker.id && aws_vpc_security_group_ingress_rule.controller_worker_public.cidr_ipv4 == null
    error_message = "The public_port listener for workers must be granted to the worker security group, never to a CIDR."
  }

  assert {
    condition     = alltrue([for rule in values(aws_vpc_security_group_ingress_rule.controller_public) : rule.referenced_security_group_id == null])
    error_message = "The client_ingress_cidrs public rules must not reference a security group."
  }
}

run "public_port_only_from_client_ingress_cidrs" {
  command = plan

  variables {
    client_ingress_cidrs = ["198.51.100.0/24", "192.0.2.7/32"]
  }

  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.controller_public) == 2
    error_message = "Each client_ingress_cidrs entry must get one public rule."
  }

  assert {
    condition     = alltrue([for rule in values(aws_vpc_security_group_ingress_rule.controller_public) : contains(var.client_ingress_cidrs, rule.cidr_ipv4) && rule.from_port == output.public_port && rule.to_port == output.public_port && rule.ip_protocol == "tcp"])
    error_message = "The public_port listener must be reachable only from client_ingress_cidrs."
  }
}

run "no_client_ingress_without_client_cidrs" {
  command = plan

  variables {
    client_ingress_cidrs = []
  }

  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.controller_public) == 0
    error_message = "No client CIDRs must create no direct client ingress rules."
  }

  assert {
    condition     = aws_vpc_security_group_ingress_rule.controller_worker_public.from_port == 443
    error_message = "Workers must retain access to the public listener on port 443 without client CIDRs."
  }
}

run "derived_hostnames_fixed_eip_eu_north_1" {
  command = plan

  override_resource {
    target = aws_eip.controller
    values = {
      public_ip = "203.0.113.10"
    }
  }

  assert {
    condition     = output.controller_public_ip == "203.0.113.10"
    error_message = "The controller public IP must be the Elastic IP."
  }

  assert {
    condition     = output.controller_public_dns == "ec2-203-0-113-10.eu-north-1.compute.amazonaws.com"
    error_message = "The controller public DNS must be derived from the Elastic IP for a non-us-east-1 region."
  }

  assert {
    condition     = output.controller_private_ip == "10.20.0.10"
    error_message = "The controller private IP must be host 10 of public subnet 0."
  }

  assert {
    condition     = output.controller_private_dns == "ip-10-20-0-10.eu-north-1.compute.internal"
    error_message = "The controller private DNS must be derived from the fixed private IP for a non-us-east-1 region."
  }
}

run "derived_hostnames_us_east_1" {
  command = plan

  override_data {
    target = data.aws_region.this
    values = {
      region = "us-east-1"
    }
  }

  override_data {
    target = data.aws_availability_zones.this
    values = {
      names    = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1e", "us-east-1f"]
      zone_ids = ["use1-az1", "use1-az2", "use1-az3", "use1-az4", "use1-az5", "use1-az6"]
    }
  }

  override_resource {
    target = aws_eip.controller
    values = {
      public_ip = "203.0.113.10"
    }
  }

  assert {
    condition     = output.controller_public_dns == "ec2-203-0-113-10.compute-1.amazonaws.com"
    error_message = "us-east-1 must use the compute-1 public DNS form."
  }

  assert {
    condition     = output.controller_private_dns == "ip-10-20-0-10.ec2.internal"
    error_message = "us-east-1 must use the ec2.internal private DNS form."
  }
}

run "rejects_unknown_nat_mode" {
  command = plan

  variables {
    nat_mode = "regional"
  }

  expect_failures = [
    var.nat_mode,
  ]
}

run "rejects_zero_availability_zones" {
  command = plan

  variables {
    availability_zone_count = 0
  }

  expect_failures = [
    var.availability_zone_count,
  ]
}

run "rejects_more_than_six_availability_zones" {
  command = plan

  variables {
    availability_zone_count = 7
  }

  expect_failures = [
    var.availability_zone_count,
  ]
}

run "rejects_ipv6_client_ingress_cidr" {
  command = plan

  variables {
    client_ingress_cidrs = ["2001:db8::/32"]
  }

  expect_failures = [
    var.client_ingress_cidrs,
  ]
}
