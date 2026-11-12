data "aws_availability_zones" "this" {
  state = "available"

  # Standard zones only: Local and Wavelength zones cannot host the subnets used here.
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

data "aws_region" "this" {}

locals {
  region             = data.aws_region.this.region
  availability_zones = slice(data.aws_availability_zones.this.names, 0, var.availability_zone_count)

  nat_count                 = var.nat_mode == "single" ? 1 : var.availability_zone_count
  private_route_table_count = var.nat_mode == "per_az" ? var.availability_zone_count : 1

  public_port = 443
  worker_port = 9090

  # The controller sits in public subnet 0 at a fixed host offset so its private
  # address, and therefore its private DNS name, is deterministic.
  controller_private_ip = cidrhost(aws_subnet.public[0].cidr_block, 10)

  controller_public_ip_dashed  = replace(aws_eip.controller.public_ip, ".", "-")
  controller_private_ip_dashed = replace(local.controller_private_ip, ".", "-")
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr_block
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-igw"
  })
}

resource "aws_subnet" "public" {
  count = var.availability_zone_count

  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.cidr_block, 4, count.index)
  map_public_ip_on_launch = true
  vpc_id                  = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-public-${local.availability_zones[count.index]}"
    Tier = "public"
  })
}

resource "aws_subnet" "private" {
  count = var.availability_zone_count

  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.cidr_block, 4, 8 + count.index)
  map_public_ip_on_launch = false
  vpc_id                  = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-private-${local.availability_zones[count.index]}"
    Tier = "private"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = merge(var.tags, {
    Name = "${var.name}-public"
  })
}

resource "aws_route_table_association" "public" {
  count = var.availability_zone_count

  route_table_id = aws_route_table.public.id
  subnet_id      = aws_subnet.public[count.index].id
}

resource "aws_route_table" "private" {
  count = local.private_route_table_count

  vpc_id = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-private-${count.index}"
  })
}

resource "aws_route_table_association" "private" {
  count = var.availability_zone_count

  # per_az gives each zone its own table routed to its own NAT gateway; otherwise all zones share table 0.
  route_table_id = aws_route_table.private[var.nat_mode == "per_az" ? count.index : 0].id
  subnet_id      = aws_subnet.private[count.index].id
}

resource "aws_eip" "nat" {
  count = local.nat_count

  domain = "vpc"

  tags = merge(var.tags, {
    Name = "${var.name}-nat-${count.index}"
  })
}

resource "aws_nat_gateway" "this" {
  count = local.nat_count

  allocation_id = aws_eip.nat[count.index].allocation_id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(var.tags, {
    Name = "${var.name}-nat-${count.index}"
  })

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route" "private_default" {
  count = local.private_route_table_count

  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[count.index].id
  route_table_id         = aws_route_table.private[count.index].id
}

resource "aws_vpc_endpoint" "this" {
  route_table_ids   = concat([aws_route_table.public.id], aws_route_table.private[*].id)
  service_name      = "com.amazonaws.${local.region}.s3"
  vpc_endpoint_type = "Gateway"
  vpc_id            = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-s3"
  })
}

resource "aws_eip" "controller" {
  domain = "vpc"

  tags = merge(var.tags, {
    Name = "${var.name}-controller"
  })

  depends_on = [aws_internet_gateway.this]
}

resource "aws_security_group" "controller" {
  name        = "${var.name}-controller"
  description = "gang controller: public endpoint from client_ingress_cidrs, worker listener from workers"
  vpc_id      = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-controller"
  })
}

resource "aws_security_group" "worker" {
  name        = "${var.name}-worker"
  description = "gang workers: no ingress, egress for NAT, S3 and controller"
  vpc_id      = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${var.name}-worker"
  })
}

resource "aws_vpc_security_group_ingress_rule" "controller_public" {
  for_each = toset(var.client_ingress_cidrs)

  security_group_id = aws_security_group.controller.id
  cidr_ipv4         = each.value
  from_port         = local.public_port
  to_port           = local.public_port
  ip_protocol       = "tcp"
  description       = "gang public endpoint listener"

  tags = merge(var.tags, {
    Name = "${var.name}-controller-public-${each.value}"
  })
}

resource "aws_vpc_security_group_ingress_rule" "controller_worker" {
  security_group_id            = aws_security_group.controller.id
  referenced_security_group_id = aws_security_group.worker.id
  from_port                    = local.worker_port
  to_port                      = local.worker_port
  ip_protocol                  = "tcp"
  description                  = "gang worker listener, workers only"

  tags = merge(var.tags, {
    Name = "${var.name}-controller-worker"
  })
}

resource "aws_vpc_security_group_ingress_rule" "controller_worker_public" {
  security_group_id            = aws_security_group.controller.id
  referenced_security_group_id = aws_security_group.worker.id
  from_port                    = local.public_port
  to_port                      = local.public_port
  ip_protocol                  = "tcp"
  description                  = "gang public endpoint listener, workers only"

  tags = merge(var.tags, {
    Name = "${var.name}-controller-worker-public"
  })
}

resource "aws_vpc_security_group_egress_rule" "controller" {
  security_group_id = aws_security_group.controller.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "all egress"

  tags = merge(var.tags, {
    Name = "${var.name}-controller-egress"
  })
}

resource "aws_vpc_security_group_egress_rule" "worker" {
  security_group_id = aws_security_group.worker.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "all egress"

  tags = merge(var.tags, {
    Name = "${var.name}-worker-egress"
  })
}
