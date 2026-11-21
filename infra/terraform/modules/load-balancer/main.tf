resource "aws_acm_certificate" "this" {
  domain_name       = var.hostname
  validation_method = "DNS"

  tags = merge(var.tags, {
    Name = var.name
  })

  lifecycle {
    create_before_destroy = true
  }
}

# DNS lives outside this module, so wait for the operator to publish the record
# in certificate_validation_records.
resource "aws_acm_certificate_validation" "this" {
  certificate_arn = aws_acm_certificate.this.arn

  timeouts {
    create = var.certificate_validation_timeout
  }
}

resource "aws_security_group" "this" {
  name        = var.name
  description = "gang load balancer: TLS from client_ingress_cidrs, forwarded to the controller public listener"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_vpc_security_group_ingress_rule" "client" {
  for_each = toset(var.client_ingress_cidrs)

  security_group_id = aws_security_group.this.id
  cidr_ipv4         = each.value
  from_port         = var.port
  to_port           = var.port
  ip_protocol       = "tcp"
  description       = "gang public endpoint"

  tags = merge(var.tags, {
    Name = "${var.name}-client-${each.value}"
  })
}

resource "aws_vpc_security_group_egress_rule" "controller" {
  security_group_id            = aws_security_group.this.id
  referenced_security_group_id = var.controller_security_group_id
  from_port                    = var.port
  to_port                      = var.port
  ip_protocol                  = "tcp"
  description                  = "gang controller public listener"

  tags = merge(var.tags, {
    Name = "${var.name}-controller-egress"
  })
}

# Client traffic and health checks both come from the load balancer's security group.
resource "aws_vpc_security_group_ingress_rule" "controller" {
  security_group_id            = var.controller_security_group_id
  referenced_security_group_id = aws_security_group.this.id
  from_port                    = var.port
  to_port                      = var.port
  ip_protocol                  = "tcp"
  description                  = "gang public endpoint listener, load balancer only"

  tags = merge(var.tags, {
    Name = "${var.name}-controller-ingress"
  })
}

resource "aws_vpc_security_group_egress_rule" "health" {
  security_group_id            = aws_security_group.this.id
  referenced_security_group_id = var.controller_security_group_id
  from_port                    = var.health_port
  to_port                      = var.health_port
  ip_protocol                  = "tcp"
  description                  = "gang controller health listener"
  tags                         = merge(var.tags, { Name = "${var.name}-health-egress" })
}

resource "aws_vpc_security_group_ingress_rule" "controller_health" {
  security_group_id            = var.controller_security_group_id
  referenced_security_group_id = aws_security_group.this.id
  from_port                    = var.health_port
  to_port                      = var.health_port
  ip_protocol                  = "tcp"
  description                  = "gang health listener, public load balancer only"
  tags                         = merge(var.tags, { Name = "${var.name}-health-ingress" })
}

resource "aws_eip" "public" {
  for_each = var.static_public_ips ? (var.shared_database ? var.public_subnets_by_az : { controller = var.subnet_id }) : {}
  domain   = "vpc"
  tags     = merge(var.tags, { Name = "${var.name}-public-${each.key}" })
}

resource "aws_lb" "this" {
  name                             = var.name
  internal                         = false
  load_balancer_type               = "network"
  subnets                          = var.static_public_ips ? null : (var.shared_database ? var.public_subnet_ids : [var.subnet_id])
  enable_cross_zone_load_balancing = var.shared_database ? true : null
  security_groups                  = [aws_security_group.this.id]

  dynamic "subnet_mapping" {
    for_each = aws_eip.public
    content {
      subnet_id     = var.shared_database ? var.public_subnets_by_az[subnet_mapping.key] : var.subnet_id
      allocation_id = subnet_mapping.value.id
    }
  }

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_lb_target_group" "this" {
  name        = var.name
  port        = var.port
  protocol    = "TLS"
  target_type = "instance"
  vpc_id      = var.vpc_id

  # PROXY preserves effective client identity while disabling source preservation
  # avoids NLB target connection tuple collisions.
  preserve_client_ip     = var.shared_database ? "false" : "true"
  proxy_protocol_v2      = var.shared_database
  deregistration_delay   = var.shared_database ? var.deregistration_delay : null
  connection_termination = var.shared_database ? false : null

  dynamic "target_health_state" {
    for_each = var.shared_database ? [1] : []
    content {
      enable_unhealthy_connection_termination = false
      unhealthy_draining_interval            = 0
    }
  }

  # Readiness requires the controller to serve, hold the lease, and accept workers.
  # HTTPS health checks fail against Gang's TLS-1.3-only targets.
  health_check {
    protocol            = "HTTP"
    port                = var.health_port
    path                = local.health.ready_path
    matcher             = "200"
    interval            = local.health.interval_seconds
    timeout             = local.health.timeout_seconds
    healthy_threshold   = local.health.threshold
    unhealthy_threshold = local.health.threshold
  }

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_lb_target_group_attachment" "this" {
  count = var.shared_database ? 0 : 1

  target_group_arn = aws_lb_target_group.this.arn
  target_id        = var.controller_instance_id
  port             = var.port
}


# The HTTPS default port means the endpoint carries no port. h2 then http/1.1
# matches the public listener's ALPN; Bazel's gRPC needs h2.
resource "aws_lb_listener" "this" {
  load_balancer_arn = aws_lb.this.arn
  port              = var.port
  protocol          = "TLS"
  certificate_arn   = aws_acm_certificate_validation.this.certificate_arn
  ssl_policy        = var.ssl_policy
  alpn_policy       = "HTTP2Preferred"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_route53_zone" "this" {
  count = var.private_zone_id == null ? 1 : 0

  name    = var.hostname
  comment = "Resolves the gang endpoint to the controller inside the VPC, so workers reach its own listener and verify the gang CA"

  vpc {
    vpc_id = var.vpc_id
  }

  tags = merge(var.tags, {
    Name = var.name
  })
}

data "aws_route53_zone" "existing" {
  count   = var.private_zone_id == null ? 0 : 1
  zone_id = var.private_zone_id
}

resource "aws_route53_record" "controller" {
  zone_id = var.private_zone_id != null ? var.private_zone_id : aws_route53_zone.this[0].zone_id
  name    = var.hostname
  type    = "A"
  ttl     = var.shared_database ? null : 60
  records = var.shared_database ? null : [var.controller_private_ip]

  lifecycle {
    precondition {
      # The provider always returns a boolean; OpenTofu's mock leaves this non-computed field null.
      condition = var.private_zone_id == null ? true : (
        data.aws_route53_zone.existing[0].private_zone != false && (
          lower(trimsuffix(var.hostname, ".")) == lower(trimsuffix(data.aws_route53_zone.existing[0].name, ".")) ||
          endswith(lower(trimsuffix(var.hostname, ".")), ".${lower(trimsuffix(data.aws_route53_zone.existing[0].name, "."))}")
        )
      )
      error_message = "The private hosted zone must be private and authoritative for hostname or a parent domain."
    }
  }

  dynamic "alias" {
    for_each = var.shared_database ? [1] : []
    content {
      name                   = aws_lb.internal[0].dns_name
      zone_id                = aws_lb.internal[0].zone_id
      evaluate_target_health = true
    }
  }
}

moved {
  from = aws_lb_target_group_attachment.this
  to   = aws_lb_target_group_attachment.this[0]
}

moved {
  from = aws_route53_zone.this
  to   = aws_route53_zone.this[0]
}

locals {
  health = jsondecode(file("${path.module}/health-check.json"))

  internal_listeners = var.shared_database ? {
    serve   = var.port
    workers = var.workers_port
  } : {}

  internal_sources = var.shared_database ? {
    for pair in setproduct(keys(local.internal_listeners), ["worker", "controller"]) :
    "${pair[0]}-${pair[1]}" => {
      port              = local.internal_listeners[pair[0]]
      security_group_id = pair[1] == "worker" ? var.worker_security_group_id : var.controller_security_group_id
    }
  } : {}
}

resource "aws_security_group" "internal" {
  count = var.shared_database ? 1 : 0

  name        = "${var.name}-internal"
  description = "gang internal load balancer"
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name}-internal" })
}

resource "aws_vpc_security_group_ingress_rule" "internal" {
  for_each = local.internal_sources

  security_group_id            = aws_security_group.internal[0].id
  referenced_security_group_id = each.value.security_group_id
  from_port                    = each.value.port
  to_port                      = each.value.port
  ip_protocol                  = "tcp"
  tags                         = merge(var.tags, { Name = "${var.name}-internal-${each.key}" })
}

resource "aws_vpc_security_group_egress_rule" "internal" {
  for_each = local.internal_listeners

  security_group_id            = aws_security_group.internal[0].id
  referenced_security_group_id = var.controller_security_group_id
  from_port                    = each.value
  to_port                      = each.value
  ip_protocol                  = "tcp"
  tags                         = merge(var.tags, { Name = "${var.name}-internal-${each.key}-egress" })
}

resource "aws_vpc_security_group_ingress_rule" "controller_internal" {
  for_each = local.internal_listeners

  security_group_id            = var.controller_security_group_id
  referenced_security_group_id = aws_security_group.internal[0].id
  from_port                    = each.value
  to_port                      = each.value
  ip_protocol                  = "tcp"
  tags                         = merge(var.tags, { Name = "${var.name}-controller-internal-${each.key}" })
}

resource "aws_vpc_security_group_egress_rule" "internal_health" {
  count = var.shared_database ? 1 : 0

  security_group_id            = aws_security_group.internal[0].id
  referenced_security_group_id = var.controller_security_group_id
  from_port                    = var.health_port
  to_port                      = var.health_port
  ip_protocol                  = "tcp"
  description                  = "gang controller health listener"
  tags                         = merge(var.tags, { Name = "${var.name}-internal-health-egress" })
}

resource "aws_vpc_security_group_ingress_rule" "controller_internal_health" {
  count = var.shared_database ? 1 : 0

  security_group_id            = var.controller_security_group_id
  referenced_security_group_id = aws_security_group.internal[0].id
  from_port                    = var.health_port
  to_port                      = var.health_port
  ip_protocol                  = "tcp"
  description                  = "gang health listener, internal load balancer only"
  tags                         = merge(var.tags, { Name = "${var.name}-internal-health-ingress" })
}

resource "aws_lb" "internal" {
  count = var.shared_database ? 1 : 0

  name                             = "${substr(var.name, 0, 23)}-internal"
  internal                         = true
  load_balancer_type               = "network"
  subnets                          = var.private_subnet_ids
  security_groups                  = [aws_security_group.internal[0].id]
  enable_cross_zone_load_balancing = true
  tags                             = merge(var.tags, { Name = "${var.name}-internal" })
}

data "aws_network_interface" "public" {
  for_each = var.shared_database ? { for index, subnet in var.public_subnet_ids : tostring(index) => subnet } : {}

  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  filter {
    name   = "subnet-id"
    values = [each.value]
  }
  filter {
    name   = "description"
    values = ["ELB ${aws_lb.this.arn_suffix}"]
  }
}

data "aws_network_interface" "internal" {
  for_each = var.shared_database ? { for index, subnet in var.private_subnet_ids : tostring(index) => subnet } : {}

  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  filter {
    name   = "subnet-id"
    values = [each.value]
  }
  filter {
    name   = "description"
    values = ["ELB ${aws_lb.internal[0].arn_suffix}"]
  }
}

resource "aws_lb_target_group" "internal" {
  for_each = local.internal_listeners

  name                   = "${substr(var.name, 0, 22)}-i-${each.key}"
  port                   = each.value
  protocol               = "TCP"
  target_type            = "instance"
  vpc_id                 = var.vpc_id
  preserve_client_ip     = "false"
  proxy_protocol_v2      = true
  deregistration_delay   = var.deregistration_delay
  connection_termination = false

  target_health_state {
    enable_unhealthy_connection_termination = false
    unhealthy_draining_interval            = 0
  }

  health_check {
    protocol            = "HTTP"
    port                = var.health_port
    path                = local.health.ready_path
    matcher             = "200"
    interval            = local.health.interval_seconds
    timeout             = local.health.timeout_seconds
    healthy_threshold   = local.health.threshold
    unhealthy_threshold = local.health.threshold
  }

  tags = merge(var.tags, { Name = "${var.name}-internal-${each.key}" })
}

resource "aws_lb_listener" "internal" {
  for_each = local.internal_listeners

  load_balancer_arn = aws_lb.internal[0].arn
  port              = each.value
  protocol          = "TCP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.internal[each.key].arn
  }
  tags = merge(var.tags, { Name = "${var.name}-internal-${each.key}" })
}
