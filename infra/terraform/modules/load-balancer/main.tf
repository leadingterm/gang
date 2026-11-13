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
  from_port         = 443
  to_port           = 443
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

resource "aws_lb" "this" {
  name               = var.name
  internal           = false
  load_balancer_type = "network"
  subnets            = [var.subnet_id]
  security_groups    = [aws_security_group.this.id]

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

  # Gang's per-address rate limits see the client's address.
  preserve_client_ip = "true"

  # A TCP check avoids spending rate-limit buckets on the only open HTTPS route.
  health_check {
    protocol = "TCP"
    port     = "traffic-port"
  }

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_lb_target_group_attachment" "this" {
  target_group_arn = aws_lb_target_group.this.arn
  target_id        = var.controller_instance_id
  port             = var.port
}

# The HTTPS default port means the endpoint carries no port. h2 then http/1.1
# matches the public listener's ALPN; Bazel's gRPC needs h2.
resource "aws_lb_listener" "this" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
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
  name    = var.hostname
  comment = "Resolves the gang endpoint to the controller inside the VPC, so workers reach its own listener and verify the gang CA"

  vpc {
    vpc_id = var.vpc_id
  }

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_route53_record" "controller" {
  zone_id = aws_route53_zone.this.zone_id
  name    = var.hostname
  type    = "A"
  ttl     = 60
  records = [var.controller_private_ip]
}
