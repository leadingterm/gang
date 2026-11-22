mock_provider "aws" {
  mock_data "aws_network_interface" {
    defaults = { private_ips = ["10.20.1.10"] }
  }

  mock_data "aws_route53_zone" {
    defaults = {
      name = "example.com."
    }
  }

  mock_resource "aws_lb" {
    defaults = {
      arn        = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:loadbalancer/net/gang/0000000000000000"
      arn_suffix = "net/gang/0000000000000000"
    }
  }

  mock_resource "aws_lb_target_group" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/gang/0000000000000000"
    }
  }

  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:eu-north-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"
    }
  }
}

variables {
  name                           = "gang"
  vpc_id                         = "vpc-00000000000000000"
  subnet_id                      = "subnet-00000000000000000"
  hostname                       = "gang.example.com"
  port                           = 443
  workers_port                   = 9090
  health_port                    = 8181
  client_ingress_cidrs           = ["198.51.100.0/24", "203.0.113.0/24"]
  controller_security_group_id   = "sg-00000000000000001"
  controller_instance_id         = "i-00000000000000000"
  controller_private_ip          = "10.20.0.10"
  ssl_policy                     = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_validation_timeout = "2h"
}

run "terminates_tls_with_http2_and_the_validated_certificate" {
  command = apply

  assert {
    condition     = aws_lb_listener.this.port == 443 && aws_lb_listener.this.protocol == "TLS" && aws_lb_listener.this.alpn_policy == "HTTP2Preferred" && aws_lb_listener.this.ssl_policy == var.ssl_policy
    error_message = "The public listener must use TLS on 443 with HTTP2Preferred ALPN and the configured TLS 1.3 policy."
  }

  assert {
    condition     = aws_lb_listener.this.certificate_arn == aws_acm_certificate_validation.this.certificate_arn && aws_lb_listener.this.default_action[0].target_group_arn == aws_lb_target_group.this.arn
    error_message = "The listener must use the validated certificate and forward to the controller target group."
  }

  assert {
    condition     = !aws_lb.this.internal && aws_lb.this.load_balancer_type == "network" && aws_lb.this.subnets == toset([var.subnet_id]) && aws_lb.this.security_groups == toset([aws_security_group.this.id])
    error_message = "The public network load balancer must use the controller subnet and its own security group."
  }
}

run "no_static_addresses_by_default" {
  command = plan
  assert {
    condition     = length(aws_eip.public) == 0 && length(output.public_static_ips) == 0
    error_message = "The default load balancer must not allocate public EIPs."
  }
}

run "static_addresses_follow_availability_zones" {
  command = apply
  variables {
    shared_database          = true
    public_subnet_ids        = ["subnet-public-a", "subnet-public-b"]
    public_subnets_by_az     = { az-a = "subnet-public-a", az-b = "subnet-public-b" }
    static_public_ips        = true
    private_subnet_ids       = ["subnet-private"]
    worker_security_group_id = "sg-workers"
    private_zone_id          = "ZPRIVATE"
  }
  assert {
    condition     = toset(keys(aws_eip.public)) == toset(["az-a", "az-b"]) && toset(keys(output.public_static_ips)) == toset(["az-a", "az-b"])
    error_message = "A static EIP must be allocated for every public availability zone."
  }
  assert {
    condition = alltrue([for key, eip in aws_eip.public :
      anytrue([for mapping in aws_lb.this.subnet_mapping : mapping.subnet_id == var.public_subnets_by_az[key] && mapping.allocation_id == eip.id])
    ])
    error_message = "Each public subnet must use its own stable EIP allocation."
  }
}

run "single_controller_uses_semantic_key" {
  command = plan
  variables {
    static_public_ips = true
  }
  assert {
    condition     = toset(keys(aws_eip.public)) == toset(["controller"])
    error_message = "The single-controller EIP must use a stable controller key."
  }
}

run "forwards_tls_to_the_controller_with_the_client_address" {
  command = apply

  assert {
    condition     = aws_lb_target_group.this.protocol == "TLS" && aws_lb_target_group.this.port == 443 && aws_lb_target_group.this.target_type == "instance" && aws_lb_target_group.this.preserve_client_ip == "true" && !aws_lb_target_group.this.proxy_protocol_v2
    error_message = "The SQLite target group must forward TLS without PROXY while preserving client addresses."
  }

  assert {
    condition     = aws_lb_target_group.this.name == var.name && length(output.target_groups) == 0
    error_message = "SQLite retains its instance target group and exposes no ECS service target groups."
  }

  assert {
    condition     = aws_lb_target_group_attachment.this[0].target_id == var.controller_instance_id && aws_lb_target_group_attachment.this[0].target_group_arn == aws_lb_target_group.this.arn
    error_message = "The SQLite controller must be registered with the public target group."
  }

  assert {
    condition     = aws_lb_target_group.this.health_check[0].protocol == "HTTP" && aws_lb_target_group.this.health_check[0].port == tostring(var.health_port) && aws_lb_target_group.this.health_check[0].path == jsondecode(file("${path.module}/health-check.json")).ready_path && aws_lb_target_group.this.health_check[0].path == "/ready" && aws_lb_target_group.this.health_check[0].matcher == "200" && aws_lb_target_group.this.health_check[0].interval == jsondecode(file("${path.module}/health-check.json")).interval_seconds && aws_lb_target_group.this.health_check[0].timeout == jsondecode(file("${path.module}/health-check.json")).timeout_seconds && aws_lb_target_group.this.health_check[0].healthy_threshold == jsondecode(file("${path.module}/health-check.json")).threshold && aws_lb_target_group.this.health_check[0].unhealthy_threshold == jsondecode(file("${path.module}/health-check.json")).threshold
    error_message = "The controller target group must use the configured HTTP readiness check."
  }
}

run "only_the_load_balancer_reaches_the_controller_listener" {
  command = apply

  assert {
    condition     = aws_vpc_security_group_ingress_rule.controller.security_group_id == var.controller_security_group_id && aws_vpc_security_group_ingress_rule.controller.referenced_security_group_id == aws_security_group.this.id && aws_vpc_security_group_ingress_rule.controller.from_port == 443 && aws_vpc_security_group_ingress_rule.controller.to_port == 443
    error_message = "The controller listener rule must allow port 443 from the load balancer security group."
  }

  assert {
    condition     = toset(keys(aws_vpc_security_group_ingress_rule.client)) == toset(var.client_ingress_cidrs) && alltrue([for rule in values(aws_vpc_security_group_ingress_rule.client) : rule.from_port == 443 && rule.to_port == 443 && rule.ip_protocol == "tcp"])
    error_message = "Each configured client CIDR must have a TCP 443 ingress rule on the load balancer."
  }

  assert {
    condition     = aws_vpc_security_group_egress_rule.controller.referenced_security_group_id == var.controller_security_group_id
    error_message = "The load balancer egress rule must reference the controller security group."
  }

  assert {
    condition     = aws_vpc_security_group_egress_rule.health.security_group_id == aws_security_group.this.id && aws_vpc_security_group_egress_rule.health.referenced_security_group_id == var.controller_security_group_id && aws_vpc_security_group_egress_rule.health.from_port == var.health_port && aws_vpc_security_group_egress_rule.health.to_port == var.health_port && aws_vpc_security_group_egress_rule.health.ip_protocol == "tcp" && aws_vpc_security_group_ingress_rule.controller_health.security_group_id == var.controller_security_group_id && aws_vpc_security_group_ingress_rule.controller_health.referenced_security_group_id == aws_security_group.this.id && aws_vpc_security_group_ingress_rule.controller_health.from_port == var.health_port && aws_vpc_security_group_ingress_rule.controller_health.to_port == var.health_port && aws_vpc_security_group_ingress_rule.controller_health.ip_protocol == "tcp"
    error_message = "The public load balancer must reach the controller health port in both directions."
  }
}

run "workers_resolve_the_hostname_to_the_controller" {
  command = apply

  assert {
    condition     = aws_route53_zone.this[0].name == "gang.example.com" && one(aws_route53_zone.this[0].vpc).vpc_id == var.vpc_id
    error_message = "The endpoint hostname must have a private hosted zone associated with the controller VPC."
  }

  assert {
    condition     = aws_route53_record.controller.type == "A" && aws_route53_record.controller.records == toset(["10.20.0.10"])
    error_message = "The private endpoint record must resolve to the controller's private IPv4 address."
  }
}

run "certificate_is_dns_validated_for_the_hostname" {
  command = apply

  assert {
    condition     = aws_acm_certificate.this.domain_name == var.hostname && aws_acm_certificate.this.validation_method == "DNS"
    error_message = "The certificate must use DNS validation for the endpoint hostname."
  }
}

run "outputs_the_certificate_validation_records" {
  command = apply

  override_resource {
    target = aws_acm_certificate.this
    values = {
      arn = "arn:aws:acm:eu-north-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"
      domain_validation_options = [{
        domain_name           = "gang.example.com"
        resource_record_name  = "_x1.gang.example.com."
        resource_record_type  = "CNAME"
        resource_record_value = "_x2.acm-validations.aws."
      }]
    }
  }

  assert {
    condition = output.certificate_validation_records == [{
      name  = "_x1.gang.example.com."
      type  = "CNAME"
      value = "_x2.acm-validations.aws."
    }]
    error_message = "The certificate validation output must expose the DNS record name, type and value."
  }
}

run "rejects_a_policy_without_tls13" {
  command = plan

  variables {
    ssl_policy = "ELBSecurityPolicy-2016-08"
  }

  expect_failures = [
    var.ssl_policy,
  ]
}

run "rejects_an_ipv6_client_cidr" {
  command = plan

  variables {
    client_ingress_cidrs = ["::/0"]
  }

  expect_failures = [
    var.client_ingress_cidrs,
  ]
}

run "shared_database_load_balancers" {
  command = plan

  variables {
    shared_database          = true
    public_subnet_ids        = ["subnet-public"]
    private_subnet_ids       = ["subnet-private"]
    worker_security_group_id = "sg-workers"
    private_zone_id          = "ZPRIVATE"
    deregistration_delay     = 120
    health_port              = 8080
  }

  assert {
    condition     = aws_lb_listener.this.protocol == "TLS" && aws_lb_listener.this.port == 443 && aws_lb_target_group.this.port == 443
    error_message = "The public NLB must use one TLS client listener on port 443."
  }
  assert {
    condition     = aws_lb_listener.this.alpn_policy == "HTTP2Preferred" && aws_lb_listener.this.ssl_policy == var.ssl_policy
    error_message = "The shared public listener must retain its TLS 1.3 policy and HTTP/2 ALPN."
  }
  assert {
    condition     = aws_lb_target_group.this.target_type == "instance" && aws_lb_target_group.this.name == var.name && alltrue([for key, tg in aws_lb_target_group.internal : tg.target_type == "instance" && tg.name == "${substr(var.name, 0, 22)}-i-${key}"])
    error_message = "Shared controllers must use the original instance target group identities."
  }
  assert {
    condition     = aws_lb.internal[0].internal && toset(keys(aws_lb_listener.internal)) == toset(["serve", "workers"]) && aws_lb_listener.internal["serve"].port == 443 && aws_lb_listener.internal["workers"].port == 9090 && toset(keys(aws_lb_target_group.internal)) == toset(["serve", "workers"]) && aws_lb_target_group.internal["serve"].port == 443 && aws_lb_target_group.internal["workers"].port == 9090 && alltrue([for listener in values(aws_lb_listener.internal) : listener.protocol == "TCP" && listener.certificate_arn == null])
    error_message = "The internal NLB must use TCP passthrough on ports 443 and 9090 only."
  }
  assert {
    condition     = aws_lb_target_group.this.protocol == "TLS" && alltrue([for tg in values(aws_lb_target_group.internal) : tg.protocol == "TCP"]) && alltrue([for tg in concat([aws_lb_target_group.this], values(aws_lb_target_group.internal)) : tonumber(tg.deregistration_delay) == 120 && !tg.connection_termination && tg.health_check[0].protocol == "HTTP" && tg.health_check[0].port == tostring(var.health_port) && tg.health_check[0].path == jsondecode(file("${path.module}/health-check.json")).ready_path && tg.health_check[0].path == "/ready" && tg.health_check[0].matcher == "200" && tg.health_check[0].interval == jsondecode(file("${path.module}/health-check.json")).interval_seconds && tg.health_check[0].timeout == jsondecode(file("${path.module}/health-check.json")).timeout_seconds && tg.health_check[0].healthy_threshold == jsondecode(file("${path.module}/health-check.json")).threshold && tg.health_check[0].unhealthy_threshold == jsondecode(file("${path.module}/health-check.json")).threshold])
    error_message = "Every shared-database target group must drain gracefully and check readiness."
  }
  assert {
    condition     = alltrue([for tg in concat([aws_lb_target_group.this], values(aws_lb_target_group.internal)) : !tg.target_health_state[0].enable_unhealthy_connection_termination && tg.target_health_state[0].unhealthy_draining_interval == 0])
    error_message = "Unhealthy targets must stop new routing immediately without terminating established connections, independently of the configured deregistration delay."
  }
  assert {
    condition     = aws_vpc_security_group_egress_rule.health.security_group_id == aws_security_group.this.id && aws_vpc_security_group_egress_rule.health.referenced_security_group_id == var.controller_security_group_id && aws_vpc_security_group_egress_rule.health.from_port == var.health_port && aws_vpc_security_group_egress_rule.health.to_port == var.health_port && aws_vpc_security_group_egress_rule.health.ip_protocol == "tcp" && aws_vpc_security_group_ingress_rule.controller_health.security_group_id == var.controller_security_group_id && aws_vpc_security_group_ingress_rule.controller_health.referenced_security_group_id == aws_security_group.this.id && aws_vpc_security_group_ingress_rule.controller_health.from_port == var.health_port && aws_vpc_security_group_ingress_rule.controller_health.to_port == var.health_port && aws_vpc_security_group_ingress_rule.controller_health.ip_protocol == "tcp"
    error_message = "The public load balancer must reach the controller health port."
  }
  assert {
    condition     = aws_vpc_security_group_egress_rule.internal_health[0].security_group_id == aws_security_group.internal[0].id && aws_vpc_security_group_egress_rule.internal_health[0].referenced_security_group_id == var.controller_security_group_id && aws_vpc_security_group_egress_rule.internal_health[0].from_port == var.health_port && aws_vpc_security_group_egress_rule.internal_health[0].to_port == var.health_port && aws_vpc_security_group_egress_rule.internal_health[0].ip_protocol == "tcp" && aws_vpc_security_group_ingress_rule.controller_internal_health[0].security_group_id == var.controller_security_group_id && aws_vpc_security_group_ingress_rule.controller_internal_health[0].referenced_security_group_id == aws_security_group.internal[0].id && aws_vpc_security_group_ingress_rule.controller_internal_health[0].from_port == var.health_port && aws_vpc_security_group_ingress_rule.controller_internal_health[0].to_port == var.health_port && aws_vpc_security_group_ingress_rule.controller_internal_health[0].ip_protocol == "tcp"
    error_message = "The internal load balancer must reach the controller health port."
  }
  assert {
    condition     = alltrue([for tg in concat([aws_lb_target_group.this], values(aws_lb_target_group.internal)) : tg.preserve_client_ip == "false" && tg.proxy_protocol_v2])
    error_message = "All three target groups must send PROXY v2 and disable source preservation."
  }
  assert {
    condition     = aws_lb_listener.this.default_action[0].target_group_arn == aws_lb_target_group.this.arn && alltrue([for key, listener in aws_lb_listener.internal : listener.default_action[0].type == "forward" && listener.default_action[0].target_group_arn == aws_lb_target_group.internal[key].arn])
    error_message = "Each listener must forward to its controller target group."
  }
  assert {
    condition     = aws_route53_record.controller.zone_id == "ZPRIVATE" && aws_route53_record.controller.name == "gang.example.com" && length(aws_route53_record.controller.alias) == 1 && length(aws_lb_target_group_attachment.this) == 0
    error_message = "The private hostname must alias the internal NLB."
  }
  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.internal) == 4 && length(aws_vpc_security_group_ingress_rule.controller_internal) == 2 && length(aws_vpc_security_group_egress_rule.internal) == 2
    error_message = "The two internal listeners must have matching security group rules."
  }
}

run "shared_service_target_group_outputs" {
  command = apply

  variables {
    shared_database          = true
    public_subnet_ids        = ["subnet-public"]
    private_subnet_ids       = ["subnet-private"]
    worker_security_group_id = "sg-workers"
  }

  assert {
    condition = (
      toset(keys(output.target_groups)) == toset(["serve", "internal_serve", "workers"]) &&
      output.target_groups.serve.arn == aws_lb_target_group.this.arn &&
      output.target_groups.serve.port == 443 &&
      output.target_groups.internal_serve.arn == aws_lb_target_group.internal["serve"].arn &&
      output.target_groups.internal_serve.port == 443 &&
      output.target_groups.workers.arn == aws_lb_target_group.internal["workers"].arn &&
      output.target_groups.workers.port == 9090
    )
    error_message = "ECS must receive all three target group ARNs and their container ports."
  }

  assert {
    condition     = aws_lb_listener.this.default_action[0].target_group_arn == output.target_groups.serve.arn && aws_lb_listener.internal["serve"].default_action[0].target_group_arn == output.target_groups.internal_serve.arn && aws_lb_listener.internal["workers"].default_action[0].target_group_arn == output.target_groups.workers.arn
    error_message = "All ECS target groups must be associated with their corresponding listeners."
  }
}


run "shared_proxy_interfaces" {
  command = apply
  variables {
    shared_database          = true
    public_subnet_ids        = ["subnet-public", "subnet-public-b"]
    private_subnet_ids       = ["subnet-private", "subnet-private-b"]
    worker_security_group_id = "sg-workers"
  }
  override_data {
    target = data.aws_network_interface.public
    values = { private_ips = ["10.20.1.11", "10.20.1.10"] }
  }
  override_data {
    target = data.aws_network_interface.internal
    values = { private_ips = ["10.20.2.10"] }
  }
  assert {
    condition     = output.proxy_protocol_trusted_cidrs == tolist(["10.20.1.10/32", "10.20.1.11/32", "10.20.2.10/32"])
    error_message = "Trust must contain every public and internal ENI address as a sorted /32."
  }
  assert {
    condition = length(data.aws_network_interface.public) == 2 && length(data.aws_network_interface.internal) == 2 && toset(flatten([
      for eni in values(data.aws_network_interface.public) : [
        for filter in eni.filter : tolist(filter.values) if filter.name == "subnet-id"
      ]
      ])) == toset(var.public_subnet_ids) && toset(flatten([
      for eni in values(data.aws_network_interface.internal) : [
        for filter in eni.filter : tolist(filter.values) if filter.name == "subnet-id"
      ]
    ])) == toset(var.private_subnet_ids)
    error_message = "ENI discovery must query every public and private subnet."
  }
  assert {
    condition = alltrue([
      for filter in data.aws_network_interface.public["0"].filter :
      filter.name == "vpc-id" ? contains(filter.values, var.vpc_id) :
      filter.name == "subnet-id" ? contains(filter.values, "subnet-public") :
      filter.name == "description" ? contains(filter.values, "ELB ${aws_lb.this.arn_suffix}") : false
      ]) && length(data.aws_network_interface.public["0"].filter) == 3 && alltrue([
      for filter in data.aws_network_interface.internal["0"].filter :
      filter.name == "vpc-id" ? contains(filter.values, var.vpc_id) :
      filter.name == "subnet-id" ? contains(filter.values, "subnet-private") :
      filter.name == "description" ? contains(filter.values, "ELB ${aws_lb.internal[0].arn_suffix}") : false
    ]) && length(data.aws_network_interface.internal["0"].filter) == 3
    error_message = "Each ENI lookup must constrain VPC, its subnet and its own NLB description."
  }
}


run "rejects_private_zone_outside_hostname" {
  command = plan

  variables {
    private_zone_id = "ZPRIVATE"
  }

  override_data {
    target = data.aws_route53_zone.existing
    values = {
      name = "otherexample.com."
    }
  }

  expect_failures = [aws_route53_record.controller]
}


run "rejects_missing_public_subnets" {
  command = plan
  variables {
    shared_database          = true
    private_subnet_ids       = ["subnet-private"]
    worker_security_group_id = "sg-workers"
  }
  expect_failures = [var.public_subnet_ids]
}

run "rejects_missing_private_subnets" {
  command = plan
  variables {
    shared_database          = true
    public_subnet_ids        = ["subnet-public"]
    worker_security_group_id = "sg-workers"
  }
  expect_failures = [var.private_subnet_ids]
}

run "rejects_missing_worker_security_group" {
  command = plan
  variables {
    shared_database    = true
    public_subnet_ids  = ["subnet-public"]
    private_subnet_ids = ["subnet-private"]
  }
  expect_failures = [var.worker_security_group_id]
}

run "shared_database_defaults_preserve_connections_without_routing_delay" {
  command = plan
  variables {
    shared_database          = true
    public_subnet_ids        = ["subnet-public"]
    private_subnet_ids       = ["subnet-private"]
    worker_security_group_id = "sg-workers"
    private_zone_id          = "ZPRIVATE"
  }
  assert {
    condition     = var.deregistration_delay == 0 && alltrue([for tg in concat([aws_lb_target_group.this], values(aws_lb_target_group.internal)) : tonumber(tg.deregistration_delay) == 0 && !tg.connection_termination && !tg.target_health_state[0].enable_unhealthy_connection_termination && tg.target_health_state[0].unhealthy_draining_interval == 0])
    error_message = "All shared-database target groups must default to zero deregistration and unhealthy draining delays with both connection-termination flags disabled."
  }
}

run "accepts_zero_deregistration_delay" {
  command = plan
  variables { deregistration_delay = 0 }
}

run "rejects_negative_deregistration_delay" {
  command = plan
  variables { deregistration_delay = -1 }
  expect_failures = [var.deregistration_delay]
}

run "rejects_invalid_workers_port" {
  command = plan
  variables { workers_port = 1.5 }
  expect_failures = [var.workers_port]
}

run "rejects_invalid_health_port" {
  command = plan
  variables { health_port = 1.5 }
  expect_failures = [var.health_port]
}

run "rejects_missing_instance" {
  command = plan
  variables { controller_instance_id = null }
  expect_failures = [var.controller_instance_id]
}


run "rejects_missing_private_ip" {
  command = plan
  variables { controller_private_ip = null }
  expect_failures = [var.controller_private_ip]
}
