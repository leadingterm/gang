mock_provider "aws" {
  mock_data "aws_network_interface" {
    defaults = { private_ips = ["10.20.1.10"] }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      region = "eu-north-1"
    }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "000000000000"
    }
  }

  mock_data "aws_availability_zones" {
    defaults = {
      names    = ["az-a", "az-b", "az-c"]
      zone_ids = ["a", "b", "c"]
    }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      instance_types = ["c6id.4xlarge", "c6id.4xlarge", "c6id.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge"]
      locations      = ["az-a", "az-b", "az-c", "az-a", "az-b", "az-c"]
    }
  }

  # The controller and worker-fleet modules require UEFI-capable instance types.
  mock_data "aws_ec2_instance_type" {
    defaults = {
      boot_modes                 = ["legacy-bios", "uefi"]
      memory_size                = 65536
      supported_architectures    = ["x86_64"]
      instance_storage_supported = true
      hypervisor                 = "nitro"
      instance_disks             = [{ count = 1, size = 950, type = "ssd" }]
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_resource "aws_kms_key" {
    defaults = {
      arn    = "arn:aws:kms:eu-north-1:000000000000:key/11111111-2222-3333-4444-555555555555"
      key_id = "11111111-2222-3333-4444-555555555555"
    }
  }

  mock_resource "aws_dynamodb_table" {
    defaults = {
      arn = "arn:aws:dynamodb:eu-north-1:000000000000:table/gang-state"
    }
  }

  mock_resource "aws_launch_template" {
    defaults = {
      id = "lt-0123456789abcdef0"
    }
  }

  mock_resource "aws_eip" {
    defaults = {
      public_ip = "203.0.113.10"
    }
  }

  mock_resource "aws_lb" {
    defaults = {
      arn        = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:loadbalancer/net/gang/0000000000000000"
      arn_suffix = "net/gang/0000000000000000"
      dns_name   = "gang-0000000000.elb.eu-north-1.amazonaws.com"
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

  mock_resource "aws_iam_outbound_web_identity_federation" {
    defaults = {
      issuer_identifier = "https://issuer.example.com/123456789012"
    }
  }
}

mock_provider "http" {
  mock_data "http" {
    defaults = {
      status_code   = 200
      response_body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-aarch64.raw.zst\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-x86_64.raw.zst\n"
    }
  }
}

override_data {
  target = data.aws_ami.image
  values = {
    name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
    boot_mode        = "uefi"
    tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
    deprecation_time = ""
  }
}

override_resource {
  target = aws_sqs_queue.events
  values = {
    arn = "arn:aws:sqs:eu-north-1:000000000000:gang-events"
    url = "https://sqs.eu-north-1.amazonaws.com/000000000000/gang-events"
    id  = "https://sqs.eu-north-1.amazonaws.com/000000000000/gang-events"
  }
}

override_resource {
  target = aws_sqs_queue.events_dlq
  values = {
    arn = "arn:aws:sqs:eu-north-1:000000000000:gang-events-dlq"
    url = "https://sqs.eu-north-1.amazonaws.com/000000000000/gang-events-dlq"
    id  = "https://sqs.eu-north-1.amazonaws.com/000000000000/gang-events-dlq"
  }
}

override_resource {
  target = aws_cloudwatch_event_rule.spot
  values = {
    arn = "arn:aws:events:eu-north-1:000000000000:rule/gang-spot-events"
  }
}

override_resource {
  target = aws_cloudwatch_event_rule.fleet
  values = {
    arn = "arn:aws:events:eu-north-1:000000000000:rule/gang-fleet-events"
  }
}

override_resource {
  target = module.worker_fleet.module.instance_role.aws_iam_instance_profile.this
  values = {
    arn = "arn:aws:iam::000000000000:instance-profile/gang-worker"
  }
}

override_resource {
  target = module.worker_fleet.module.instance_role.aws_iam_role.this
  values = {
    arn = "arn:aws:iam::000000000000:role/gang-worker"
  }
}

override_resource {
  target = module.controller.module.instance_role.aws_iam_role.this
  values = {
    arn = "arn:aws:iam::000000000000:role/gang-controller"
  }
}

override_resource {
  target = module.controller.module.instance_role.aws_iam_instance_profile.this
  values = {
    arn = "arn:aws:iam::000000000000:instance-profile/gang-controller"
  }
}

override_data {
  target = module.storage.data.aws_iam_policy_document.ca_key
  values = {
    json = "{}"
  }
}

variables {
  client_token_providers   = ["google", "github-actions"]
  region                   = "eu-north-1"
  cidr_block               = "10.20.0.0/16"
  cache_bucket_name        = "example-gang-cache"
  controller_instance_type = "c7i.large"
  default_isa              = "x86-64"
  trust_domain             = "example.com"
  fleets = [
    {
      name               = "gang-worker-x86-64"
      isa                = "x86-64"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "200GiB"
    },
    {
      name               = "gang-worker-aarch64"
      isa                = "aarch64"
      capacity           = "on-demand"
      instance_types     = ["c7gd.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "200GiB"
    },
  ]
}

run "mixed_instance_fleets_use_smallest_nominal_memory" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type.worker["c6id.4xlarge"]
    values = { boot_modes = ["uefi"], memory_size = 65536 }
  }

  override_data {
    target = data.aws_ec2_instance_type.worker["c7gd.4xlarge"]
    values = { boot_modes = ["uefi"], memory_size = 131072 }
  }

  variables {
    fleets = [
      {
        name               = "small-first"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge", "c7gd.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
      },
      {
        name               = "large-first"
        isa                = "x86-64"
        capacity           = "on-demand"
        instance_types     = ["c7gd.4xlarge", "c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
      },
    ]
  }

  assert {
    condition     = length(regexall("memory_per_instance = \"65536MiB\"", output.gang_toml)) == 2
    error_message = "Both fleets must render the smallest nominal EC2 memory regardless of instance type order."
  }
}

run "direct_endpoint_without_load_balancer" {
  command = plan

  assert {
    condition     = can(regex("(?s)\\[server\\.http2\\][^\\[]*max_connections = 4096\n", output.gang_toml))
    error_message = "The direct endpoint must default to 4096 public listener connections."
  }

  assert {
    condition     = length(module.load_balancer) == 0
    error_message = "The direct endpoint must not create a load balancer."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "endpoint_trust")
    error_message = "The direct endpoint must use the default endpoint trust."
  }

  assert {
    condition     = output.load_balancer_dns_name == null && length(output.certificate_validation_records) == 0
    error_message = "The direct endpoint must not expose load balancer DNS or certificate validation records."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "health_listen")
    error_message = "The direct endpoint must not enable the load balancer health listener."
  }
  assert {
    condition     = !strcontains(output.gang_toml, "standby_routability_delay") && !strcontains(output.gang_toml, "[server.leadership]")
    error_message = "A direct SQLite deployment must use the default zero standby delay."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "[server.proxy_protocol]")
    error_message = "SQLite direct deployments must not enable PROXY."
  }
  assert {
    condition     = !strcontains(output.gang_toml, "[execution.egress]") && !strcontains(output.gang_toml, "[sandbox.bwrap.egress]") && !strcontains(output.gang_toml, "[fleets.actions]") && !strcontains(output.gang_toml, "[fleets.actions.resources]")
    error_message = "Default fleets must deny network and omit action policy and egress tables."
  }
}

run "network_grant_requires_egress" {
  command = plan
  variables {
    fleets = [{
      name              = "worker", isa = "x86-64", capacity = "spot"
      instance_types    = ["c6id.4xlarge"]
      max_instances     = 4, vcpus_per_instance = 15
      disk_per_instance = "200GiB"
      actions           = { grants = ["network"] }
    }]
  }
  expect_failures = [terraform_data.input_checks]
}

run "rejects_unknown_capability" {
  command = plan
  variables {
    fleets = [{
      name              = "worker", isa = "x86-64", capacity = "spot"
      instance_types    = ["c6id.4xlarge"]
      max_instances     = 4, vcpus_per_instance = 15
      disk_per_instance = "200GiB"
      actions           = { grants = ["unknown"] }
    }]
  }
  expect_failures = [var.fleets]
}

run "rejects_invalid_action_cpu" {
  command = plan
  variables {
    fleets = [{
      name              = "worker", isa = "x86-64", capacity = "spot"
      instance_types    = ["c6id.4xlarge"]
      max_instances     = 4, vcpus_per_instance = 15
      disk_per_instance = "200GiB"
      actions           = { resources = { cpu_count = 0 } }
    }]
  }
  expect_failures = [var.fleets]
}

run "rejects_empty_egress_dns" {
  command = plan
  variables {
    execution_egress = { protected_networks = ["10.0.0.0/8"], dns_servers = [] }
  }
  expect_failures = [var.execution_egress]
}

run "protects_assigned_addresses_with_empty_additional_networks" {
  command = apply
  variables {
    hostname      = "gang.example.com"
    load_balancer = {}
    execution_egress = {
      protected_networks = []
      dns_servers        = ["1.1.1.1"]
      setup_timeout      = "12s"
    }
    fleets = [{
      name              = "worker", isa = "x86-64", capacity = "spot"
      instance_types    = ["c6id.4xlarge"]
      max_instances     = 4, vcpus_per_instance = 15
      disk_per_instance = "200GiB"
      actions           = { grants = ["network"], resources = { cpu_count = 2, memory = "1GiB", disk = "2GiB" } }
    }]
  }
  override_resource {
    target = module.network.aws_vpc.this
    # IPv4 comes from var.cidr_block; only override the computed IPv6 attribute.
    values = { ipv6_cidr_block = "2600:1f00:abcd:1200::/56" }
  }
  assert {
    condition = alltrue([
      contains(local.protected_networks, "10.20.0.0/16"),
      contains(local.protected_networks, "2600:1f00:abcd:1200::/56"),
      contains(local.protected_networks, "${module.network.controller_public_ip}/32"),
      alltrue([for ip in values(module.load_balancer[0].public_static_ips) : contains(local.protected_networks, "${ip}/32")]),
      length(module.load_balancer[0].public_static_ips) == 1,
    ])
    error_message = "Even with no additional networks, protection must include the assigned VPC CIDRs, controller EIP and every public NLB EIP."
  }
  assert {
    condition     = strcontains(output.gang_toml, "[execution.egress]\nprotected_networks = ${jsonencode(local.protected_networks)}") && length(local.protected_networks) > 0 && strcontains(output.gang_toml, "[sandbox.bwrap.egress]")
    error_message = "The rendered egress policy must include the nonempty derived protected networks and helper section."
  }
  assert {
    condition     = strcontains(output.gang_toml, "[fleets.actions]\ngrants = ${jsonencode(sort(tolist(var.fleets[0].actions.grants)))}") && strcontains(output.gang_toml, "[fleets.actions.resources]\ncpu_count = 2\nmemory = \"1GiB\"\ndisk = \"2GiB\"")
    error_message = "The rendered action tables must include the sorted grants and configured resource ceilings."
  }
}

run "network_grant_defaults_additional_networks_to_empty" {
  command = apply
  variables {
    execution_egress = { dns_servers = ["1.1.1.1"] }
    fleets = [{
      name              = "worker", isa = "x86-64", capacity = "spot"
      instance_types    = ["c6id.4xlarge"]
      max_instances     = 4, vcpus_per_instance = 15
      disk_per_instance = "200GiB"
      actions           = { grants = ["network"] }
    }]
  }
  override_resource {
    target = module.network.aws_vpc.this
    values = { ipv6_cidr_block = "2600:1f00:abcd:1200::/56" }
  }
  assert {
    condition     = length(var.execution_egress.protected_networks) == 0 && length(local.protected_networks) > 0 && contains(local.protected_networks, "10.20.0.0/16") && strcontains(output.gang_toml, "[execution.egress]\nprotected_networks = ${jsonencode(local.protected_networks)}")
    error_message = "Omitting additional protected networks must default to an empty operator list while rendering derived address protection."
  }
  assert {
    condition     = strcontains(output.gang_toml, "[fleets.actions]\ngrants = ${jsonencode(sort(tolist(var.fleets[0].actions.grants)))}") && !strcontains(output.gang_toml, "[fleets.actions.resources]")
    error_message = "A grants-only fleet must render sorted grants without an empty resource table."
  }
}

run "resource_ceilings_without_grants_omit_grants_table" {
  command = apply
  variables {
    fleets = [{
      name              = "worker", isa = "x86-64", capacity = "spot"
      instance_types    = ["c6id.4xlarge"]
      max_instances     = 4, vcpus_per_instance = 15
      disk_per_instance = "200GiB"
      actions           = { resources = { cpu_count = 2 } }
    }]
  }
  assert {
    condition     = !strcontains(output.gang_toml, "[fleets.actions]") && strcontains(output.gang_toml, "[fleets.actions.resources]\ncpu_count = 2\n\n[fleets.options]") && !strcontains(output.gang_toml, "[execution.egress]")
    error_message = "A resources-only fleet must render only its configured ceiling table, without empty grants or egress tables."
  }
}

run "load_balancer_fronts_the_public_listener" {
  command = apply

  variables {
    hostname             = "gang.example.com"
    load_balancer        = {}
    client_ingress_cidrs = ["198.51.100.0/24"]
  }

  assert {
    condition     = can(regex("(?s)\\[server\\][^\\[]*\\n[ \\t]*ca_pin[ \\t]*=[ \\t]*\"signer\"", output.gang_toml)) && can(regex("(?s)\\[server\\][^\\[]*\\n[ \\t]*endpoint_trust[ \\t]*=[ \\t]*\"system\"", output.gang_toml))
    error_message = "The load balancer endpoint must use system roots in gang.toml."
  }

  assert {
    condition     = output.endpoint == "https://gang.example.com" && output.load_balancer_dns_name != null
    error_message = "The load balancer must expose its DNS name and the configured public endpoint."
  }

  assert {
    condition     = length(module.load_balancer) == 1 && module.load_balancer[0].security_group_id != null
    error_message = "The load balancer and its security group must be created."
  }

  assert {
    condition     = strcontains(output.gang_toml, "health_listen = \"0.0.0.0:${module.network.health_port}\"") && module.load_balancer[0].health_port == module.network.health_port
    error_message = "The network health port must reach the server listener and load balancer."
  }
  assert {
    condition     = !strcontains(output.gang_toml, "standby_routability_delay") && !strcontains(output.gang_toml, "[server.leadership]")
    error_message = "A load-balanced SQLite deployment must use the default zero standby delay."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "[server.proxy_protocol]")
    error_message = "SQLite load-balanced deployments must not enable PROXY."
  }
}


run "load_balancer_requires_hostname" {
  command = plan

  variables {
    load_balancer = {}
  }

  expect_failures = [
    var.load_balancer,
  ]
}

run "renders_gang_toml_for_every_fleet" {
  command = apply

  assert {
    condition     = can(regex("(?s)name = \"gang-worker-x86-64\"[^\\[]*isa = \"x86-64\"", output.gang_toml)) && can(regex("(?s)name = \"gang-worker-aarch64\"[^\\[]*isa = \"aarch64\"", output.gang_toml))
    error_message = "Each fleet section must carry its isa."
  }

  assert {
    condition     = can(regex("(?s)name = \"gang-worker-x86-64\"[^\\[]*\\[fleets\\.options\\][^\\[]*capacity = \"spot\"", output.gang_toml)) && can(regex("(?s)name = \"gang-worker-aarch64\"[^\\[]*\\[fleets\\.options\\][^\\[]*capacity = \"on-demand\"", output.gang_toml))
    error_message = "Each fleet's [fleets.options] must carry its capacity."
  }

  assert {
    condition     = can(regex("(?s)name = \"gang-worker-x86-64\"[^\\[]*sandbox = \"bwrap\"", output.gang_toml)) && can(regex("(?s)name = \"gang-worker-aarch64\"[^\\[]*sandbox = \"bwrap\"", output.gang_toml)) && !strcontains(output.gang_toml, "default_sandbox")
    error_message = "Each fleet must carry its sandbox and gang.toml must not carry a top-level default_sandbox."
  }

  assert {
    condition     = length(regexall("action_environment = \"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\"", output.gang_toml)) == 2 && strcontains(output.gang_toml, "root = \"/etc/gang/action-rootfs\"") && !strcontains(output.gang_toml, "readonly_bind")
    error_message = "Each fleet must use its selected AMI action environment and the rootfs sandbox."
  }

  assert {
    condition     = can(regex("(?m)^name = \"gang-worker-x86-64\"$", output.gang_toml)) && can(regex("(?m)^name = \"gang-worker-aarch64\"$", output.gang_toml))
    error_message = "Each fleet section must carry the ASG name as its name key."
  }

  assert {
    condition     = can(regex("gang-worker-x86-64(.|\n)*gang-worker-aarch64", output.gang_toml))
    error_message = "gang.toml must list fleets in the declared preference order."
  }

  assert {
    condition     = output.asg_names == ["gang-worker-x86-64", "gang-worker-aarch64"]
    error_message = "The worker-fleet module must create ASGs named after each fleet name."
  }

  assert {
    condition     = can(regex("(?s)\\[identity\\.providers\\.aws-workers\\.oidc\\][^\\[]*required_claims = \\{ sub = \"arn:aws:iam::000000000000:role/gang-worker\" \\}", output.gang_toml))
    error_message = "The aws-workers OIDC provider must require the worker role ARN as the sub claim."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "[fleets.options.fallback]")
    error_message = "Plain fleets must not render [fleets.options.fallback]."
  }

  assert {
    condition     = can(regex("(?s)name = \"gang-worker-x86-64\"[^\\[]*disk_per_instance = \"200GiB\"", output.gang_toml)) && can(regex("(?s)name = \"gang-worker-aarch64\"[^\\[]*disk_per_instance = \"200GiB\"", output.gang_toml))
    error_message = "Each fleet must carry its disk_per_instance."
  }
}

run "renders_scheduled_minimum" {
  command = apply

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        min_instances      = 0
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
        scheduled_minimum  = [{
          instances = 1
          days      = ["mon", "tue", "wed", "thu", "fri"]
          start     = "07:55"
          end       = "19:00"
          time_zone = "Europe/Paris"
        }]
      },
      {
        name               = "gang-worker-aarch64"
        isa                = "aarch64"
        capacity           = "on-demand"
        instance_types     = ["c7gd.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
      },
    ]
  }

  assert {
    condition     = can(regex("(?s)name = \"gang-worker-x86-64\".*\\[\\[fleets\\.scheduled_minimum\\]\\]\ninstances = 1\ndays = \\[\"mon\",\"tue\",\"wed\",\"thu\",\"fri\"\\]\nstart = \"07:55\"\nend = \"19:00\"\ntime_zone = \"Europe/Paris\"\n.*name = \"gang-worker-aarch64\"", output.gang_toml))
    error_message = "The exact scheduled minimum must appear between the first and second fleet names."
  }

  assert {
    condition     = length(split("[[fleets.scheduled_minimum]]", output.gang_toml)) == 2
    error_message = "A fleet without windows must not render a scheduled minimum."
  }
}

run "rejects_scheduled_minimum_at_min_instances" {
  command = plan
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 1, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 1, days = ["mon"], start = "07:55", end = "19:00", time_zone = "Europe/Paris" }]
    }]
  }
  expect_failures = [var.fleets]
}

run "rejects_scheduled_minimum_bad_day" {
  command = plan
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 0, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 1, days = ["monday"], start = "07:55", end = "19:00", time_zone = "Europe/Paris" }]
    }]
  }
  expect_failures = [var.fleets]
}

run "rejects_scheduled_minimum_end_before_start" {
  command = plan
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 0, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 1, days = ["mon"], start = "19:00", end = "08:00", time_zone = "Europe/Paris" }]
    }]
  }
  expect_failures = [var.fleets]
}

run "accepts_scheduled_minimum_until_midnight" {
  command = apply
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 0, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 4, days = ["sat", "sun"], start = "22:00", end = "24:00", time_zone = "UTC" }]
    }]
  }
  assert {
    condition     = strcontains(output.gang_toml, "end = \"24:00\"")
    error_message = "A scheduled minimum may end at midnight."
  }
}

run "rejects_scheduled_minimum_above_max_instances" {
  command = plan
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 0, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 5, days = ["mon"], start = "07:55", end = "19:00", time_zone = "UTC" }]
    }]
  }
  expect_failures = [var.fleets]
}

run "rejects_scheduled_minimum_without_days" {
  command = plan
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 0, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 1, days = [], start = "07:55", end = "19:00", time_zone = "UTC" }]
    }]
  }
  expect_failures = [var.fleets]
}

run "rejects_scheduled_minimum_with_a_repeated_day" {
  command = plan
  variables {
    fleets = [{
      name                = "gang-worker-x86-64", isa = "x86-64", capacity = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4, min_instances = 0, vcpus_per_instance = 15
      disk_per_instance   = "200GiB"
      scheduled_minimum   = [{ instances = 1, days = ["mon", "mon"], start = "07:55", end = "19:00", time_zone = "UTC" }]
    }]
  }
  expect_failures = [var.fleets]
}

run "renders_gang_settings_override" {
  command = plan

  variables {
    gang_settings = {
      poll_interval = "3s"
    }
  }

  assert {
    condition     = strcontains(output.gang_toml, "poll_interval = \"3s\"")
    error_message = "gang_settings must override the rendered poll interval."
  }
}

run "pins_ca_to_signer_and_wires_endpoints" {
  command = apply

  assert {
    condition     = can(regex("(?s)\\[server\\][^\\[]*ca_pin = \"signer\"", output.gang_toml))
    error_message = "gang.toml [server] must render ca_pin = \"signer\" so workers pin the signer's public key."
  }

  assert {
    condition     = can(regex("(?s)\\A[^\\[]*\\nendpoint = \"${output.endpoint}\"", output.gang_toml)) && can(regex("(?s)\\A[^\\[]*\\nworker_endpoint = \"${output.worker_endpoint}\"", output.gang_toml))
    error_message = "gang.toml must carry the public and worker endpoints at the top level."
  }

  assert {
    condition     = strcontains(output.worker_endpoint, "ip-10-20-0-10.") && strcontains(output.worker_endpoint, ":9090")
    error_message = "Workers must reach the controller on its private DNS name and the worker port."
  }

  assert {
    condition     = startswith(output.endpoint, "https://") && strcontains(output.endpoint, "ec2-203-0-113-10.")
    error_message = "The endpoint must default to the controller Elastic IP public DNS name over https."
  }
}

run "identity_and_provider_sections" {
  command = apply

  assert {
    condition     = can(regex("(?s)\\[identity\\.providers\\.aws-workers\\.oidc\\][^\\[]*issuer_url = \"${output.issuer_url}\"", output.gang_toml))
    error_message = "The aws-workers OIDC issuer must be the account outbound federation issuer."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "worker_mode") && !strcontains(output.gang_toml, "asg_names")
    error_message = "gang.toml must not carry worker_mode or asg_names; the ASG name is [[fleets]].name."
  }
}

run "separates_extra_config_without_trailing_blank_lines" {
  command = plan

  variables {
    extra_config_toml = "[[server.roles]]\nname = \"administrator\"\n"
  }

  assert {
    condition     = endswith(output.gang_toml, "required_claims = { sub = \"arn:aws:iam::000000000000:role/gang-worker\" }\n\n[[server.roles]]\nname = \"administrator\"\n")
    error_message = "Extra configuration must follow one blank line and end without an extra blank line."
  }
}

run "rejects_default_isa_outside_fleets" {
  command = plan

  variables {
    default_isa = "riscv"
  }

  expect_failures = [
    terraform_data.input_checks,
  ]
}

run "rejects_zero_http2_max_connections" {
  command = plan

  variables {
    http2_max_connections = 0
  }

  expect_failures = [var.http2_max_connections]
}

run "rejects_fractional_http2_max_connections" {
  command = plan

  variables {
    http2_max_connections = 1.5
  }

  expect_failures = [var.http2_max_connections]
}

run "rejects_bare_top_level_extra_config" {
  command = plan

  variables {
    extra_config_toml = "max_pids = 4096\n"
  }

  expect_failures = [
    var.extra_config_toml,
  ]
}

run "rejects_fleet_isa_outside_pattern" {
  command = plan

  variables {
    default_isa = "X86"
    fleets = [
      {
        name               = "gang-worker-x86"
        isa                = "X86"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_disk_per_instance_without_unit" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200"
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "publishes_config_parameter" {
  command = apply

  assert {
    condition     = startswith(output.config_parameter_name, "/gang/gang.toml/generations/") && output.config_parameter_name == aws_ssm_parameter.config_generation.name
    error_message = "The active generation must use the default /<name>/gang.toml namespace."
  }

  assert {
    condition     = aws_ssm_parameter.config_generation.value == output.gang_toml
    error_message = "The active generation value must equal the rendered gang.toml."
  }

  assert {
    condition     = aws_ssm_parameter.config_generation.type == "String" && aws_ssm_parameter.config_generation.tier == "Intelligent-Tiering"
    error_message = "The config parameter must be a String in the Intelligent-Tiering tier: it holds no secrets."
  }
}

run "rejects_nat_mode_none" {
  command = plan

  variables {
    nat_mode = "none"
  }

  expect_failures = [
    var.nat_mode,
  ]
}

run "iam_inputs_default_to_unprefixed_unbounded_roles" {
  command = apply

  assert {
    condition     = module.controller.role_name == "gang-controller" && module.worker_fleet.role_name == "gang-worker"
    error_message = "Without iam_name_prefix the roles must carry the default names."
  }
  assert {
    condition     = module.controller.permissions_boundary_arn == null && module.worker_fleet.permissions_boundary_arn == null
    error_message = "Without iam_permissions_boundary_arn no role may carry a permissions boundary."
  }
}

run "iam_inputs_reach_both_roles" {
  command = apply

  variables {
    iam_name_prefix              = "example-"
    iam_permissions_boundary_arn = "arn:aws:iam::000000000000:policy/example-boundary"
  }

  assert {
    condition     = module.controller.role_name == "example-gang-controller" && module.worker_fleet.role_name == "example-gang-worker"
    error_message = "iam_name_prefix must prefix the controller and worker role names."
  }
  assert {
    condition     = module.controller.permissions_boundary_arn == "arn:aws:iam::000000000000:policy/example-boundary" && module.worker_fleet.permissions_boundary_arn == "arn:aws:iam::000000000000:policy/example-boundary"
    error_message = "iam_permissions_boundary_arn must be the permissions boundary of both roles."
  }
}

run "rejects_empty_client_token_providers" {
  command = plan

  variables {
    client_token_providers = []
  }

  expect_failures = [
    var.client_token_providers,
  ]
}

run "rejects_reserved_fleet_tag_key" {
  command = plan

  variables {
    fleet_tag_key = "_index"
  }

  expect_failures = [
    var.fleet_tag_key,
  ]
}

run "fallback_fleet_gets_an_on_demand_twin" {
  command = apply

  variables {
    aws_events_enabled = true
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
        fallback           = {}
      },
      {
        name               = "gang-worker-aarch64"
        isa                = "aarch64"
        capacity           = "on-demand"
        instance_types     = ["c7gd.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
        warm_pool_size     = 2
      },
    ]
  }

  assert {
    condition     = output.asg_names == ["gang-worker-x86-64", "gang-worker-x86-64-on-demand", "gang-worker-aarch64"]
    error_message = "A fallback fleet's on-demand twin must follow its spot group in asg_names, which the controller receives as its worker fleet names."
  }

  assert {
    condition     = toset(jsondecode(aws_cloudwatch_event_rule.fleet[0].event_pattern).detail.AutoScalingGroupName) == toset(output.asg_names)
    error_message = "The fleet rule must include exactly the managed primary and fallback ASGs."
  }

  assert {
    condition     = aws_sqs_queue.events[0].sqs_managed_sse_enabled && aws_sqs_queue.events_dlq[0].sqs_managed_sse_enabled && aws_sqs_queue.events[0].message_retention_seconds == 86400 && aws_sqs_queue.events_dlq[0].message_retention_seconds == 345600 && jsondecode(aws_sqs_queue.events[0].redrive_policy).deadLetterTargetArn == aws_sqs_queue.events_dlq[0].arn && jsondecode(aws_sqs_queue.events[0].redrive_policy).maxReceiveCount == 5
    error_message = "Event queues must use SQS-managed encryption, bounded retention and a five-receive dead-letter policy."
  }

  assert {
    condition     = toset(jsondecode(aws_cloudwatch_event_rule.spot[0].event_pattern).source) == toset(["aws.ec2"]) && toset(jsondecode(aws_cloudwatch_event_rule.spot[0].event_pattern)["detail-type"]) == toset(["EC2 Instance Rebalance Recommendation", "EC2 Spot Instance Interruption Warning"]) && toset(jsondecode(aws_cloudwatch_event_rule.spot[0].event_pattern).account) == toset(["000000000000"]) && toset(jsondecode(aws_cloudwatch_event_rule.spot[0].event_pattern).region) == toset(["eu-north-1"])
    error_message = "The spot rule must select only the two account and region scoped EC2 notices."
  }

  assert {
    condition     = toset(jsondecode(aws_cloudwatch_event_rule.fleet[0].event_pattern).source) == toset(["aws.autoscaling"]) && toset(jsondecode(aws_cloudwatch_event_rule.fleet[0].event_pattern)["detail-type"]) == toset(["EC2 Instance Launch Successful", "EC2 Instance Launch Unsuccessful", "EC2 Instance Terminate Successful"]) && toset(jsondecode(aws_cloudwatch_event_rule.fleet[0].event_pattern).account) == toset(["000000000000"]) && toset(jsondecode(aws_cloudwatch_event_rule.fleet[0].event_pattern).region) == toset(["eu-north-1"])
    error_message = "The fleet rule must select only the specified account and region scoped ASG events."
  }

  assert {
    condition     = jsondecode(aws_sqs_queue_policy.events[0].policy).Statement[0].Principal.Service == "events.amazonaws.com" && jsondecode(aws_sqs_queue_policy.events[0].policy).Statement[0].Action == "sqs:SendMessage" && toset(jsondecode(aws_sqs_queue_policy.events[0].policy).Statement[0].Condition.ArnEquals["aws:SourceArn"]) == toset([aws_cloudwatch_event_rule.spot[0].arn, aws_cloudwatch_event_rule.fleet[0].arn]) && jsondecode(aws_sqs_queue_policy.events[0].policy).Statement[0].Condition.StringEquals["aws:SourceAccount"] == "000000000000"
    error_message = "Only the two deployment rules in this account may send events to the queue."
  }

  assert {
    condition     = jsondecode(aws_sqs_queue_policy.events[0].policy).Statement[1].Effect == "Deny" && jsondecode(aws_sqs_queue_policy.events[0].policy).Statement[1].Condition.Bool["aws:SecureTransport"] == "false" && jsondecode(aws_sqs_queue_policy.events_dlq[0].policy).Statement[0].Effect == "Deny" && jsondecode(aws_sqs_queue_policy.events_dlq[0].policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false"
    error_message = "Both queues must reject insecure transport."
  }

  assert {
    condition     = aws_cloudwatch_event_target.spot[0].arn == aws_sqs_queue.events[0].arn && aws_cloudwatch_event_target.fleet[0].arn == aws_sqs_queue.events[0].arn && strcontains(output.gang_toml, "events_queue_url = \"${output.events_queue_url}\"")
    error_message = "Both rules must target the queue and the shared configuration must expose its URL."
  }

  assert {
    condition     = strcontains(output.gang_toml, "[fleets.options.fallback]") && strcontains(output.gang_toml, "retry_spot_after = \"5m\"") && strcontains(output.gang_toml, "drain_on_demand_first = true")
    error_message = "A fallback fleet must render [fleets.options.fallback] with the default retry_spot_after and drain_on_demand_first."
  }

  assert {
    condition     = can(regex("gang-worker-x86-64(.|\n)*\\[fleets\\.options\\.fallback\\](.|\n)*gang-worker-aarch64", output.gang_toml)) && length(regexall("\\[fleets\\.options\\.fallback\\]", output.gang_toml)) == 1
    error_message = "The fallback table must sit in the spot fleet's section, and no other fleet may render it."
  }
}

run "rejects_fallback_on_on_demand_fleet" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "on-demand"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
        fallback           = {}
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_warm_pool_without_on_demand_group" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
        warm_pool_size     = 2
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "listeners_use_the_network_ports" {
  command = plan

  assert {
    condition     = can(regex("(?m)^listen = \"0\\.0\\.0\\.0:${module.network.public_port}\"$", output.gang_toml))
    error_message = "gang.toml listen must bind the public listener to the network module's public_port."
  }

  assert {
    condition     = can(regex("(?m)^worker_listen = \"0\\.0\\.0\\.0:${module.network.worker_port}\"$", output.gang_toml))
    error_message = "gang.toml worker_listen must bind the worker listener to the network module's worker_port."
  }

  assert {
    condition     = strcontains(output.worker_endpoint, ":${module.network.worker_port}")
    error_message = "The worker endpoint must carry the network module's worker_port."
  }
}

run "workload_token_audience_is_rendered_escaped" {
  command = plan

  variables {
    workload_token_audience = "exa\"mple\naudience"
  }

  assert {
    condition     = strcontains(output.gang_toml, "[provisioner.aws.workload_token]\naudience = \"exa\\\"mple\\naudience\"")
    error_message = "[provisioner.aws.workload_token] audience must be the JSON-escaped form of the audience."
  }

  assert {
    condition     = strcontains(output.gang_toml, "[identity.providers.aws-workers.oidc]") && strcontains(output.gang_toml, "audience = \"exa\\\"mple\\naudience\"")
    error_message = "The aws-workers OIDC audience must be the JSON-escaped form of the audience."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "exa\"mple\naudience")
    error_message = "The audience must never appear with a raw double quote or newline in gang.toml."
  }
}

run "controller_secret_parameters_are_accepted_and_not_rendered" {
  command = plan

  variables {
    controller_secret_parameters = {
      "dashboard-client-secret" = "/gang/secrets/dashboard-client-secret"
      "github-app-key"          = "/gang/secrets/github-app-key"
    }
    controller_secret_parameters_kms_key_arn = "arn:aws:kms:eu-north-1:000000000000:key/example"
  }

  assert {
    condition     = !strcontains(output.gang_toml, "/gang/secrets/")
    error_message = "gang.toml must not carry controller secret parameter names or values; secrets are referenced only by file path under /etc/gang/secrets."
  }
}

run "manage_outbound_web_identity_federation_false_plans_without_creating_federation" {
  command = plan

  variables {
    manage_outbound_web_identity_federation = false
  }

  assert {
    condition     = output.issuer_url != ""
    error_message = "With manage_outbound_web_identity_federation = false the issuer URL must still resolve from the account's existing federation."
  }
}

run "ca_signer_is_the_controller_role" {
  command = plan

  assert {
    condition     = module.storage.ca_signer_role_arn == module.controller.role_arn
    error_message = "The CA key must let only the controller role sign."
  }

  assert {
    condition     = module.storage.ca_signer_role_arn != module.worker_fleet.role_arn
    error_message = "The worker role must never be the CA signer."
  }
}

run "rejects_unknown_fleet_isa" {
  command = plan

  variables {
    default_isa = "riscv"
    fleets = [{
      name               = "worker"
      isa                = "riscv"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "200GiB"
    }]
  }

  expect_failures = [var.fleets]
}

run "rejects_zero_fleet_disk" {
  command = plan

  variables {
    fleets = [{
      name               = "worker"
      isa                = "x86-64"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "0B"
    }]
  }

  expect_failures = [var.fleets]
}

run "rejects_overflowing_fleet_disk" {
  command = plan

  variables {
    fleets = [{
      name               = "worker"
      isa                = "x86-64"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "16777216TiB"
    }]
  }

  expect_failures = [var.fleets]
}

run "accepts_nonzero_fleet_disk_with_leading_zeroes" {
  command = plan

  variables {
    fleets = [{
      name               = "worker"
      isa                = "x86-64"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "0001B"
    }]
  }
}

run "rejects_min_instances_above_max_instances" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        min_instances      = 5
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_zero_vcpus_per_instance" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 0
        disk_per_instance  = "200GiB"
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_vcpus_per_instance_above_u32" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 4
        vcpus_per_instance = 4294967296
        disk_per_instance  = "200GiB"
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_zero_max_instances" {
  command = plan

  variables {
    fleets = [
      {
        name               = "gang-worker-x86-64"
        isa                = "x86-64"
        capacity           = "spot"
        instance_types     = ["c6id.4xlarge"]
        max_instances      = 0
        vcpus_per_instance = 15
        disk_per_instance  = "200GiB"
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_input_links_outside_keep_and_break" {
  command = plan

  variables {
    gang_settings = {
      input_links = "copy"
    }
  }

  expect_failures = [
    var.gang_settings,
  ]
}

run "rejects_zero_poll_interval" {
  command = plan

  variables {
    gang_settings = {
      poll_interval = "0s"
    }
  }

  expect_failures = [
    var.gang_settings,
  ]
}

run "rejects_sub_second_client_action_timeout" {
  command = plan

  variables {
    gang_settings = {
      client_action_timeout = "1500ms"
    }
  }

  expect_failures = [
    var.gang_settings,
  ]
}

run "accepts_whole_second_client_action_timeout_in_ms" {
  command = plan

  variables {
    gang_settings = {
      client_action_timeout = "2000ms"
      poll_interval         = "500ms"
    }
  }

  assert {
    condition     = strcontains(output.gang_toml, "client_action_timeout = \"2000ms\"")
    error_message = "A whole-second client_action_timeout written in ms must be accepted and rendered verbatim."
  }
}

run "sqlite_instance_shape" {
  command = plan

  assert {
    condition     = output.controller_shape == "instance" && strcontains(output.gang_toml, "[database.sqlite]") && !strcontains(output.gang_toml, "execution_listen")
    error_message = "The default deployment must retain the single SQLite controller configuration."
  }
  assert {
    condition     = output.controller_config_digest == null
    error_message = "SQLite must not pass a group configuration digest to its controller."
  }
}

run "rejects_unknown_database_engine" {
  command = plan
  variables { database = { engine = "mysql" } }
  expect_failures = [var.database]
}

run "rejects_multiple_sqlite_controllers" {
  command = plan
  variables {
    controller_count = 2
    nat_mode         = "per_az"
  }
  expect_failures = [var.database]
}

# Correctness: Defaults resolve each used architecture from this module's release.
run "default_images_from_release" {
  command = plan

  assert {
    condition = toset(keys(local.image_lookups)) == toset([
      "072320588884/x86_64/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "072320588884/aarch64/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ])
    error_message = "The default images must use the release publisher and both fleet architectures."
  }

  assert {
    condition     = data.http.sha256sums[0].url == "https://github.com/leadingterm/gang/releases/download/v${local.version}/SHA256SUMS" && can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+", local.version))
    error_message = "The release URL must use the module's Cargo version and default repository."
  }
}

# Correctness: Select the x86 digest even when the ARM digest appears first.
run "single_architecture_uses_matching_digest" {
  command = plan

  variables {
    fleets = [{
      name               = "worker"
      isa                = "x86-64"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "200GiB"
    }]
  }

  override_data {
    target = data.http.sha256sums
    values = {
      status_code   = 200
      response_body = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  gang-image-aarch64.raw.zst\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-x86_64.raw.zst\n"
    }
  }

  assert {
    condition     = toset(keys(local.image_lookups)) == toset(["072320588884/x86_64/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"])
    error_message = "The x86-only deployment must use the x86 digest, not the first manifest line."
  }

  assert {
    condition     = alltrue([for image in data.aws_ami.image : anytrue([for filter in image.filter : filter.name == "architecture" && toset(filter.values) == toset(["x86_64"])])])
    error_message = "The x86 AMI lookup must filter for the x86_64 architecture."
  }
}

# Correctness: An unavailable release cannot provide trusted image hashes.
run "release_missing_fails" {
  command = plan

  override_data {
    target = data.http.sha256sums
    values = {
      status_code   = 404
      response_body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-aarch64.raw.zst\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-x86_64.raw.zst\n"
    }
  }

  expect_failures = [data.http.sha256sums]
}

# Correctness: A successful manifest response must list every required architecture.
run "release_missing_architecture_fails" {
  command = plan

  override_data {
    target = data.http.sha256sums
    values = {
      status_code   = 200
      response_body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-x86_64.raw.zst\n"
    }
  }

  expect_failures = [data.http.sha256sums]
}

# Correctness: Each required image must appear exactly once in the manifest.
run "release_duplicate_architecture_fails" {
  command = plan

  override_data {
    target = data.http.sha256sums
    values = {
      status_code   = 200
      response_body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-aarch64.raw.zst\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-x86_64.raw.zst\nbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  gang-image-x86_64.raw.zst\n"
    }
  }

  expect_failures = [data.http.sha256sums]
}

# Security: An AMI whose shared digest differs from the release is rejected.
run "tag_mismatch_fails" {
  command = plan

  override_data {
    target = data.aws_ami.image
    values = {
      name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
      boot_mode        = "uefi"
      tags             = { "ec2:SharedTag/ImageSha256" = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
      deprecation_time = ""
    }
  }

  expect_failures = [data.aws_ami.image]
}

run "missing_action_environment_fails" {
  command = plan
  override_data {
    target = data.aws_ami.image
    values = {
      name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
      boot_mode        = "uefi"
      tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
      deprecation_time = ""
    }
  }
  expect_failures = [data.aws_ami.image]
}

run "malformed_action_environment_fails" {
  command = plan
  override_data {
    target = data.aws_ami.image
    values = {
      name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
      boot_mode        = "uefi"
      tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "host:invalid" }
      deprecation_time = ""
    }
  }
  expect_failures = [data.aws_ami.image]
}

# Security: Gang images must boot using UEFI.
run "legacy_boot_mode_fails" {
  command = plan

  override_data {
    target = data.aws_ami.image
    values = {
      name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
      boot_mode        = "legacy-bios"
      tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
      deprecation_time = ""
    }
  }

  expect_failures = [data.aws_ami.image]
}

# Correctness: Explicit architecture images do not request the release manifest.
run "explicit_images_skip_release" {
  command = plan

  variables {
    images = {
      x86_64  = { owner = "self", sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
      aarch64 = { owner = "self", sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
    }
  }

  assert {
    condition     = length(data.http.sha256sums) == 0 && alltrue([for image in values(local.image_lookups) : image.owner == "self"]) && length(local.image_lookups) == 2
    error_message = "Explicit images must use self and skip the release manifest."
  }
}

# Correctness: A fleet override selects its own image instead of its architecture default.
run "fleet_image_override" {
  command = plan

  variables {
    images = {
      x86_64 = { owner = "self", sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }
    }
    fleets = [{
      name               = "worker"
      isa                = "x86-64"
      capacity           = "spot"
      image              = { owner = "000000000000", sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "200GiB"
    }]
  }

  override_data {
    target = data.aws_ami.image
    values = {
      id               = "ami-00000000000000001"
      name             = "gang-x86_64-bbbbbbbbbbbbbbbb"
      boot_mode        = "uefi"
      tags             = { "ec2:SharedTag/ImageSha256" = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "ec2:SharedTag/ActionEnvironment" = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" }
      deprecation_time = ""
    }
  }

  assert {
    condition     = output.images["worker"] == data.aws_ami.image["000000000000/x86_64/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"].id
    error_message = "The worker image must be the AMI resolved for its fleet override."
  }

  assert {
    condition     = strcontains(output.gang_toml, "action_environment = \"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"")
    error_message = "The override's selected AMI supplies its own action environment."
  }
}

# Correctness: An ARM controller selects the aarch64 image.
run "arm_controller_uses_aarch64_image" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type.controller
    values = {
      boot_modes              = ["uefi"]
      supported_architectures = ["arm64"]
    }
  }

  assert {
    condition     = local.node_images["controller"].architecture == "aarch64"
    error_message = "An ARM controller must use the aarch64 image."
  }

  assert {
    condition     = anytrue([for filter in data.aws_ami.image["072320588884/aarch64/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"].filter : filter.name == "architecture" && toset(filter.values) == toset(["arm64"])])
    error_message = "The aarch64 AMI lookup must filter for the AWS arm64 architecture."
  }
}

# Correctness: Images already deprecated trigger the retention warning.
run "deprecation_warns" {
  command = plan

  override_data {
    target = data.aws_ami.image
    values = {
      name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
      boot_mode        = "uefi"
      tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
      deprecation_time = "2000-01-01T00:00:00Z"
    }
  }

  expect_failures = [check.images_current]
}

# Correctness: A deprecation beyond the warning window does not warn.
run "deprecation_beyond_window_passes" {
  command = plan

  override_data {
    target = data.aws_ami.image
    values = {
      name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
      boot_mode        = "uefi"
      tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
      deprecation_time = "2099-01-01T00:00:00Z"
    }
  }
}

# Correctness: A fleet cannot collide with the controller's image key.
run "fleet_named_controller_rejected" {
  command = plan

  variables {
    fleets = [{
      name               = "controller"
      isa                = "x86-64"
      capacity           = "spot"
      instance_types     = ["c6id.4xlarge"]
      max_instances      = 4
      vcpus_per_instance = 15
      disk_per_instance  = "200GiB"
    }]
  }

  expect_failures = [var.fleets]
}

# Correctness: Only supported architectures may appear as image keys.
run "images_invalid_architecture_rejected" {
  command = plan

  variables {
    images = {
      riscv = { owner = "self", sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
    }
  }

  expect_failures = [var.images]
}

# Security: Image digests must be lowercase, complete SHA-256 values.
run "images_invalid_sha256_rejected" {
  command = plan

  variables {
    images = {
      x86_64 = { owner = "self", sha256 = "not-a-sha256" }
    }
  }

  expect_failures = [var.images]
}

# Security: Image owners must be self or an AWS account ID.
run "images_invalid_owner_rejected" {
  command = plan

  variables {
    images = {
      x86_64 = { owner = "not-an-account", sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
    }
  }

  expect_failures = [var.images]
}

run "aws_events_disabled_by_default" {
  command = apply

  assert {
    condition     = length(aws_sqs_queue.events) == 0 && length(aws_sqs_queue.events_dlq) == 0 && length(aws_cloudwatch_event_rule.spot) == 0 && length(aws_cloudwatch_event_rule.fleet) == 0 && output.events_queue_url == null && output.events_dead_letter_queue_url == null && !strcontains(output.gang_toml, "events_queue_url")
    error_message = "AWS event resources and configuration must be absent until explicitly enabled after upgrading images."
  }
}

run "aws_events_custom_retention_and_redrive" {
  command = apply

  variables {
    aws_events_enabled                       = true
    aws_events_retention_seconds             = 7200
    aws_events_dead_letter_retention_seconds = 172800
    aws_events_max_receive_count             = 9
  }

  assert {
    condition     = aws_sqs_queue.events[0].message_retention_seconds == 7200 && aws_sqs_queue.events_dlq[0].message_retention_seconds == 172800 && jsondecode(aws_sqs_queue.events[0].redrive_policy).maxReceiveCount == 9
    error_message = "Configured retention and receive limits must reach the queues."
  }
}

run "aws_events_rejects_short_retention" {
  command = plan

  variables {
    aws_events_retention_seconds = 59
  }

  expect_failures = [var.aws_events_retention_seconds]
}

run "aws_events_rejects_excessive_dead_letter_retention" {
  command = plan

  variables {
    aws_events_dead_letter_retention_seconds = 1209601
  }

  expect_failures = [var.aws_events_dead_letter_retention_seconds]
}

run "aws_events_rejects_shorter_dead_letter_retention" {
  command = plan

  variables {
    aws_events_retention_seconds             = 86400
    aws_events_dead_letter_retention_seconds = 3600
  }

  expect_failures = [var.aws_events_dead_letter_retention_seconds]
}

run "aws_events_rejects_invalid_receive_count" {
  command = plan

  variables {
    aws_events_max_receive_count = 1001
  }

  expect_failures = [var.aws_events_max_receive_count]
}

run "aws_events_rejects_fractional_retention" {
  command = plan

  variables {
    aws_events_retention_seconds = 60.5
  }

  expect_failures = [var.aws_events_retention_seconds]
}

run "aws_events_rejects_fractional_dead_letter_retention" {
  command = plan

  variables {
    aws_events_dead_letter_retention_seconds = 86400.5
  }

  expect_failures = [var.aws_events_dead_letter_retention_seconds]
}

run "aws_events_rejects_fractional_receive_count" {
  command = plan

  variables {
    aws_events_max_receive_count = 1.5
  }

  expect_failures = [var.aws_events_max_receive_count]
}

run "rejects_config_name_with_empty_segment" {
  command = plan
  variables { config_parameter_name = "/gang//config" }
  expect_failures = [var.config_parameter_name]
}

run "rejects_reserved_config_name" {
  command = plan
  variables { config_parameter_name = "/SSM/config" }
  expect_failures = [var.config_parameter_name]
}

run "rejects_config_name_too_deep" {
  command = plan
  variables { config_parameter_name = "/a/a/a/a/a/a/a/a/a/a/a/a/a/a" }
  expect_failures = [var.config_parameter_name]
}

run "accepts_base_namespace_secret" {
  command = plan
  variables { controller_secret_parameters = { token = "/gang/gang.toml" } }
  assert {
    condition     = contains(module.controller.secret_parameter_arns, "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml")
    error_message = "The namespace itself is not a managed configuration parameter."
  }
}

run "rejects_generation_secret_overlap" {
  command = plan
  variables { controller_secret_parameters = { token = "/gang/gang.toml/generations/secret" } }
  expect_failures = [terraform_data.input_checks]
}

run "accepts_unrelated_secret" {
  command = plan
  variables { controller_secret_parameters = { token = "/gang/secrets/token" } }
  assert {
    condition     = contains(module.controller.secret_parameter_arns, "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/secrets/token")
    error_message = "An unrelated controller secret remains accessible to the controller."
  }
}
