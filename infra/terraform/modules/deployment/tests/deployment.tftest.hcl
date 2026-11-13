mock_provider "aws" {
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
      boot_modes = ["legacy-bios", "uefi"]
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

  mock_resource "aws_eip" {
    defaults = {
      public_ip = "203.0.113.10"
    }
  }

  mock_resource "aws_lb" {
    defaults = {
      arn      = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:loadbalancer/net/gang/0000000000000000"
      dns_name = "gang-0000000000.elb.eu-north-1.amazonaws.com"
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
  controller_ami_id        = "ami-00000000000000000"
  controller_instance_type = "c7i.large"
  default_isa              = "x86-64"
  trust_domain             = "example.com"
  fleets = [
    {
      name                        = "gang-worker-x86-64"
      isa                         = "x86-64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    },
    {
      name                        = "gang-worker-aarch64"
      isa                         = "aarch64"
      capacity                    = "on-demand"
      ami_id                      = "ami-00000000000000002"
      instance_types              = ["c7gd.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    },
  ]
}

run "direct_endpoint_without_load_balancer" {
  command = plan

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
}

run "load_balancer_fronts_the_public_listener" {
  command = apply

  variables {
    hostname             = "gang.example.com"
    load_balancer        = {}
    client_ingress_cidrs = ["198.51.100.0/24"]
  }

  assert {
    condition     = can(regex("(?s)\\[server\\][^\\[]*ca_pin = \"signer\"\n# A load balancer presents a publicly trusted certificate for endpoint, so clients use their system roots.\nendpoint_trust = \"system\"\n\n\\[authentication", output.gang_toml))
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

run "renders_gang_settings_override" {
  command = plan

  variables {
    gang_settings = {
      poll_interval           = "3s"
      permit_network_property = true
    }
  }

  assert {
    condition     = strcontains(output.gang_toml, "poll_interval = \"3s\"")
    error_message = "gang_settings must override the rendered poll interval."
  }

  assert {
    condition     = strcontains(output.gang_toml, "permit_network_property = true")
    error_message = "gang_settings must override the sandbox network setting."
  }
}

run "pins_ca_to_signer_and_wires_endpoints" {
  command = apply

  assert {
    condition     = can(regex("(?s)\\[server\\][^\\[]*ca_pin = \"signer\"", output.gang_toml))
    error_message = "gang.toml [server] must render ca_pin = \"signer\" so workers pin the signer's public key."
  }

  assert {
    condition     = can(regex("(?m)^endpoint = \"${output.endpoint}\"$", output.gang_toml)) && can(regex("(?s)\\[scheduler\\.nativelink\\][^\\[]*worker_endpoint = \"${output.worker_endpoint}\"", output.gang_toml))
    error_message = "gang.toml must carry the public endpoint at the top level and the worker endpoint in [scheduler.nativelink]."
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
        name                        = "gang-worker-x86"
        isa                         = "X86"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000003"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
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
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200"
        target_actions_per_instance = 16
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
    condition     = aws_ssm_parameter.config.name == "/gang/gang.toml" && output.config_parameter_name == "/gang/gang.toml"
    error_message = "The config parameter must default to /<name>/gang.toml."
  }

  assert {
    condition     = aws_ssm_parameter.config.value == output.gang_toml
    error_message = "The config parameter value must equal the rendered gang.toml."
  }

  assert {
    condition     = aws_ssm_parameter.config.type == "String" && aws_ssm_parameter.config.tier == "Intelligent-Tiering"
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
    fleets = [
      {
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
        fallback                    = {}
      },
      {
        name                        = "gang-worker-aarch64"
        isa                         = "aarch64"
        capacity                    = "on-demand"
        ami_id                      = "ami-00000000000000002"
        instance_types              = ["c7gd.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
        warm_pool_size              = 2
      },
    ]
  }

  assert {
    condition     = output.asg_names == ["gang-worker-x86-64", "gang-worker-x86-64-on-demand", "gang-worker-aarch64"]
    error_message = "A fallback fleet's on-demand twin must follow its spot group in asg_names, which the controller receives as its worker fleet names."
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
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "on-demand"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
        fallback                    = {}
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
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
        warm_pool_size              = 2
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
      name                        = "worker"
      isa                         = "riscv"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    }]
  }

  expect_failures = [var.fleets]
}

run "rejects_zero_fleet_disk" {
  command = plan

  variables {
    fleets = [{
      name                        = "worker"
      isa                         = "x86-64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "0B"
      target_actions_per_instance = 16
    }]
  }

  expect_failures = [var.fleets]
}

run "rejects_zero_fleet_memory" {
  command = plan

  variables {
    fleets = [{
      name                        = "worker"
      isa                         = "x86-64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "0B"
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    }]
  }

  expect_failures = [var.fleets]
}

run "rejects_overflowing_fleet_size" {
  command = plan

  variables {
    fleets = [{
      name                        = "worker"
      isa                         = "x86-64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "16777216TiB"
      target_actions_per_instance = 16
    }]
  }

  expect_failures = [var.fleets]
}

run "accepts_nonzero_sizes_with_leading_zeroes" {
  command = plan

  variables {
    fleets = [{
      name                        = "worker"
      isa                         = "x86-64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "0001GiB"
      disk_per_instance           = "0001B"
      target_actions_per_instance = 16
    }]
  }
}

run "rejects_min_instances_above_max_instances" {
  command = plan

  variables {
    fleets = [
      {
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        min_instances               = 5
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
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
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 0
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
      },
    ]
  }

  expect_failures = [
    var.fleets,
  ]
}

run "rejects_zero_target_actions_per_instance" {
  command = plan

  variables {
    fleets = [
      {
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 4
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 0
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
        name                        = "gang-worker-x86-64"
        isa                         = "x86-64"
        capacity                    = "spot"
        ami_id                      = "ami-00000000000000001"
        instance_types              = ["c6id.4xlarge"]
        max_instances               = 0
        vcpus_per_instance          = 15
        memory_per_instance         = "29GiB"
        disk_per_instance           = "200GiB"
        target_actions_per_instance = 16
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
