override_data {
  target = module.controller.data.aws_ec2_instance_type.this
  values = { supported_architectures = ["arm64"], boot_modes = ["uefi"], burstable_performance_supported = true }
}

mock_provider "aws" {
  mock_data "aws_ami" {
    defaults = { root_device_name = "/dev/xvda" }
  }
  mock_data "aws_ssm_parameter" {
    defaults = { value = "ami-0123456789abcdef0" }
  }
  mock_data "aws_security_group" {
    defaults = { vpc_id = "vpc-0123456789abcdef0" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::000000000000:role/gang-ecs-execution" }
  }
  mock_resource "aws_autoscaling_group" {
    defaults = { arn = "arn:aws:autoscaling:eu-north-1:000000000000:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/gang-ecs-capacity" }
  }
  mock_resource "aws_ecs_cluster" {
    defaults = { arn = "arn:aws:ecs:eu-north-1:000000000000:cluster/gang-controller" }
  }
  mock_resource "aws_ecs_task_definition" {
    defaults = { arn = "arn:aws:ecs:eu-north-1:000000000000:task-definition/gang-controller:1" }
  }
  mock_resource "aws_ecr_repository" {
    defaults = { arn = "arn:aws:ecr:eu-north-1:000000000000:repository/gang-controller", repository_url = "000000000000.dkr.ecr.eu-north-1.amazonaws.com/gang-controller" }
  }

  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000" }
  }

  mock_data "aws_network_interface" {
    defaults = { private_ips = ["10.20.2.10", "10.20.2.11"] }
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
  client_token_providers  = ["google", "github-actions"]
  region                  = "eu-north-1"
  cidr_block              = "10.20.0.0/16"
  cache_bucket_name       = "example-gang-cache"
  controller_image_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  default_isa             = "x86-64"
  trust_domain            = "example.com"
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

run "dynamodb_group_shape" {
  command = apply

  variables {
    database                     = { engine = "dynamodb" }
    controller_count             = 1
    hostname                     = "gang.example.com"
    load_balancer                = {}
    nat_mode                     = "per_az"
    controller_secret_parameters = { token = "/gang/secrets/token" }
  }

  assert {
    condition     = output.controller_shape == "service" && strcontains(output.gang_toml, "[database.dynamodb]") && !strcontains(output.gang_toml, "[database.sqlite]") && strcontains(output.gang_toml, "[cache.s3]")
    error_message = "The group must render DynamoDB and the shared S3 cache."
  }

  assert {
    condition     = output.controller_count == 1 && output.controller_instance_id == null && output.controller_service_name != null && output.controller_cluster_name != null && output.controller_task_definition_arn != null && output.controller_capacity_asg_name != null
    error_message = "A shared database must expose its ECS service, cluster, task definition, and capacity ASG."
  }

  assert {
    condition     = var.controller_instance_type == "t4g.large" && var.default_isa == "x86-64"
    error_message = "The ARM controller capacity default must be independent of the worker ISA."
  }

  assert {
    condition     = output.controller_repository_url == module.controller.repository_url && output.controller_repository_url != null && output.controller_image_digest == var.controller_image_digest
    error_message = "The deployment must expose the private controller repository and configured immutable image digest."
  }

  assert {
    condition     = length(module.load_balancer[0].target_groups) == 3 && module.load_balancer[0].target_groups["serve"].port == module.network.public_port && module.load_balancer[0].target_groups["internal_serve"].port == module.network.public_port && module.load_balancer[0].target_groups["workers"].port == module.network.worker_port && length(output.controller_target_group_arns) == 3
    error_message = "All three load balancer targets must reach the correct ECS container ports."
  }
  assert {
    condition     = module.load_balancer[0].target_groups["serve"].arn == module.load_balancer[0].serve_target_group_arn && module.load_balancer[0].target_groups["internal_serve"].arn == module.load_balancer[0].internal_serve_target_group_arn && module.load_balancer[0].target_groups["workers"].arn == module.load_balancer[0].workers_target_group_arn
    error_message = "The controller must use the existing listener target groups, not migration target groups."
  }

  assert {
    condition     = strcontains(output.gang_toml, "stop_timeout = \"2460s\"") && !contains(keys(output.images), "controller") && alltrue([for fleet in var.fleets : contains(keys(output.images), fleet.name)])
    error_message = "The service must use its drain timeout without resolving a controller Gang AMI, while retaining worker AMIs."
  }

  assert {
    condition     = module.controller.secret_parameter_arns == ["arn:aws:ssm:eu-north-1:000000000000:parameter/gang/secrets/token"] && output.config_parameter_reference == "${output.config_parameter_name}:1" && output.controller_config_digest == sha256(output.gang_toml)
    error_message = "The ECS controller must receive only secret parameter references and a version-pinned configuration with its digest."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "execution_listen") && !strcontains(output.gang_toml, "execution_endpoint") && strcontains(output.gang_toml, "endpoint = \"https://gang.example.com\"") && strcontains(output.gang_toml, "worker_endpoint = \"grpcs://gang.example.com:9090\"")
    error_message = "The group must use one public endpoint and the private worker load-balancer endpoint."
  }

  assert {
    condition     = can(regex("(?s)\\[server\\.http2\\][^\\[]*max_connections = 4096\n", output.gang_toml))
    error_message = "The group must default to 4096 public listener connections."
  }

  assert {
    condition     = !strcontains(output.gang_toml, "shutdown_grace")
    error_message = "The generated controller configuration must not specify shutdown grace."
  }

  assert {
    condition     = strcontains(output.gang_toml, "health_listen = \"0.0.0.0:${module.network.health_port}\"") && module.load_balancer[0].health_port == module.network.health_port
    error_message = "The group must use the network health port in its server listener and load balancer."
  }
  assert {
    condition     = !strcontains(output.gang_toml, "standby_routability_delay") && strcontains(output.gang_toml, "[server.leadership]\nrenew_interval = \"${local.health.renew_interval_seconds}s\"\ncall_timeout = \"${local.health.call_timeout_seconds}s\"")
    error_message = "A shared deployment must use the leadership intervals without a standby delay."
  }


  assert {
    condition     = module.storage.dynamodb_table_name == "gang-state" && output.controller_public_ip == null
    error_message = "The group must use its deployment-named table without a controller Elastic IP."
  }
  assert {
    condition     = strcontains(output.gang_toml, "[server.proxy_protocol]\ntrusted_cidrs = [\"10.20.2.10/32\",\"10.20.2.11/32\"]") && !strcontains(output.gang_toml, "10.20.0.0/16")
    error_message = "DynamoDB must render exact NLB interface addresses."
  }
}

run "dynamodb_custom_http2_max_connections" {
  command = plan

  variables {
    database              = { engine = "dynamodb" }
    hostname              = "gang.example.com"
    load_balancer         = {}
    http2_max_connections = 8192
  }

  assert {
    condition     = can(regex("(?s)\\[server\\.http2\\][^\\[]*max_connections = 8192\n", output.gang_toml))
    error_message = "The group must render the configured public listener connection limit."
  }
}


run "rejects_dynamodb_without_load_balancer" {
  command = plan
  variables { database = { engine = "dynamodb" } }
  expect_failures = [var.database]
}

run "rejects_dynamodb_filesystem_cache" {
  command = plan
  variables {
    database          = { engine = "dynamodb" }
    hostname          = "gang.example.com"
    load_balancer     = {}
    extra_config_toml = "[cache.filesystem]\npath = \"/tmp/cache\""
  }
  expect_failures = [var.database]
}

run "rejects_nonpositive_controller_count" {
  command = plan
  variables {
    database         = { engine = "dynamodb" }
    hostname         = "gang.example.com"
    load_balancer    = {}
    controller_count = 0
  }
  expect_failures = [var.controller_count]
}

run "rejects_fractional_controller_count" {
  command = plan
  variables {
    database         = { engine = "dynamodb" }
    hostname         = "gang.example.com"
    load_balancer    = {}
    controller_count = 1.5
    nat_mode         = "per_az"
  }
  expect_failures = [var.controller_count]
}

run "rejects_multiple_controllers" {
  command = plan
  variables {
    database         = { engine = "dynamodb" }
    hostname         = "gang.example.com"
    load_balancer    = {}
    controller_count = 2
    network          = { availability_zone_count = 2 }
    nat_mode         = "per_az"
  }
  expect_failures = [var.controller_count]
}

run "rejects_missing_controller_image" {
  command = plan
  variables {
    database                = { engine = "dynamodb" }
    hostname                = "gang.example.com"
    load_balancer           = {}
    controller_image_digest = null
  }
  expect_failures = [var.controller_image_digest]
}

run "rejects_mutable_controller_image" {
  command = plan
  variables {
    database                = { engine = "dynamodb" }
    hostname                = "gang.example.com"
    load_balancer           = {}
    controller_image_digest = "latest"
  }
  expect_failures = [var.controller_image_digest]
}

run "renders_configured_controller_drain_timeout" {
  command = plan
  variables {
    database                        = { engine = "dynamodb" }
    hostname                        = "gang.example.com"
    load_balancer                   = {}
    controller_stop_timeout_seconds = 3600
  }
  assert {
    condition     = strcontains(output.gang_toml, "stop_timeout = \"3600s\"")
    error_message = "The configured stop timeout must reach the controller configuration."
  }
}
