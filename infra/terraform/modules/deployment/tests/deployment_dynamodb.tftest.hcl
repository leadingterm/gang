mock_provider "aws" {
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
      boot_modes              = ["legacy-bios", "uefi"]
      supported_architectures = ["x86_64"]
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
  client_token_providers   = ["google", "github-actions"]
  region                   = "eu-north-1"
  cidr_block               = "10.20.0.0/16"
  cache_bucket_name        = "example-gang-cache"
  controller_instance_type = "c7i.large"
  default_isa              = "x86-64"
  trust_domain             = "example.com"
  fleets = [
    {
      name                = "gang-worker-x86-64"
      isa                 = "x86-64"
      capacity            = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4
      vcpus_per_instance  = 15
      memory_per_instance = "29GiB"
      disk_per_instance   = "200GiB"
    },
    {
      name                = "gang-worker-aarch64"
      isa                 = "aarch64"
      capacity            = "on-demand"
      instance_types      = ["c7gd.4xlarge"]
      max_instances       = 4
      vcpus_per_instance  = 15
      memory_per_instance = "29GiB"
      disk_per_instance   = "200GiB"
    },
  ]
}

run "dynamodb_group_shape" {
  command = apply

  variables {
    database         = { engine = "dynamodb" }
    controller_count = 2
    hostname         = "gang.example.com"
    load_balancer    = {}
    nat_mode         = "per_az"
  }

  assert {
    condition     = output.controller_shape == "group" && strcontains(output.gang_toml, "[database.dynamodb]") && !strcontains(output.gang_toml, "[database.sqlite]") && strcontains(output.gang_toml, "[cache.s3]")
    error_message = "The group must render DynamoDB and the shared S3 cache."
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
    condition     = can(regex("(?s)\\[server\\.http2\\][^\\[]*shutdown_grace = \"300s\"", output.gang_toml))
    error_message = "The group must configure shutdown_grace in [server.http2]."
  }

  assert {
    condition     = strcontains(output.gang_toml, "health_listen = \"0.0.0.0:${module.network.health_port}\"") && module.load_balancer[0].health_port == module.network.health_port
    error_message = "The group must use the network health port in its server listener and load balancer."
  }
  assert {
    condition     = strcontains(output.gang_toml, "standby_routability_delay = \"${local.health.interval_seconds * local.health.threshold}s\"") && strcontains(output.gang_toml, "[server.leadership]\nrenew_interval = \"${local.health.renew_interval_seconds}s\"\ncall_timeout = \"${local.health.call_timeout_seconds}s\"")
    error_message = "A shared deployment must use the health window and the refresh policy's renewal interval and call timeout."
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

run "rejects_boot_grace_at_shutdown_grace" {
  command = plan
  variables {
    database      = { engine = "dynamodb" }
    hostname      = "gang.example.com"
    load_balancer = {}
    fleets = [{
      name                = "gang-worker-x86-64"
      isa                 = "x86-64"
      capacity            = "spot"
      instance_types      = ["c6id.4xlarge"]
      max_instances       = 4
      vcpus_per_instance  = 15
      memory_per_instance = "29GiB"
      disk_per_instance   = "200GiB"
      boot_grace          = "5m"
    }]
  }
  expect_failures = [terraform_data.input_checks]
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

run "rejects_multiple_controllers_in_one_zone" {
  command = plan
  variables {
    database         = { engine = "dynamodb" }
    hostname         = "gang.example.com"
    load_balancer    = {}
    controller_count = 2
    network          = { availability_zone_count = 1 }
    nat_mode         = "per_az"
  }
  expect_failures = [var.controller_count]
}
