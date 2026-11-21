mock_provider "aws" {
  mock_data "aws_ami" {
    defaults = { root_device_name = "/dev/xvda" }
  }

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
  target = module.network.aws_subnet.private[0]
  values = {
    id = "subnet-00000000000000001"
  }
}

override_resource {
  target = module.network.aws_subnet.private[1]
  values = {
    id = "subnet-00000000000000002"
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
  database                 = { engine = "dynamodb" }
  hostname                 = "gang.example.com"
  load_balancer            = {}
  network                  = { availability_zone_count = 2 }
  tags                     = { Environment = "test" }
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

run "accepts_maximum_config_name_length" {
  command = plan
  variables { config_parameter_name = "/${join("", [for i in range(916) : "a"])}" }
  assert {
    condition     = length(local.config_parameter_name) == 917
    error_message = "A 917-character namespace must fit the standard partition and eu-north-1 generation ARN budget."
  }
}

run "rejects_config_name_over_arn_budget" {
  command = plan
  variables { config_parameter_name = "/${join("", [for i in range(917) : "a"])}" }
  expect_failures = [terraform_data.input_checks]
}

run "accepts_govcloud_maximum_config_name_length" {
  command = plan
  variables {
    region                = "us-gov-west-1"
    config_parameter_name = "/${join("", [for i in range(906) : "a"])}"
  }
  override_data {
    target = data.aws_partition.this
    values = { partition = "aws-us-gov" }
  }
  assert {
    condition     = length(local.config_parameter_name) == 907
    error_message = "A 907-character namespace must fit the GovCloud generation ARN budget."
  }
}

run "rejects_govcloud_config_name_over_arn_budget" {
  command = plan
  variables {
    region                = "us-gov-west-1"
    config_parameter_name = "/${join("", [for i in range(907) : "a"])}"
  }
  override_data {
    target = data.aws_partition.this
    values = { partition = "aws-us-gov" }
  }
  expect_failures = [terraform_data.input_checks]
}

run "rejects_oversized_config_name" {
  command = plan
  variables { config_parameter_name = "/${join("", [for i in range(962) : "a"])}" }
  expect_failures = [var.config_parameter_name]
}


run "applyA" {
  command = apply
  assert {
    condition     = aws_ssm_parameter.config_generation.value == output.gang_toml && aws_ssm_parameter.config_generation.overwrite == false
    error_message = "The immutable generation must contain the rendered configuration."
  }
  assert {
    condition     = aws_ssm_parameter.config_generation.tags["Environment"] == "test" && aws_ssm_parameter.config_generation.tags["Name"] == "gang-config-generation"
    error_message = "Caller tags must reach the generation with its own Name tag."
  }
  assert {
    condition     = output.config_parameter_reference == "${output.config_parameter_name}:1" && startswith(output.config_parameter_name, "${local.config_parameter_name}/generations/") && local.config_parameter_arns == ["arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml/generations/*"]
    error_message = "The pinned generation and readable namespace must be exact."
  }
}

run "planUnchanged" {
  command = plan
  assert {
    condition     = aws_ssm_parameter.config_generation.value == output.gang_toml
    error_message = "Unchanged configuration must retain the rendered generation bytes."
  }
}

run "planB" {
  command = plan
  variables { gang_settings = { poll_interval = "3s" } }
}

run "applyB" {
  command = apply
  variables { gang_settings = { poll_interval = "3s" } }
  assert {
    condition     = aws_ssm_parameter.config_generation.value == output.gang_toml && aws_ssm_parameter.config_generation.name == output.config_parameter_name && startswith(output.config_parameter_name, "${local.config_parameter_name}/generations/") && output.config_parameter_reference == "${output.config_parameter_name}:1" && aws_ssm_parameter.config_generation.overwrite == false
    error_message = "B must publish its rendered bytes at an immutable pinned generation."
  }
  assert {
    condition     = local.config_parameter_arns == ["arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml/generations/*"]
    error_message = "The previous pinned selector must remain covered by the generation IAM grant."
  }
}

run "planRollbackA" {
  command = plan
}

run "applyRollbackA" {
  command = apply
  assert {
    condition     = aws_ssm_parameter.config_generation.value == output.gang_toml && aws_ssm_parameter.config_generation.name == output.config_parameter_name && startswith(output.config_parameter_name, "${local.config_parameter_name}/generations/") && output.config_parameter_reference == "${output.config_parameter_name}:1" && aws_ssm_parameter.config_generation.overwrite == false
    error_message = "Rollback must publish its rendered bytes at an immutable pinned generation."
  }
}
