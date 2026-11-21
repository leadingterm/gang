mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000" }
  }
  mock_data "aws_region" {
    defaults = { region = "eu-north-1" }
  }
  mock_data "aws_network_interfaces" {
    defaults = { ids = ["eni-0123456789abcdef0"] }
  }
  mock_data "aws_network_interface" {
    defaults = {
      private_ip  = "10.20.1.10"
      private_ips = ["10.20.1.10"]
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
  mock_data "aws_ec2_instance_type" {
    defaults = {
      boot_modes                 = ["legacy-bios", "uefi"]
      supported_architectures    = ["x86_64"]
      instance_storage_supported = true
      hypervisor                 = "nitro"
      instance_disks             = [{ count = 1, size = 950, type = "ssd" }]
    }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_resource "aws_kms_key" {
    defaults = {
      arn    = "arn:aws:kms:eu-north-1:000000000000:key/11111111-2222-3333-4444-555555555555"
      key_id = "11111111-2222-3333-4444-555555555555"
    }
  }
  mock_resource "aws_dynamodb_table" {
    defaults = { arn = "arn:aws:dynamodb:eu-north-1:000000000000:table/gang-state" }
  }
  mock_resource "aws_ssm_parameter" {
    defaults = {
      arn     = "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml"
      version = 1
    }
  }
  mock_resource "aws_launch_template" {
    defaults = { id = "lt-0123456789abcdef0" }
  }
  mock_resource "aws_eip" {
    defaults = { public_ip = "203.0.113.10" }
  }
  mock_resource "aws_lb" {
    defaults = {
      arn      = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:loadbalancer/net/gang/0000000000000000"
      dns_name = "gang-0000000000.elb.eu-north-1.amazonaws.com"
    }
  }
  mock_resource "aws_lb_target_group" {
    defaults = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/gang/0000000000000000" }
  }
  mock_resource "aws_acm_certificate" {
    defaults = { arn = "arn:aws:acm:eu-north-1:000000000000:certificate/00000000-0000-0000-0000-000000000000" }
  }
  mock_resource "aws_iam_outbound_web_identity_federation" {
    defaults = { issuer_identifier = "https://issuer.example.com/123456789012" }
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
  values = { arn = "arn:aws:iam::000000000000:instance-profile/gang-worker" }
}
override_resource {
  target = module.worker_fleet.module.instance_role.aws_iam_role.this
  values = { arn = "arn:aws:iam::000000000000:role/gang-worker" }
}
override_resource {
  target = module.controller.module.instance_role.aws_iam_role.this
  values = { arn = "arn:aws:iam::000000000000:role/gang-controller" }
}
override_resource {
  target = module.controller.module.instance_role.aws_iam_instance_profile.this
  values = { arn = "arn:aws:iam::000000000000:instance-profile/gang-controller" }
}
override_data {
  target = module.storage.data.aws_iam_policy_document.ca_key
  values = { json = "{}" }
}

variables {
  client_token_providers    = ["google", "github-actions"]
  region                   = "eu-north-1"
  cidr_block               = "10.20.0.0/16"
  cache_bucket_name        = "example-gang-cache"
  controller_instance_type = "c7i.large"
  default_isa              = "x86-64"
  trust_domain             = "example.com"
  database                 = { engine = "dynamodb" }
  controller_count         = 1
  hostname                 = "gang.example.com"
  load_balancer            = {}
  nat_mode                 = "per_az"
  fleets = [{
    name                        = "gang-worker-x86-64"
    isa                         = "x86-64"
    capacity                    = "spot"
    instance_types              = ["c6id.4xlarge"]
    min_instances               = 1
    max_instances               = 1
    vcpus_per_instance          = 2
    memory_per_instance         = "4GiB"
    disk_per_instance           = "20GiB"
    target_actions_per_instance = 2
  }]
}

run "seed" {
  command = apply
  variables {
    gang_settings = { poll_interval = "51ms" }
  }
}

run "transition" {
  command = plan
  variables {
    gang_settings = { poll_interval = "50ms" }
  }
}

run "promote" {
  command = apply
  variables {
    gang_settings = { poll_interval = "50ms" }
  }
}

run "upgrade" {
  command = plan
  variables {
    gang_settings = { poll_interval = "50ms" }
  }
}

# Probe the engine's retention action without replacing the applied baseline.
run "retention" {
  command = plan
  variables {
    gang_settings = { poll_interval = "50ms" }
  }
  plan_options {
    replace = [aws_ssm_parameter.config_generation]
  }
}
