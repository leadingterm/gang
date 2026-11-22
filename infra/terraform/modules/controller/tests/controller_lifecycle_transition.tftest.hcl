# Only apply runs use the mock; real provider plans own the replacement verdict.
mock_provider "aws" {
  mock_resource "aws_autoscaling_group" {
    defaults = { id = "gang-controller" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}

provider "aws" {
  alias                       = "offline"
  region                      = "eu-north-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
  max_retries                 = 0

  # Tripwires: even an unintended API call cannot reach AWS.
  endpoints {
    autoscaling = "http://127.0.0.1:1"
    ec2         = "http://127.0.0.1:1"
    iam         = "http://127.0.0.1:1"
    sts         = "http://127.0.0.1:1"
  }
}

override_data {
  target = data.aws_partition.this
  values = { partition = "aws", dns_suffix = "amazonaws.com" }
}
override_data {
  target = data.aws_region.this
  values = { region = "eu-north-1" }
}
override_data {
  target = data.aws_caller_identity.this
  values = { account_id = "000000000000" }
}
override_data {
  target = data.aws_ec2_instance_type.this
  values = { boot_modes = ["uefi"] }
}
override_data {
  target = data.aws_ami.controller[0]
  values = { root_device_name = "/dev/xvda" }
}
# Keep the unrelated launch-template diff out of the ASG hook regression.
override_resource {
  target = aws_launch_template.controller[0]
  values = { id = "lt-0123456789abcdef0", latest_version = 1, default_version = 1 }
}

variables {
  shape                      = "group"
  name                       = "gang-controller"
  ami_id                     = "ami-0123456789abcdef0"
  instance_type              = "m7i.large"
  security_group_id          = "sg-0123456789abcdef0"
  subnet_ids                 = ["subnet-0123456789abcdef0"]
  target_group_arns          = ["arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/gang/0123456789abcdef"]
  config_parameter_reference = "/gang-controller/gang.toml/generations/example:1"
  config_parameter_arns      = ["arn:aws:ssm:eu-north-1:000000000000:parameter/gang-controller/gang.toml/generations/*"]
  config_digest              = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  cache_bucket_arn           = null
  ca_key_arn                 = "arn:aws:kms:eu-north-1:000000000000:key/00000000-0000-0000-0000-000000000000"
  worker_fleet_names         = ["gang-workers"]
  tags                       = { Contract = "legacy" }
}

run "seed_legacy_inline_hooks" {
  command = apply
  variables {
    test_legacy_hooks = true
  }
  assert {
    condition     = length(aws_autoscaling_group.controller[0].initial_lifecycle_hook) == 2 && alltrue([for hook in aws_autoscaling_group.controller[0].initial_lifecycle_hook : hook.heartbeat_timeout == 420]) && length(aws_autoscaling_lifecycle_hook.controller_launch) == 0 && length(aws_autoscaling_lifecycle_hook.controller_terminate) == 0
    error_message = "The seed must represent an existing controller ASG with only the two legacy inline 420-second hooks."
  }
}

run "adopt_live_hooks" {
  command   = plan
  providers = { aws = aws.offline }
  plan_options {
    refresh = false
  }
  variables {
    # A harmless tag change makes the safe action exactly update, not no-op.
    tags = { Contract = "current" }
  }
  assert {
    condition     = aws_autoscaling_group.controller[0].name == "gang-controller" && aws_autoscaling_lifecycle_hook.controller_launch[0].autoscaling_group_name == aws_autoscaling_group.controller[0].name && aws_autoscaling_lifecycle_hook.controller_terminate[0].autoscaling_group_name == aws_autoscaling_group.controller[0].name && aws_autoscaling_lifecycle_hook.controller_launch[0].name == "launch" && aws_autoscaling_lifecycle_hook.controller_terminate[0].name == "terminate" && aws_autoscaling_lifecycle_hook.controller_launch[0].heartbeat_timeout == 3600 && aws_autoscaling_lifecycle_hook.controller_terminate[0].heartbeat_timeout == 3600
    error_message = "The same controller group must adopt the same-name live hooks at the canonical 3600-second heartbeat."
  }
}

# Only a mock apply advances the shared native state; teardown remains mocked.
run "seed_managed_hooks" {
  command = apply
  variables {
    tags = { Contract = "current" }
  }
  assert {
    condition     = aws_autoscaling_lifecycle_hook.controller_launch[0].heartbeat_timeout == 3600 && aws_autoscaling_lifecycle_hook.controller_terminate[0].heartbeat_timeout == 3600
    error_message = "The managed-hook seed must start at the current heartbeat."
  }
}

run "update_live_hooks" {
  command   = plan
  providers = { aws = aws.offline }
  plan_options {
    refresh = false
  }
  variables {
    test_managed_heartbeat = 7200
    tags                   = { Contract = "future" }
  }
  assert {
    condition     = aws_autoscaling_group.controller[0].name == "gang-controller" && aws_autoscaling_lifecycle_hook.controller_launch[0].heartbeat_timeout == 7200 && aws_autoscaling_lifecycle_hook.controller_terminate[0].heartbeat_timeout == 7200
    error_message = "Future live-hook heartbeat changes must remain mutable on the existing controller group."
  }
}
