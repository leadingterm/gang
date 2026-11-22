mock_provider "aws" {
  mock_resource "aws_launch_template" {
    defaults = {
      id = "lt-0123456789abcdef0"
    }
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

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:ListBucket\",\"Resource\":\"*\"}]}"
    }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "000000000000"
    }
  }

  mock_data "aws_subnet" {
    defaults = {
      availability_zone = "eu-north-1a"
    }
  }

  mock_data "aws_ec2_instance_type" {
    defaults = {
      boot_modes = ["legacy-bios", "uefi"]
    }
  }
}

variables {
  name                       = "gang-controller"
  ami_id                     = "ami-0123456789abcdef0"
  instance_type              = "m7i.large"
  subnet_id                  = "subnet-0123456789abcdef0"
  private_ip                 = "10.20.0.10"
  security_group_id          = "sg-0123456789abcdef0"
  eip_allocation_id          = "eipalloc-0123456789abcdef0"
  config_parameter_reference = "/gang-controller/gang.toml/generations/example:1"
  config_parameter_arns      = ["arn:aws:ssm:eu-north-1:000000000000:parameter/gang-controller/gang.toml/generations/*"]
  cache_bucket_arn           = "arn:aws:s3:::example-gang-cache"
  ca_key_arn                 = "arn:aws:kms:eu-north-1:000000000000:key/00000000-0000-0000-0000-000000000000"
  worker_fleet_names = [
    "gang-x86-64",
  ]
  tags = {
    Project = "gang"
  }
}

run "group_resources_and_permissions" {
  command = plan

  variables {
    shape            = "group"
    controller_count = 2
    subnet_ids       = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
    target_group_arns = [
      "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/serve/0123456789abcdef",
      "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/execute/0123456789abcdef",
      "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/workers/0123456789abcdef",
    ]
    config_digest      = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    dynamodb_table_arn = "arn:aws:dynamodb:eu-north-1:000000000000:table/gang"
    secret_parameters  = { token = "/gang/token" }
  }

  assert {
    condition     = length(aws_instance.this) == 0 && length(aws_ebs_volume.state) == 0 && length(aws_volume_attachment.state) == 0 && length(aws_eip_association.this) == 0 && length(aws_cloudwatch_metric_alarm.recover) == 0
    error_message = "The group shape must not create instance-only resources."
  }

  assert {
    condition     = output.instance_id == null && output.state_volume_id == null && output.autoscaling_group_name == var.name
    error_message = "Outputs must reflect the selected shape."
  }

  assert {
    condition     = base64decode(aws_launch_template.controller[0].user_data) == join("\n", ["controller ${var.config_parameter_reference}", "token=/gang/token", "# config-digest ${var.config_digest}"])
    error_message = "Group user data must carry the controller configuration, secrets and digest without a state device."
  }

  assert {
    condition     = aws_launch_template.controller[0].update_default_version == true
    error_message = "The controller launch template must promote new versions to the default."
  }

  assert {
    condition     = aws_launch_template.controller[0].metadata_options[0].http_tokens == "required" && aws_launch_template.controller[0].metadata_options[0].http_put_response_hop_limit == 1 && aws_launch_template.controller[0].metadata_options[0].instance_metadata_tags == "enabled"
    error_message = "Group instances must use IMDSv2 and expose instance tags."
  }

  assert {
    condition     = length(aws_launch_template.controller[0].tag_specifications) == 2 && alltrue([for kind in ["instance", "volume"] : anytrue([for spec in aws_launch_template.controller[0].tag_specifications : spec.resource_type == kind && spec.tags == tomap(merge(var.tags, { Name = var.name }))])])
    error_message = "Controller instances and volumes must carry the deployment tags."
  }

  assert {
    condition     = aws_autoscaling_group.controller[0].min_size == 2 && aws_autoscaling_group.controller[0].desired_capacity == 2 && aws_autoscaling_group.controller[0].max_size == 4 && aws_autoscaling_group.controller[0].health_check_type == "EC2" && toset(aws_autoscaling_group.controller[0].target_group_arns) == toset(var.target_group_arns) && toset(aws_autoscaling_group.controller[0].vpc_zone_identifier) == toset(var.subnet_ids)
    error_message = "The group must use private subnets, all target groups, EC2 health and the required capacity bounds."
  }

  assert {
    condition     = length(aws_autoscaling_group.controller[0].termination_policies) == 1 && tolist(aws_autoscaling_group.controller[0].termination_policies)[0] == "OldestInstance"
    error_message = "Controller scale-in must prefer the oldest host."
  }

  assert {
    condition     = aws_autoscaling_group.controller[0].instance_refresh[0].strategy == "Rolling" && aws_autoscaling_group.controller[0].instance_refresh[0].preferences[0].min_healthy_percentage == 100 && aws_autoscaling_group.controller[0].instance_refresh[0].preferences[0].max_healthy_percentage == 200
    error_message = "Group refresh must roll with 100 to 200 percent healthy capacity."
  }

  assert {
    condition     = tonumber(aws_autoscaling_group.controller[0].instance_refresh[0].preferences[0].instance_warmup) == 0 && length(aws_autoscaling_group.controller[0].termination_policies) == 1 && tolist(aws_autoscaling_group.controller[0].termination_policies)[0] == "OldestInstance"
    error_message = "Refresh must disable timer-based warmup and ordinary scale-in must prefer the oldest host."
  }

  assert {
    condition     = length([for hook in aws_autoscaling_group.controller[0].initial_lifecycle_hook : hook if hook.lifecycle_transition == "autoscaling:EC2_INSTANCE_LAUNCHING" && hook.default_result == "ABANDON" && hook.heartbeat_timeout == local.hook.heartbeat_timeout_seconds]) == 1 && length([for hook in aws_autoscaling_group.controller[0].initial_lifecycle_hook : hook if hook.lifecycle_transition == "autoscaling:EC2_INSTANCE_TERMINATING" && hook.default_result == "CONTINUE" && hook.heartbeat_timeout == 3600]) == 1
    error_message = "Launch and termination hooks must use the canonical heartbeat and safe defaults."
  }

  assert {
    condition     = anytrue([for s in data.aws_iam_policy_document.this.statement : s.sid == "ControllerLifecycle" && s.effect == "Allow" && toset(s.actions) == toset(["autoscaling:CompleteLifecycleAction", "autoscaling:RecordLifecycleActionHeartbeat"]) && toset(s.resources) == toset(["arn:aws:autoscaling:eu-north-1:000000000000:autoScalingGroup:*:autoScalingGroupName/${var.name}"])]) && anytrue([for s in data.aws_iam_policy_document.this.statement : s.sid == "Database" && length(s.resources) == 1 && contains(s.resources, var.dynamodb_table_arn) && contains(s.actions, "dynamodb:ConditionCheckItem")])
    error_message = "Lifecycle completion and DynamoDB permissions must be scoped to the controller group and table."
  }

  assert {
    condition     = anytrue([for s in data.aws_iam_policy_document.this.statement : s.sid == "RegisterControllerTarget" && s.effect == "Allow" && toset(s.actions) == toset(["elasticloadbalancing:RegisterTargets"]) && toset(s.resources) == toset(var.target_group_arns)])
    error_message = "Target registration must be scoped to the controller group's target groups."
  }

  assert {
    condition = alltrue([
      for action in ["autoscaling:DescribeAutoScalingGroups", "autoscaling:DescribeLifecycleHooks", "elasticloadbalancing:DescribeTargetHealth"] :
      anytrue([for s in data.aws_iam_policy_document.this.statement : s.effect == "Allow" && contains(s.actions, action) && toset(s.resources) == toset(["*"])])
    ])
    error_message = "Controller group lifecycle discovery must allow both describe actions on all resources."
  }
}

run "group_requires_subnets" {
  command = plan
  variables {
    shape             = "group"
    target_group_arns = ["serve"]
    config_digest     = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  }
  expect_failures = [var.subnet_ids]
}

run "group_requires_targets" {
  command = plan
  variables {
    shape         = "group"
    subnet_ids    = ["subnet-private"]
    config_digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  }
  expect_failures = [var.target_group_arns]
}

run "group_requires_digest" {
  command = plan
  variables {
    shape             = "group"
    subnet_ids        = ["subnet-private"]
    target_group_arns = ["serve"]
    config_digest     = "invalid"
  }
  expect_failures = [var.config_digest]
}

