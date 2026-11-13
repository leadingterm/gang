mock_provider "aws" {
  mock_data "aws_region" {
    defaults = { region = "eu-north-1" }
  }

  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000" }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:ListBucket\",\"Resource\":\"*\"}]}"
    }
  }

  mock_data "aws_iam_outbound_web_identity_federation" {
    defaults = { issuer_identifier = "https://example.invalid" }
  }

  # Every instance type the tests use is offered in both zones unless a run overrides it.
  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      instance_types = ["c6id.4xlarge", "c6id.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge", "m6id.4xlarge", "m6id.4xlarge", "m7g.2xlarge", "m7g.2xlarge"]
      locations      = ["eu-north-1a", "eu-north-1b", "eu-north-1a", "eu-north-1b", "eu-north-1a", "eu-north-1b", "eu-north-1a", "eu-north-1b"]
    }
  }

  mock_resource "aws_iam_instance_profile" {
    defaults = { arn = "arn:aws:iam::000000000000:instance-profile/gang-worker" }
  }

  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::000000000000:role/gang-worker" }
  }

  mock_resource "aws_iam_outbound_web_identity_federation" {
    defaults = { issuer_identifier = "https://example.invalid" }
  }
}

override_data {
  target = data.aws_ami.this
  values = { root_device_name = "/dev/xvda" }
}

variables {
  cache_bucket_arn  = "arn:aws:s3:::gang-cache-example"
  ca_key_arn        = "arn:aws:kms:eu-north-1:000000000000:key/11111111-2222-3333-4444-555555555555"
  security_group_id = "sg-0000000000000000"
  subnet_ids = {
    "eu-north-1a" = "subnet-0000000000000001"
    "eu-north-1b" = "subnet-0000000000000002"
  }
  config_parameter_name = "/gang/gang.toml"
  config_parameter_arn  = "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml"
  fleet_tag_key         = "gang-fleet"

  fleets = [
    {
      name           = "gang-worker-x86-64"
      capacity       = "spot"
      ami_id         = "ami-0000000000000001"
      instance_types = ["c6id.4xlarge", "c7gd.4xlarge"]
      max_instances  = 8
    },
    {
      name               = "gang-worker-aarch64"
      capacity           = "spot"
      ami_id             = "ami-0000000000000002"
      instance_types     = ["m7g.2xlarge"]
      max_instances      = 4
      scratch_volume_gib = 200
    },
  ]
}

run "one_spot_asg_per_fleet" {
  command = plan

  assert {
    condition     = length(aws_autoscaling_group.this) == 2
    error_message = "Expected exactly one Auto Scaling group per fleet."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      aws_autoscaling_group.this[fleet.name].name == fleet.name
    ])
    error_message = "Each Auto Scaling group must be named exactly fleets[k].name."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      aws_autoscaling_group.this[fleet.name].min_size == 0
      && aws_autoscaling_group.this[fleet.name].max_size == fleet.max_instances
      && aws_autoscaling_group.this[fleet.name].desired_capacity == 0
    ])
    error_message = "Each fleet must scale from 0 up to max_instances, with desired capacity owned by the controller."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      aws_autoscaling_group.this[fleet.name].mixed_instances_policy[0].instances_distribution[0].on_demand_base_capacity == 0
      && aws_autoscaling_group.this[fleet.name].mixed_instances_policy[0].instances_distribution[0].on_demand_percentage_above_base_capacity == 0
      && aws_autoscaling_group.this[fleet.name].mixed_instances_policy[0].instances_distribution[0].spot_allocation_strategy == "price-capacity-optimized"
      && aws_autoscaling_group.this[fleet.name].capacity_rebalance
      if fleet.capacity == "spot"
    ])
    error_message = "Each spot fleet must be spot-only with price-capacity-optimized allocation and capacity rebalancing."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      length(aws_autoscaling_group.this[fleet.name].mixed_instances_policy[0].launch_template[0].override) == length(fleet.instance_types)
    ])
    error_message = "Each fleet must have exactly one mixed-instances override per instance type."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      aws_launch_template.this[fleet.name].metadata_options[0].http_tokens == "required"
      && aws_launch_template.this[fleet.name].metadata_options[0].http_put_response_hop_limit == 1
      && aws_launch_template.this[fleet.name].metadata_options[0].instance_metadata_tags == "enabled"
    ])
    error_message = "Worker instances must require IMDSv2 with a hop limit of 1 and expose instance tags in metadata."
  }

  assert {
    condition = anytrue([
      for tag in aws_autoscaling_group.this["gang-worker-aarch64"].tag :
      tag.key == "gang-fleet" && tag.value == "gang-worker-aarch64" && tag.propagate_at_launch
    ])
    error_message = "Each Auto Scaling group must propagate the gang-fleet tag set to its fleet name at launch."
  }

  assert {
    condition = length([
      for mapping in aws_launch_template.this["gang-worker-x86-64"].block_device_mappings : mapping
      if mapping.device_name == "/dev/sdf"
    ]) == 0
    error_message = "An instance-store fleet without scratch_volume_gib must not get an EBS scratch device."
  }

  assert {
    condition = length([
      for mapping in aws_launch_template.this["gang-worker-aarch64"].block_device_mappings : mapping
      if mapping.device_name == "/dev/sdf" && mapping.ebs[0].volume_size == 200 && mapping.ebs[0].encrypted
    ]) == 1
    error_message = "A fleet with scratch_volume_gib must get one encrypted EBS scratch device at /dev/sdf."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      length([
        for mapping in aws_launch_template.this[fleet.name].block_device_mappings : mapping
        if mapping.device_name == "/dev/xvda" && mapping.ebs[0].encrypted && mapping.ebs[0].volume_size == 30
      ]) == 1
    ])
    error_message = "Each worker must get an encrypted root volume of the default 30 GiB on the AMI's root device."
  }

  assert {
    condition = alltrue([
      for fleet in var.fleets :
      base64decode(aws_launch_template.this[fleet.name].user_data) == var.config_parameter_name
    ])
    error_message = "Worker user data must be the config parameter name, so workers read the current gang.toml at boot."
  }

  assert {
    condition     = output.asg_names == [for fleet in var.fleets : fleet.name]
    error_message = "asg_names must list the Auto Scaling groups in fleet order."
  }
}

run "worker_role_reads_cache_and_requests_bounded_identity_tokens_only" {
  command = plan

  assert {
    condition = anytrue([
      for statement in data.aws_iam_policy_document.worker.statement :
      contains(statement.actions, "sts:GetWebIdentityToken")
      && anytrue([
        for condition in statement.condition :
        condition.test == "ForAllValues:StringEquals"
        && condition.variable == "sts:IdentityTokenAudience"
        && toset(condition.values) == toset(["gang"])
      ])
      && anytrue([
        for condition in statement.condition :
        condition.test == "NumericLessThanEquals"
        && condition.variable == "sts:DurationSeconds"
        && toset(condition.values) == toset(["3600"])
      ])
    ])
    error_message = "sts:GetWebIdentityToken must be limited by the audience and duration conditions."
  }

  assert {
    condition = alltrue([
      for statement in data.aws_iam_policy_document.worker.statement :
      alltrue([
        for action in statement.actions :
        !startswith(action, "autoscaling:") && !startswith(action, "iam:") && (!startswith(action, "kms:") || action == "kms:GetPublicKey")
      ])
    ])
    error_message = "The worker role must hold no autoscaling or IAM permissions, and its only KMS permission is kms:GetPublicKey."
  }

  assert {
    condition = anytrue([
      for statement in data.aws_iam_policy_document.worker.statement :
      contains(statement.resources, "arn:aws:s3:::gang-cache-example/*")
      && contains(statement.actions, "s3:GetObject")
      && contains(statement.actions, "s3:PutObject")
    ])
    error_message = "The worker role must read and write objects in the cache bucket."
  }

  assert {
    condition = toset([for statement in data.aws_iam_policy_document.worker.statement : statement.sid]) == toset([
      "CacheObjects",
      "CacheBucket",
      "WorkloadIdentityToken",
      "ReadCaPublicKey",
    ])
    error_message = "The worker policy must hold exactly the cache, token and CA public key statements; the instance-role module adds SsmCore, ReadConfig and DenyOtherParameters."
  }
}

run "worker_role_reads_only_the_ca_public_key" {
  command = plan

  assert {
    condition = anytrue([
      for statement in data.aws_iam_policy_document.worker.statement :
      statement.sid == "ReadCaPublicKey"
      && toset(statement.actions) == toset(["kms:GetPublicKey"])
      && toset(statement.resources) == toset(["arn:aws:kms:eu-north-1:000000000000:key/11111111-2222-3333-4444-555555555555"])
    ])
    error_message = "The worker role must read the CA public key with kms:GetPublicKey on the CA key only."
  }
}

run "rejects_ebs_only_type_without_scratch_volume" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-aarch64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000002"
        instance_types = ["m7g.2xlarge"]
        max_instances  = 4
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "rejects_bad_asg_name" {
  command = plan

  variables {
    fleets = [
      {
        name           = "Gang_Worker"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "rejects_duplicate_fleet_names" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
      {
        name           = "gang-worker-x86-64"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "rejects_unknown_capacity" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "reserved"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "rejects_empty_instance_types" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = []
        max_instances  = 2
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "rejects_zero_max_instances" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 0
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "on_demand_fleet_buys_only_on_demand_capacity" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].mixed_instances_policy[0].instances_distribution[0].on_demand_percentage_above_base_capacity == 100
    error_message = "An on-demand fleet must buy only on-demand capacity."
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].mixed_instances_policy[0].instances_distribution[0].spot_allocation_strategy == "lowest-price" && !aws_autoscaling_group.this["gang-worker-x86-64"].capacity_rebalance
    error_message = "An on-demand fleet keeps the default allocation strategy and never rebalances."
  }
}


run "rejects_token_lifetime_above_one_hour" {
  command = plan

  variables {
    workload_token_max_seconds = 7200
  }

  expect_failures = [var.workload_token_max_seconds]
}

run "rejects_empty_config_parameter_name" {
  command = plan

  variables {
    config_parameter_name = ""
  }

  expect_failures = [var.config_parameter_name]
}

run "rejects_malformed_ca_key_arn" {
  command = plan

  variables {
    ca_key_arn = "arn:aws:kms:eu-north-1:000000000000:alias/gang-ca"
  }

  expect_failures = [var.ca_key_arn]
}

run "role_defaults_to_the_name_prefix" {
  command = plan

  assert {
    condition     = output.role_name == "gang-worker"
    error_message = "Without iam_name_prefix the role must carry the worker name."
  }
  assert {
    condition     = output.permissions_boundary_arn == null
    error_message = "Without iam_permissions_boundary_arn the role must carry no permissions boundary."
  }
}

run "iam_name_prefix_names_the_role" {
  command = plan

  variables {
    iam_name_prefix = "example-"
  }

  assert {
    condition     = output.role_name == "example-gang-worker"
    error_message = "iam_name_prefix must prefix the role name."
  }
}

run "permissions_boundary_is_set_on_the_role" {
  command = plan

  variables {
    iam_permissions_boundary_arn = "arn:aws:iam::000000000000:policy/example-boundary"
  }

  assert {
    condition     = output.permissions_boundary_arn == "arn:aws:iam::000000000000:policy/example-boundary"
    error_message = "iam_permissions_boundary_arn must be the permissions boundary of the role."
  }
}

run "accepts_an_aws_managed_permissions_boundary" {
  command = plan

  variables {
    iam_permissions_boundary_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
  }

  assert {
    condition     = output.permissions_boundary_arn == "arn:aws:iam::aws:policy/PowerUserAccess"
    error_message = "An AWS-managed policy ARN must be accepted as the permissions boundary."
  }
}

run "on_demand_fleet_keeps_instance_type_order_as_priority" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-ondemand"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000003"
        instance_types = ["m6id.4xlarge", "c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }

  assert {
    condition = [
      for o in aws_autoscaling_group.this["gang-worker-ondemand"].mixed_instances_policy[0].launch_template[0].override : o.instance_type
    ] == ["m6id.4xlarge", "c6id.4xlarge"]
    error_message = "instance_types order is the on-demand priority: overrides must be listed in block order."
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-ondemand"].mixed_instances_policy[0].instances_distribution[0].on_demand_allocation_strategy == "prioritized" && aws_autoscaling_group.this["gang-worker-ondemand"].mixed_instances_policy[0].instances_distribution[0].on_demand_percentage_above_base_capacity == 100
    error_message = "instance_types order is the on-demand priority: an on-demand fleet must use the prioritized allocation strategy with 100% on-demand capacity."
  }
}

run "rejects_duplicate_instance_types" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge", "c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }

  expect_failures = [var.fleets]
}

run "plain_spot_fleet_has_one_unprotected_group_and_no_pool" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }
  assert {
    condition     = length(aws_autoscaling_group.this) == 1 && output.asg_names == ["gang-worker-x86-64"]
    error_message = "A plain spot fleet must plan exactly one Auto Scaling group named after the fleet."
  }
  assert {
    condition     = !aws_autoscaling_group.this["gang-worker-x86-64"].protect_from_scale_in
    error_message = "A plain spot fleet must not be protected from scale-in."
  }
  assert {
    condition     = length(aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool) == 0 && !aws_autoscaling_group.this["gang-worker-x86-64"].force_delete_warm_pool
    error_message = "A plain spot fleet must plan no warm pool."
  }
}

run "fallback_fleet_plans_spot_group_and_on_demand_twin" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge", "m6id.4xlarge"]
        max_instances  = 4
        fallback       = {}
      },
    ]
  }
  assert {
    condition     = length(aws_autoscaling_group.this) == 2 && output.asg_names == ["gang-worker-x86-64", "gang-worker-x86-64-on-demand"]
    error_message = "A fallback fleet must plan its spot group followed by the on-demand twin."
  }
  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].mixed_instances_policy[0].instances_distribution[0].on_demand_percentage_above_base_capacity == 0 && aws_autoscaling_group.this["gang-worker-x86-64"].capacity_rebalance && aws_autoscaling_group.this["gang-worker-x86-64"].max_size == 4
    error_message = "The spot group of a fallback fleet must stay spot-only with capacity rebalancing."
  }
  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].mixed_instances_policy[0].instances_distribution[0].on_demand_percentage_above_base_capacity == 100 && !aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].capacity_rebalance
    error_message = "The on-demand twin must buy 100% on-demand capacity without rebalancing."
  }
  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].min_size == 0 && aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].max_size == 4 && aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].mixed_instances_policy[0].instances_distribution[0].on_demand_allocation_strategy == "prioritized"
    error_message = "The on-demand twin must scale from 0 to max_instances with prioritized allocation."
  }
  assert {
    condition = [
      for o in aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].mixed_instances_policy[0].launch_template[0].override : o.instance_type
    ] == ["c6id.4xlarge", "m6id.4xlarge"]
    error_message = "The on-demand twin must use the fleet's instance types in priority order."
  }
  assert {
    condition = alltrue([
      for name in ["gang-worker-x86-64", "gang-worker-x86-64-on-demand"] :
      anytrue([for tag in aws_autoscaling_group.this[name].tag : tag.key == "gang-fleet" && tag.value == "gang-worker-x86-64" && tag.propagate_at_launch])
      && anytrue([for tag in aws_autoscaling_group.this[name].tag : tag.key == "Name" && tag.value == name])
    ])
    error_message = "Both groups of a fallback fleet must carry the gang-fleet tag of the fleet and their own group name."
  }
  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].protect_from_scale_in && aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].protect_from_scale_in
    error_message = "Both groups of a fallback fleet must be protected from scale-in."
  }
  assert {
    condition     = length(aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool) == 0 && length(aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].warm_pool) == 0
    error_message = "A fallback fleet without warm_pool_size must plan no warm pool."
  }
}

run "on_demand_fleet_warm_pool_is_stopped_and_reused" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 6
        warm_pool_size = 2
      },
    ]
  }
  assert {
    condition     = length(aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool) == 1
    error_message = "An on-demand fleet with warm_pool_size must plan one warm pool on its group."
  }
  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool[0].pool_state == "Stopped" && aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool[0].min_size == 2 && aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool[0].max_group_prepared_capacity == 2 && aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool[0].instance_reuse_policy[0].reuse_on_scale_in
    error_message = "The warm pool must hold stopped instances, keep min and prepared capacity at warm_pool_size, and reuse instances on scale-in."
  }
  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].force_delete_warm_pool && aws_autoscaling_group.this["gang-worker-x86-64"].protect_from_scale_in
    error_message = "A group with a warm pool must force-delete its pool and be protected from scale-in."
  }
}

run "fallback_fleet_warm_pool_sits_on_the_on_demand_twin" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 4
        fallback       = {}
        warm_pool_size = 1
      },
    ]
  }
  assert {
    condition     = length(aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool) == 0 && !aws_autoscaling_group.this["gang-worker-x86-64"].force_delete_warm_pool
    error_message = "The spot group of a fallback fleet must never carry the warm pool."
  }
  assert {
    condition     = length(aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].warm_pool) == 1 && aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].warm_pool[0].min_size == 1 && aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].force_delete_warm_pool
    error_message = "The on-demand twin must carry the fleet's warm pool."
  }
}

run "warm_pool_size_zero_plans_no_pool" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
        warm_pool_size = 0
      },
    ]
  }
  assert {
    condition     = length(aws_autoscaling_group.this["gang-worker-x86-64"].warm_pool) == 0 && !aws_autoscaling_group.this["gang-worker-x86-64"].force_delete_warm_pool && !aws_autoscaling_group.this["gang-worker-x86-64"].protect_from_scale_in
    error_message = "warm_pool_size 0 must plan no warm pool and no scale-in protection."
  }
}

run "rejects_warm_pool_on_plain_spot_fleet" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
        warm_pool_size = 1
      },
    ]
  }
  expect_failures = [var.fleets]
}

run "rejects_fallback_on_on_demand_fleet" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
        fallback       = {}
      },
    ]
  }
  expect_failures = [var.fleets]
}

run "rejects_negative_warm_pool_size" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
        warm_pool_size = -1
      },
    ]
  }
  expect_failures = [var.fleets]
}

run "rejects_fleet_named_like_a_fallback_twin" {
  command = plan
  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
        fallback       = {}
      },
      {
        name           = "gang-worker-x86-64-on-demand"
        capacity       = "on-demand"
        ami_id         = "ami-0000000000000001"
        instance_types = ["c6id.4xlarge"]
        max_instances  = 2
      },
    ]
  }
  expect_failures = [var.fleets]
}

run "fleet_tag_key_names_the_fleet_tag" {
  command = apply

  variables {
    fleet_tag_key = "example-fleet"
  }

  assert {
    condition = anytrue([
      for tag in aws_autoscaling_group.this["gang-worker-aarch64"].tag :
      tag.key == "example-fleet" && tag.value == "gang-worker-aarch64" && tag.propagate_at_launch
    ])
    error_message = "Each Auto Scaling group must propagate the fleet_tag_key tag set to its fleet name at launch."
  }

  assert {
    condition = !anytrue([
      for tag in aws_autoscaling_group.this["gang-worker-aarch64"].tag :
      tag.key == "gang-fleet"
    ])
    error_message = "The default gang-fleet tag key must not be written when fleet_tag_key is set."
  }
}

run "default_manages_the_federation_resource" {
  command = plan

  assert {
    condition     = length(aws_iam_outbound_web_identity_federation.this) == 1 && length(data.aws_iam_outbound_web_identity_federation.this) == 0
    error_message = "By default the module must create the outbound web identity federation resource and read no data source."
  }

  assert {
    condition     = output.issuer_url == "https://example.invalid"
    error_message = "By default issuer_url must be the issuer of the managed federation resource."
  }
}

run "unmanaged_federation_reads_the_account_issuer" {
  command = plan

  variables {
    manage_outbound_web_identity_federation = false
  }

  assert {
    condition     = length(aws_iam_outbound_web_identity_federation.this) == 0 && length(data.aws_iam_outbound_web_identity_federation.this) == 1
    error_message = "With manage_outbound_web_identity_federation false the module must create no federation resource and read the account federation through the data source."
  }

  assert {
    condition     = output.issuer_url == "https://example.invalid"
    error_message = "With manage_outbound_web_identity_federation false issuer_url must be the issuer read from the data source."
  }
}

run "rejects_fleet_name_with_trailing_dash" {
  command = plan

  variables {
    fleets = [{
      name           = "worker-"
      capacity       = "spot"
      ami_id         = "ami-0000000000000001"
      instance_types = ["c6id.4xlarge"]
      max_instances  = 1
    }]
  }

  expect_failures = [var.fleets]
}

run "rejects_newline_in_config_parameter_name" {
  command = plan

  variables {
    config_parameter_name = "/gang/gang.toml\nextra"
  }

  expect_failures = [var.config_parameter_name]
}

run "groups_span_every_subnet_when_every_zone_offers_their_types" {
  command = plan

  assert {
    condition     = alltrue([for group in aws_autoscaling_group.this : group.vpc_zone_identifier == toset(values(var.subnet_ids))])
    error_message = "With every instance type offered in every zone, each group must span every subnet."
  }

  assert {
    condition     = data.aws_ec2_instance_type_offerings.this.location_type == "availability-zone"
    error_message = "Instance type offerings must be read per availability zone."
  }

  assert {
    condition = (
      toset(one([for f in data.aws_ec2_instance_type_offerings.this.filter : f.values if f.name == "location"])) == toset(["eu-north-1a", "eu-north-1b"])
      && toset(one([for f in data.aws_ec2_instance_type_offerings.this.filter : f.values if f.name == "instance-type"])) == toset(["c6id.4xlarge", "c7gd.4xlarge", "m7g.2xlarge"])
    )
    error_message = "Instance type offerings must be filtered to the subnet zones and the fleets' instance types."
  }
}

run "zone_offering_none_of_a_fleets_types_drops_its_subnet" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type_offerings.this
    values = {
      instance_types = ["c6id.4xlarge", "c6id.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge", "m7g.2xlarge"]
      locations      = ["eu-north-1a", "eu-north-1b", "eu-north-1a", "eu-north-1b", "eu-north-1a"]
    }
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-aarch64"].vpc_zone_identifier == toset(["subnet-0000000000000001"])
    error_message = "A zone that offers none of a fleet's instance types must drop its subnet from the fleet's group."
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].vpc_zone_identifier == toset(["subnet-0000000000000001", "subnet-0000000000000002"])
    error_message = "A type missing from one fleet must not drop subnets from a fleet whose types every zone offers."
  }
}

run "zone_offering_one_of_a_fleets_types_keeps_its_subnet" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type_offerings.this
    values = {
      instance_types = ["c6id.4xlarge", "c7gd.4xlarge", "m7g.2xlarge", "m7g.2xlarge"]
      locations      = ["eu-north-1a", "eu-north-1b", "eu-north-1a", "eu-north-1b"]
    }
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].vpc_zone_identifier == toset(["subnet-0000000000000001", "subnet-0000000000000002"])
    error_message = "A zone that offers at least one of a fleet's instance types must keep its subnet in the fleet's group."
  }

  assert {
    condition = [
      for o in aws_autoscaling_group.this["gang-worker-x86-64"].mixed_instances_policy[0].launch_template[0].override : o.instance_type
    ] == ["c6id.4xlarge", "c7gd.4xlarge"]
    error_message = "Zone filtering must keep every instance type as a group-wide override, in priority order."
  }
}

run "fallback_twin_spans_the_same_zones_as_its_spot_group" {
  command = plan

  variables {
    fleets = [
      {
        name           = "gang-worker-x86-64"
        capacity       = "spot"
        ami_id         = "ami-0000000000000001"
        instance_types = ["m6id.4xlarge"]
        max_instances  = 4
        fallback       = {}
      },
    ]
  }

  override_data {
    target = data.aws_ec2_instance_type_offerings.this
    values = {
      instance_types = ["m6id.4xlarge"]
      locations      = ["eu-north-1b"]
    }
  }

  assert {
    condition     = aws_autoscaling_group.this["gang-worker-x86-64"].vpc_zone_identifier == toset(["subnet-0000000000000002"]) && aws_autoscaling_group.this["gang-worker-x86-64-on-demand"].vpc_zone_identifier == toset(["subnet-0000000000000002"])
    error_message = "A fallback fleet's spot group and on-demand twin must both span only the zones that offer its instance types."
  }
}

run "rejects_fleet_whose_instance_types_no_zone_offers" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type_offerings.this
    values = {
      instance_types = ["c6id.4xlarge", "c6id.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge"]
      locations      = ["eu-north-1a", "eu-north-1b", "eu-north-1a", "eu-north-1b"]
    }
  }

  expect_failures = [aws_autoscaling_group.this]
}

run "rejects_empty_subnet_ids" {
  command = plan

  variables {
    subnet_ids = {}
  }

  expect_failures = [var.subnet_ids]
}

run "rejects_empty_fleets" {
  command = plan

  variables {
    fleets = []
  }

  expect_failures = [var.fleets]
}
