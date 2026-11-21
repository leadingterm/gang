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

run "s3_cache_metrics_grant_prevents_access_denied" {
  command = plan

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.this.statement : statement
      if statement.sid == "CacheStorageMetrics" && statement.effect == "Allow"
      && toset(statement.actions) == toset(["cloudwatch:GetMetricData"])
      && toset(statement.resources) == toset(["*"])
    ]) == 1
    error_message = "S3 cache metrics require exactly cloudwatch:GetMetricData on * or the controller receives AccessDenied."
  }

  assert {
    condition = alltrue(flatten([
      for statement in data.aws_iam_policy_document.this.statement : [
        for action in statement.actions : action == "cloudwatch:GetMetricData"
        if startswith(lower(action), "cloudwatch:")
      ]
    ]))
    error_message = "The controller must not receive additional CloudWatch actions."
  }
}

run "filesystem_cache_has_no_s3_or_cloudwatch_overgrant" {
  command = plan

  variables {
    cache_bucket_arn = null
  }

  assert {
    condition = !anytrue(flatten([
      for statement in data.aws_iam_policy_document.this.statement : [
        for action in statement.actions :
        lower(action) == "*" || startswith(lower(action), "s3:") || startswith(lower(action), "cloudwatch:")
        if statement.effect == "Allow"
      ]
    ]))
    error_message = "A filesystem cache must not grant S3 or CloudWatch permissions."
  }
}

run "instance_requires_imdsv2_with_hop_limit_one" {
  command = plan

  assert {
    condition     = aws_instance.this[0].metadata_options[0].http_tokens == "required"
    error_message = "The controller must require IMDSv2 session tokens."
  }

  assert {
    condition     = aws_instance.this[0].metadata_options[0].http_put_response_hop_limit == 1
    error_message = "The IMDS PUT response hop limit must be 1."
  }

  assert {
    condition     = aws_instance.this[0].root_block_device[0].encrypted == true
    error_message = "The root volume must be encrypted."
  }
}

run "user_data_is_role_and_config_parameter_reference" {
  command = plan

  assert {
    condition     = aws_instance.this[0].user_data == "controller ${var.config_parameter_reference} state=/dev/sdg"
    error_message = "EC2 user-data must identify the controller, configuration parameter and state device."
  }
}

run "user_data_lists_each_secret_after_config_reference" {
  command = plan

  variables {
    secret_parameters = {
      "dashboard-client-secret" = "/gang-controller/secrets/dashboard-client-secret"
      "ca-password"             = "/gang-controller/secrets/ca-password"
    }
  }

  assert {
    condition = aws_instance.this[0].user_data == join("\n", [
      "controller ${var.config_parameter_reference} state=/dev/sdg",
      "ca-password=/gang-controller/secrets/ca-password",
      "dashboard-client-secret=/gang-controller/secrets/dashboard-client-secret",
    ])
    error_message = "EC2 user-data must start with the role line `controller <config parameter reference> state=/dev/sdg` followed by one file=parameter line per secret, in file-name order."
  }
}

run "state_volume_is_encrypted_and_in_subnet_az" {
  command = plan

  assert {
    condition     = aws_ebs_volume.state[0].encrypted == true
    error_message = "The state volume must be encrypted."
  }

  assert {
    condition     = aws_ebs_volume.state[0].type == "gp3"
    error_message = "The state volume must be gp3."
  }

  assert {
    condition     = aws_ebs_volume.state[0].availability_zone == data.aws_subnet.this[0].availability_zone
    error_message = "The state volume must be created in the availability zone of the controller subnet."
  }
}

run "state_volume_attached_at_sdg_and_stopped_safely" {
  command = plan

  assert {
    condition     = aws_volume_attachment.state[0].device_name == "/dev/sdg"
    error_message = "The state volume must be attached as /dev/sdg."
  }

  assert {
    condition     = aws_volume_attachment.state[0].stop_instance_before_detaching == true
    error_message = "The instance must be stopped before the state volume is detached."
  }
}

run "state_volume_accepts_minimum_size" {
  command = plan

  variables {
    state_volume_gib = 8
  }

  assert {
    condition     = aws_ebs_volume.state[0].size == 8
    error_message = "An 8 GiB state volume must be accepted."
  }
}

run "state_volume_below_minimum_is_rejected" {
  command = plan

  variables {
    state_volume_gib = 7
  }

  expect_failures = [
    var.state_volume_gib,
  ]
}

run "root_volume_below_minimum_is_rejected" {
  command = plan

  variables {
    root_volume_gib = 7
  }

  expect_failures = [
    var.root_volume_gib,
  ]
}

run "empty_config_parameter_reference_is_rejected" {
  command = plan

  variables {
    config_parameter_reference = ""
  }

  expect_failures = [
    var.config_parameter_reference,
  ]
}

run "config_parameter_reference_cannot_inject_user_data_lines" {
  command = plan

  variables {
    config_parameter_reference = "/gang/config\n/gang/other:1"
  }

  expect_failures = [
    var.config_parameter_reference,
  ]
}

run "secret_parameter_names_cannot_inject_user_data_lines" {
  command = plan

  variables {
    secret_parameters = { "token" = "/gang/token\nextra=/gang/other" }
  }

  expect_failures = [
    var.secret_parameters,
  ]
}

run "rejects_secret_file_name_outside_secrets_directory" {
  command = plan

  variables {
    secret_parameters = {
      "../x" = "/gang-controller/secrets/x"
    }
  }

  expect_failures = [
    var.secret_parameters,
  ]
}

run "rejects_secret_parameter_name_without_leading_slash" {
  command = plan

  variables {
    secret_parameters = {
      "ca-password" = "gang-controller/secrets/ca-password"
    }
  }

  expect_failures = [
    var.secret_parameters,
  ]
}

run "rejects_malformed_secret_parameters_kms_key_arn" {
  command = plan

  variables {
    secret_parameters_kms_key_arn = "arn:aws:kms:eu-north-1:000000000000:alias/gang-secrets"
  }

  expect_failures = [
    var.secret_parameters_kms_key_arn,
  ]
}

run "recover_alarm_uses_ec2_recover_automate_action" {
  command = plan

  assert {
    condition     = aws_cloudwatch_metric_alarm.recover[0].metric_name == "StatusCheckFailed_System"
    error_message = "The recover alarm must watch the EC2 system status check."
  }

  assert {
    condition = length(aws_cloudwatch_metric_alarm.recover[0].alarm_actions) == 1 && contains(
      aws_cloudwatch_metric_alarm.recover[0].alarm_actions,
      "arn:${data.aws_partition.this.partition}:automate:${data.aws_region.this.region}:ec2:recover",
    )
    error_message = "The recover alarm must use the ec2:recover automate action and nothing else."
  }
}

run "kms_access_limited_to_ca_key" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.this.statement : s
      if anytrue([for a in s.actions : startswith(a, "kms:")])
    ]) == 1
    error_message = "Exactly one statement may grant KMS actions."
  }

  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.this.statement :
      length(s.resources) == 1 && contains(s.resources, var.ca_key_arn)
      if anytrue([for a in s.actions : contains(["kms:Sign", "kms:GetPublicKey", "kms:DescribeKey"], a)])
    ])
    error_message = "kms:Sign, kms:GetPublicKey and kms:DescribeKey must apply only to the CA key ARN."
  }

  assert {
    condition = !anytrue([
      for s in data.aws_iam_policy_document.this.statement : contains(s.actions, "kms:Decrypt")
    ])
    error_message = "kms:Decrypt must not be granted without secret_parameters_kms_key_arn."
  }

  assert {
    condition = alltrue([
      for a in flatten([
        for s in data.aws_iam_policy_document.this.statement : s.actions
        if anytrue([for x in s.actions : startswith(x, "kms:")])
      ]) : contains(["kms:Sign", "kms:GetPublicKey", "kms:DescribeKey", "kms:Decrypt"], a)
    ])
    error_message = "Only kms:Sign, kms:GetPublicKey, kms:DescribeKey and kms:Decrypt may be granted."
  }
}

run "no_wildcard_s3_resources" {
  command = plan

  assert {
    condition = !anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      contains(s.resources, "*") && anytrue([for a in s.actions : startswith(a, "s3:")])
    ])
    error_message = "No S3 action may apply to a wildcard resource."
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      s.sid == "CacheObjects" && length(s.resources) == 1 && contains(s.resources, "${var.cache_bucket_arn}/*")
    ])
    error_message = "Object-level S3 actions must apply only to keys in the cache bucket."
  }
}

run "autoscaling_mutations_scoped_to_fleet_names" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.this.statement : s
      if s.sid == "ControlFleetGroups"
    ]) == 1
    error_message = "Fleet mutation actions must be granted in exactly one statement."
  }

  assert {
    condition = alltrue([
      for n in var.worker_fleet_names : anytrue([
        for s in data.aws_iam_policy_document.this.statement :
        s.sid == "ControlFleetGroups" && anytrue([
          for r in s.resources : endswith(r, ":autoScalingGroupName/${n}")
        ])
      ])
    ])
    error_message = "Every worker fleet name must be an Auto Scaling group resource."
  }

  assert {
    condition = !anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      contains(s.resources, "*") && anytrue([
        for a in s.actions : startswith(a, "autoscaling:") && a != "autoscaling:DescribeAutoScalingGroups" && a != "autoscaling:DescribeScalingActivities"
      ])
    ])
    error_message = "Autoscaling mutation actions must never apply to a wildcard resource."
  }
}

run "autoscaling_mutations_cover_each_fleet" {
  command = plan

  variables {
    worker_fleet_names = ["gang-x86-64", "gang-aarch64"]
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      s.sid == "ControlFleetGroups" && length(s.resources) == 2
    ])
    error_message = "One resource per worker fleet must be granted."
  }
}

run "fleet_protection_and_scaling_activity_grants" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.this.statement : s
      if anytrue([for a in s.actions : a == "autoscaling:SetInstanceProtection"])
    ]) == 1
    error_message = "SetInstanceProtection must be granted in exactly one statement."
  }

  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.this.statement :
      alltrue([
        for r in s.resources : anytrue([for n in var.worker_fleet_names : endswith(r, ":autoScalingGroupName/${n}")])
      ])
      if contains(s.actions, "autoscaling:SetInstanceProtection")
    ])
    error_message = "SetInstanceProtection must apply only to the worker fleet Auto Scaling groups."
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      s.effect == "Allow" && contains(s.actions, "autoscaling:DescribeScalingActivities") && contains(s.resources, "*")
    ])
    error_message = "DescribeScalingActivities must be granted on a wildcard resource, since Auto Scaling describe actions do not support resource-level scoping."
  }
}

run "empty_fleet_list_is_rejected" {
  command = plan

  variables {
    worker_fleet_names = []
  }

  expect_failures = [
    var.worker_fleet_names,
  ]
}

run "no_ssm_statement_in_controller_policy" {
  command = plan

  variables {
    secret_parameters = {
      "ca-password" = "/gang-controller/secrets/ca-password"
    }
  }

  assert {
    condition     = !anytrue([for s in data.aws_iam_policy_document.this.statement : anytrue([for a in s.actions : startswith(a, "ssm:")])])
    error_message = "The controller policy must carry no SSM statement: the instance-role module grants every parameter read."
  }

  assert {
    condition     = output.role_name == "gang-controller"
    error_message = "The controller role name must follow name."
  }
}

run "secret_parameter_arns_empty_without_secrets" {
  command = plan

  assert {
    condition     = length(output.secret_parameter_arns) == 0
    error_message = "secret_parameter_arns must be empty when no secrets are configured."
  }
}

run "secret_parameter_arns_lists_each_secret" {
  command = plan

  variables {
    secret_parameters = {
      a-file = "/gang/secrets/a"
      b-file = "/gang/secrets/b"
    }
  }

  assert {
    condition = output.secret_parameter_arns == [
      "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/secrets/a",
      "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/secrets/b",
    ]
    error_message = "secret_parameter_arns must list the ARN of each configured secret parameter."
  }
}

run "secret_key_grants_decrypt_only_through_ssm" {
  command = plan

  variables {
    secret_parameters = {
      "ca-password" = "/gang-controller/secrets/ca-password"
    }
    secret_parameters_kms_key_arn = "arn:aws:kms:eu-north-1:000000000000:key/11111111-1111-1111-1111-111111111111"
  }

  assert {
    condition     = length([for s in data.aws_iam_policy_document.this.statement : s if contains(s.actions, "kms:Decrypt")]) == 1
    error_message = "kms:Decrypt must be granted by exactly one statement."
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      s.sid == "DecryptSecrets" && length(s.actions) == 1 && contains(s.actions, "kms:Decrypt") && length(s.resources) == 1 && contains(s.resources, var.secret_parameters_kms_key_arn) && anytrue([
        for c in s.condition : c.test == "StringEquals" && c.variable == "kms:ViaService" && contains(c.values, "ssm.${data.aws_region.this.region}.amazonaws.com")
      ])
    ])
    error_message = "DecryptSecrets must grant only kms:Decrypt on the secret parameters key, restricted to calls made through SSM."
  }

  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.this.statement :
      length(s.resources) == 1 && contains(s.resources, var.ca_key_arn)
      if anytrue([for a in s.actions : contains(["kms:Sign", "kms:GetPublicKey", "kms:DescribeKey"], a)])
    ])
    error_message = "kms:Sign, kms:GetPublicKey and kms:DescribeKey must apply only to the CA key ARN."
  }
}

run "no_iam_or_wildcard_actions" {
  command = plan

  assert {
    condition = !anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      anytrue([for a in s.actions : startswith(a, "iam:") || a == "*" || endswith(a, ":*")])
    ])
    error_message = "The controller policy must not grant iam:* or wildcard actions."
  }
}

run "rejects_instance_type_without_uefi" {
  command = plan

  override_data {
    target = data.aws_ec2_instance_type.this
    values = {
      boot_modes = ["legacy-bios"]
    }
  }

  expect_failures = [aws_instance.this[0]]
}

run "instance_shape_has_no_group_permissions" {
  command = plan

  assert {
    condition     = length(aws_launch_template.controller) == 0 && length(aws_autoscaling_group.controller) == 0 && output.autoscaling_group_name == null && !anytrue([for s in data.aws_iam_policy_document.this.statement : contains(["ControllerLifecycle", "Database"], s.sid)])
    error_message = "The instance shape must not create a group or grant group and database permissions."
  }
}

run "invalid_shape" {
  command = plan
  variables { shape = "unknown" }
  expect_failures = [var.shape]
}

run "invalid_controller_count" {
  command = plan
  variables { controller_count = 1.5 }
  expect_failures = [var.controller_count]
}

run "invalid_shutdown_grace" {
  command = plan
  variables { shutdown_grace_seconds = 3481 }
  expect_failures = [var.shutdown_grace_seconds]
}

run "invalid_table_arn" {
  command = plan
  variables { dynamodb_table_arn = "not-an-arn" }
  expect_failures = [var.dynamodb_table_arn]
}

run "instance_requires_subnet" {
  command = plan
  variables { subnet_id = null }
  expect_failures = [var.subnet_id]
}

run "instance_requires_private_ip" {
  command = plan
  variables { private_ip = null }
  expect_failures = [var.private_ip]
}

run "instance_requires_eip" {
  command = plan
  variables { eip_allocation_id = null }
  expect_failures = [var.eip_allocation_id]
}

run "group_uses_ec2_health_for_refresh" {
  command = plan
  variables {
    shape                  = "group"
    controller_count       = 2
    subnet_ids             = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
    target_group_arns      = ["arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/serve/0123456789abcdef"]
    config_digest          = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    dynamodb_table_arn     = "arn:aws:dynamodb:eu-north-1:000000000000:table/gang"
    shutdown_grace_seconds = 300
  }
  assert {
    condition     = aws_autoscaling_group.controller[0].health_check_type == "EC2"
    error_message = "Standby controllers must not fail ASG health during refresh."
  }
}
run "aws_events_queue_permissions" {
  command = plan

  variables {
    events_queue_arn = "arn:aws:sqs:eu-north-1:000000000000:gang-events"
  }

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.this.statement : statement
      if statement.effect == "Allow" && statement.sid == "ObserveAwsEvents"
      && toset(statement.actions) == toset(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"])
      && toset(statement.resources) == toset([var.events_queue_arn])
    ]) == 1
    error_message = "The controller must receive only the three required SQS actions on its exact queue."
  }

  assert {
    condition = alltrue(flatten([
      for statement in data.aws_iam_policy_document.this.statement : [
        for action in statement.actions : statement.sid == "ObserveAwsEvents"
        if statement.effect == "Allow" && (lower(action) == "*" || startswith(lower(action), "sqs:"))
      ]
    ]))
    error_message = "No other controller IAM statement may grant SQS actions."
  }
}

run "aws_events_queue_disabled" {
  command = plan

  assert {
    condition = !anytrue(flatten([
      for statement in data.aws_iam_policy_document.this.statement : [
        for action in statement.actions : lower(action) == "*" || startswith(lower(action), "sqs:")
        if statement.effect == "Allow"
      ]
    ]))
    error_message = "Without an events queue, the controller must not receive SQS permissions."
  }
}

run "rejects_wildcard_events_queue" {
  command = plan

  variables {
    events_queue_arn = "arn:aws:sqs:eu-north-1:000000000000:*"
  }

  expect_failures = [var.events_queue_arn]
}

run "rejects_unversioned_config_reference" {
  command = plan
  variables { config_parameter_reference = "/gang/config" }
  expect_failures = [var.config_parameter_reference]
}

run "rejects_zero_config_version" {
  command = plan
  variables { config_parameter_reference = "/gang/config:0" }
  expect_failures = [var.config_parameter_reference]
}

run "rejects_empty_config_arns" {
  command = plan
  variables { config_parameter_arns = [] }
  expect_failures = [var.config_parameter_arns]
}

run "rejects_oversized_config_path" {
  command = plan
  variables { config_parameter_reference = "/${join("", [for i in range(1011) : "a"])}:1" }
  expect_failures = [var.config_parameter_reference]
}

run "rejects_non_numeric_config_version" {
  command = plan
  variables { config_parameter_reference = "/gang/config:latest" }
  expect_failures = [var.config_parameter_reference]
}

run "accepts_positive_config_version" {
  command = plan
  variables { config_parameter_reference = "/gang/config:2" }
  assert {
    condition     = aws_instance.this[0].user_data == "controller /gang/config:2 state=/dev/sdg"
    error_message = "The controller must preserve a positive version selector in user data."
  }
}
