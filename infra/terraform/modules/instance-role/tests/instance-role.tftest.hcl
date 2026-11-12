mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  name = "gang-instance"
  readable_parameter_arns = [
    "arn:aws:ssm:eu-north-1:000000000000:parameter/gang-instance/gang.toml",
    "arn:aws:ssm:eu-north-1:000000000000:parameter/gang-instance/secrets/ca-password",
  ]
  policy_json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"CallerOnly\",\"Effect\":\"Allow\",\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::example-cache\"}]}"
  tags = {
    Project = "gang"
  }
}

run "role_and_profile_default_to_the_name" {
  command = plan

  assert {
    condition     = aws_iam_role.this.name == "gang-instance" && aws_iam_instance_profile.this.name == "gang-instance" && output.role_name == "gang-instance" && output.instance_profile_name == "gang-instance"
    error_message = "Without iam_name_prefix the role and the instance profile must carry the name."
  }
  assert {
    condition     = aws_iam_role.this.permissions_boundary == null && output.permissions_boundary_arn == null
    error_message = "Without iam_permissions_boundary_arn the role must carry no permissions boundary."
  }
}

run "iam_name_prefix_names_the_role_and_the_instance_profile" {
  command = plan

  variables {
    iam_name_prefix = "example-"
  }

  assert {
    condition     = aws_iam_role.this.name == "example-gang-instance" && aws_iam_instance_profile.this.name == "example-gang-instance" && output.role_name == "example-gang-instance" && output.instance_profile_name == "example-gang-instance"
    error_message = "iam_name_prefix must prefix the role and the instance profile names."
  }
}

run "permissions_boundary_is_set_on_the_role" {
  command = plan

  variables {
    iam_permissions_boundary_arn = "arn:aws:iam::000000000000:policy/example-boundary"
  }

  assert {
    condition     = aws_iam_role.this.permissions_boundary == "arn:aws:iam::000000000000:policy/example-boundary" && output.permissions_boundary_arn == "arn:aws:iam::000000000000:policy/example-boundary"
    error_message = "iam_permissions_boundary_arn must be the permissions boundary of the role."
  }
}

run "no_managed_policy_is_attached" {
  command = plan

  assert {
    condition     = aws_iam_role_policy_attachments_exclusive.this.role_name == aws_iam_role.this.name && length(aws_iam_role_policy_attachments_exclusive.this.policy_arns) == 0
    error_message = "The role must have an empty exclusive managed-policy attachment set."
  }
}

run "ssm_core_is_inlined_without_parameter_reads" {
  command = plan

  assert {
    condition     = length([for s in data.aws_iam_policy_document.base.statement : s if s.sid == "SsmCore"]) == 1
    error_message = "Exactly one SsmCore statement must carry the SSM agent permissions."
  }
  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.base.statement :
      s.effect == "Allow" && s.resources == toset(["*"]) && contains(s.actions, "ssm:UpdateInstanceInformation") && contains(s.actions, "ssmmessages:OpenControlChannel") && contains(s.actions, "ec2messages:GetMessages")
      if s.sid == "SsmCore"
    ])
    error_message = "SsmCore must allow the SSM agent actions on every resource."
  }
  assert {
    condition = !anytrue([
      for s in data.aws_iam_policy_document.base.statement :
      anytrue([for a in s.actions : startswith(a, "ssm:GetParameter")])
      if s.sid == "SsmCore"
    ])
    error_message = "SsmCore must grant no parameter reads."
  }
}

run "base_policy_has_exactly_the_module_statements" {
  command = plan

  assert {
    condition     = toset([for s in data.aws_iam_policy_document.base.statement : s.sid]) == toset(["SsmCore", "ReadParameters", "DenyOtherParameters"]) && length(data.aws_iam_policy_document.base.statement) == 3
    error_message = "The base policy must consist of exactly SsmCore, ReadParameters and DenyOtherParameters."
  }
}

run "read_parameters_grants_only_the_readable_arns" {
  command = plan

  assert {
    condition     = length([for s in data.aws_iam_policy_document.base.statement : s if s.sid == "ReadParameters"]) == 1
    error_message = "Exactly one ReadParameters statement must exist."
  }
  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.base.statement :
      s.effect == "Allow" && toset(s.actions) == toset(["ssm:GetParameter"]) && toset(s.resources) == toset(var.readable_parameter_arns) && length(s.resources) == 2
      if s.sid == "ReadParameters"
    ])
    error_message = "ReadParameters must allow ssm:GetParameter on exactly the readable_parameter_arns."
  }
}

run "other_parameters_denied_except_readable_arns" {
  command = plan

  assert {
    condition     = length([for s in data.aws_iam_policy_document.base.statement : s if s.sid == "DenyOtherParameters"]) == 1
    error_message = "Exactly one DenyOtherParameters statement must exist."
  }
  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.base.statement :
      s.effect == "Deny" && toset(s.actions) == toset(["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath", "ssm:GetParameterHistory"])
      if s.sid == "DenyOtherParameters"
    ])
    error_message = "DenyOtherParameters must deny exactly the four ssm Get* parameter actions."
  }
  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.base.statement :
      toset(s.not_resources) == toset(var.readable_parameter_arns) && length(s.not_resources) == 2
      if s.sid == "DenyOtherParameters"
    ])
    error_message = "DenyOtherParameters must exempt exactly the readable_parameter_arns and nothing else."
  }
}

run "combined_policy_is_base_followed_by_caller_policy" {
  command = plan

  assert {
    condition     = length(data.aws_iam_policy_document.combined.source_policy_documents) == 2 && data.aws_iam_policy_document.combined.source_policy_documents[1] == var.policy_json
    error_message = "The combined policy must be the base policy followed by the caller's policy_json."
  }
  assert {
    condition     = aws_iam_role_policy.this.name == "gang-instance"
    error_message = "The combined policy must be the inline policy named after the role."
  }
}

run "trust_policy_only_lets_ec2_assume_the_role" {
  command = plan

  assert {
    condition     = length(data.aws_iam_policy_document.assume_role.statement) == 1
    error_message = "The trust policy must consist of exactly one statement."
  }
  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.assume_role.statement :
      s.effect == "Allow" && toset(s.actions) == toset(["sts:AssumeRole"]) && length(s.principals) == 1 && alltrue([
        for p in s.principals : p.type == "Service" && toset(p.identifiers) == toset(["ec2.amazonaws.com"])
      ])
    ])
    error_message = "The trust policy must allow only ec2.amazonaws.com to assume the role."
  }
}

run "rejects_empty_readable_parameter_arns" {
  command = plan

  variables {
    readable_parameter_arns = []
  }

  expect_failures = [var.readable_parameter_arns]
}

run "rejects_invalid_iam_name_prefix" {
  command = plan

  variables {
    iam_name_prefix = "example prefix/"
  }

  expect_failures = [var.iam_name_prefix]
}

run "rejects_malformed_permissions_boundary_arn" {
  command = plan

  variables {
    iam_permissions_boundary_arn = "example-boundary"
  }

  expect_failures = [var.iam_permissions_boundary_arn]
}

run "accepts_an_aws_managed_permissions_boundary" {
  command = plan

  variables {
    iam_permissions_boundary_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
  }

  assert {
    condition     = aws_iam_role.this.permissions_boundary == "arn:aws:iam::aws:policy/PowerUserAccess"
    error_message = "An AWS-managed policy ARN must be accepted as the permissions boundary."
  }
}
