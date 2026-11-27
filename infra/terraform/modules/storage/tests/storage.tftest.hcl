mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }

  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000" }
  }

  mock_data "aws_iam_policy_document" {
    defaults = { json = "{}" }
  }

  # The ephemeral password read validates its ARN argument whenever it opens.
  mock_resource "aws_ssm_parameter" {
    defaults = { arn = "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/database-password" }
  }
}

variables {
  name               = "gang"
  cache_bucket_name  = "gang-cache-test"
  ca_signer_role_arn = "arn:aws:iam::000000000000:role/example-gang-controller"
}

run "cache_bucket_blocks_public_access" {
  command = plan

  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.this.block_public_acls,
      aws_s3_bucket_public_access_block.this.block_public_policy,
      aws_s3_bucket_public_access_block.this.ignore_public_acls,
      aws_s3_bucket_public_access_block.this.restrict_public_buckets,
    ])
    error_message = "The cache bucket must block every form of public access."
  }

  assert {
    condition     = aws_s3_bucket_ownership_controls.this.rule[0].object_ownership == "BucketOwnerEnforced"
    error_message = "The cache bucket must enforce bucket-owner ownership of objects."
  }
}

run "cache_bucket_denies_non_tls_requests" {
  command = plan

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.this.statement :
      s.effect == "Deny" && anytrue([
        for c in s.condition :
        c.test == "Bool" && c.variable == "aws:SecureTransport" && contains(c.values, "false")
      ])
    ])
    error_message = "The bucket policy must contain a Deny for requests without TLS."
  }

  assert {
    condition     = aws_s3_bucket_policy.this.bucket == aws_s3_bucket.this.id
    error_message = "The TLS-only policy must be attached to the cache bucket."
  }
}

run "cache_objects_expire_after_three_days_by_default" {
  command = plan

  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[0].expiration[0].days == 3
    error_message = "Cache objects must expire after 3 days by default."
  }

  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[0].abort_incomplete_multipart_upload[0].days_after_initiation == 1
    error_message = "Incomplete multipart uploads must be aborted after one day."
  }
}

run "cache_objects_expire_after_configured_days" {
  command = plan

  variables {
    cache_expiration_days = 45
  }

  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[0].expiration[0].days == var.cache_expiration_days
    error_message = "Cache objects must expire after cache_expiration_days."
  }

  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[0].abort_incomplete_multipart_upload[0].days_after_initiation == 1
    error_message = "Incomplete multipart uploads must be aborted after one day."
  }
}

run "ca_key_is_ecc_p256_sign_verify" {
  command = plan

  assert {
    condition     = aws_kms_key.ca.customer_master_key_spec == "ECC_NIST_P256" && aws_kms_key.ca.key_usage == "SIGN_VERIFY"
    error_message = "The CA key must be an ECC_NIST_P256 signing key."
  }

  assert {
    condition     = aws_kms_alias.this.target_key_id == aws_kms_key.ca.key_id
    error_message = "The CA alias must point at the CA key."
  }
}

run "ca_key_policy_is_the_explicit_policy_document" {
  command = plan

  assert {
    condition     = aws_kms_key.ca.policy == data.aws_iam_policy_document.ca_key.json
    error_message = "The CA key must carry the explicit key policy document."
  }
}

run "only_the_signer_role_may_sign" {
  command = plan

  assert {
    condition     = length([for s in data.aws_iam_policy_document.ca_key.statement : s if contains(s.actions, "kms:Sign")]) == 1
    error_message = "Exactly one key policy statement may grant kms:Sign."
  }

  assert {
    condition     = flatten([for p in one([for s in data.aws_iam_policy_document.ca_key.statement : s if contains(s.actions, "kms:Sign")]).principals : tolist(p.identifiers)]) == [var.ca_signer_role_arn]
    error_message = "The kms:Sign statement must name only the signer role."
  }
}

run "no_statement_grants_wildcard_or_grant_creation" {
  command = plan

  assert {
    condition = alltrue([
      for s in data.aws_iam_policy_document.ca_key.statement :
      alltrue([for a in s.actions : a != "kms:*" && a != "*" && a != "kms:CreateGrant" && a != "kms:Verify"])
    ])
    error_message = "No key policy statement may grant kms:*, *, kms:CreateGrant or kms:Verify."
  }
}

run "account_root_administers_and_reads" {
  command = plan

  assert {
    condition     = contains(flatten([for p in one([for s in data.aws_iam_policy_document.ca_key.statement : s if s.sid == "AdministerAndRead"]).principals : p.identifiers]), "arn:aws:iam::000000000000:root")
    error_message = "The account root must be the principal of the AdministerAndRead statement."
  }

  assert {
    condition     = alltrue([for a in ["kms:Put*", "kms:Get*", "kms:Describe*"] : contains(one([for s in data.aws_iam_policy_document.ca_key.statement : s if s.sid == "AdministerAndRead"]).actions, a)])
    error_message = "The AdministerAndRead statement must allow kms:Put*, kms:Get* and kms:Describe*."
  }
}

run "signer_role_arn_must_be_a_role_arn" {
  command = plan

  variables {
    ca_signer_role_arn = "arn:aws:iam::000000000000:user/someone"
  }

  expect_failures = [
    var.ca_signer_role_arn,
  ]
}

run "ca_key_deletion_window_below_minimum_fails" {
  command = plan

  variables {
    ca_key_deletion_window_days = 6
  }

  expect_failures = [
    var.ca_key_deletion_window_days,
  ]
}

run "ca_key_deletion_window_above_maximum_fails" {
  command = plan

  variables {
    ca_key_deletion_window_days = 31
  }

  expect_failures = [
    var.ca_key_deletion_window_days,
  ]
}

run "cache_expiration_below_one_day_fails" {
  command = plan

  variables {
    cache_expiration_days = 0
  }

  expect_failures = [
    var.cache_expiration_days,
  ]
}

run "cache_bucket_is_named_by_cache_bucket_name" {
  command = plan

  assert {
    condition     = aws_s3_bucket.this.bucket == "gang-cache-test" && output.cache_bucket_name == "gang-cache-test"
    error_message = "cache_bucket_name must name the bucket and be output as cache_bucket_name."
  }
}

run "ca_key_alias_is_named_after_the_deployment" {
  command = plan

  variables {
    name = "other"
  }

  assert {
    condition     = aws_kms_alias.this.name == "alias/other-ca"
    error_message = "The CA key alias must be named after the deployment."
  }
}

run "shared_state_table" {
  command = plan
  variables {
    dynamodb_table = { name = "gang-state" }
  }
  assert {
    condition     = aws_dynamodb_table.this[0].name == "gang-state" && aws_dynamodb_table.this[0].billing_mode == "PAY_PER_REQUEST" && aws_dynamodb_table.this[0].hash_key == "pk" && aws_dynamodb_table.this[0].range_key == "sk" && aws_dynamodb_table.this[0].deletion_protection_enabled
    error_message = "The shared state table must use on-demand billing, pk/sk keys and deletion protection."
  }
  assert {
    condition     = toset([for a in aws_dynamodb_table.this[0].attribute : "${a.name}:${a.type}"]) == toset(["pk:S", "sk:S"]) && aws_dynamodb_table.this[0].point_in_time_recovery[0].enabled && aws_dynamodb_table.this[0].server_side_encryption[0].enabled && aws_dynamodb_table.this[0].ttl[0].enabled && aws_dynamodb_table.this[0].ttl[0].attribute_name == "ttl"
    error_message = "The shared state table must use string keys, PITR, SSE and ttl expiration."
  }
  assert {
    condition     = output.dynamodb_table_name == "gang-state"
    error_message = "The table name must be exposed to the controller."
  }
}

run "no_shared_state_table_by_default" {
  command = plan
  assert {
    condition     = length(aws_dynamodb_table.this) == 0 && output.dynamodb_table_name == null && output.dynamodb_table_arn == null
    error_message = "The default shape must not create a shared state table."
  }
}

run "rejects_invalid_table_name" {
  command = plan
  variables {
    dynamodb_table = { name = "bad name" }
  }
  expect_failures = [var.dynamodb_table]
}

run "no_postgres_by_default" {
  command = plan

  assert {
    condition = alltrue([
      length(aws_db_subnet_group.postgres) == 0,
      length(aws_security_group.postgres) == 0,
      length(aws_vpc_security_group_ingress_rule.postgres) == 0,
      length(aws_db_parameter_group.postgres) == 0,
      length(aws_db_instance.postgres) == 0,
      length(aws_ssm_parameter.postgres_password) == 0,
      output.postgres_address == null,
      output.postgres_port == null,
      output.postgres_database == null,
      output.postgres_user == null,
      output.postgres_password_parameter_name == null,
    ])
    error_message = "PostgreSQL resources and outputs must be absent by default."
  }
}

run "postgres_rds_is_private_and_protected" {
  command = plan

  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = true
      storage_gib              = 20
      engine_version           = "16"
      password_version         = 3
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }

  assert {
    condition = alltrue([
      aws_db_instance.postgres[0].identifier == "gang-state",
      aws_db_instance.postgres[0].engine == "postgres",
      aws_db_instance.postgres[0].engine_version == "16",
      aws_db_instance.postgres[0].instance_class == "db.t4g.medium",
      aws_db_instance.postgres[0].allocated_storage == 20,
      aws_db_instance.postgres[0].storage_type == "gp3",
      aws_db_instance.postgres[0].storage_encrypted,
      aws_db_instance.postgres[0].multi_az,
      !aws_db_instance.postgres[0].publicly_accessible,
      aws_db_instance.postgres[0].backup_retention_period == 7,
      aws_db_instance.postgres[0].deletion_protection,
      !aws_db_instance.postgres[0].skip_final_snapshot,
      aws_db_instance.postgres[0].final_snapshot_identifier == "gang-state-final",
      aws_db_instance.postgres[0].copy_tags_to_snapshot,
      aws_db_instance.postgres[0].auto_minor_version_upgrade,
      aws_db_instance.postgres[0].ca_cert_identifier == "rds-ca-rsa2048-g1",
      aws_db_instance.postgres[0].password_wo_version == 3,
    ])
    error_message = "The PostgreSQL instance must be private, encrypted, backed up and protected from deletion."
  }

  assert {
    condition = alltrue([
      aws_security_group.postgres[0].vpc_id == var.postgres_rds.vpc_id,
      toset(aws_db_subnet_group.postgres[0].subnet_ids) == toset(var.postgres_rds.subnet_ids),
      length(aws_vpc_security_group_ingress_rule.postgres) == 1,
      aws_vpc_security_group_ingress_rule.postgres[0].referenced_security_group_id == var.postgres_rds.client_security_group_id,
      aws_vpc_security_group_ingress_rule.postgres[0].ip_protocol == "tcp",
      aws_vpc_security_group_ingress_rule.postgres[0].from_port == 5432,
      aws_vpc_security_group_ingress_rule.postgres[0].to_port == 5432,
    ])
    error_message = "PostgreSQL must accept TCP 5432 only from the client security group."
  }

  assert {
    condition = aws_db_parameter_group.postgres[0].name_prefix == "gang-state-" && aws_db_parameter_group.postgres[0].family == "postgres16" && anytrue([
      for p in aws_db_parameter_group.postgres[0].parameter :
      p.name == "rds.force_ssl" && p.value == "1" && p.apply_method == "immediate"
    ])
    error_message = "The PostgreSQL parameter group must force SSL."
  }

  assert {
    condition = alltrue([
      output.postgres_database == "gang",
      output.postgres_user == "gang",
      output.postgres_password_parameter_name == "/gang/database-password",
      aws_ssm_parameter.postgres_password[0].type == "SecureString",
      aws_ssm_parameter.postgres_password[0].value_wo_version == 3,
    ])
    error_message = "The PostgreSQL credentials must use the shared database identity and SecureString parameter."
  }
}

run "rejects_small_postgres_storage" {
  command = plan
  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = true
      storage_gib              = 19
      engine_version           = "16"
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }
  expect_failures = [var.postgres_rds]
}

run "rejects_postgres_minor_version" {
  command = plan
  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = true
      storage_gib              = 20
      engine_version           = "16.4"
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }
  expect_failures = [var.postgres_rds]
}

run "rejects_duplicate_postgres_subnets" {
  command = plan
  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = false
      storage_gib              = 20
      engine_version           = "16"
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-12345678"]
      client_security_group_id = "sg-12345678"
    }
  }
  expect_failures = [var.postgres_rds]
}

run "rejects_fractional_postgres_password_version" {
  command = plan
  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = false
      storage_gib              = 20
      engine_version           = "16"
      password_version         = 1.5
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }
  expect_failures = [var.postgres_rds]
}

run "rejects_zero_postgres_password_version" {
  command = plan
  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = false
      storage_gib              = 20
      engine_version           = "16"
      password_version         = 0
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }
  expect_failures = [var.postgres_rds]
}

# The two runs below must stay last: the apply leaves PostgreSQL in this file's state, and the plan that follows it
# reads the existing password parameter during planning, as every pull-request plan does once it exists.
run "postgres_password_is_decrypted_when_applying" {
  command = apply

  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = true
      storage_gib              = 20
      engine_version           = "16"
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }

  assert {
    condition     = ephemeral.aws_ssm_parameter.postgres_password[0].with_decryption == true
    error_message = "Apply must decrypt the password parameter to set the RDS password."
  }
}

run "postgres_password_is_not_decrypted_when_planning" {
  command = plan

  variables {
    postgres_rds = {
      instance_class           = "db.t4g.medium"
      multi_az                 = true
      storage_gib              = 20
      engine_version           = "16"
      vpc_id                   = "vpc-12345678"
      subnet_ids               = ["subnet-12345678", "subnet-87654321"]
      client_security_group_id = "sg-12345678"
    }
  }

  assert {
    condition     = ephemeral.aws_ssm_parameter.postgres_password[0].with_decryption == false
    error_message = "Plan must read the password parameter without decryption, so the plan role needs no kms:Decrypt."
  }
}
