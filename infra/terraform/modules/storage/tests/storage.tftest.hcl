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
