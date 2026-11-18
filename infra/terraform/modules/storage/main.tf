# Private build-cache bucket. Objects expire after var.cache_expiration_days;
# versioning is intentionally left disabled.
resource "aws_s3_bucket" "this" {
  bucket = var.cache_bucket_name

  tags = merge(var.tags, {
    Name = var.cache_bucket_name
  })
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    bucket_key_enabled = true

    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "expire-cache-objects"
    status = "Enabled"

    filter {}

    expiration {
      days = var.cache_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  depends_on = [aws_s3_bucket_ownership_controls.this]
}

data "aws_iam_policy_document" "this" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.this.json

  depends_on = [aws_s3_bucket_public_access_block.this]
}

data "aws_caller_identity" "this" {}

data "aws_partition" "this" {}

# The account root delegates key administration and public-key reads to IAM but is NOT granted kms:Sign, kms:Verify or
# grants, so an IAM policy cannot grant signing; only the signer role named in the policy can sign. An account
# administrator can still change the policy itself (the root statement includes kms:Put*), which is by design:
# administrators own keys.
data "aws_iam_policy_document" "ca_key" {
  statement {
    sid    = "AdministerAndRead"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.this.partition}:iam::${data.aws_caller_identity.this.account_id}:root"]
    }
    actions = [
      "kms:CancelKeyDeletion", "kms:CreateAlias", "kms:DeleteAlias", "kms:Describe*", "kms:Disable*", "kms:Enable*",
      "kms:Get*", "kms:List*", "kms:Put*", "kms:ScheduleKeyDeletion", "kms:TagResource", "kms:UntagResource",
      "kms:UpdateAlias", "kms:UpdateKeyDescription",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "SignWithSignerRole"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = [var.ca_signer_role_arn]
    }
    actions   = ["kms:Sign", "kms:GetPublicKey", "kms:DescribeKey"]
    resources = ["*"]
  }
}

# Only this key policy grants kms:Sign (the root statement does not delegate it to IAM).
resource "aws_kms_key" "ca" {
  description              = "Gang certificate authority signing key"
  customer_master_key_spec = "ECC_NIST_P256"
  key_usage                = "SIGN_VERIFY"
  deletion_window_in_days  = var.ca_key_deletion_window_days
  policy                   = data.aws_iam_policy_document.ca_key.json

  tags = merge(var.tags, {
    Name = "alias/${var.name}-ca"
  })
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}-ca"
  target_key_id = aws_kms_key.ca.key_id
}

resource "aws_dynamodb_table" "this" {
  count = var.dynamodb_table == null ? 0 : 1

  name                        = var.dynamodb_table.name
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "pk"
  range_key                   = "sk"
  deletion_protection_enabled = true

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = merge(var.tags, { Name = var.dynamodb_table.name })
}

resource "aws_db_subnet_group" "postgres" {
  count = var.postgres_rds == null ? 0 : 1

  name       = "${var.name}-state"
  subnet_ids = var.postgres_rds.subnet_ids
  tags       = merge(var.tags, { Name = "${var.name}-state" })
}

resource "aws_security_group" "postgres" {
  count = var.postgres_rds == null ? 0 : 1

  name        = "${var.name}-state"
  description = "PostgreSQL access from the controller"
  vpc_id      = var.postgres_rds.vpc_id
  tags        = merge(var.tags, { Name = "${var.name}-state" })
}

resource "aws_vpc_security_group_ingress_rule" "postgres" {
  count = var.postgres_rds == null ? 0 : 1

  security_group_id            = aws_security_group.postgres[0].id
  referenced_security_group_id = var.postgres_rds.client_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  tags                         = merge(var.tags, { Name = "${var.name}-state" })
}

resource "aws_db_parameter_group" "postgres" {
  count = var.postgres_rds == null ? 0 : 1

  name_prefix = "${var.name}-state-"
  family      = "postgres${var.postgres_rds.engine_version}"

  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "immediate"
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(var.tags, { Name = "${var.name}-state" })
}

ephemeral "aws_secretsmanager_random_password" "postgres" {
  count = var.postgres_rds == null ? 0 : 1

  password_length     = 40
  exclude_punctuation = true
}

# Write-only arguments keep the password out of state and plan; increment password_version to rotate.
resource "aws_db_instance" "postgres" {
  count = var.postgres_rds == null ? 0 : 1

  identifier                 = "${var.name}-state"
  engine                     = "postgres"
  engine_version             = var.postgres_rds.engine_version
  instance_class             = var.postgres_rds.instance_class
  allocated_storage          = var.postgres_rds.storage_gib
  storage_type               = "gp3"
  storage_encrypted          = true
  multi_az                   = var.postgres_rds.multi_az
  db_name                    = "gang"
  username                   = "gang"
  password_wo                = ephemeral.aws_ssm_parameter.postgres_password[0].value
  password_wo_version        = var.postgres_rds.password_version
  db_subnet_group_name       = aws_db_subnet_group.postgres[0].name
  vpc_security_group_ids     = [aws_security_group.postgres[0].id]
  parameter_group_name       = aws_db_parameter_group.postgres[0].name
  publicly_accessible        = false
  backup_retention_period    = 7
  deletion_protection        = true
  skip_final_snapshot        = false
  final_snapshot_identifier  = "${var.name}-state-final"
  copy_tags_to_snapshot      = true
  auto_minor_version_upgrade = true
  ca_cert_identifier         = "rds-ca-rsa2048-g1"

  depends_on = [aws_ssm_parameter.postgres_password]

  tags = merge(var.tags, { Name = "${var.name}-state" })
}

resource "aws_ssm_parameter" "postgres_password" {
  count = var.postgres_rds == null ? 0 : 1

  name             = "/${var.name}/database-password"
  type             = "SecureString"
  value_wo         = ephemeral.aws_secretsmanager_random_password.postgres[0].random_password
  value_wo_version = var.postgres_rds.password_version

  tags = merge(var.tags, { Name = "/${var.name}/database-password" })
}

# A failed RDS apply retry must reuse the committed parameter, not a newly generated password. Plans read it without
# decryption so the plan role needs no kms:Decrypt; apply re-opens it decrypted for password_wo.
ephemeral "aws_ssm_parameter" "postgres_password" {
  count = var.postgres_rds == null ? 0 : 1

  arn             = aws_ssm_parameter.postgres_password[0].arn
  with_decryption = terraform.applying
}
