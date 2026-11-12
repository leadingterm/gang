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
