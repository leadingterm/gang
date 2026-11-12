data "aws_iam_policy_document" "worker" {
  statement {
    sid    = "CacheObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${var.cache_bucket_arn}/*"]
  }

  statement {
    sid       = "CacheBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.cache_bucket_arn]
  }

  statement {
    sid       = "ReadCaPublicKey"
    effect    = "Allow"
    actions   = ["kms:GetPublicKey"]
    resources = [var.ca_key_arn]
  }

  statement {
    sid       = "WorkloadIdentityToken"
    effect    = "Allow"
    actions   = ["sts:GetWebIdentityToken"]
    resources = ["*"]

    condition {
      test     = "ForAllValues:StringEquals"
      variable = "sts:IdentityTokenAudience"
      values   = [var.workload_token_audience]
    }

    condition {
      test     = "NumericLessThanEquals"
      variable = "sts:DurationSeconds"
      values   = [tostring(var.workload_token_max_seconds)]
    }
  }
}

module "instance_role" {
  source = "../instance-role"

  name                         = var.name_prefix
  iam_name_prefix              = var.iam_name_prefix
  iam_permissions_boundary_arn = var.iam_permissions_boundary_arn
  tags                         = var.tags
  readable_parameter_arns      = [var.config_parameter_arn]
  policy_json                  = data.aws_iam_policy_document.worker.json
}
