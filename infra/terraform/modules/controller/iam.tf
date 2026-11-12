data "aws_caller_identity" "this" {}

locals {
  secret_parameter_arns = [for name in values(var.secret_parameters) : "arn:${data.aws_partition.this.partition}:ssm:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:parameter${name}"]
}

data "aws_iam_policy_document" "this" {
  # Sign certificates and read the public key with the CA key only.
  statement {
    sid       = "CaSigningKey"
    effect    = "Allow"
    actions   = ["kms:Sign", "kms:GetPublicKey", "kms:DescribeKey"]
    resources = [var.ca_key_arn]
  }

  # Cache objects. Object-level actions apply to keys inside the bucket only.
  statement {
    sid    = "CacheObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
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

  # Describing Auto Scaling groups does not support resource-level scoping.
  statement {
    sid       = "DescribeFleetGroups"
    effect    = "Allow"
    actions   = ["autoscaling:DescribeAutoScalingGroups", "autoscaling:DescribeScalingActivities"]
    resources = ["*"]
  }

  # Scale and terminate only the worker fleets this controller owns.
  statement {
    sid    = "ControlFleetGroups"
    effect = "Allow"
    actions = [
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
      "autoscaling:SetInstanceProtection",
    ]
    resources = [
      for name in var.worker_fleet_names :
      "arn:${data.aws_partition.this.partition}:autoscaling:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:autoScalingGroup:*:autoScalingGroupName/${name}"
    ]
  }

  # Describing instances does not support resource-level scoping.
  statement {
    sid       = "DescribeInstances"
    effect    = "Allow"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }

  # Decrypt the customer managed key of the secret parameters, and only for calls made through SSM. Present only when secret_parameters_kms_key_arn is set; the AWS-managed key needs no grant.
  dynamic "statement" {
    for_each = length(var.secret_parameters) > 0 && var.secret_parameters_kms_key_arn != null ? [1] : []
    content {
      sid       = "DecryptSecrets"
      effect    = "Allow"
      actions   = ["kms:Decrypt"]
      resources = [var.secret_parameters_kms_key_arn]

      condition {
        test     = "StringEquals"
        variable = "kms:ViaService"
        values   = ["ssm.${data.aws_region.this.region}.amazonaws.com"]
      }
    }
  }
}

# The instance-role module owns the role, the instance profile, the SSM agent permissions and every parameter read: the configuration parameter and the secret parameters.
module "instance_role" {
  source = "../instance-role"

  name                         = var.name
  iam_name_prefix              = var.iam_name_prefix
  iam_permissions_boundary_arn = var.iam_permissions_boundary_arn
  tags                         = var.tags
  readable_parameter_arns      = concat([var.config_parameter_arn], local.secret_parameter_arns)
  policy_json                  = data.aws_iam_policy_document.this.json
}
