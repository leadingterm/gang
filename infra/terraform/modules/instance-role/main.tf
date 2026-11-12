locals {
  iam_name = "${var.iam_name_prefix}${var.name}"
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    sid     = "AssumeByEc2"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = local.iam_name
  assume_role_policy   = data.aws_iam_policy_document.assume_role.json
  permissions_boundary = var.iam_permissions_boundary_arn

  tags = merge(var.tags, { Name = local.iam_name })
}

resource "aws_iam_instance_profile" "this" {
  name = local.iam_name
  role = aws_iam_role.this.name

  tags = merge(var.tags, { Name = local.iam_name })
}

data "aws_iam_policy_document" "base" {
  # The SSM agent's permissions, inlined in place of the AWS-managed AmazonSSMManagedInstanceCore policy; parameter reads are left out: ReadParameters grants the readable parameters only.
  statement {
    sid    = "SsmCore"
    effect = "Allow"
    actions = [
      "ssm:DescribeAssociation",
      "ssm:DescribeDocument",
      "ssm:GetDeployablePatchSnapshotForInstance",
      "ssm:GetDocument",
      "ssm:GetManifest",
      "ssm:ListAssociations",
      "ssm:ListInstanceAssociations",
      "ssm:PutComplianceItems",
      "ssm:PutConfigurePackageResult",
      "ssm:PutInventory",
      "ssm:UpdateAssociationStatus",
      "ssm:UpdateInstanceAssociationStatus",
      "ssm:UpdateInstanceInformation",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
      "ec2messages:AcknowledgeMessage",
      "ec2messages:DeleteMessage",
      "ec2messages:FailMessage",
      "ec2messages:GetEndpoint",
      "ec2messages:GetMessages",
      "ec2messages:SendReply",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "ReadParameters"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = var.readable_parameter_arns
  }

  # Guards against reading any parameter other than the readable_parameter_arns.
  statement {
    sid           = "DenyOtherParameters"
    effect        = "Deny"
    actions       = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath", "ssm:GetParameterHistory"]
    not_resources = var.readable_parameter_arns
  }
}

data "aws_iam_policy_document" "combined" {
  source_policy_documents = [data.aws_iam_policy_document.base.json, var.policy_json]
}

resource "aws_iam_role_policy" "this" {
  name   = var.name
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.combined.json
}

# No AWS-managed policy is attached; the exclusive attachment set also detaches any attached out of band.
resource "aws_iam_role_policy_attachments_exclusive" "this" {
  role_name   = aws_iam_role.this.name
  policy_arns = []
}
