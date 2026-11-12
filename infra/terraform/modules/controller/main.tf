data "aws_partition" "this" {}

data "aws_region" "this" {}

data "aws_subnet" "this" {
  id = var.subnet_id
}

resource "aws_instance" "this" {
  ami                         = var.ami_id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  private_ip                  = var.private_ip
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = module.instance_role.instance_profile_name
  monitoring                  = true
  user_data                   = join("\n", concat([var.config_parameter_name], [for file, parameter in var.secret_parameters : "${file}=${parameter}"]))
  user_data_replace_on_change = true

  # IMDSv2 only; a hop limit of 1 keeps the metadata service unreachable from containers.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gib
    encrypted   = true
  }

  tags = merge(var.tags, { Name = var.name })
}

# Controller state (mounted at /var/lib/gang). It survives instance replacement:
# the instance is replaced on AMI or user data change and this volume is
# detached and re-attached to the new instance.
#
# prevent_destroy is deliberate: destroying the module must never silently
# discard controller state. To remove the volume, first remove this lifecycle
# block and snapshot the volume.
resource "aws_ebs_volume" "state" {
  availability_zone = data.aws_subnet.this.availability_zone
  size              = var.state_volume_gib
  type              = "gp3"
  encrypted         = true

  tags = merge(var.tags, { Name = "${var.name}-state" })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_volume_attachment" "state" {
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.state.id
  instance_id                    = aws_instance.this.id
  stop_instance_before_detaching = true
}

resource "aws_eip_association" "this" {
  allocation_id = var.eip_allocation_id
  instance_id   = aws_instance.this.id
}

# Replaces the instance on the underlying host failure reported by EC2 system status checks.
resource "aws_cloudwatch_metric_alarm" "recover" {
  alarm_name          = "${var.name}-system-status-recover"
  alarm_description   = "Recover the controller onto healthy hardware when the EC2 system status check fails."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_System"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  dimensions          = { InstanceId = aws_instance.this.id }
  alarm_actions       = ["arn:${data.aws_partition.this.partition}:automate:${data.aws_region.this.region}:ec2:recover"]

  tags = merge(var.tags, { Name = "${var.name}-system-status-recover" })
}
