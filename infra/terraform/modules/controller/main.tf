data "aws_partition" "this" {}

data "aws_region" "this" {}
locals {
  instance_shape = var.shape == "instance"
  group_shape    = var.shape == "group"
  # The image's gang.stateDevice; the role line names it.
  state_device = "/dev/sdg"
  hook         = jsondecode(file("${path.module}/lifecycle-hook.json"))
}

moved {
  from = aws_instance.this
  to   = aws_instance.this[0]
}

moved {
  from = aws_ebs_volume.state
  to   = aws_ebs_volume.state[0]
}

moved {
  from = aws_volume_attachment.state
  to   = aws_volume_attachment.state[0]
}

moved {
  from = aws_eip_association.this
  to   = aws_eip_association.this[0]
}

moved {
  from = aws_cloudwatch_metric_alarm.recover
  to   = aws_cloudwatch_metric_alarm.recover[0]
}


data "aws_subnet" "this" {
  count = local.instance_shape ? 1 : 0
  id    = var.subnet_id
}

# Gang's images boot UEFI only, so the controller's instance type must support it. Read at plan time.
data "aws_ec2_instance_type" "this" {
  instance_type = var.instance_type
}

resource "aws_instance" "this" {
  depends_on             = [module.instance_role]
  count                  = local.instance_shape ? 1 : 0
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  private_ip             = var.private_ip
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = module.instance_role.instance_profile_name
  monitoring             = true
  # Line 1 names the role, config parameter and state device; then one "<file>=<parameter>" line per secret.
  user_data                   = join("\n", concat(["controller ${var.config_parameter_reference} state=${local.state_device}"], [for file, parameter in var.secret_parameters : "${file}=${parameter}"]))
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

  lifecycle {
    precondition {
      condition     = contains(data.aws_ec2_instance_type.this.boot_modes, "uefi")
      error_message = "Instance type ${var.instance_type} does not support UEFI boot, which Gang's images require."
    }
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
  count             = local.instance_shape ? 1 : 0
  availability_zone = data.aws_subnet.this[0].availability_zone
  size              = var.state_volume_gib
  type              = "gp3"
  encrypted         = true

  tags = merge(var.tags, { Name = "${var.name}-state" })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_volume_attachment" "state" {
  count                          = local.instance_shape ? 1 : 0
  device_name                    = local.state_device
  volume_id                      = aws_ebs_volume.state[0].id
  instance_id                    = aws_instance.this[0].id
  stop_instance_before_detaching = true
}

resource "aws_eip_association" "this" {
  count         = local.instance_shape ? 1 : 0
  allocation_id = var.eip_allocation_id
  instance_id   = aws_instance.this[0].id
}

# Replaces the instance on the underlying host failure reported by EC2 system status checks.
resource "aws_cloudwatch_metric_alarm" "recover" {
  count               = local.instance_shape ? 1 : 0
  alarm_name          = "${var.name}-system-status-recover"
  alarm_description   = "Recover the controller onto healthy hardware when the EC2 system status check fails."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_System"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  dimensions          = { InstanceId = aws_instance.this[0].id }
  alarm_actions       = ["arn:${data.aws_partition.this.partition}:automate:${data.aws_region.this.region}:ec2:recover"]

  tags = merge(var.tags, { Name = "${var.name}-system-status-recover" })
}

data "aws_ami" "controller" {
  count = local.group_shape ? 1 : 0

  filter {
    name   = "image-id"
    values = [var.ami_id]
  }
}

resource "aws_launch_template" "controller" {
  depends_on             = [module.instance_role]
  count                  = local.group_shape ? 1 : 0
  name_prefix            = "${var.name}-"
  image_id               = var.ami_id
  instance_type          = var.instance_type
  vpc_security_group_ids = [var.security_group_id]
  user_data              = base64encode(join("\n", concat(["controller ${var.config_parameter_reference}"], [for file, parameter in var.secret_parameters : "${file}=${parameter}"], ["# config-digest ${var.config_digest}"])))
  update_default_version = true

  iam_instance_profile {
    name = module.instance_role.instance_profile_name
  }

  monitoring {
    enabled = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "enabled"
  }

  block_device_mappings {
    device_name = data.aws_ami.controller[0].root_device_name
    ebs {
      volume_type           = "gp3"
      volume_size           = var.root_volume_gib
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(var.tags, { Name = var.name })
  }

  tag_specifications {
    resource_type = "volume"
    tags          = merge(var.tags, { Name = var.name })
  }

  lifecycle {
    precondition {
      condition     = contains(data.aws_ec2_instance_type.this.boot_modes, "uefi")
      error_message = "Instance type ${var.instance_type} does not support UEFI boot, which Gang's images require."
    }
  }

  tags = merge(var.tags, { Name = var.name })
}

resource "aws_autoscaling_group" "controller" {
  count               = local.group_shape ? 1 : 0
  name                = var.name
  vpc_zone_identifier = var.subnet_ids
  min_size            = var.controller_count
  max_size            = 2 * var.controller_count
  desired_capacity    = var.controller_count
  # Followers are unhealthy in leader-only target groups. Launch hooks enforce
  # serving readiness; systemd restarts a crashed controller.
  health_check_type = "EC2"
  target_group_arns = var.target_group_arns
  # Newer prepared hosts lead; ordinary scale-in should retire an older passive host.
  termination_policies = ["OldestInstance"]

  launch_template {
    id      = aws_launch_template.controller[0].id
    version = aws_launch_template.controller[0].latest_version
  }

  instance_refresh {
    strategy = "Rolling"
    preferences {
      min_healthy_percentage = 100
      max_healthy_percentage = 200
      # Launch lifecycle completion follows promotion; do not add platform warmup.
      instance_warmup = 0
    }
  }

  initial_lifecycle_hook {
    name                 = "launch"
    lifecycle_transition = "autoscaling:EC2_INSTANCE_LAUNCHING"
    heartbeat_timeout    = local.hook.heartbeat_timeout_seconds
    default_result       = "ABANDON"
  }

  initial_lifecycle_hook {
    name                 = "terminate"
    lifecycle_transition = "autoscaling:EC2_INSTANCE_TERMINATING"
    heartbeat_timeout    = local.hook.heartbeat_timeout_seconds
    default_result       = "CONTINUE"
  }

  lifecycle {
    # Inline hooks protect the first instances at creation; standalone resources
    # own subsequent hook updates without replacing the group.
    ignore_changes = [initial_lifecycle_hook]
  }

  dynamic "tag" {
    for_each = merge(var.tags, { Name = var.name })
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }
}

resource "aws_autoscaling_lifecycle_hook" "controller_launch" {
  count = local.group_shape ? 1 : 0

  name                   = "launch"
  autoscaling_group_name = aws_autoscaling_group.controller[0].name
  lifecycle_transition   = "autoscaling:EC2_INSTANCE_LAUNCHING"
  heartbeat_timeout      = local.hook.heartbeat_timeout_seconds
  default_result         = "ABANDON"
}

resource "aws_autoscaling_lifecycle_hook" "controller_terminate" {
  count = local.group_shape ? 1 : 0

  name                   = "terminate"
  autoscaling_group_name = aws_autoscaling_group.controller[0].name
  lifecycle_transition   = "autoscaling:EC2_INSTANCE_TERMINATING"
  heartbeat_timeout      = local.hook.heartbeat_timeout_seconds
  default_result         = "CONTINUE"
}
