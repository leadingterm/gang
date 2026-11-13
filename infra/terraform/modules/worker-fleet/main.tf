locals {
  fleets = { for fleet in var.fleets : fleet.name => fleet }

  on_demand_suffix = "-on-demand"

  # One group per fleet, plus an on-demand twin for each fleet that opts into fallback.
  # Groups are keyed by group name, so a plain fleet keeps its address and state.
  groups = merge(
    { for fleet in var.fleets : fleet.name => {
      fleet          = fleet.name
      role           = "primary"
      capacity       = fleet.capacity
      warm_pool_size = fleet.capacity == "on-demand" ? fleet.warm_pool_size : 0
      protected      = fleet.fallback != null || fleet.warm_pool_size > 0
    } },
    { for fleet in var.fleets : "${fleet.name}${local.on_demand_suffix}" => {
      fleet          = fleet.name
      role           = "on_demand"
      capacity       = "on-demand"
      warm_pool_size = fleet.warm_pool_size
      protected      = true
    } if fleet.fallback != null },
  )

  # Group names in fleet order, each fleet's twin directly after its own group.
  group_names = flatten([
    for fleet in var.fleets :
    fleet.fallback == null ? [fleet.name] : [fleet.name, "${fleet.name}${local.on_demand_suffix}"]
  ])

  # Per fleet, the worker subnets whose zone offers at least one of its instance types; the
  # on-demand twin uses its fleet's list. Capacity pools are per instance type and zone, and a
  # mixed-instances group chooses among the pools of the zone it launches in, so one offered type
  # makes a zone usable. Requiring every type would drop usable zones and shrink the fleet.
  fleet_subnet_ids = {
    for fleet in var.fleets : fleet.name => [
      for zone, subnet_id in var.subnet_ids : subnet_id
      if anytrue([
        for index, offered_zone in data.aws_ec2_instance_type_offerings.this.locations :
        offered_zone == zone && contains(fleet.instance_types, data.aws_ec2_instance_type_offerings.this.instance_types[index])
      ])
    ]
  }
}

# Root volume device name comes from the AMI, so the launch template maps the right device.
data "aws_ami" "this" {
  for_each = local.fleets

  filter {
    name   = "image-id"
    values = [each.value.ami_id]
  }
}

# Which worker subnet zones offer which instance types. Read at plan time, so each group spans only zones that can launch its fleet.
data "aws_ec2_instance_type_offerings" "this" {
  location_type = "availability-zone"

  filter {
    name   = "location"
    values = keys(var.subnet_ids)
  }

  filter {
    name   = "instance-type"
    values = distinct(flatten(var.fleets[*].instance_types))
  }
}

resource "aws_launch_template" "this" {
  for_each = local.fleets

  name_prefix = "${var.name_prefix}-${each.key}-"
  image_id    = each.value.ami_id
  user_data   = base64encode(var.config_parameter_name)
  tags        = merge(var.tags, { Name = each.value.name })

  iam_instance_profile {
    arn = module.instance_role.instance_profile_arn
  }

  vpc_security_group_ids = [var.security_group_id]

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "enabled"
  }

  block_device_mappings {
    device_name = data.aws_ami.this[each.key].root_device_name

    ebs {
      volume_size           = each.value.root_volume_gib
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  dynamic "block_device_mappings" {
    for_each = each.value.scratch_volume_gib == null ? [] : [each.value.scratch_volume_gib]

    content {
      device_name = "/dev/sdf"

      ebs {
        volume_size           = block_device_mappings.value
        volume_type           = "gp3"
        encrypted             = true
        delete_on_termination = true
      }
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(var.tags, { Name = each.value.name })
  }

  tag_specifications {
    resource_type = "volume"
    tags          = merge(var.tags, { Name = each.value.name })
  }
}

resource "aws_autoscaling_group" "this" {
  for_each = local.groups

  name                   = each.key
  min_size               = 0
  max_size               = local.fleets[each.value.fleet].max_instances
  desired_capacity       = 0
  vpc_zone_identifier    = local.fleet_subnet_ids[each.value.fleet]
  capacity_rebalance     = each.value.capacity == "spot"
  protect_from_scale_in  = each.value.protected
  force_delete_warm_pool = each.value.warm_pool_size > 0

  dynamic "warm_pool" {
    for_each = each.value.warm_pool_size > 0 ? [each.value.warm_pool_size] : []

    content {
      pool_state                  = "Stopped"
      min_size                    = warm_pool.value
      max_group_prepared_capacity = warm_pool.value

      instance_reuse_policy {
        reuse_on_scale_in = true
      }
    }
  }

  mixed_instances_policy {
    instances_distribution {
      on_demand_base_capacity                  = 0
      on_demand_percentage_above_base_capacity = each.value.capacity == "spot" ? 0 : 100
      # The order of instance_types is the on-demand priority; spot allocation ignores it.
      on_demand_allocation_strategy = "prioritized"
      # Set explicitly so the planned value is known; lowest-price is AWS's default and has no effect for 100% on-demand.
      spot_allocation_strategy = each.value.capacity == "spot" ? "price-capacity-optimized" : "lowest-price"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.this[each.value.fleet].id
        version            = aws_launch_template.this[each.value.fleet].latest_version
      }

      dynamic "override" {
        for_each = local.fleets[each.value.fleet].instance_types

        content {
          instance_type = override.value
        }
      }
    }
  }

  dynamic "tag" {
    for_each = merge(var.tags, { Name = each.key, (var.fleet_tag_key) = each.value.fleet })

    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }

  # The controller owns desired capacity; Terraform sets it only at creation.
  lifecycle {
    ignore_changes = [desired_capacity]

    precondition {
      condition     = length(local.fleet_subnet_ids[each.value.fleet]) > 0
      error_message = "Fleet ${each.value.fleet}: no worker subnet zone (${join(", ", keys(var.subnet_ids))}) offers any of its instance types (${join(", ", local.fleets[each.value.fleet].instance_types)})."
    }
  }
}

# Account-wide singleton: enables outbound identity federation for the account when managed here.
resource "aws_iam_outbound_web_identity_federation" "this" {
  count = var.manage_outbound_web_identity_federation ? 1 : 0
}

data "aws_iam_outbound_web_identity_federation" "this" {
  count = var.manage_outbound_web_identity_federation ? 0 : 1
}
