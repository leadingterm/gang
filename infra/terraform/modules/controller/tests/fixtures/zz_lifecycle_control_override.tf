# OpenTofu merges ignore_changes only for nonempty override lists; [] retains
# the base list. Capacity is constant here, so ignore it instead of the hooks.
# The pinned real provider must now replace the same ASG on heartbeat changes.
resource "aws_autoscaling_group" "controller" {
  lifecycle {
    ignore_changes = [desired_capacity]
  }
}
