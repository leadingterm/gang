# Remove only the fix: the pinned real provider must now replace the same ASG.
resource "aws_autoscaling_group" "controller" {
  lifecycle {
    ignore_changes = []
  }
}
