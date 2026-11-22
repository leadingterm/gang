# Installed only in the contract runner's disposable copy of the actual module.
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.72.0"
    }
  }
}

variable "test_legacy_hooks" {
  type    = bool
  default = false
}

variable "test_managed_heartbeat" {
  type    = number
  default = null
}

locals {
  hook = merge(jsondecode(file("${path.module}/lifecycle-hook.json")), var.test_legacy_hooks ? { heartbeat_timeout_seconds = 420 } : var.test_managed_heartbeat == null ? {} : { heartbeat_timeout_seconds = var.test_managed_heartbeat })
}

# The released configuration had only inline hooks, not standalone resources.
resource "aws_autoscaling_lifecycle_hook" "controller_launch" {
  count = var.test_legacy_hooks ? 0 : local.group_shape ? 1 : 0
}

resource "aws_autoscaling_lifecycle_hook" "controller_terminate" {
  count = var.test_legacy_hooks ? 0 : local.group_shape ? 1 : 0
}
