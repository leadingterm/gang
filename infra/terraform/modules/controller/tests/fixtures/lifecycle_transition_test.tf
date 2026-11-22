# Primary declarations installed only in the contract runner's disposable root.
variable "test_legacy_hooks" {
  type    = bool
  default = false
}

variable "test_managed_heartbeat" {
  type    = number
  default = null
}
