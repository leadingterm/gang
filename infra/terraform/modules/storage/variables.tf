variable "name" {
  description = "Prefix of the module's named resources. Required; no default."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$", var.name))
    error_message = "name must be 1 to 32 lowercase letters, digits and inner hyphens."
  }
}

variable "cache_bucket_name" {
  description = "Name of the private S3 bucket that stores the build cache. Required; no default."
  type        = string
}

variable "cache_expiration_days" {
  description = "Days after creation until every object in the cache bucket expires."
  type        = number
  default     = 30
  nullable    = false

  validation {
    condition     = var.cache_expiration_days >= 1
    error_message = "cache_expiration_days must be at least 1."
  }
}

variable "ca_key_deletion_window_days" {
  description = "Waiting period in days before the CA KMS key is deleted after scheduled deletion (7 to 30)."
  type        = number
  default     = 30
  nullable    = false

  validation {
    condition     = var.ca_key_deletion_window_days >= 7 && var.ca_key_deletion_window_days <= 30
    error_message = "ca_key_deletion_window_days must be between 7 and 30."
  }
}

variable "ca_signer_role_arn" {
  description = "ARN of the IAM role that signs with the CA key (the controller role). The key policy lets only this principal use kms:Sign. Required; no default."
  type        = string

  validation {
    condition     = can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:role/.+$", var.ca_signer_role_arn))
    error_message = "ca_signer_role_arn must be an IAM role ARN (arn:aws:iam::<account>:role/<name>)."
  }
}

variable "tags" {
  description = "Tags merged into every taggable resource in this module."
  type        = map(string)
  default     = {}
}
