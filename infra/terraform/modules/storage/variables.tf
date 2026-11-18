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

variable "dynamodb_table" {
  description = "Optional DynamoDB table for shared controller state, identified by its table name."
  type        = object({ name = string })
  default     = null

  validation {
    condition     = var.dynamodb_table == null ? true : can(regex("^[a-zA-Z0-9_.-]{3,255}$", var.dynamodb_table.name))
    error_message = "dynamodb_table.name must be 3 to 255 letters, digits, underscores, hyphens or periods."
  }
}

variable "postgres_rds" {
  description = "Optional PostgreSQL RDS instance and its private network configuration. Increment password_version to rotate the database password."
  type = object({
    instance_class           = string
    multi_az                 = bool
    storage_gib              = number
    engine_version           = string
    password_version         = optional(number, 1)
    vpc_id                   = string
    subnet_ids               = list(string)
    client_security_group_id = string
  })
  default = null

  validation {
    condition     = var.postgres_rds == null ? true : try(var.postgres_rds.storage_gib >= 20, false)
    error_message = "postgres_rds.storage_gib must be at least 20."
  }

  validation {
    condition     = var.postgres_rds == null ? true : var.postgres_rds.multi_az != null
    error_message = "postgres_rds.multi_az must be a boolean."
  }

  validation {
    condition     = var.postgres_rds == null ? true : can(regex("^[0-9]+$", var.postgres_rds.engine_version))
    error_message = "postgres_rds.engine_version must contain only major-version digits."
  }

  validation {
    condition     = var.postgres_rds == null ? true : try(length(distinct(var.postgres_rds.subnet_ids)) >= 2, false)
    error_message = "postgres_rds.subnet_ids must contain at least two distinct subnets in different availability zones."
  }

  validation {
    condition     = var.postgres_rds == null ? true : var.postgres_rds.password_version >= 1 && floor(var.postgres_rds.password_version) == var.postgres_rds.password_version
    error_message = "postgres_rds.password_version must be a whole number of at least 1."
  }
}

variable "tags" {
  description = "Tags merged into every taggable resource in this module."
  type        = map(string)
  default     = {}
}
