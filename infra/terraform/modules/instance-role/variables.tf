variable "name" {
  description = "Name of the IAM role. Also the name of the inline role policy and the base of the Name tag. The role and the instance profile are named iam_name_prefix followed by this name."
  type        = string
  nullable    = false
}

variable "iam_name_prefix" {
  description = "Prefix of the name of the IAM role and instance profile, including any separator (for example `example-`). Empty keeps the module's own names. Use it where IAM names must sit under a prefix."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]*$", var.iam_name_prefix))
    error_message = "iam_name_prefix may contain only letters, digits and the characters + = , . @ _ -."
  }
}

variable "iam_permissions_boundary_arn" {
  description = "ARN of the IAM managed policy set as the permissions boundary of the IAM role. Null sets no boundary."
  type        = string
  default     = null

  validation {
    condition     = var.iam_permissions_boundary_arn == null || can(regex("^arn:aws[a-z-]*:iam::([0-9]{12}|aws):policy/.+$", var.iam_permissions_boundary_arn))
    error_message = "iam_permissions_boundary_arn must be null or an IAM policy ARN (arn:aws:iam::<account or aws>:policy/<name>)."
  }
}

variable "tags" {
  description = "Tags merged into the IAM role and instance profile."
  type        = map(string)
  default     = {}
  nullable    = false
}

variable "readable_parameter_arns" {
  description = "ARNs of every SSM parameter the role may read. The role is denied every other parameter. Must not be empty."
  type        = list(string)
  nullable    = false

  validation {
    condition     = length(var.readable_parameter_arns) > 0
    error_message = "readable_parameter_arns must list at least one SSM parameter ARN."
  }
}

variable "policy_json" {
  description = "JSON policy document with the role-specific statements of the caller. It is combined with the module's base statements in the inline role policy."
  type        = string
  nullable    = false
}
