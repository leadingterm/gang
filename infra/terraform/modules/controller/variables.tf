variable "name" {
  description = "Name of the controller. Used for the instance, IAM role, instance profile and alarm names, and as the Name tag prefix."
  type        = string
  default     = "gang-controller"
  nullable    = false
}

variable "shape" {
  description = "Controller resource shape: instance for SQLite or group for shared state."
  type        = string
  default     = "instance"
  nullable    = false

  validation {
    condition     = contains(["instance", "group"], var.shape)
    error_message = "shape must be instance or group."
  }
}

variable "controller_count" {
  description = "Desired number of controllers in the group."
  type        = number
  default     = 1
  nullable    = false

  validation {
    condition     = var.controller_count >= 1 && floor(var.controller_count) == var.controller_count
    error_message = "controller_count must be a positive integer."
  }
}

variable "subnet_ids" {
  description = "Private subnet IDs for the controller group; required for the group shape."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = var.shape != "group" || (length(var.subnet_ids) > 0 && alltrue([for id in var.subnet_ids : length(trimspace(id)) > 0]))
    error_message = "subnet_ids must contain nonempty subnet IDs for the group shape."
  }
}

variable "target_group_arns" {
  description = "Load balancer target group ARNs attached to the controller group."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = var.shape != "group" || (length(var.target_group_arns) > 0 && alltrue([for arn in var.target_group_arns : length(trimspace(arn)) > 0]))
    error_message = "target_group_arns must contain nonempty target group ARNs for the group shape."
  }
}

variable "shutdown_grace_seconds" {
  description = "Controller shutdown grace in seconds; lifecycle hook timeouts include a 120-second margin."
  type        = number
  default     = 300
  nullable    = false

  validation {
    condition     = var.shutdown_grace_seconds >= 1 && var.shutdown_grace_seconds <= 3480 && floor(var.shutdown_grace_seconds) == var.shutdown_grace_seconds
    error_message = "shutdown_grace_seconds must be an integer between 1 and 3480."
  }
}

variable "config_digest" {
  description = "SHA-256 digest of rendered gang.toml, required for group instance refresh on configuration changes."
  type        = string
  default     = null

  validation {
    condition     = var.shape != "group" || can(regex("^[0-9a-f]{64}$", var.config_digest))
    error_message = "config_digest must be a lowercase SHA-256 hex digest for the group shape."
  }
}

variable "dynamodb_table_arn" {
  description = "DynamoDB table ARN to grant item access; null omits the grant."
  type        = string
  default     = null

  validation {
    condition     = var.dynamodb_table_arn == null || can(regex("^arn:aws[a-z-]*:dynamodb:[a-z0-9-]+:[0-9]{12}:table/[^/]+$", var.dynamodb_table_arn))
    error_message = "dynamodb_table_arn must be null or a DynamoDB table ARN."
  }
}

variable "events_queue_arn" {
  description = "SQS queue ARN for AWS event observation; null omits the grant."
  type        = string
  default     = null

  validation {
    condition     = var.events_queue_arn == null || can(regex("^arn:aws[a-z-]*:sqs:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]+$", var.events_queue_arn))
    error_message = "events_queue_arn must be null or a standard SQS queue ARN."
  }
}

variable "iam_name_prefix" {
  description = "Prefix of the name of every IAM role and instance profile, including any separator (for example `example-`). Empty keeps the module's own names. Use it where IAM names must sit under a prefix."
  type        = string
  default     = ""
  nullable    = false
}

variable "iam_permissions_boundary_arn" {
  description = "ARN of the IAM managed policy set as the permissions boundary of every IAM role. Null sets no boundary."
  type        = string
  default     = null
}

variable "ami_id" {
  description = "AMI ID for the controller instance. Changing it replaces the instance."
  type        = string
  nullable    = false
}

variable "instance_type" {
  description = "EC2 instance type of the controller."
  type        = string
  nullable    = false
}

variable "subnet_id" {
  description = "Public subnet for the instance shape; the state volume is created in its availability zone."
  type        = string
  default     = null

  validation {
    condition     = var.shape != "instance" || try(length(trimspace(var.subnet_id)) > 0, false)
    error_message = "subnet_id is required for the instance shape."
  }
}

variable "private_ip" {
  description = "Fixed private IPv4 address for the instance shape."
  type        = string
  default     = null

  validation {
    condition     = var.shape != "instance" || try(length(trimspace(var.private_ip)) > 0, false)
    error_message = "private_ip is required for the instance shape."
  }
}

variable "security_group_id" {
  description = "Security group attached to the controller instance."
  type        = string
  nullable    = false
}

variable "eip_allocation_id" {
  description = "Elastic IP allocation ID for the instance shape."
  type        = string
  default     = null

  validation {
    condition     = var.shape != "instance" || try(length(trimspace(var.eip_allocation_id)) > 0, false)
    error_message = "eip_allocation_id is required for the instance shape."
  }
}

variable "root_volume_gib" {
  description = "Size in GiB of the encrypted gp3 root volume."
  type        = number
  default     = 30
  nullable    = false

  validation {
    condition     = var.root_volume_gib >= 8
    error_message = "root_volume_gib must be at least 8 GiB."
  }
}

variable "state_volume_gib" {
  description = "Size in GiB of the encrypted gp3 state volume attached as /dev/sdg and mounted at /var/lib/gang."
  type        = number
  default     = 20
  nullable    = false

  validation {
    condition     = var.state_volume_gib >= 8
    error_message = "state_volume_gib must be at least 8 GiB."
  }
}

variable "config_parameter_reference" {
  description = "Absolute SSM configuration parameter path with a positive numeric version selector."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^/([A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+:[0-9]*[1-9][0-9]*$", var.config_parameter_reference)) && length(split(":", var.config_parameter_reference)[0]) <= 1011
    error_message = "config_parameter_reference must be an absolute SSM path of at most 1011 characters followed by a positive numeric :version."
  }
}

variable "config_parameter_arns" {
  description = "SSM parameter ARNs readable by the controller."
  type        = list(string)
  nullable    = false

  validation {
    condition     = length(var.config_parameter_arns) > 0
    error_message = "config_parameter_arns must not be empty."
  }
}

variable "secret_parameters" {
  description = "SSM SecureString parameters holding confidential values, keyed by the file name each value is written to under /etc/gang/secrets on the controller. Values are parameter names. The parameters are created outside Terraform so their values never enter state; the controller role may read them and nothing else. Empty means the controller has no secrets."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = alltrue([
      for file, parameter in var.secret_parameters :
      can(regex("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", file)) && length(parameter) <= 1011 && can(regex("^/[A-Za-z0-9_./-]+$", parameter))
    ])
    error_message = "secret_parameters keys must be file names matching ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ and values must be SSM parameter names: / followed by A-Z a-z 0-9 _ . - / characters, at most 1011 in total."
  }
}

variable "secret_parameters_kms_key_arn" {
  description = "ARN of the customer managed KMS key that encrypts the secret_parameters. Null means the parameters use the AWS-managed SSM key, which needs no IAM grant."
  type        = string
  default     = null

  validation {
    condition     = var.secret_parameters_kms_key_arn == null || can(regex("^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/.+$", var.secret_parameters_kms_key_arn))
    error_message = "secret_parameters_kms_key_arn must be null or a KMS key ARN (arn:aws:kms:<region>:<account>:key/<id>)."
  }
}

variable "cache_bucket_arn" {
  description = "ARN of the S3 cache bucket the controller reads and writes; null means a filesystem cache with no S3 access."
  type        = string
  nullable    = true
}

variable "ca_key_arn" {
  description = "ARN of the KMS signing key (ECC_NIST_P256, SIGN_VERIFY) the controller uses to sign certificates."
  type        = string
  nullable    = false
}

variable "worker_fleet_names" {
  description = "Names of the worker Auto Scaling groups the controller may scale and terminate instances in. Must not be empty."
  type        = list(string)
  nullable    = false

  validation {
    condition     = length(var.worker_fleet_names) > 0
    error_message = "worker_fleet_names must contain at least one Auto Scaling group name."
  }
}

variable "tags" {
  description = "Tags merged into every taggable resource of the module."
  type        = map(string)
  default     = {}
  nullable    = false
}
