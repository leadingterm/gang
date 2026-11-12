variable "name_prefix" {
  description = "Prefix for the worker IAM role, instance profile, inline policy and launch template names."
  type        = string
  default     = "gang-worker"
  nullable    = false
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

variable "subnet_ids" {
  description = "Private subnet IDs the worker Auto Scaling groups launch into."
  type        = list(string)
  nullable    = false
}

variable "security_group_id" {
  description = "Security group attached to every worker instance (the worker security group of the network module)."
  type        = string
  nullable    = false
}

variable "fleets" {
  description = <<-EOT
    Worker fleets in preference order. Each fleet has one Auto Scaling group; a fleet with `fallback` also has an on-demand twin group named `<name>-on-demand`. Names must be unique.
    - `name`: the ASG name, which is also the fleet name the controller uses. 1 to 40 lowercase letters, digits and hyphens; starts and ends with a letter or digit.
    - `capacity`: `spot` or `on-demand`. Spot fleets buy interruptible capacity with price-capacity-optimized allocation; on-demand fleets buy only on-demand capacity.
    - `ami_id`: the worker AMI for this fleet.
    - `instance_types`: instance types, one mixed-instances override each. The list order is the priority for on-demand fleets: the first type is tried first. Spot fleets ignore the order. Types must be distinct. Unless `scratch_volume_gib` is set, every type must have instance-store NVMe (a `d` family such as `c6id.4xlarge` or `c7gd.4xlarge`).
    - `max_instances`: the ASG maximum size.
    - `scratch_volume_gib`: size of an encrypted gp3 EBS data volume at `/dev/sdf`, for EBS-only instance types. `null` attaches no scratch volume.
    - `root_volume_gib`: size of the encrypted gp3 root volume. Defaults to 30 GiB.
    - `warm_pool_size`: stopped standby instances kept warm on the fleet's on-demand group (the fleet's own group for on-demand fleets, the `<name>-on-demand` twin for fallback fleets). `0` keeps no pool. Above `0` the fleet needs an on-demand group: `capacity` `on-demand` or `fallback`.
    - `fallback`: opts a spot fleet in to on-demand fallback. The fleet keeps its spot group and gains the on-demand twin, which takes capacity while Spot is unavailable. Presence opts in; `null` keeps Spot only. Requires `capacity` `spot`.
  EOT
  type = list(object({
    name               = string
    capacity           = string
    ami_id             = string
    instance_types     = list(string)
    max_instances      = number
    scratch_volume_gib = optional(number, null)
    root_volume_gib    = optional(number, 30)
    warm_pool_size     = optional(number, 0)
    fallback           = optional(object({}), null)
  }))
  nullable = false

  validation {
    condition     = alltrue([for fleet in var.fleets : can(regex("^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$", fleet.name))])
    error_message = "Each fleet name must be 1 to 40 lowercase letters, digits or hyphens, and start and end with a letter or digit."
  }

  validation {
    condition     = length(distinct([for fleet in var.fleets : fleet.name])) == length(var.fleets)
    error_message = "Fleet names must be unique."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : contains(["spot", "on-demand"], fleet.capacity)])
    error_message = "Each fleet capacity must be \"spot\" or \"on-demand\"."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : length(fleet.instance_types) > 0])
    error_message = "Each fleet needs at least one instance type."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : length(distinct(fleet.instance_types)) == length(fleet.instance_types)])
    error_message = "Each fleet's instance types must be distinct."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.max_instances >= 1])
    error_message = "Each fleet needs max_instances >= 1."
  }

  validation {
    condition = alltrue([
      for fleet in var.fleets :
      fleet.scratch_volume_gib != null || alltrue([
        for instance_type in fleet.instance_types :
        can(regex("^[a-z]+[0-9]+[a-z]*d[a-z]*\\.[a-z0-9]+$", instance_type))
      ])
    ])
    error_message = "Without scratch_volume_gib every instance type must have instance-store NVMe (a 'd' family such as c6id.4xlarge). Set scratch_volume_gib for EBS-only types."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.fallback == null || fleet.capacity == "spot"])
    error_message = "A fleet with fallback must have capacity \"spot\"."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.warm_pool_size >= 0])
    error_message = "Each fleet's warm_pool_size must be >= 0."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.warm_pool_size == 0 || fleet.capacity == "on-demand" || fleet.fallback != null])
    error_message = "A warm pool needs an on-demand group: warm_pool_size > 0 requires capacity \"on-demand\" or fallback."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.fallback == null || !anytrue([for other in var.fleets : other.name == "${fleet.name}-on-demand"])])
    error_message = "No fleet may be named `<name>-on-demand` when fleet `<name>` has fallback: that name is the on-demand twin's group name."
  }
}

variable "cache_bucket_arn" {
  description = "ARN of the S3 cache bucket the workers read and write (the bucket itself, not objects)."
  type        = string
  nullable    = false
}

variable "ca_key_arn" {
  description = "ARN of the KMS key (ECC_NIST_P256, SIGN_VERIFY) that signs the server certificates. Workers get kms:GetPublicKey on it only, to derive the CA pin from its public key."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/.+$", var.ca_key_arn))
    error_message = "ca_key_arn must be a KMS key ARN (arn:aws:kms:<region>:<account>:key/<id>)."
  }
}

variable "workload_token_audience" {
  description = "Audience of the STS web identity tokens the workers request (sts:IdentityTokenAudience). Must match the identity provider's audience."
  type        = string
  default     = "gang"
  nullable    = false
}

variable "workload_token_max_seconds" {
  description = "Maximum lifetime, in seconds, of the STS web identity tokens the workers may request (sts:DurationSeconds)."
  type        = number
  default     = 3600
  nullable    = false

  validation {
    condition     = var.workload_token_max_seconds >= 60 && var.workload_token_max_seconds <= 3600
    error_message = "workload_token_max_seconds must be between 60 and 3600."
  }
}

variable "config_parameter_name" {
  description = "Name of the SSM parameter holding the rendered gang.toml. Becomes the EC2 user data; workers read the parameter at boot."
  type        = string
  nullable    = false

  validation {
    condition     = length(var.config_parameter_name) <= 1011 && can(regex("^/[A-Za-z0-9_./-]+$", var.config_parameter_name))
    error_message = "config_parameter_name must be an SSM parameter name: / followed by A-Z a-z 0-9 _ . - / characters, at most 1011 in total."
  }
}

variable "config_parameter_arn" {
  description = "ARN of the SSM parameter named by config_parameter_name. The worker role may read it (ssm:GetParameter)."
  type        = string
  nullable    = false
}

variable "tags" {
  description = "Tags merged into every taggable resource, and propagated to the worker instances and volumes."
  type        = map(string)
  default     = {}
  nullable    = false
}

variable "fleet_tag_key" {
  description = "Instance and Auto Scaling group tag key that carries the fleet name. The gang-aws plugin owns the validation rule for this key; the deployment module repeats it for plan-time feedback. The deployment module writes the same key to [provisioner.aws] fleet_tag_key."
  type        = string
  nullable    = false
}

variable "manage_outbound_web_identity_federation" {
  description = "Whether this module enables outbound web identity federation for the account. False reads the account's already-enabled federation instead, so a second deployment in the same account does not conflict."
  type        = bool
  default     = true
  nullable    = false
}
