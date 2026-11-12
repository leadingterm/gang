variable "name" {
  description = "Name of the controller. Used for the instance, IAM role, instance profile and alarm names, and as the Name tag prefix."
  type        = string
  default     = "gang-controller"
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
  description = "Public subnet the controller runs in. The state volume is created in its availability zone."
  type        = string
  nullable    = false
}

variable "private_ip" {
  description = "Fixed private IPv4 address of the controller inside subnet_id. Workers reach the controller worker listener at this address."
  type        = string
  nullable    = false
}

variable "security_group_id" {
  description = "Security group attached to the controller instance."
  type        = string
  nullable    = false
}

variable "eip_allocation_id" {
  description = "Allocation ID of the Elastic IP associated with the controller instance."
  type        = string
  nullable    = false
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
  description = "Size in GiB of the encrypted gp3 state volume attached as /dev/sdf and mounted at /var/lib/gang."
  type        = number
  default     = 20
  nullable    = false

  validation {
    condition     = var.state_volume_gib >= 8
    error_message = "state_volume_gib must be at least 8 GiB."
  }
}

variable "config_parameter_name" {
  description = "Name of the SSM parameter holding the rendered gang.toml. Delivered as plain-text EC2 user data; the instance reads the parameter at boot."
  type        = string
  nullable    = false

  validation {
    condition     = length(var.config_parameter_name) <= 1011 && can(regex("^/[A-Za-z0-9_./-]+$", var.config_parameter_name))
    error_message = "config_parameter_name must be an SSM parameter name: / followed by A-Z a-z 0-9 _ . - / characters, at most 1011 in total."
  }
}

variable "config_parameter_arn" {
  description = "ARN of the SSM parameter named by config_parameter_name. The controller role may read only this parameter."
  type        = string
  nullable    = false
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
  description = "ARN of the S3 cache bucket the controller reads and writes objects in."
  type        = string
  nullable    = false
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
