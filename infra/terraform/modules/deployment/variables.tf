variable "region" {
  type        = string
  description = "AWS region of the deployment."
}

variable "cidr_block" {
  type        = string
  description = "IPv4 CIDR block of the VPC. Subnets are carved from it by the network module."
}

variable "name" {
  type        = string
  description = "Deployment name. Prefixes the VPC, the controller and the worker resources."
  default     = "gang"

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$", var.name))
    error_message = "name must be 1 to 32 lowercase letters, digits and inner hyphens."
  }
}

variable "iam_name_prefix" {
  type        = string
  description = "Prefix of the name of every IAM role and instance profile of the controller and the workers, including any separator (for example `example-`). Empty keeps the default names."
  default     = ""
}

variable "iam_permissions_boundary_arn" {
  type        = string
  description = "ARN of the IAM managed policy set as the permissions boundary of the controller and worker roles. Null sets no boundary."
  default     = null
}

variable "cache_bucket_name" {
  type        = string
  description = "Name of the S3 bucket that holds the build cache. Globally unique."
}

variable "network" {
  type = object({
    availability_zone_count = optional(number)
  })
  description = "Network tunables. Unset attributes use the network module's defaults."
  default     = {}
  nullable    = false
}

variable "storage" {
  type = object({
    cache_expiration_days       = optional(number)
    ca_key_deletion_window_days = optional(number)
  })
  description = "Storage tunables. Unset attributes use the storage module's defaults."
  default     = {}
  nullable    = false
}

variable "hostname" {
  type        = string
  description = "Public hostname of the Gang endpoint (port 443). Null uses the EC2 public DNS name of the controller Elastic IP. DNS is managed outside this module. Required by load_balancer."
  default     = null
}

variable "client_ingress_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to reach the public endpoint (443): the controller's listener, or the load balancer when load_balancer is set. Access is authenticated by gang."
  default     = ["0.0.0.0/0"]
}

variable "load_balancer" {
  type = object({
    ssl_policy                     = optional(string, "ELBSecurityPolicy-TLS13-1-2-2021-06")
    certificate_validation_timeout = optional(string, "2h")
  })
  description = "Publicly trusted TLS in front of the controller: an ACM certificate for hostname on a Network Load Balancer that terminates TLS (ALPN h2 then http/1.1) and re-encrypts to the controller's public listener, so browsers, Bazel and gang use their system roots; gang.toml then sets [server] endpoint_trust = \"system\". Requires hostname. The controller's public port then accepts only the load balancer and the workers, and its Elastic IP remains its egress address. Inside the VPC hostname resolves to the controller (a private hosted zone), so workers keep reaching its own listener. ssl_policy must be an ELBSecurityPolicy-TLS13-* policy (validated by the load-balancer module). The first apply waits up to certificate_validation_timeout until the record in certificate_validation_records is published. Null (the default) serves the public listener directly on the controller's Elastic IP with the gang CA certificate."
  default     = null

  validation {
    condition     = var.load_balancer == null || var.hostname != null
    error_message = "load_balancer requires hostname: it is the certificate's domain."
  }
}

variable "nat_mode" {
  type        = string
  description = "NAT gateway layout for the private subnets: single or per_az. none is rejected by this module."
  default     = "single"
  validation {
    condition     = contains(["single", "per_az"], var.nat_mode)
    error_message = "nat_mode must be single or per_az. none is rejected: private-subnet instances need SSM and STS reachability, and no interface endpoints are provided."
  }
}

variable "controller_ami_id" {
  type        = string
  description = "AMI id of the controller instance (the Gang controller NixOS image). Changing it replaces the controller."
}

variable "controller_instance_type" {
  type        = string
  description = "EC2 instance type of the controller."
}

variable "controller_root_volume_gib" {
  type        = number
  description = "Controller root volume size in GiB. Null uses the controller module's default."
  default     = null
}

variable "controller_state_volume_gib" {
  type        = number
  description = "Controller state volume size in GiB. Null uses the controller module's default."
  default     = null
}

variable "fleets" {
  type = list(object({
    name                        = string
    isa                         = string
    sandbox                     = optional(string, "bwrap")
    capacity                    = string
    machine_class               = optional(string, null)
    ami_id                      = string
    instance_types              = list(string)
    max_instances               = number
    min_instances               = optional(number, 0)
    vcpus_per_instance          = number
    memory_per_instance         = string
    disk_per_instance           = string
    target_actions_per_instance = number
    idle_timeout                = optional(string, "10m")
    max_drain_wait              = optional(string, "25m")
    boot_grace                  = optional(string, "10m")
    scratch_volume_gib          = optional(number, null)
    root_volume_gib             = optional(number)
    warm_pool_size              = optional(number, 0)
    fallback                    = optional(object({ retry_spot_after = optional(string, "5m"), drain_on_demand_first = optional(bool, true) }), null)
  }))
  description = <<-EOT
    Worker fleets in preference order: the scheduler tries them in list order. Each fleet is one Auto Scaling group; a fleet with `fallback` also has an on-demand twin group `<name>-on-demand`.
    name: Auto Scaling group name, 1 to 40 lowercase letters, digits or hyphens, starting and ending with a letter or digit, unique across fleets. Written to gang.toml as [[fleets]].name.
    isa: instruction set of the fleet, x86-64 or aarch64. Written to gang.toml as [[fleets]].isa.
    sandbox: sandbox the workers run actions in. Only bwrap is supported. Written to gang.toml as [[fleets]].sandbox.
    capacity: spot or on-demand. Selects the Auto Scaling group purchase option and is written to [fleets.options].capacity.
    machine_class: optional qualitative machine class (for example gpu), written to [fleets.options].machine_class when set.
    ami_id: worker AMI for this fleet.
    instance_types: EC2 instance types. For on-demand fleets the order is the launch priority (the first type is tried first); spot fleets ignore the order. Every type needs instance-store NVMe unless scratch_volume_gib is set.
    max_instances: upper bound of the Auto Scaling group.
    min_instances, vcpus_per_instance, target_actions_per_instance, idle_timeout,
    max_drain_wait, boot_grace: scheduling policy written to gang.toml.
    memory_per_instance: non-zero memory size in B, KiB, MiB, GiB or TiB, written to gang.toml.
    disk_per_instance: non-zero disk size in B, KiB, MiB, GiB or TiB; lower bound of the disk the fleet's actions may fill in total, valid for every instance type of the fleet (scratch capacity minus the fast CAS budget minus an overshoot reserve); written to gang.toml.
    scratch_volume_gib: EBS scratch volume on /dev/sdf for EBS-only instance types. Null uses instance-store NVMe.
    root_volume_gib: size of the encrypted gp3 root volume in GiB. Unset uses the worker-fleet module's default.
    warm_pool_size: stopped standby instances kept warm on the fleet's on-demand group: the fleet's own group for on-demand fleets, the <name>-on-demand twin for fallback fleets. 0 keeps no pool. Above 0 the fleet needs an on-demand group (capacity on-demand, or fallback). Not written to gang.toml.
    fallback: opts a spot fleet in to on-demand fallback. The fleet keeps its spot group and gains the on-demand twin <name>-on-demand, which takes capacity while Spot is unavailable. Presence opts in; null keeps Spot only. Requires capacity spot. Written to gang.toml as [fleets.options.fallback]: retry_spot_after (default 5m) is how long capacity stays on the twin after a failed Spot launch before Spot is tried again; drain_on_demand_first (default true) releases on-demand instances before Spot ones when the fleet has surplus.
  EOT

  validation {
    condition     = alltrue([for fleet in var.fleets : can(regex("^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$", fleet.name))])
    error_message = "Each fleet name must be 1 to 40 lowercase letters, digits or hyphens, and start and end with a letter or digit."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : contains(["aarch64", "x86-64"], fleet.isa)])
    error_message = "Each fleet isa must be aarch64 or x86-64."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : contains(["bwrap"], fleet.sandbox)])
    error_message = "Each fleet sandbox must be bwrap, the only sandbox this module renders."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.machine_class == null || can(regex("^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$", fleet.machine_class))])
    error_message = "Each fleet machine_class must be 1 to 40 characters of a-z, 0-9 and '-', and must not start or end with '-'."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.fallback == null || fleet.capacity == "spot"])
    error_message = "A fleet with fallback must have capacity \"spot\"."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.warm_pool_size == 0 || fleet.capacity == "on-demand" || fleet.fallback != null])
    error_message = "A warm pool needs an on-demand group: warm_pool_size > 0 requires capacity \"on-demand\" or fallback."
  }

  validation {
    condition = alltrue([
      for fleet in var.fleets : try(
        tonumber(regex("^(0*[1-9][0-9]*)(B|KiB|MiB|GiB|TiB)$", fleet.disk_per_instance)[0]) *
        { B = 1, KiB = 1024, MiB = 1048576, GiB = 1073741824, TiB = 1099511627776 }[regex("^(0*[1-9][0-9]*)(B|KiB|MiB|GiB|TiB)$", fleet.disk_per_instance)[1]] <= 18446744073709551615,
        false
      )
    ])
    error_message = "Each fleet disk_per_instance must be a non-zero size in B, KiB, MiB, GiB or TiB that fits in 64 bits."
  }

  validation {
    condition = alltrue([
      for fleet in var.fleets : try(
        tonumber(regex("^(0*[1-9][0-9]*)(B|KiB|MiB|GiB|TiB)$", fleet.memory_per_instance)[0]) *
        { B = 1, KiB = 1024, MiB = 1048576, GiB = 1073741824, TiB = 1099511627776 }[regex("^(0*[1-9][0-9]*)(B|KiB|MiB|GiB|TiB)$", fleet.memory_per_instance)[1]] <= 18446744073709551615,
        false
      )
    ])
    error_message = "Each fleet memory_per_instance must be a non-zero size in B, KiB, MiB, GiB or TiB that fits in 64 bits."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.max_instances >= 1 && fleet.max_instances <= 4294967295 && floor(fleet.max_instances) == fleet.max_instances])
    error_message = "Each fleet max_instances must be a whole number from 1 to 4294967295."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.min_instances >= 0 && fleet.min_instances <= fleet.max_instances && floor(fleet.min_instances) == fleet.min_instances])
    error_message = "Each fleet min_instances must be a whole number from 0 to its max_instances."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.vcpus_per_instance >= 1 && fleet.vcpus_per_instance <= 4294967295 && floor(fleet.vcpus_per_instance) == fleet.vcpus_per_instance])
    error_message = "Each fleet vcpus_per_instance must be a whole number from 1 to 4294967295."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.target_actions_per_instance >= 1 && fleet.target_actions_per_instance <= 4294967295 && floor(fleet.target_actions_per_instance) == fleet.target_actions_per_instance])
    error_message = "Each fleet target_actions_per_instance must be a whole number from 1 to 4294967295."
  }
}

variable "default_isa" {
  type        = string
  description = "ISA used when a request does not name one, written to gang.toml as the top-level default_isa. Some fleet must have this isa (enforced by a precondition in main.tf)."
}

variable "trust_domain" {
  type        = string
  description = "Trust domain of the identity certificates issued by gang, written to [server].trust_domain."
}

variable "workload_token_audience" {
  type        = string
  description = "Audience of the STS web identity tokens that workers present. Must equal the audience of the aws-workers OIDC provider."
  default     = "gang"
}

variable "scratch_mount_point" {
  type        = string
  description = "Where workers mount the XFS scratch volume (instance-store NVMe RAID0 or /dev/sdf)."
  default     = "/mnt/nvme"
}

variable "gang_settings" {
  type = object({
    poll_interval           = optional(string, "1s")
    client_action_timeout   = optional(string, "2m")
    input_links             = optional(string, "break")
    permit_network_property = optional(bool, false)
  })
  nullable    = false
  default     = {}
  description = <<-EOT
    Gang settings owned by the generated tables. poll_interval controls autoscaler reconciliation;
    client_action_timeout bounds client action waits. Both are required by gang, so the deployment
    supplies 1s and 2m. input_links defaults to break rather than gang's keep to protect the CAS.
    permit_network_property controls action-requested network access. Gang requires it; the
    deployment defaults to false to disable action-requested network access.
    Each value must satisfy gang's own rule; the validations repeat it for plan-time feedback.
  EOT

  validation {
    condition     = contains(["keep", "break"], var.gang_settings.input_links)
    error_message = "gang_settings.input_links must be keep or break."
  }

  validation {
    condition = try(
      tonumber(regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", var.gang_settings.poll_interval)[0]) *
      { ms = 1, s = 1000, m = 60000, h = 3600000, d = 86400000 }[regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", var.gang_settings.poll_interval)[1]] <= 18446744073709551615,
      false
    )
    error_message = "gang_settings.poll_interval must be a non-zero duration in ms, s, m, h or d, such as 1s, of at most 18446744073709551615ms."
  }

  validation {
    condition = try(
      (tonumber(regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", var.gang_settings.client_action_timeout)[0]) *
      { ms = 1, s = 1000, m = 60000, h = 3600000, d = 86400000 }[regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", var.gang_settings.client_action_timeout)[1]]) % 1000 == 0 &&
      tonumber(regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", var.gang_settings.client_action_timeout)[0]) *
      { ms = 1, s = 1000, m = 60000, h = 3600000, d = 86400000 }[regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", var.gang_settings.client_action_timeout)[1]] <= 18446744073000,
      false
    )
    error_message = "gang_settings.client_action_timeout must be a whole number of seconds in ms, s, m, h or d, such as 2m, from 1s to 18446744073s."
  }
}

variable "extra_config_toml" {
  type        = string
  description = <<-EOT
    Raw TOML appended verbatim to the rendered gang.toml, after the last generated section
    ([identity.providers.aws-workers.oidc]). It is spliced in unescaped, unlike the generated values, because it is
    TOML tables, not a string: JSON-encoding it would quote it. It must consist of complete [table] sections (developer
    and CI identity providers, tunables), never bare top-level keys: a key there would join the last table. TOML tables
    already opened by this module cannot be reopened here, even for settings omitted from the template such as
    [database.sqlite].busy_timeout and [authentication.workers.mtls].certificate_ttl. Its first non-blank,
    non-comment line must start with "[". Content is public: it lands in the SSM parameter (gang.toml), not
    in EC2 user data. Put confidential values in controller_secret_parameters, never here, and reference them by their
    absolute path under /etc/gang/secrets.
  EOT
  default     = ""

  validation {
    condition     = startswith(element(concat([for l in split("\n", var.extra_config_toml) : trimspace(l) if trimspace(l) != "" && !startswith(trimspace(l), "#")], ["["]), 0), "[")
    error_message = "extra_config_toml must consist of [table] sections: its first non-blank, non-comment line must start with \"[\"."
  }
}

variable "config_parameter_name" {
  type        = string
  description = "Name of the SSM parameter that holds the rendered gang.toml. Null uses /<name>/gang.toml. The name is plain-text EC2 user data and instances read the parameter at boot. The content lives in the parameter, so changing the configuration replaces no instance."
  default     = null
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to every resource through the provider default_tags and to the modules."
  default     = {}
}

variable "client_token_providers" {
  type        = list(string)
  description = "Identity providers that may obtain client tokens, written to [authentication.clients.token] providers. Each name must have an [identity.providers.<name>] section, either generated by this module (aws-workers) or appended through extra_config_toml."

  validation {
    condition     = length(var.client_token_providers) >= 1
    error_message = "client_token_providers needs at least one provider."
  }
}

variable "fleet_tag_key" {
  type        = string
  description = "Instance and Auto Scaling group tag key that carries the fleet name. Written to [provisioner.aws] fleet_tag_key. The rule is owned by the gang-aws plugin; this validation repeats it for plan-time feedback."
  default     = "gang-fleet"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.:=+@,-]{1,128}$", var.fleet_tag_key)) && !contains([".", "..", "_index"], var.fleet_tag_key) && !startswith(lower(var.fleet_tag_key), "aws:")
    error_message = "fleet_tag_key must be 1 to 128 characters of letters, digits and _ . : = + @ , -, must not be ., .. or _index, and must not start with aws:."
  }
}

variable "controller_secret_parameters" {
  type        = map(string)
  description = "Confidential values the controller reads at boot. Keys are file names under /etc/gang/secrets on the controller; values are the names of SSM SecureString parameters created outside this configuration, so the values never enter Terraform state. Reference a file from extra_config_toml by its absolute path. Workers receive none. Validated by the controller module."
  default     = {}
}

variable "controller_secret_parameters_kms_key_arn" {
  type        = string
  description = "ARN of the KMS key that encrypts the controller_secret_parameters SecureStrings. The controller role is granted decrypt on it. Null means the AWS-managed SSM key, which needs no grant. Validated by the controller module."
  default     = null
}

variable "manage_outbound_web_identity_federation" {
  type        = bool
  description = "Whether this deployment enables outbound web identity federation for the account. False reads the account's already-enabled federation instead of creating it, so a second deployment in the same account does not conflict."
  default     = true
}
