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

variable "aws_events_enabled" {
  type        = bool
  description = "Enable AWS event observation. Set only after all controller and worker images support events_queue_url."
  default     = false
  nullable    = false
}

variable "aws_events_retention_seconds" {
  type        = number
  description = "Retention in seconds for the AWS events queue."
  default     = 86400
  nullable    = false

  validation {
    condition     = var.aws_events_retention_seconds >= 60 && var.aws_events_retention_seconds <= 1209600 && floor(var.aws_events_retention_seconds) == var.aws_events_retention_seconds
    error_message = "aws_events_retention_seconds must be an integer from 60 to 1209600."
  }
}

variable "aws_events_dead_letter_retention_seconds" {
  type        = number
  description = "Retention in seconds for the AWS events dead-letter queue; at least the main queue retention."
  default     = 345600
  nullable    = false

  validation {
    condition     = var.aws_events_dead_letter_retention_seconds >= 60 && var.aws_events_dead_letter_retention_seconds <= 1209600 && floor(var.aws_events_dead_letter_retention_seconds) == var.aws_events_dead_letter_retention_seconds && var.aws_events_dead_letter_retention_seconds >= var.aws_events_retention_seconds
    error_message = "aws_events_dead_letter_retention_seconds must be an integer from 60 to 1209600 and at least aws_events_retention_seconds."
  }
}

variable "aws_events_max_receive_count" {
  type        = number
  description = "Number of unsuccessful receives before an AWS event is moved to the dead-letter queue."
  default     = 5
  nullable    = false

  validation {
    condition     = var.aws_events_max_receive_count >= 1 && var.aws_events_max_receive_count <= 1000 && floor(var.aws_events_max_receive_count) == var.aws_events_max_receive_count
    error_message = "aws_events_max_receive_count must be an integer from 1 to 1000."
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
variable "database" {
  type = object({
    engine = optional(string, "sqlite")
    postgres = optional(object({
      rds = optional(object({
        instance_class   = optional(string, "db.t4g.micro")
        multi_az         = optional(bool, true)
        storage_gib      = optional(number, 20)
        engine_version   = optional(string, "17")
        password_version = optional(number, 1)
      }))
      external = optional(object({
        host               = string
        port               = optional(number, 5432)
        database           = string
        user               = string
        password_parameter = string
        ca_parameter       = optional(string)
      }))
    }))
  })
  description = "Database engine: sqlite for one controller, or postgres or dynamodb for one or more controllers behind load balancers. postgres.rds creates an RDS PostgreSQL instance verified against the Amazon RDS CA bundle in infra/nixos/rds-ca-bundle.pem; increment postgres.rds.password_version to rotate its password. Refresh the bundle from https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem when AWS rotates its CAs. postgres.external connects to an existing server using an SSM password parameter and optional CA bundle parameter; without a CA parameter the system trust store is used."
  default     = {}
  nullable    = false

  validation {
    condition     = contains(["sqlite", "postgres", "dynamodb"], var.database.engine)
    error_message = "database.engine must be sqlite, postgres, or dynamodb."
  }

  validation {
    condition     = var.database.engine != "postgres" || ((try(var.database.postgres.rds, null) != null) != (try(var.database.postgres.external, null) != null))
    error_message = "postgres requires exactly one of database.postgres.rds or database.postgres.external."
  }

  validation {
    condition     = var.database.engine == "postgres" || var.database.postgres == null
    error_message = "database.postgres may only be set when database.engine is postgres."
  }

  validation {
    condition     = try(var.database.postgres.rds, null) == null ? true : coalesce(var.network.availability_zone_count, 3) >= 2
    error_message = "postgres.rds requires at least two availability zones."
  }

  validation {
    condition     = try(var.database.postgres.rds, null) == null ? true : var.database.postgres.rds.password_version >= 1 && floor(var.database.postgres.rds.password_version) == var.database.postgres.rds.password_version
    error_message = "database.postgres.rds.password_version must be a whole number of at least 1."
  }

  validation {
    condition = try(var.database.postgres.external, null) == null ? true : (
      try(trimspace(var.database.postgres.external.host) != "", false) &&
      try(trimspace(var.database.postgres.external.database) != "", false) &&
      try(trimspace(var.database.postgres.external.user) != "", false) &&
      try(trimspace(var.database.postgres.external.password_parameter) != "", false) &&
      var.database.postgres.external.port >= 1 &&
      var.database.postgres.external.port <= 65535 &&
      floor(var.database.postgres.external.port) == var.database.postgres.external.port
    )
    error_message = "database.postgres.external requires non-empty host, database, user and password_parameter, and a whole-number port from 1 to 65535."
  }


  validation {
    condition     = var.database.engine == "sqlite" || var.load_balancer != null
    error_message = "postgres and dynamodb require load_balancer."
  }

  validation {
    condition     = var.database.engine == "sqlite" || !can(regex("(?m)^\\s*\\[cache\\.filesystem\\]", var.extra_config_toml))
    error_message = "postgres and dynamodb require the shared S3 cache, not a filesystem cache."
  }
}

variable "controller_count" {
  type        = number
  description = "Number of controllers. Must be one because ECS health checks route only to the leader."
  default     = 1
  nullable    = false

  validation {
    condition     = var.controller_count == 1
    error_message = "controller_count must be 1."
  }
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
  description = "Public TLS load balancer using an ACM certificate for hostname; required for shared database engines (postgres and dynamodb). SQLite keeps its controller Elastic IP and private DNS. Shared database engines use public and internal load balancers with PROXY v2, with the hostname resolving privately to the internal load balancer. ssl_policy must be an ELBSecurityPolicy-TLS13-* policy. The first apply waits up to certificate_validation_timeout for DNS validation."
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

variable "image_publisher" {
  type        = string
  description = "AWS account that publishes Gang's public AMIs, a fact of Gang's distribution. A fork that publishes its own images sets its account."
  default     = "072320588884"

  validation {
    condition     = can(regex("^[0-9]{12}$", var.image_publisher))
    error_message = "image_publisher must be a 12-digit AWS account ID."
  }
}

variable "release_repository" {
  type        = string
  description = "GitHub repository whose release of this module's version lists each image's SHA-256 in SHA256SUMS. A fork sets its own."
  default     = "leadingterm/gang"

  validation {
    condition     = can(regex("^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$", var.release_repository))
    error_message = "release_repository must be a GitHub owner/repository."
  }
}

variable "images" {
  type        = map(object({ owner = string, sha256 = string }))
  description = "Image per architecture: the AMI named gang-<architecture>-<first 16 hex digits of sha256> that owner (an account id, or self for an imported image) registered. The selected AMI must carry its own ec2:SharedTag/ActionEnvironment rootfs digest. An architecture not set here uses the public image of this module's release from image_publisher."
  default     = {}

  validation {
    condition     = alltrue([for architecture, image in var.images : contains(["x86_64", "aarch64"], architecture) && can(regex("^(self|[0-9]{12})$", image.owner)) && can(regex("^[0-9a-f]{64}$", image.sha256))])
    error_message = "Each images key must be x86_64 or aarch64, with owner self or a 12-digit account ID and sha256 of 64 lowercase hex digits."
  }
}

variable "controller_instance_type" {
  type        = string
  description = "EC2 instance type of the SQLite controller or ECS capacity host."
  default     = "t4g.large"
}

variable "controller_image_digest" {
  type        = string
  description = "SHA-256 digest of the controller image in the deployment's private ECR repository, required for a shared database."
  default     = null

  validation {
    condition     = !contains(["postgres", "dynamodb"], var.database.engine) || can(regex("^sha256:[0-9a-f]{64}$", var.controller_image_digest))
    error_message = "controller_image_digest must be a sha256 digest for a shared database."
  }
}

variable "controller_stop_timeout_seconds" {
  type        = number
  description = "Graceful controller drain timeout in seconds; also configures the ECS host stop timeout for a service."
  default     = 2460
  nullable    = false

  validation {
    condition     = var.controller_stop_timeout_seconds > 0 && floor(var.controller_stop_timeout_seconds) == var.controller_stop_timeout_seconds
    error_message = "controller_stop_timeout_seconds must be a positive whole number."
  }
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
    name    = string
    isa     = string
    sandbox = optional(string, "bwrap")
    actions = optional(object({
      grants = optional(set(string), [])
      resources = optional(object({
        cpu_count = optional(number)
        memory    = optional(string)
        disk      = optional(string)
      }), {})
    }), {})
    capacity                   = string
    machine_class              = optional(string, null)
    image                      = optional(object({ owner = string, sha256 = string }), null)
    instance_types             = list(string)
    minimum_availability_zones = optional(number, 2)
    spot_allocation_strategy   = optional(string, "price-capacity-optimized")
    max_instances              = number
    min_instances              = optional(number, 0)
    vcpus_per_instance         = number
    disk_per_instance          = string
    idle_timeout               = optional(string, "10m")
    max_drain_wait             = optional(string, "25m")
    boot_grace                 = optional(string, "10m")
    scratch_volume_gib         = optional(number, null)
    root_volume_gib            = optional(number)
    warm_pool_size             = optional(number, 0)
    fallback                   = optional(object({ retry_spot_after = optional(string, "5m"), drain_on_demand_first = optional(bool, true) }), null)
    scheduled_minimum          = optional(list(object({ instances = number, days = list(string), start = string, end = string, time_zone = string })), [])
  }))
  description = <<-EOT
    Worker fleets in preference order: the scheduler tries them in list order. Each fleet is one Auto Scaling group; a fleet with `fallback` also has an on-demand twin group `<name>-on-demand`.
    name: Auto Scaling group name, 1 to 40 lowercase letters, digits or hyphens, starting and ending with a letter or digit, unique across fleets. Written to gang.toml as [[fleets]].name.
    isa: instruction set of the fleet, x86-64 or aarch64. Written to gang.toml as [[fleets]].isa.
    sandbox: sandbox the workers run actions in. Only bwrap is supported. Written to gang.toml as [[fleets]].sandbox.
    capacity: spot or on-demand. Selects the Auto Scaling group purchase option and is written to [fleets.options].capacity.
    machine_class: optional qualitative machine class (for example gpu), written to [fleets.options].machine_class when set.
    image: optional image of this fleet (for example a GPU image), {owner, sha256} as in images; null uses images[<the isa's architecture>]. The selected AMI supplies this fleet's action environment.
    instance_types: EC2 instance types. Order is the on-demand launch priority (the first type is tried first); capacity-optimized-prioritized Spot uses it on a best-effort basis after capacity considerations. Every type needs Nitro instance-store SSD (NVMe) unless scratch_volume_gib is set. The fleet's groups span only private subnets in availability zones offering every configured type.
    minimum_availability_zones: minimum number of common availability zones required for the fleet, a positive integer defaulting to 2. The plan fails if too few allowed zones offer every type. Not written to gang.toml.
    spot_allocation_strategy: Spot allocation strategy, default price-capacity-optimized. Set to capacity-optimized-prioritized to prefer instance type order after capacity considerations.
    max_instances: upper bound of the Auto Scaling group.
    min_instances, vcpus_per_instance, idle_timeout,
    max_drain_wait, boot_grace: scheduling policy written to gang.toml.
    Fleet memory_per_instance in gang.toml is a prelaunch estimate derived from the smallest nominal EC2 memory size among the configured instance types. Live workers register their actual available memory instead.
    disk_per_instance: non-zero disk size in B, KiB, MiB, GiB or TiB; lower bound of the disk the fleet's actions may fill in total, valid for every instance type of the fleet (scratch capacity minus the fast CAS budget minus an overshoot reserve); written to gang.toml.
    scratch_volume_gib: EBS scratch volume on /dev/sdf for EBS-only instance types. Null uses instance-store NVMe.
    root_volume_gib: size of the encrypted gp3 root volume in GiB. Unset uses the worker-fleet module's default.
    warm_pool_size: stopped standby instances kept warm on the fleet's on-demand group: the fleet's own group for on-demand fleets, the <name>-on-demand twin for fallback fleets. 0 keeps no pool. Above 0 the fleet needs an on-demand group (capacity on-demand, or fallback). Not written to gang.toml.
    fallback: opts a spot fleet in to on-demand fallback. The fleet keeps its spot group and gains the on-demand twin <name>-on-demand, which takes capacity while Spot is unavailable. Presence opts in; null keeps Spot only. Requires capacity spot. Written to gang.toml as [fleets.options.fallback]: retry_spot_after (default 5m) is how long capacity stays on the twin after a failed Spot launch before Spot is tried again; drain_on_demand_first (default true) releases on-demand instances before Spot ones when the fleet has surplus.
    scheduled_minimum: weekly windows during which the fleet keeps at least instances instances (days of mon..sun, start and end as HH:MM local time from 00:00 to 24:00 with start before end, an IANA time_zone). Written to gang.toml as [[fleets.scheduled_minimum]]; the controller rejects an unknown time zone when it starts.
  EOT

  validation {
    condition     = alltrue(flatten([for fleet in var.fleets : [for grant in fleet.actions.grants : grant == "network"]]))
    error_message = "Fleet action grants may contain only network."
  }

  validation {
    condition = alltrue([for fleet in var.fleets :
      (fleet.actions.resources.cpu_count == null ? true : fleet.actions.resources.cpu_count > 0 && floor(fleet.actions.resources.cpu_count) == fleet.actions.resources.cpu_count && fleet.actions.resources.cpu_count <= 4294967295) &&
      alltrue([for size in [fleet.actions.resources.memory, fleet.actions.resources.disk] : size == null ? true : can(regex("^[1-9][0-9]*(B|KiB|MiB|GiB|TiB)$", size))])
    ])
    error_message = "Action CPU ceilings must be positive whole u32 values; memory and disk ceilings must be nonzero byte sizes."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : can(regex("^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$", fleet.name))])
    error_message = "Each fleet name must be 1 to 40 lowercase letters, digits or hyphens, and start and end with a letter or digit."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.name != "controller"])
    error_message = "No fleet may be named controller."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.image == null ? true : can(regex("^(self|[0-9]{12})$", fleet.image.owner)) && can(regex("^[0-9a-f]{64}$", fleet.image.sha256))])
    error_message = "Each fleet image owner must be self or a 12-digit account ID and sha256 must be 64 lowercase hex digits."
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
    condition     = alltrue([for fleet in var.fleets : fleet.max_instances >= 1 && fleet.max_instances <= 4294967295 && floor(fleet.max_instances) == fleet.max_instances])
    error_message = "Each fleet max_instances must be a whole number from 1 to 4294967295."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.minimum_availability_zones > 0 && floor(fleet.minimum_availability_zones) == fleet.minimum_availability_zones])
    error_message = "Each fleet minimum_availability_zones must be a positive whole number."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.min_instances >= 0 && fleet.min_instances <= fleet.max_instances && floor(fleet.min_instances) == fleet.min_instances])
    error_message = "Each fleet min_instances must be a whole number from 0 to its max_instances."
  }

  validation {
    condition = alltrue(flatten([
      for fleet in var.fleets : [
        for window in fleet.scheduled_minimum : floor(window.instances) == window.instances && window.instances > fleet.min_instances && window.instances <= fleet.max_instances
      ]
    ]))
    error_message = "Each scheduled minimum instances must be a whole number above min_instances and at most max_instances."
  }

  validation {
    condition = alltrue(flatten([
      for fleet in var.fleets : [
        for window in fleet.scheduled_minimum : length(window.days) > 0 && length(distinct(window.days)) == length(window.days) && alltrue([for day in window.days : contains(["mon", "tue", "wed", "thu", "fri", "sat", "sun"], day)])
      ]
    ]))
    error_message = "Each scheduled minimum days list must be non-empty, distinct, and contain only mon through sun."
  }

  validation {
    condition = alltrue(flatten([
      for fleet in var.fleets : [
        for window in fleet.scheduled_minimum : can(regex("^([01][0-9]|2[0-3]):[0-5][0-9]$", window.start)) && can(regex("^(([01][0-9]|2[0-3]):[0-5][0-9]|24:00)$", window.end)) && try(tonumber(replace(window.start, ":", "")) < tonumber(replace(window.end, ":", "")), false)
      ]
    ]))
    error_message = "Each scheduled minimum start and end must be HH:MM from 00:00 to 24:00, with start before end."
  }

  validation {
    condition     = alltrue([for fleet in var.fleets : fleet.vcpus_per_instance >= 1 && fleet.vcpus_per_instance <= 4294967295 && floor(fleet.vcpus_per_instance) == fleet.vcpus_per_instance])
    error_message = "Each fleet vcpus_per_instance must be a whole number from 1 to 4294967295."
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
    poll_interval         = optional(string, "2s")
    client_action_timeout = optional(string, "2m")
    input_links           = optional(string, "break")
  })
  nullable    = false
  default     = {}
  description = <<-EOT
    Gang settings owned by the generated tables. poll_interval controls reconciliation and dashboard
    observation cadence; client_action_timeout bounds client action waits. The deployment supplies
    2s and 2m. input_links defaults to break rather than gang's keep to protect the CAS.
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

variable "execution_egress" {
  type = object({
    protected_networks = optional(list(string), [])
    dns_servers        = list(string)
    setup_timeout      = optional(string, "10s")
  })
  default     = null
  description = "Additional host and network CIDRs to protect (default none), public DNS server IP literals, and setup timeout. Required when a fleet grants network. The module always derives protection for the VPC CIDRs, controller public IP when present, and public load-balancer static IPs."

  validation {
    condition = var.execution_egress == null ? true : (
      alltrue([for cidr in var.execution_egress.protected_networks : can(cidrhost(cidr, 0))]) &&
      length(var.execution_egress.dns_servers) > 0 &&
      alltrue([for ip in var.execution_egress.dns_servers : can(cidrhost("${ip}/${strcontains(ip, ":") ? 128 : 32}", 0))]) &&
      can(regex("^0*[1-9][0-9]*(ms|s|m|h|d)$", var.execution_egress.setup_timeout))
    )
    error_message = "execution_egress requires valid CIDRs, nonempty DNS IP literals and a nonzero setup duration."
  }
}

variable "http2_max_connections" {
  type        = number
  nullable    = false
  default     = 4096
  description = "Maximum concurrent public listener connections (default 4096)."

  validation {
    condition     = var.http2_max_connections > 0 && var.http2_max_connections == floor(var.http2_max_connections) && var.http2_max_connections <= 9223372036854775807
    error_message = "http2_max_connections must be a positive integer no greater than 9223372036854775807."
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
    [database.sqlite].busy_timeout and [authentication.workers.mtls].certificate_ttl. The module always opens
    [server.http2] and owns max_connections. Unexposed settings such as receive_window retain their runtime defaults.
    For load-balanced shared databases, the
    module also opens [server.proxy_protocol], so it cannot be reopened here; header_timeout, max_pending_headers, and
    max_header_size are not exposed and retain their runtime defaults. Its first non-blank, non-comment line must start
    with "[". Content is public: it lands in the SSM parameter (gang.toml), not
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
  description = "SSM configuration generation namespace. Null uses /<name>/gang.toml. Instances read an immutable generation beneath this name. The partition and region determine the available ARN length budget."
  default     = null

  validation {
    condition     = var.config_parameter_name == null ? true : length(var.config_parameter_name) <= 962 && length(split("/", trimprefix(var.config_parameter_name, "/"))) <= 13 && can(regex("^/([A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+$", var.config_parameter_name)) && !can(regex("(?i)^/(aws|ssm)", var.config_parameter_name))
    error_message = "config_parameter_name must be an absolute SSM path of at most 962 characters and 13 segments, without an aws or ssm prefix. The partition and region may impose a smaller generation ARN budget."
  }
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

  validation {
    condition     = var.database.engine != "postgres" || (!contains(keys(var.controller_secret_parameters), "database-password") && !contains(keys(var.controller_secret_parameters), "database-ca.pem"))
    error_message = "controller_secret_parameters must not use database-password or database-ca.pem with postgres."
  }
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
