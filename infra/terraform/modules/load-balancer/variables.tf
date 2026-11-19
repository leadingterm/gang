variable "name" {
  description = "Name of the load balancer, its target group and its security group, and the Name tag prefix."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$", var.name))
    error_message = "name must be 1 to 32 lowercase letters, digits and inner hyphens."
  }
}

variable "vpc_id" {
  description = "VPC of the controller."
  type        = string
  nullable    = false
}

variable "subnet_id" {
  description = "Public subnet the load balancer is placed in: the controller's, since the single controller lives in one availability zone."
  type        = string
  nullable    = false
}

variable "hostname" {
  description = "Public hostname of the endpoint: the certificate's domain, and the name of the private hosted zone that resolves it to the controller inside the VPC."
  type        = string
  nullable    = false
}

variable "port" {
  description = "TCP port of the controller's public listener, which the load balancer forwards to."
  type        = number
  nullable    = false
}

variable "client_ingress_cidrs" {
  description = "IPv4 CIDR blocks allowed to reach the load balancer on 443."
  type        = list(string)
  nullable    = false

  validation {
    condition     = alltrue([for cidr in var.client_ingress_cidrs : can(cidrnetmask(cidr)) && !strcontains(cidr, ":")])
    error_message = "client_ingress_cidrs must contain only IPv4 CIDR blocks."
  }
}

variable "controller_security_group_id" {
  description = "Security group ID of the controller."
  type        = string
  nullable    = false
}

variable "controller_instance_id" {
  description = "Instance ID of the controller."
  type        = string
  default     = null

  validation {
    condition     = var.shared_database || (var.controller_instance_id != null && var.controller_instance_id != "")
    error_message = "controller_instance_id is required when shared_database is disabled."
  }
}

variable "controller_private_ip" {
  description = "Private IPv4 address of the controller."
  type        = string
  default     = null

  validation {
    condition     = var.shared_database || (var.controller_private_ip != null && var.controller_private_ip != "")
    error_message = "controller_private_ip is required when shared_database is disabled."
  }
}

variable "ssl_policy" {
  description = "TLS security policy of the listener. Must be an ELBSecurityPolicy-TLS13-* policy: under any other policy the load balancer reaches its targets with TLS 1.2 at most, which Gang's TLS 1.3-only listener refuses."
  type        = string
  nullable    = false

  validation {
    condition     = startswith(var.ssl_policy, "ELBSecurityPolicy-TLS13-")
    error_message = "ssl_policy must be an ELBSecurityPolicy-TLS13-* policy: Gang's listener accepts TLS 1.3 only."
  }
}

variable "certificate_validation_timeout" {
  description = "How long apply waits for the certificate's DNS validation record to be published, as an OpenTofu duration such as 2h."
  type        = string
  nullable    = false
}

variable "shared_database" {
  description = "Enable the shared-database controller group and internal load balancer shape."
  type        = bool
  default     = false
  nullable    = false
}

variable "public_subnet_ids" {
  description = "Public subnets for the multi-controller public load balancer."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = !var.shared_database || length(var.public_subnet_ids) > 0
    error_message = "public_subnet_ids must not be empty when shared_database is enabled."
  }
}

variable "private_subnet_ids" {
  description = "Private subnets for the internal load balancer."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = !var.shared_database || length(var.private_subnet_ids) > 0
    error_message = "private_subnet_ids must not be empty when shared_database is enabled."
  }
}

variable "worker_security_group_id" {
  description = "Worker security group allowed to reach the internal load balancer."
  type        = string
  default     = null

  validation {
    condition     = !var.shared_database || (var.worker_security_group_id != null && var.worker_security_group_id != "")
    error_message = "worker_security_group_id is required when shared_database is enabled."
  }
}

variable "private_zone_id" {
  description = "Existing private hosted zone ID, authoritative for hostname or a parent domain; null creates the hostname's private zone."
  type        = string
  default     = null
}

variable "health_port" {
  description = "Controller plaintext HTTP health listener port (`health_listen`): every target group checks /serve or /leader on it."
  type        = number
  nullable    = false

  validation {
    condition     = var.health_port >= 1 && var.health_port <= 65535 && floor(var.health_port) == var.health_port
    error_message = "health_port must be an integer TCP port from 1 to 65535."
  }
}

variable "workers_port" {
  description = "Controller worker port: the internal worker listener's port and its target port."
  type        = number
  nullable    = false

  validation {
    condition     = var.workers_port >= 1 && var.workers_port <= 65535 && floor(var.workers_port) == var.workers_port
    error_message = "workers_port must be an integer TCP port from 1 to 65535."
  }
}

variable "shutdown_grace" {
  description = "Controller shutdown grace period in seconds."
  type        = number
  default     = 300
  nullable    = false

  validation {
    condition     = var.shutdown_grace >= 0 && floor(var.shutdown_grace) == var.shutdown_grace
    error_message = "shutdown_grace must be a nonnegative integer number of seconds."
  }
}

variable "deregistration_delay" {
  description = "Target deregistration delay in seconds; must cover shutdown_grace."
  type        = number
  default     = 300
  nullable    = false

  validation {
    condition     = var.deregistration_delay >= var.shutdown_grace && var.deregistration_delay <= 3600 && floor(var.deregistration_delay) == var.deregistration_delay
    error_message = "deregistration_delay must be an integer between shutdown_grace and 3600 seconds."
  }
}

variable "tags" {
  description = "Tags merged into every taggable resource of the module."
  type        = map(string)
  default     = {}
  nullable    = false
}
