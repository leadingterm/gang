variable "name" {
  description = "Name prefix for network resources."
  type        = string
  default     = "gang"
  nullable    = false
}

variable "cidr_block" {
  description = "IPv4 CIDR block of the VPC. Public subnets take the first 16 /20 slots, private subnets the slots from index 8."
  type        = string
  nullable    = false

  validation {
    condition     = can(cidrnetmask(var.cidr_block)) && !strcontains(var.cidr_block, ":")
    error_message = "cidr_block must be a valid IPv4 CIDR block."
  }
}

variable "availability_zone_count" {
  description = "Number of availability zones to span, taken in name order from the standard zones of the region."
  type        = number
  default     = 3
  nullable    = false

  validation {
    condition     = var.availability_zone_count >= 1 && var.availability_zone_count <= 6
    error_message = "availability_zone_count must be between 1 and 6."
  }
}

variable "nat_mode" {
  description = "NAT gateway layout for private subnet egress: single (one NAT gateway in the first public subnet) or per_az (one NAT gateway per zone)."
  type        = string
  default     = "single"
  nullable    = false

  validation {
    condition     = contains(["single", "per_az"], var.nat_mode)
    error_message = "nat_mode must be one of: single, per_az."
  }
}

variable "client_ingress_cidrs" {
  description = "IPv4 CIDR blocks allowed to reach the controller public endpoint (443). Access is authenticated by gang itself (token or mTLS, per the authentication plugins)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
  nullable    = false

  validation {
    condition     = alltrue([for cidr in var.client_ingress_cidrs : can(cidrnetmask(cidr)) && !strcontains(cidr, ":")])
    error_message = "client_ingress_cidrs must contain only IPv4 CIDR blocks."
  }
}

variable "tags" {
  description = "Tags merged into every taggable resource of the module."
  type        = map(string)
  default     = {}
  nullable    = false
}
