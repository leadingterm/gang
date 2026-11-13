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
  nullable    = false
}

variable "controller_private_ip" {
  description = "Private IPv4 address of the controller."
  type        = string
  nullable    = false
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

variable "tags" {
  description = "Tags merged into every taggable resource of the module."
  type        = map(string)
  default     = {}
  nullable    = false
}
