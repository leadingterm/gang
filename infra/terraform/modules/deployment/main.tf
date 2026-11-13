resource "terraform_data" "input_checks" {
  lifecycle {
    precondition {
      condition     = contains([for fleet in var.fleets : fleet.isa], var.default_isa)
      error_message = "default_isa must be the isa of some fleet."
    }
  }
}

module "network" {
  source = "../network"

  name       = var.name
  cidr_block = var.cidr_block
  nat_mode   = var.nat_mode

  # With a load balancer, clients reach it instead of the controller.
  client_ingress_cidrs    = var.load_balancer == null ? var.client_ingress_cidrs : []
  availability_zone_count = var.network.availability_zone_count
  tags                    = var.tags
}

module "storage" {
  source = "../storage"

  name                        = var.name
  cache_bucket_name           = var.cache_bucket_name
  cache_expiration_days       = var.storage.cache_expiration_days
  ca_key_deletion_window_days = var.storage.ca_key_deletion_window_days
  tags                        = var.tags
  ca_signer_role_arn          = module.controller.role_arn
}

locals {
  public_port = module.network.public_port
  worker_port = module.network.worker_port
  public_host = coalesce(var.hostname, module.network.controller_public_dns)
  worker_host = module.network.controller_private_dns

  endpoint = local.public_port == 443 ? "https://${local.public_host}" : "https://${local.public_host}:${local.public_port}"

  worker_endpoint = "grpcs://${local.worker_host}:${local.worker_port}"

  gang_toml = templatefile("${path.module}/gang.toml.tftpl", {
    default_isa             = var.default_isa
    gang_settings           = var.gang_settings
    fleets                  = var.fleets
    region                  = var.region
    public_port             = local.public_port
    worker_port             = local.worker_port
    endpoint                = local.endpoint
    endpoint_trust          = var.load_balancer == null ? null : "system"
    worker_endpoint         = local.worker_endpoint
    scratch_mount_point     = var.scratch_mount_point
    workload_token_audience = var.workload_token_audience
    client_token_providers  = var.client_token_providers
    fleet_tag_key           = var.fleet_tag_key
    cache_bucket_name       = module.storage.cache_bucket_name
    ca_key_alias            = module.storage.ca_key_alias
    trust_domain            = var.trust_domain
    issuer_url              = module.worker_fleet.issuer_url
    worker_role_arn         = module.worker_fleet.role_arn
    extra_config_toml       = var.extra_config_toml
  })
}

resource "aws_ssm_parameter" "config" {
  name        = coalesce(var.config_parameter_name, "/${var.name}/gang.toml")
  description = "Rendered gang.toml read by gang-config.service at boot. Contains no secrets: confidential values live in the SecureString parameters named by controller_secret_parameters and are referenced by file path."
  type        = "String"
  tier        = "Intelligent-Tiering"
  value       = local.gang_toml
  tags        = merge(var.tags, { Name = "${var.name}-config" })
}

module "controller" {
  source = "../controller"

  name                          = "${var.name}-controller"
  ami_id                        = var.controller_ami_id
  instance_type                 = var.controller_instance_type
  root_volume_gib               = var.controller_root_volume_gib
  state_volume_gib              = var.controller_state_volume_gib
  subnet_id                     = module.network.public_subnet_ids[0]
  private_ip                    = module.network.controller_private_ip
  security_group_id             = module.network.controller_security_group_id
  eip_allocation_id             = module.network.controller_eip_allocation_id
  config_parameter_name         = aws_ssm_parameter.config.name
  config_parameter_arn          = aws_ssm_parameter.config.arn
  cache_bucket_arn              = module.storage.cache_bucket_arn
  ca_key_arn                    = module.storage.ca_key_arn
  worker_fleet_names            = module.worker_fleet.asg_names
  secret_parameters             = var.controller_secret_parameters
  secret_parameters_kms_key_arn = var.controller_secret_parameters_kms_key_arn
  tags                          = var.tags
  iam_name_prefix               = var.iam_name_prefix
  iam_permissions_boundary_arn  = var.iam_permissions_boundary_arn
}

module "load_balancer" {
  count  = var.load_balancer == null ? 0 : 1
  source = "../load-balancer"

  name                           = var.name
  vpc_id                         = module.network.vpc_id
  subnet_id                      = module.network.public_subnet_ids[0]
  hostname                       = var.hostname
  port                           = local.public_port
  client_ingress_cidrs           = var.client_ingress_cidrs
  controller_security_group_id   = module.network.controller_security_group_id
  controller_instance_id         = module.controller.instance_id
  controller_private_ip          = module.network.controller_private_ip
  ssl_policy                     = var.load_balancer.ssl_policy
  certificate_validation_timeout = var.load_balancer.certificate_validation_timeout
  tags                           = var.tags
}

module "worker_fleet" {
  source = "../worker-fleet"

  name_prefix                             = "${var.name}-worker"
  subnet_ids                              = module.network.private_subnet_ids
  security_group_id                       = module.network.worker_security_group_id
  cache_bucket_arn                        = module.storage.cache_bucket_arn
  ca_key_arn                              = module.storage.ca_key_arn
  workload_token_audience                 = var.workload_token_audience
  fleet_tag_key                           = var.fleet_tag_key
  config_parameter_name                   = aws_ssm_parameter.config.name
  config_parameter_arn                    = aws_ssm_parameter.config.arn
  tags                                    = var.tags
  iam_name_prefix                         = var.iam_name_prefix
  iam_permissions_boundary_arn            = var.iam_permissions_boundary_arn
  manage_outbound_web_identity_federation = var.manage_outbound_web_identity_federation

  fleets = [
    for fleet in var.fleets : {
      name               = fleet.name
      capacity           = fleet.capacity
      ami_id             = fleet.ami_id
      instance_types     = fleet.instance_types
      max_instances      = fleet.max_instances
      root_volume_gib    = fleet.root_volume_gib
      scratch_volume_gib = fleet.scratch_volume_gib
      warm_pool_size     = fleet.warm_pool_size
      fallback           = fleet.fallback == null ? null : {}
    }
  ]
}
