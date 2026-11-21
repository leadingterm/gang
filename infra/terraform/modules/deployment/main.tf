data "aws_partition" "this" {}
data "aws_caller_identity" "this" {}

resource "terraform_data" "input_checks" {
  lifecycle {
    precondition {
      condition     = contains([for fleet in var.fleets : fleet.isa], var.default_isa)
      error_message = "default_isa must be the isa of some fleet."
    }
    precondition {
      condition = !local.group || alltrue([for fleet in var.fleets : try(
        tonumber(regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", fleet.boot_grace)[0]) *
        { ms = 1, s = 1000, m = 60000, h = 3600000, d = 86400000 }[regex("^(0*[1-9][0-9]*)(ms|s|m|h|d)$", fleet.boot_grace)[1]] > local.shutdown_grace_seconds * 1000,
        false
      )])
      error_message = "Each fleet boot_grace must be a valid duration strictly above the controller group shutdown grace."
    }
    precondition {
      # The 1011-character SSM limit includes the partition, region, and account ARN prefix.
      condition     = length("arn:${data.aws_partition.this.partition}:ssm:${var.region}:000000000000:parameter${local.config_parameter_name}/generations/00000000-0000-0000-0000-000000000000") <= 1011 && length(split("/", trimprefix(local.config_parameter_name, "/"))) <= 13 && can(regex("^/([A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+$", local.config_parameter_name)) && !can(regex("(?i)^/(aws|ssm)", local.config_parameter_name))
      error_message = "The effective config parameter name must be an absolute SSM path of at most 13 segments, without an aws or ssm prefix, and leave room for the generation ARN within the 1011-character limit for the selected partition and region."
    }
    precondition {
      condition = alltrue([for secret in concat(values(var.controller_secret_parameters), compact([
        var.database.engine == "postgres" && try(var.database.postgres.rds, null) != null ? "/${var.name}/database-password" : null,
        var.database.engine == "postgres" ? try(var.database.postgres.external.password_parameter, null) : null,
        var.database.engine == "postgres" ? try(var.database.postgres.external.ca_parameter, null) : null,
      ])) : !startswith(secret, "${local.config_parameter_name}/generations/")])
      error_message = "Controller secret parameters must not overlap the configuration generations subtree."
    }
    precondition {
      condition     = !local.network_granted || var.execution_egress != null
      error_message = "A network grant requires execution_egress."
    }
  }
}

check "multi_controller_nat" {
  assert {
    condition     = var.controller_count <= 1 || var.nat_mode != "single"
    error_message = "Multiple controllers with a single NAT gateway share an availability-zone failure point."
  }
}

module "network" {
  source = "../network"

  name       = var.name
  cidr_block = var.cidr_block
  nat_mode   = var.nat_mode

  # With a load balancer, clients reach it instead of the controller.
  client_ingress_cidrs      = var.load_balancer == null ? var.client_ingress_cidrs : []
  availability_zone_count   = var.network.availability_zone_count
  dynamodb_gateway_endpoint = var.database.engine == "dynamodb"
  controller_eip            = !local.group
  shared_database           = local.group
  tags                      = var.tags
}

module "storage" {
  source = "../storage"

  name                        = var.name
  cache_bucket_name           = var.cache_bucket_name
  cache_expiration_days       = var.storage.cache_expiration_days
  ca_key_deletion_window_days = var.storage.ca_key_deletion_window_days
  dynamodb_table              = var.database.engine == "dynamodb" ? { name = "${var.name}-state" } : null
  postgres_rds = var.database.engine == "postgres" && try(var.database.postgres.rds, null) != null ? {
    instance_class           = try(var.database.postgres.rds.instance_class, null)
    multi_az                 = try(var.database.postgres.rds.multi_az, null)
    storage_gib              = try(var.database.postgres.rds.storage_gib, null)
    engine_version           = try(var.database.postgres.rds.engine_version, null)
    password_version         = try(var.database.postgres.rds.password_version, null)
    vpc_id                   = module.network.vpc_id
    subnet_ids               = values(module.network.private_subnet_ids)
    client_security_group_id = module.network.controller_security_group_id
  } : null
  tags               = var.tags
  ca_signer_role_arn = module.controller.role_arn
}

locals {
  config_parameter_name  = coalesce(var.config_parameter_name, "/${var.name}/gang.toml")
  group                  = var.database.engine != "sqlite"
  controller_shape       = local.group ? "group" : "instance"
  public_port            = module.network.public_port
  worker_port            = module.network.worker_port
  shutdown_grace_seconds = 300
  network_granted        = anytrue([for fleet in var.fleets : contains(fleet.actions.grants, "network")])
  public_subnets         = module.network.public_subnets_by_az
  protected_networks = local.network_granted && var.execution_egress != null ? concat(
    var.execution_egress.protected_networks,
    [module.network.vpc_cidr_block],
    module.network.vpc_ipv6_cidr_blocks,
    local.group ? [] : ["${module.network.controller_public_ip}/32"],
    var.load_balancer == null ? [] : [for ip in module.load_balancer[0].public_static_ips : "${ip}/32"]
  ) : []
  public_host                       = var.hostname != null ? var.hostname : module.network.controller_public_dns
  worker_host                       = local.group ? var.hostname : module.network.controller_private_dns
  endpoint                          = local.group && var.hostname == null ? null : (local.public_port == 443 ? "https://${local.public_host}" : "https://${local.public_host}:${local.public_port}")
  worker_endpoint                   = local.group && var.hostname == null ? null : "grpcs://${local.worker_host}:${local.worker_port}"
  health                            = jsondecode(file("${path.module}/../load-balancer/health-check.json"))
  standby_routability_delay_seconds = local.group && var.load_balancer != null ? local.health.interval_seconds * local.health.threshold : 0

  database_secret_parameters = var.database.engine != "postgres" ? {} : merge(
    try(var.database.postgres.rds, null) != null ? {
      "database-password" = module.storage.postgres_password_parameter_name
      } : try(var.database.postgres.external, null) != null ? {
      "database-password" = var.database.postgres.external.password_parameter
    } : {},
    try(var.database.postgres.external.ca_parameter, null) != null && try(var.database.postgres.rds, null) == null ? {
      "database-ca.pem" = var.database.postgres.external.ca_parameter
    } : {}
  )

  database_postgres = var.database.engine != "postgres" ? null : try(var.database.postgres.rds, null) != null ? {
    host     = module.storage.postgres_address
    port     = module.storage.postgres_port
    database = module.storage.postgres_database
    user     = module.storage.postgres_user
    ca_file  = "/etc/gang/rds-ca-bundle.pem"
    } : {
    host     = try(var.database.postgres.external.host, null)
    port     = try(var.database.postgres.external.port, null)
    database = try(var.database.postgres.external.database, null)
    user     = try(var.database.postgres.external.user, null)
    ca_file  = try(var.database.postgres.external.ca_parameter, null) != null ? "/etc/gang/secrets/database-ca.pem" : null
  }

  gang_toml = templatefile("${path.module}/gang.toml.tftpl", {
    default_isa                       = var.default_isa
    gang_settings                     = var.gang_settings
    fleets                            = [for fleet in var.fleets : merge(fleet, { action_environment = local.node_action_environments[fleet.name] })]
    network_granted                   = local.network_granted && var.execution_egress != null
    execution_egress                  = var.execution_egress
    protected_networks                = local.protected_networks
    region                            = var.region
    events_queue_url                  = var.aws_events_enabled ? aws_sqs_queue.events[0].url : null
    public_port                       = local.public_port
    worker_port                       = local.worker_port
    health_port                       = var.load_balancer == null ? null : module.network.health_port
    proxy_protocol_enabled            = local.group && var.load_balancer != null
    proxy_trusted_cidrs               = local.group && var.load_balancer != null ? module.load_balancer[0].proxy_protocol_trusted_cidrs : []
    endpoint                          = local.endpoint
    endpoint_trust                    = var.load_balancer == null ? null : "system"
    worker_endpoint                   = local.worker_endpoint
    database_engine                   = var.database.engine
    database_table_name               = module.storage.dynamodb_table_name
    database_postgres                 = local.database_postgres
    standby_routability_delay_seconds = local.standby_routability_delay_seconds
    leadership_renew_interval_seconds = local.group ? local.health.renew_interval_seconds : null
    leadership_call_timeout_seconds   = local.group ? local.health.call_timeout_seconds : null
    shutdown_grace_seconds            = local.shutdown_grace_seconds
    scratch_mount_point               = var.scratch_mount_point
    workload_token_audience           = var.workload_token_audience
    client_token_providers            = var.client_token_providers
    fleet_tag_key                     = var.fleet_tag_key
    cache_bucket_name                 = module.storage.cache_bucket_name
    ca_key_alias                      = module.storage.ca_key_alias
    trust_domain                      = var.trust_domain
    issuer_url                        = module.worker_fleet.issuer_url
    worker_role_arn                   = module.worker_fleet.role_arn
    extra_config_toml                 = var.extra_config_toml
  })

  # Controllers read the password file only at boot, so rotation must refresh the group.
  controller_config_digest = sha256(join("\n", compact([
    local.gang_toml,
    try("password_version=${var.database.postgres.rds.password_version}", null),
  ])))
}


resource "terraform_data" "config_generation" {
  triggers_replace = sha256(local.gang_toml)
}

resource "aws_ssm_parameter" "config_generation" {
  name      = "${local.config_parameter_name}/generations/${terraform_data.config_generation.id}"
  type      = "String"
  tier      = "Intelligent-Tiering"
  value     = local.gang_toml
  overwrite = false
  tags      = merge(var.tags, { Name = "${var.name}-config-generation" })

  lifecycle {
    # Each publication identity requires a new immutable parameter.
    replace_triggered_by = [terraform_data.config_generation]
    destroy              = false
  }
}

locals {
  config_parameter_arns = ["arn:${data.aws_partition.this.partition}:ssm:${var.region}:${data.aws_caller_identity.this.account_id}:parameter${local.config_parameter_name}/generations/*"]
}

module "controller" {
  source = "../controller"

  name             = "${var.name}-controller"
  shape            = local.controller_shape
  controller_count = var.controller_count
  subnet_ids       = values(module.network.private_subnet_ids)
  target_group_arns = local.group && var.load_balancer != null ? [
    module.load_balancer[0].serve_target_group_arn,
    module.load_balancer[0].internal_serve_target_group_arn,
    module.load_balancer[0].workers_target_group_arn,
  ] : []
  shutdown_grace_seconds        = local.shutdown_grace_seconds
  config_digest                 = local.group ? local.controller_config_digest : null
  dynamodb_table_arn            = module.storage.dynamodb_table_arn
  ami_id                        = local.node_ami_ids["controller"]
  instance_type                 = var.controller_instance_type
  root_volume_gib               = var.controller_root_volume_gib
  state_volume_gib              = var.controller_state_volume_gib
  subnet_id                     = local.group ? null : module.network.public_subnet_ids[0]
  private_ip                    = local.group ? null : module.network.controller_private_ip
  security_group_id             = module.network.controller_security_group_id
  eip_allocation_id             = local.group ? null : module.network.controller_eip_allocation_id
  config_parameter_reference    = "${aws_ssm_parameter.config_generation.name}:1"
  config_parameter_arns         = local.config_parameter_arns
  cache_bucket_arn              = module.storage.cache_bucket_arn
  ca_key_arn                    = module.storage.ca_key_arn
  events_queue_arn              = var.aws_events_enabled ? aws_sqs_queue.events[0].arn : null
  worker_fleet_names            = module.worker_fleet.asg_names
  secret_parameters             = merge(var.controller_secret_parameters, local.database_secret_parameters)
  secret_parameters_kms_key_arn = var.controller_secret_parameters_kms_key_arn
  tags                          = var.tags
  iam_name_prefix               = var.iam_name_prefix
  iam_permissions_boundary_arn  = var.iam_permissions_boundary_arn
}

output "controller_config_digest" {
  description = "Controller group configuration digest used to trigger instance refreshes, or null for a single instance."
  value       = module.controller.config_digest
}

module "load_balancer" {
  count  = var.load_balancer == null ? 0 : 1
  source = "../load-balancer"

  name                           = var.name
  vpc_id                         = module.network.vpc_id
  shared_database                = local.group
  public_subnet_ids              = module.network.public_subnet_ids
  public_subnets_by_az           = local.public_subnets
  static_public_ips              = local.network_granted
  private_subnet_ids             = values(module.network.private_subnet_ids)
  worker_security_group_id       = module.network.worker_security_group_id
  workers_port                   = local.worker_port
  health_port                    = module.network.health_port
  shutdown_grace                 = local.shutdown_grace_seconds
  deregistration_delay           = local.shutdown_grace_seconds
  subnet_id                      = module.network.public_subnet_ids[0]
  hostname                       = var.hostname
  port                           = local.public_port
  client_ingress_cidrs           = var.client_ingress_cidrs
  controller_security_group_id   = module.network.controller_security_group_id
  controller_instance_id         = local.group ? null : module.controller.instance_id
  controller_private_ip          = local.group ? null : module.network.controller_private_ip
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
  config_parameter_reference              = "${aws_ssm_parameter.config_generation.name}:1"
  config_parameter_arns                   = local.config_parameter_arns
  tags                                    = var.tags
  iam_name_prefix                         = var.iam_name_prefix
  iam_permissions_boundary_arn            = var.iam_permissions_boundary_arn
  manage_outbound_web_identity_federation = var.manage_outbound_web_identity_federation

  fleets = [
    for fleet in var.fleets : {
      name               = fleet.name
      capacity           = fleet.capacity
      ami_id             = local.node_ami_ids[fleet.name]
      instance_types     = fleet.instance_types
      max_instances      = fleet.max_instances
      root_volume_gib    = fleet.root_volume_gib
      scratch_volume_gib = fleet.scratch_volume_gib
      warm_pool_size     = fleet.warm_pool_size
      fallback           = fleet.fallback == null ? null : {}
    }
  ]
}
