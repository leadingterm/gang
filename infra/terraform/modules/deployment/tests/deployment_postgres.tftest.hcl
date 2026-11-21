mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000" }
  }

  mock_data "aws_network_interface" {
    defaults = { private_ips = ["10.20.1.10", "10.20.1.11"] }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      region = "eu-north-1"
    }
  }

  mock_data "aws_availability_zones" {
    defaults = {
      names    = ["az-a", "az-b", "az-c"]
      zone_ids = ["a", "b", "c"]
    }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      instance_types = ["c6id.4xlarge", "c6id.4xlarge", "c6id.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge", "c7gd.4xlarge"]
      locations      = ["az-a", "az-b", "az-c", "az-a", "az-b", "az-c"]
    }
  }

  # The controller and worker-fleet modules require UEFI-capable instance types.
  mock_data "aws_ec2_instance_type" {
    defaults = {
      boot_modes                 = ["legacy-bios", "uefi"]
      memory_size                = 65536
      supported_architectures    = ["x86_64"]
      instance_storage_supported = true
      hypervisor                 = "nitro"
      instance_disks             = [{ count = 1, size = 950, type = "ssd" }]
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_resource "aws_kms_key" {
    defaults = {
      arn    = "arn:aws:kms:eu-north-1:000000000000:key/11111111-2222-3333-4444-555555555555"
      key_id = "11111111-2222-3333-4444-555555555555"
    }
  }

  mock_resource "aws_dynamodb_table" {
    defaults = {
      arn = "arn:aws:dynamodb:eu-north-1:000000000000:table/gang-state"
    }
  }

  mock_resource "aws_launch_template" {
    defaults = {
      id = "lt-0123456789abcdef0"
    }
  }

  mock_resource "aws_eip" {
    defaults = {
      public_ip = "203.0.113.10"
    }
  }

  mock_resource "aws_lb" {
    defaults = {
      arn        = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:loadbalancer/net/gang/0000000000000000"
      arn_suffix = "net/gang/0000000000000000"
      dns_name   = "gang-0000000000.elb.eu-north-1.amazonaws.com"
    }
  }

  mock_resource "aws_lb_target_group" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/gang/0000000000000000"
    }
  }

  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:eu-north-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"
    }
  }

  mock_resource "aws_iam_outbound_web_identity_federation" {
    defaults = {
      issuer_identifier = "https://issuer.example.com/123456789012"
    }
  }
}

mock_provider "http" {
  mock_data "http" {
    defaults = {
      status_code   = 200
      response_body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-aarch64.raw.zst\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  gang-image-x86_64.raw.zst\n"
    }
  }
}

override_data {
  target = data.aws_ami.image
  values = {
    name             = "gang-x86_64-aaaaaaaaaaaaaaaa"
    boot_mode        = "uefi"
    tags             = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
    deprecation_time = ""
  }
}

override_resource {
  target = module.worker_fleet.module.instance_role.aws_iam_instance_profile.this
  values = {
    arn = "arn:aws:iam::000000000000:instance-profile/gang-worker"
  }
}

override_resource {
  target = module.worker_fleet.module.instance_role.aws_iam_role.this
  values = {
    arn = "arn:aws:iam::000000000000:role/gang-worker"
  }
}

override_resource {
  target = module.controller.module.instance_role.aws_iam_role.this
  values = {
    arn = "arn:aws:iam::000000000000:role/gang-controller"
  }
}

override_resource {
  target = module.controller.module.instance_role.aws_iam_instance_profile.this
  values = {
    arn = "arn:aws:iam::000000000000:instance-profile/gang-controller"
  }
}

override_resource {
  target = module.storage.aws_ssm_parameter.postgres_password
  values = {
    arn = "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/database-password"
  }
}

override_data {
  target = module.storage.data.aws_iam_policy_document.ca_key
  values = {
    json = "{}"
  }
}

override_module {
  target = module.network
  outputs = {
    vpc_id                       = "vpc-test"
    vpc_cidr_block               = "10.20.0.0/16"
    vpc_ipv6_cidr_blocks         = []
    availability_zones           = ["az-a", "az-b"]
    public_subnet_ids            = ["subnet-public-a", "subnet-public-b"]
    public_subnet_cidr_blocks    = ["10.20.0.0/20", "10.20.16.0/20"]
    public_subnets_by_az         = { "az-a" = "subnet-public-a", "az-b" = "subnet-public-b" }
    private_subnet_ids           = { "az-a" = "subnet-private-a", "az-b" = "subnet-private-b" }
    controller_security_group_id = "sg-controller"
    worker_security_group_id     = "sg-worker"
    controller_eip_allocation_id = null
    controller_public_ip         = null
    controller_public_dns        = null
    controller_private_ip        = "10.20.0.10"
    controller_private_dns       = "ip-10-20-0-10.eu-north-1.compute.internal"
    nat_public_ips               = ["203.0.113.10"]
    public_port                  = 443
    worker_port                  = 9090
    health_port                  = 8080
  }
}

variables {
  client_token_providers   = ["google", "github-actions"]
  region                   = "eu-north-1"
  cidr_block               = "10.20.0.0/16"
  cache_bucket_name        = "example-gang-cache"
  controller_instance_type = "c7i.large"
  default_isa              = "x86-64"
  trust_domain             = "example.com"
  fleets = [
    {
      name                        = "gang-worker-x86-64"
      isa                         = "x86-64"
      capacity                    = "spot"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    },
    {
      name                        = "gang-worker-aarch64"
      isa                         = "aarch64"
      capacity                    = "on-demand"
      instance_types              = ["c7gd.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    },
  ]
}



run "rds_group_shape" {
  command = apply
  variables {
    database         = { engine = "postgres", postgres = { rds = {} } }
    controller_count = 2
    hostname         = "gang.example.com"
    load_balancer    = {}
    nat_mode         = "per_az"
  }

  assert {
    condition     = output.controller_shape == "group" && strcontains(output.gang_toml, "[database.postgres]") && !strcontains(output.gang_toml, "[database.sqlite]") && strcontains(output.gang_toml, "host = \"${module.storage.postgres_address}\"") && module.storage.postgres_address != ""
    error_message = "RDS must use the group shape and render the instance address in the PostgreSQL section."
  }
  assert {
    condition     = !strcontains(output.gang_toml, "execution_listen") && !strcontains(output.gang_toml, "execution_endpoint") && strcontains(output.gang_toml, "endpoint = \"https://gang.example.com\"") && strcontains(output.gang_toml, "worker_endpoint = \"grpcs://gang.example.com:9090\"")
    error_message = "The PostgreSQL group must use one client endpoint and the private worker endpoint."
  }
  assert {
    condition     = strcontains(output.gang_toml, "standby_routability_delay = \"${local.health.interval_seconds * local.health.threshold}s\"") && strcontains(output.gang_toml, "[server.leadership]\nrenew_interval = \"${local.health.renew_interval_seconds}s\"\ncall_timeout = \"${local.health.call_timeout_seconds}s\"")
    error_message = "PostgreSQL must render the same routing, renewal and call timeout policy as the controller refresh."
  }

  assert {
    condition     = strcontains(output.gang_toml, "password_file = \"/etc/gang/secrets/database-password\"") && strcontains(output.gang_toml, "ca_file = \"/etc/gang/rds-ca-bundle.pem\"")
    error_message = "RDS must use the secret password file and the bundled RDS CA."
  }
  assert {
    condition     = local.database_secret_parameters["database-password"] == module.storage.postgres_password_parameter_name && module.storage.postgres_password_parameter_name != "" && length(module.controller.secret_parameter_arns) == 1 && endswith(module.controller.secret_parameter_arns[0], ":parameter${module.storage.postgres_password_parameter_name}")
    error_message = "The controller must receive the RDS password parameter."
  }
  assert {
    condition     = output.controller_config_digest == sha256("${output.gang_toml}\npassword_version=1")
    error_message = "The RDS controller digest must include the initial password version."
  }
  assert {
    condition     = strcontains(output.gang_toml, "[server.proxy_protocol]\ntrusted_cidrs = [\"10.20.1.10/32\",\"10.20.1.11/32\"]") && !strcontains(output.gang_toml, "10.20.0.0/16")
    error_message = "Shared deployment must trust only exact NLB ENI addresses."
  }
}


run "rds_password_rotation_refreshes_controllers" {
  command = apply
  variables {
    database           = { engine = "postgres", postgres = { rds = { password_version = 2 } } }
    controller_count   = 2
    hostname           = "gang.example.com"
    load_balancer      = {}
    nat_mode           = "per_az"
    previous_gang_toml = run.rds_group_shape.gang_toml
    previous_digest    = run.rds_group_shape.controller_config_digest
  }

  assert {
    condition     = output.gang_toml == var.previous_gang_toml && output.controller_config_digest != var.previous_digest && output.controller_config_digest == sha256("${output.gang_toml}\npassword_version=2")
    error_message = "Changing only the RDS password version must change the controller digest without changing gang.toml."
  }
}


run "dynamodb_digest_unchanged" {
  command = apply
  variables {
    database      = { engine = "dynamodb" }
    hostname      = "gang.example.com"
    load_balancer = {}
  }

  assert {
    condition     = output.controller_config_digest == sha256(output.gang_toml)
    error_message = "DynamoDB controller digests must remain the SHA-256 of gang.toml."
  }
}

run "external_with_ca" {
  command = apply
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", database = "gang", user = "gang", password_parameter = "/gang/db/password", ca_parameter = "/gang/db/ca"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  assert {
    condition     = output.controller_shape == "group" && strcontains(output.gang_toml, "host = \"db.example.com\"") && strcontains(output.gang_toml, "port = 5432") && strcontains(output.gang_toml, "ca_file = \"/etc/gang/secrets/database-ca.pem\"")
    error_message = "External PostgreSQL must use the group shape and render its host, port and CA file."
  }
  assert {
    condition     = local.database_secret_parameters["database-password"] == "/gang/db/password" && local.database_secret_parameters["database-ca.pem"] == "/gang/db/ca" && length(module.controller.secret_parameter_arns) == 2 && anytrue([for arn in module.controller.secret_parameter_arns : endswith(arn, ":parameter/gang/db/password")]) && anytrue([for arn in module.controller.secret_parameter_arns : endswith(arn, ":parameter/gang/db/ca")])
    error_message = "The controller must receive both external PostgreSQL secret parameters."
  }
  assert {
    condition     = output.controller_config_digest == sha256(output.gang_toml)
    error_message = "External PostgreSQL controller digests must remain the SHA-256 of gang.toml."
  }
}

run "external_without_ca" {
  command = apply
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", port = 6432, database = "builds", user = "builder", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  assert {
    condition     = strcontains(output.gang_toml, "[database.postgres]") && strcontains(output.gang_toml, "port = 6432") && strcontains(output.gang_toml, "database = \"builds\"") && strcontains(output.gang_toml, "user = \"builder\"") && !strcontains(output.gang_toml, "ca_file =") && !contains(keys(local.database_secret_parameters), "database-ca.pem") && length(module.controller.secret_parameter_arns) == 1
    error_message = "External PostgreSQL without a custom CA must use system trust and render its connection settings."
  }
}

run "rejects_postgres_without_load_balancer" {
  command = plan
  variables { database = { engine = "postgres", postgres = { rds = {} } } }
  expect_failures = [var.database]
}

run "rejects_rds_and_external" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { rds = {}, external = {
      host = "db.example.com", database = "gang", user = "gang", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_missing_postgres_source" {
  command = plan
  variables {
    database      = { engine = "postgres" }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_postgres_with_dynamodb" {
  command = plan
  variables {
    database      = { engine = "dynamodb", postgres = { rds = {} } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_postgres_filesystem_cache" {
  command = plan
  variables {
    database          = { engine = "postgres", postgres = { rds = {} } }
    hostname          = "gang.example.com"
    load_balancer     = {}
    extra_config_toml = "[cache.filesystem]\npath = \"/tmp/cache\""
  }
  expect_failures = [var.database]
}

run "rejects_reserved_password_secret" {
  command = plan
  variables {
    database                     = { engine = "postgres", postgres = { rds = {} } }
    hostname                     = "gang.example.com"
    load_balancer                = {}
    controller_secret_parameters = { "database-password" = "/other/password" }
  }
  expect_failures = [var.controller_secret_parameters]
}

run "rejects_reserved_ca_secret" {
  command = plan
  variables {
    database                     = { engine = "postgres", postgres = { rds = {} } }
    hostname                     = "gang.example.com"
    load_balancer                = {}
    controller_secret_parameters = { "database-ca.pem" = "/other/ca" }
  }
  expect_failures = [var.controller_secret_parameters]
}

run "rejects_single_az_rds" {
  command = plan
  variables {
    database      = { engine = "postgres", postgres = { rds = { multi_az = false } } }
    network       = { availability_zone_count = 1 }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_zero_password_version" {
  command = plan
  variables {
    database      = { engine = "postgres", postgres = { rds = { password_version = 0 } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_fractional_password_version" {
  command = plan
  variables {
    database      = { engine = "postgres", postgres = { rds = { password_version = 1.5 } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_empty_external_host" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "", database = "gang", user = "gang", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_empty_external_database" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", database = " ", user = "gang", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_empty_external_user" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", database = "gang", user = "", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_empty_external_password_parameter" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", database = "gang", user = "gang", password_parameter = ""
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_external_port_zero" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", port = 0, database = "gang", user = "gang", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_external_port_above_range" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", port = 65536, database = "gang", user = "gang", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_fractional_external_port" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", port = 5432.5, database = "gang", user = "gang", password_parameter = "/gang/db/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [var.database]
}

run "rejects_external_password_in_generations" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", database = "gang", user = "gang", password_parameter = "/gang/gang.toml/generations/password"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [terraform_data.input_checks]
}

run "rejects_external_ca_in_generations" {
  command = plan
  variables {
    database = { engine = "postgres", postgres = { external = {
      host = "db.example.com", database = "gang", user = "gang", password_parameter = "/gang/db/password", ca_parameter = "/gang/gang.toml/generations/ca"
    } } }
    hostname      = "gang.example.com"
    load_balancer = {}
  }
  expect_failures = [terraform_data.input_checks]
}
