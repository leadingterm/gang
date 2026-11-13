# Example root with neutral values. Contract-tested and never applied.
# A real deployment keeps its own root that calls the module by release tag:
#   source = "git::https://github.com/example-org/gang//infra/terraform/modules/deployment?ref=v1.0.0"

locals {
  account_id = "000000000000"
  region     = "eu-north-1"
  tags       = { Project = "gang" }
}

module "gang" {
  source = "../../modules/deployment"

  region                       = local.region
  cidr_block                   = "10.20.0.0/16"
  hostname                     = "gang.example.com"
  cache_bucket_name            = "example-gang-cache"
  controller_ami_id            = "ami-00000000000000000"
  controller_instance_type     = "c7i.large"
  default_isa                  = "x86-64"
  trust_domain                 = "example.com"
  iam_name_prefix              = "example-"
  iam_permissions_boundary_arn = "arn:aws:iam::000000000000:policy/example-boundary"
  client_token_providers       = ["google", "github-actions"]
  # Confidential values: the SecureString parameter is created out of band (for example
  # `aws ssm put-parameter --type SecureString`), so its value never enters the OpenTofu state.
  # The controller writes it to /etc/gang/secrets/dashboard-client-secret.
  controller_secret_parameters = { dashboard-client-secret = "/example/gang/dashboard-client-secret" }
  tags                         = local.tags

  # Publicly trusted TLS: an ACM certificate for hostname on a load balancer, so browsers and Bazel need no CA. Publish the certificate_validation_records, then point hostname at load_balancer_dns_name.
  load_balancer = {}

  fleets = [
    {
      name                        = "gang-worker-x86-64"
      isa                         = "x86-64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000001"
      instance_types              = ["c6id.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    },
    {
      name                        = "gang-worker-aarch64"
      isa                         = "aarch64"
      capacity                    = "spot"
      ami_id                      = "ami-00000000000000002"
      instance_types              = ["c7gd.4xlarge"]
      max_instances               = 4
      vcpus_per_instance          = 15
      memory_per_instance         = "29GiB"
      disk_per_instance           = "200GiB"
      target_actions_per_instance = 16
    },
    # Optional: a spot fleet with an on-demand twin. `fallback` adds the group <name>-on-demand, which takes
    # capacity while Spot is unavailable. Presence opts in; the values shown are the defaults.
    # {
    #   name                        = "gang-worker-x86-64-fallback"
    #   isa                         = "x86-64"
    #   capacity                    = "spot"
    #   ami_id                      = "ami-00000000000000003"
    #   instance_types              = ["c6id.4xlarge"]
    #   max_instances               = 4
    #   vcpus_per_instance          = 15
    #   memory_per_instance         = "29GiB"
    #   disk_per_instance           = "200GiB"
    #   target_actions_per_instance = 16
    #   fallback                    = { retry_spot_after = "5m" }
    # },
    # Optional: an on-demand fleet with a warm pool. `warm_pool_size` keeps stopped standby instances on its group.
    # {
    #   name                        = "gang-worker-aarch64-warm"
    #   isa                         = "aarch64"
    #   capacity                    = "on-demand"
    #   ami_id                      = "ami-00000000000000004"
    #   instance_types              = ["c7gd.4xlarge"]
    #   max_instances               = 4
    #   vcpus_per_instance          = 15
    #   memory_per_instance         = "29GiB"
    #   disk_per_instance           = "200GiB"
    #   target_actions_per_instance = 16
    #   warm_pool_size              = 2
    # },
  ]

  extra_config_toml = <<-EOT
    [[server.roles]]
    name        = "administrator"
    permissions = ["*"]
    principals  = [{ provider = "google", principal = "operator@example.com" }]

    [identity.providers.google.oidc]
    issuer_url = "https://accounts.google.com"
    audience = "000000000000-example.apps.googleusercontent.com"
    principal_claim = "email"
    required_claims = { hd = "example.com", email_verified = true }

    [identity.providers.google.oidc.login]
    # The native-app client secret is not confidential (RFC 8252): every CLI receives it, so it may stay in the public configuration.
    client_secret = "GOCSPX-example"
    scopes = ["openid", "email"]

    # Sent only on logins that must yield a refresh token (ADR-38). Clients must be upgraded before this table is set: older clients reject unknown login keys.
    [identity.providers.google.oidc.login.offline]
    authorization_params = { access_type = "offline", prompt = "consent" }

    # The dashboard's confidential web client: its secret lives in a SecureString parameter, never in this public configuration.
    [dashboard.login]
    provider = "google"
    client_id = "000000000000-dashboard.apps.googleusercontent.com"
    client_secret_file = "/etc/gang/secrets/dashboard-client-secret"

    [identity.providers.github-actions.oidc]
    issuer_url = "https://token.actions.githubusercontent.com"
    audience = "gang"
    principal_claim = "sub"
    required_claims = { repository_owner_id = "0000000" }
  EOT
}
