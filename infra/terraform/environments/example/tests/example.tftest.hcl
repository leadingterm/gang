mock_provider "aws" {
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

  mock_resource "aws_eip" {
    defaults = {
      public_ip = "203.0.113.10"
    }
  }

  mock_resource "aws_lb" {
    defaults = {
      arn      = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:loadbalancer/net/gang/0000000000000000"
      dns_name = "gang-0000000000.elb.eu-north-1.amazonaws.com"
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

override_resource {
  target = module.gang.module.worker_fleet.module.instance_role.aws_iam_instance_profile.this
  values = {
    arn = "arn:aws:iam::000000000000:instance-profile/gang-worker"
  }
}

override_resource {
  target = module.gang.module.worker_fleet.module.instance_role.aws_iam_role.this
  values = {
    arn = "arn:aws:iam::000000000000:role/gang-worker"
  }
}

override_resource {
  target = module.gang.module.controller.module.instance_role.aws_iam_role.this
  values = {
    arn = "arn:aws:iam::000000000000:role/example-gang-controller"
  }
}

override_resource {
  target = module.gang.module.controller.module.instance_role.aws_iam_instance_profile.this
  values = {
    arn = "arn:aws:iam::000000000000:instance-profile/example-gang-controller"
  }
}

override_data {
  target = module.gang.module.storage.data.aws_iam_policy_document.ca_key
  values = {
    json = "{}"
  }
}

run "plans_the_example_deployment" {
  command = apply

  assert {
    condition     = output.gang.endpoint == "https://gang.example.com"
    error_message = "The example endpoint must be the example hostname over https."
  }

  assert {
    condition     = output.gang.load_balancer_dns_name != null
    error_message = "The example must expose a load balancer DNS name."
  }

  assert {
    condition     = strcontains(output.gang.gang_toml, "[[server.roles]]")
    error_message = "gang.toml must carry the administrator role from extra_config_toml."
  }

  assert {
    condition     = trimspace(output.gang.gang_toml) == trimspace(file("${path.module}/../../../../configs/gang.aws.toml"))
    error_message = "The rendered gang.toml must equal configs/gang.aws.toml; regenerate the file from the example test output when the template changes."
  }

  assert {
    condition     = strcontains(output.gang.gang_toml, "[authentication.clients.token]\nproviders = [\"google\",\"github-actions\"]")
    error_message = "gang.toml must list the example client token providers."
  }
}

run "dashboard_login_reads_its_secret_from_a_file" {
  command = plan

  assert {
    condition     = can(regex("(?m)^\\[dashboard\\.login\\]\\n(?:[^\\[\\n][^\\n]*\\n)*client_secret_file = \"/etc/gang/secrets/dashboard-client-secret\"$", output.gang.gang_toml))
    error_message = "gang.toml must reference the dashboard client secret file inside the [dashboard.login] table."
  }

  assert {
    condition     = length(regexall("GOCSPX", output.gang.gang_toml)) == 1 && can(regex("(?m)^\\[identity\\.providers\\.google\\.oidc\\.login\\]\\n(?:#[^\\n]*\\n)?client_secret = \"GOCSPX-example\"$", output.gang.gang_toml))
    error_message = "gang.toml must carry the GOCSPX native-app secret only inside the [identity.providers.google.oidc.login] table."
  }
}
