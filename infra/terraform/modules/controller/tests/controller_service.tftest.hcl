mock_provider "aws" {
  mock_resource "aws_ecr_repository" {
    defaults = { repository_url = "000000000000.dkr.ecr.eu-north-1.amazonaws.com/gang-controller" }
  }
  mock_resource "aws_launch_template" { defaults = { id = "lt-0123456789abcdef0", latest_version = 1 } }
  mock_resource "aws_iam_role" { defaults = { arn = "arn:aws:iam::000000000000:role/gang-controller" } }
  mock_resource "aws_autoscaling_group" { defaults = { arn = "arn:aws:autoscaling:eu-north-1:000000000000:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/gang-controller" } }
  mock_resource "aws_ecs_cluster" { defaults = { arn = "arn:aws:ecs:eu-north-1:000000000000:cluster/gang-controller" } }
  mock_resource "aws_ecs_task_definition" { defaults = { arn = "arn:aws:ecs:eu-north-1:000000000000:task-definition/gang-controller:1" } }
  mock_resource "aws_ecs_capacity_provider" { defaults = { arn = "arn:aws:ecs:eu-north-1:000000000000:capacity-provider/gang-controller" } }
  mock_data "aws_partition" { defaults = { partition = "aws" } }
  mock_data "aws_region" { defaults = { region = "eu-north-1" } }
  mock_data "aws_caller_identity" { defaults = { account_id = "000000000000" } }
  mock_data "aws_iam_policy_document" { defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" } }
  mock_data "aws_ec2_instance_type" { defaults = { supported_architectures = ["arm64"], boot_modes = ["uefi"], burstable_performance_supported = true } }
  mock_data "aws_ami" { defaults = { id = "ami-0123456789abcdef0", root_device_name = "/dev/xvda" } }
  mock_data "aws_ssm_parameter" { defaults = { value = "ami-0123456789abcdef0" } }
}

variables {
  shape                         = "service"
  controller_count              = 1
  controller_image_digest       = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  ecs_ami_id                    = "ami-0123456789abcdef0"
  subnet_ids                    = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
  security_group_id             = "sg-0123456789abcdef0"
  config_parameter_reference    = "/gang/generations/config:1"
  config_parameter_arns         = ["arn:aws:ssm:eu-north-1:000000000000:parameter/gang/generations/*"]
  config_digest                 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  cache_bucket_arn              = null
  ca_key_arn                    = "arn:aws:kms:eu-north-1:000000000000:key/00000000-0000-0000-0000-000000000000"
  worker_fleet_names            = ["gang-worker"]
  secret_parameters             = { z = "/gang/z", a = "/gang/a" }
  secret_parameters_kms_key_arn = "arn:aws:kms:eu-north-1:000000000000:key/00000000-0000-0000-0000-000000000001"
  target_groups = {
    public   = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/public/0123456789abcdef", port = 443 }
    internal = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/internal/0123456789abcdef", port = 443 }
    worker   = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/worker/0123456789abcdef", port = 9090 }
  }
}

run "rolling_service_and_network" {
  command = plan
  assert {
    condition     = aws_ecs_service.controller[0].deployment_minimum_healthy_percent == 100 && aws_ecs_service.controller[0].deployment_maximum_percent == 200 && aws_ecs_service.controller[0].deployment_circuit_breaker[0].enable && aws_ecs_service.controller[0].deployment_circuit_breaker[0].rollback && !aws_ecs_service.controller[0].wait_for_steady_state
    error_message = "Rolling deployments must preserve capacity and roll back failures without blocking the external migration stop."
  }
  assert {
    condition     = aws_ecs_service.controller[0].health_check_grace_period_seconds == 300 && aws_ecs_service.controller[0].health_check_grace_period_seconds > jsondecode(file("${path.module}/../load-balancer/health-check.json")).interval_seconds * jsondecode(file("${path.module}/../load-balancer/health-check.json")).threshold
    error_message = "ECS needs a five-minute startup grace longer than the NLB health window so a preparing candidate is not evicted before it can become leader."
  }
  assert {
    condition     = aws_launch_template.capacity[0].instance_type == "t4g.large" && aws_launch_template.capacity[0].credit_specification[0].cpu_credits == "unlimited" && aws_ecs_task_definition.controller[0].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.controller[0].runtime_platform[0].operating_system_family == "LINUX"
    error_message = "Service capacity defaults to ARM64 t4g.large with unlimited CPU credits."
  }
  assert {
    condition     = aws_ecs_service.controller[0].timeouts.update == "3060s" && aws_ecs_service.controller[0].timeouts.delete == "3060s"
    error_message = "Service operation timeouts must cover the configured stop budget and platform margin."
  }
  assert {
    condition     = aws_ecr_repository.controller[0].image_tag_mutability == "IMMUTABLE" && aws_ecr_repository.controller[0].encryption_configuration[0].encryption_type == "AES256" && aws_ecr_repository.controller[0].image_scanning_configuration[0].scan_on_push
    error_message = "The private image repository must be immutable, encrypted and scanned."
  }
  assert {
    condition     = jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].image == "${aws_ecr_repository.controller[0].repository_url}@${var.controller_image_digest}"
    error_message = "The task must pull the pinned digest from the private repository."
  }
  assert {
    condition     = aws_autoscaling_group.capacity[0].min_size == 1 && aws_autoscaling_group.capacity[0].max_size == 2 && aws_autoscaling_group.capacity[0].desired_capacity == 1 && aws_autoscaling_group.capacity[0].protect_from_scale_in && aws_ecs_service.controller[0].desired_count == 1
    error_message = "Capacity must allow one protected replacement host for the single controller."
  }
  assert {
    condition     = aws_autoscaling_group.capacity[0].launch_template[0].version == tostring(aws_launch_template.capacity[0].latest_version) && aws_autoscaling_group.capacity[0].instance_refresh[0].strategy == "Rolling" && aws_autoscaling_group.capacity[0].instance_refresh[0].preferences[0].min_healthy_percentage == 100 && aws_autoscaling_group.capacity[0].instance_refresh[0].preferences[0].max_healthy_percentage == 200 && aws_autoscaling_group.capacity[0].instance_refresh[0].preferences[0].scale_in_protected_instances == "Refresh" && aws_autoscaling_group.capacity[0].instance_refresh[0].preferences[0].skip_matching
    error_message = "Launch template changes must refresh protected hosts with replacement capacity and skip matching instances."
  }
  assert {
    condition     = aws_ecs_capacity_provider.controller[0].auto_scaling_group_provider[0].managed_termination_protection == "ENABLED" && aws_ecs_capacity_provider.controller[0].auto_scaling_group_provider[0].managed_draining == "ENABLED" && aws_ecs_capacity_provider.controller[0].auto_scaling_group_provider[0].managed_scaling[0].status == "ENABLED" && aws_ecs_capacity_provider.controller[0].auto_scaling_group_provider[0].managed_scaling[0].target_capacity == 100 && aws_ecs_capacity_provider.controller[0].auto_scaling_group_provider[0].managed_scaling[0].minimum_scaling_step_size == 1 && aws_ecs_capacity_provider.controller[0].auto_scaling_group_provider[0].managed_scaling[0].maximum_scaling_step_size == 1
    error_message = "ECS must manage one-host scaling steps and protect and drain task hosts."
  }
  assert {
    condition     = aws_ecs_task_definition.controller[0].network_mode == "host" && length(aws_ecs_service.controller[0].network_configuration) == 0 && toset([for lb in aws_ecs_service.controller[0].load_balancer : "${lb.target_group_arn}:${lb.container_port}"]) == toset([for group in values(var.target_groups) : "${group.arn}:${group.port}"])
    error_message = "Host-mode tasks must attach each existing instance target group to its configured port."
  }
  assert {
    condition     = toset([for p in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].portMappings : p.containerPort]) == toset([443, 8080, 9090]) && alltrue([for p in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].portMappings : p.containerPort == p.hostPort && p.protocol == "tcp"]) && !contains(keys(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0]), "stopTimeout")
    error_message = "Tasks must reserve fixed host ports and honor the configured stop budget."
  }
  assert {
    condition     = !contains(keys(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0]), "healthCheck")
    error_message = "Readiness must be checked through the NLB, not an untrusted loopback probe."
  }
  assert {
    condition     = strcontains(base64decode(aws_launch_template.capacity[0].user_data), "ECS_CONTAINER_STOP_TIMEOUT=2460s") && strcontains(base64decode(aws_launch_template.capacity[0].user_data), "ECS_RESERVED_MEMORY=512") && strcontains(base64decode(aws_launch_template.capacity[0].user_data), "ECS_ENABLE_TASK_IAM_ROLE_NETWORK_HOST=true") && aws_launch_template.capacity[0].metadata_options[0].http_tokens == "required" && toset(aws_launch_template.capacity[0].vpc_security_group_ids) == toset([var.security_group_id])
    error_message = "Capacity hosts must use the controller security group, reserve memory, honor the stop budget and require IMDSv2."
  }
  assert {
    condition     = strcontains(file("${path.module}/ecs.tf"), "ignore_changes = [desired_capacity]") && !strcontains(file("${path.module}/ecs.tf"), "target_group_arns")
    error_message = "ECS owns capacity desired count and only task hosts may register in target groups."
  }
  assert {
    condition     = alltrue([for source in fileset(path.module, "*.tf") : !strcontains(file("${path.module}/${source}"), "resource \"aws_iam_service_linked_role\"") && !strcontains(file("${path.module}/${source}"), "data \"aws_iam_role\"")])
    error_message = "The ECS service-linked role must remain an unmanaged prerequisite."
  }
}

run "secrets_and_roles" {
  command = apply
  assert {
    condition     = aws_iam_role_policy.ecs_host[0].role == aws_iam_role.ecs_host[0].id && aws_iam_role_policy.execution[0].role == aws_iam_role.execution[0].id && aws_ecs_task_definition.controller[0].task_role_arn == module.instance_role.role_arn
    error_message = "Inline host and execution permissions must remain separate from the task application role."
  }
  assert {
    condition     = aws_iam_role_policy_attachments_exclusive.ecs_host[0].role_name == aws_iam_role.ecs_host[0].name && length(aws_iam_role_policy_attachments_exclusive.ecs_host[0].policy_arns) == 0 && aws_iam_role_policy_attachments_exclusive.execution[0].role_name == aws_iam_role.execution[0].name && length(aws_iam_role_policy_attachments_exclusive.execution[0].policy_arns) == 0
    error_message = "Neither host nor execution role may retain managed-policy attachments."
  }
  assert {
    condition     = [for e in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].environment : e.value if e.name == "AWS_EC2_METADATA_DISABLED"] == ["true"]
    error_message = "The application SDK must not fall back to host instance credentials."
  }
  assert {
    condition     = length([for s in data.aws_iam_policy_document.ecs_host.statement : s if s.sid == "SsmCore" && contains(s.actions, "ssm:UpdateInstanceInformation") && contains(s.actions, "ssmmessages:OpenControlChannel") && !anytrue([for a in s.actions : startswith(a, "ssm:GetParameter")])]) == 1 && length([for s in data.aws_iam_policy_document.ecs_host.statement : s if s.sid == "EcsAgent" && contains(s.actions, "ecs:RegisterContainerInstance") && !contains(s.actions, "ecs:CreateCluster")]) == 1
    error_message = "Host permissions must support SSM and ECS registration without parameter reads or cluster creation."
  }
  assert {
    condition     = length([for s in data.aws_iam_policy_document.ecs_host.statement : s if s.sid == "TagEcsRegistration" && contains(s.actions, "ecs:TagResource") && anytrue([for c in s.condition : c.variable == "ecs:CreateAction" && contains(c.values, "RegisterContainerInstance")])]) == 1 && length([for s in data.aws_iam_policy_document.ecs_host.statement : s if s.sid == "ListEcsTags" && contains(s.actions, "ecs:ListTagsForResource") && length(s.resources) == 2]) == 1
    error_message = "ECS registration tagging and tag reads must use scoped permissions."
  }
  assert {
    condition     = length([for s in data.aws_iam_policy_document.execution[0].statement : s if s.sid == "PullControllerImage" && s.resources == toset([aws_ecr_repository.controller[0].arn]) && contains(s.actions, "ecr:BatchGetImage")]) == 1 && length([for s in data.aws_iam_policy_document.execution[0].statement : s if s.sid == "WriteControllerLogs" && s.resources == toset(["${aws_cloudwatch_log_group.controller[0].arn}:*"]) && contains(s.actions, "logs:PutLogEvents")]) == 1
    error_message = "Execution permissions must scope image pulls and log writes to the controller resources."
  }
  assert {
    condition     = toset([for s in data.aws_iam_policy_document.execution_secrets.statement : s.resources if s.sid == "ReadParameters"][0]) == toset(concat(var.config_parameter_arns, local.secret_parameter_arns)) && contains([for s in data.aws_iam_policy_document.execution_secrets.statement : s.sid], "DecryptSecrets")
    error_message = "Execution role must read only configured parameters and decrypt the supplied key."
  }
  assert {
    condition     = [for e in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].environment : e.value if e.name == "GANG_SECRET_FILES"][0] == "0=a\n1=z" && [for s in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].secrets : s.name] == ["GANG_CONFIG", "GANG_SECRET_0", "GANG_SECRET_1"]
    error_message = "Secret filenames and injected values must share deterministic order."
  }
  assert {
    condition     = [for s in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].secrets : s.valueFrom if s.name == "GANG_CONFIG"][0] == "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/generations/config"
    error_message = "ECS must receive the immutable configuration parameter ARN without a version suffix."
  }
  assert {
    condition     = !contains(keys(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0]), "systemControls") && toset(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].dnsServers) == toset(["127.0.0.1"])
    error_message = "Host-mode tasks must use host DNS and must not set network namespace sysctls."
  }
  assert {
    condition     = length([for s in data.aws_iam_policy_document.this.statement : s if s.sid == "ControlFleetGroups" && length(s.resources) == 1 && endswith(one(s.resources), ":autoScalingGroupName/gang-worker")]) == 1 && alltrue([for s in data.aws_iam_policy_document.this.statement : !contains(["ControllerLifecycle", "DescribeControllerLifecycleHooks", "RegisterControllerTarget", "DescribeControllerTargetHealth"], s.sid)])
    error_message = "The existing controller role is retained without lifecycle or target registration grants."
  }
}

run "reject_unpinned_image" {
  command = plan
  variables { controller_image_digest = "latest" }
  expect_failures = [var.controller_image_digest]
}

run "reject_multiple_controllers" {
  command = plan
  variables { controller_count = 2 }
  expect_failures = [var.controller_count]
}

run "default_arm64_ecs_ami" {
  command = plan
  variables { ecs_ami_id = null }
  assert {
    condition     = data.aws_ssm_parameter.ecs_ami[0].name == "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id"
    error_message = "The default capacity AMI must be the ECS-optimized AL2023 ARM64 image."
  }
}
