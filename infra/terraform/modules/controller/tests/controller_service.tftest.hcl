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
  mock_data "aws_security_group" { defaults = { vpc_id = "vpc-0123456789abcdef0" } }
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
    condition     = aws_ecs_service.controller[0].deployment_minimum_healthy_percent == 100 && aws_ecs_service.controller[0].deployment_maximum_percent == 200 && aws_ecs_service.controller[0].deployment_circuit_breaker[0].enable && aws_ecs_service.controller[0].deployment_circuit_breaker[0].rollback && aws_ecs_service.controller[0].wait_for_steady_state
    error_message = "Rolling deployment must preserve serving capacity and roll back failures."
  }
  assert {
    condition     = aws_launch_template.capacity[0].instance_type == "t4g.large" && aws_launch_template.capacity[0].credit_specification[0].cpu_credits == "unlimited" && aws_ecs_task_definition.controller[0].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.controller[0].runtime_platform[0].operating_system_family == "LINUX"
    error_message = "Service capacity defaults to ARM64 t4g.large with unlimited CPU credits."
  }
  assert {
    condition     = aws_ecs_service.controller[0].timeouts.update == "3060s" && aws_ecs_service.controller[0].timeouts.delete == "3060s"
    error_message = "Service wait and deletion timeouts must cover the configured stop budget and platform margin."
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
    condition     = aws_autoscaling_group.capacity[0].min_size == 1 && aws_autoscaling_group.capacity[0].max_size == 1 && aws_autoscaling_group.capacity[0].desired_capacity == 1 && aws_ecs_service.controller[0].desired_count == 1
    error_message = "Capacity and service must run one controller."
  }
  assert {
    condition     = aws_ecs_task_definition.controller[0].network_mode == "awsvpc" && toset([for lb in aws_ecs_service.controller[0].load_balancer : "${lb.target_group_arn}:${lb.container_port}"]) == toset([for group in values(var.target_groups) : "${group.arn}:${group.port}"])
    error_message = "Every IP target group must attach to its configured task port."
  }
  assert {
    condition     = toset([for p in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].portMappings : p.containerPort]) == toset([443, 8080, 9090]) && jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].stopTimeout == 2460
    error_message = "Tasks must expose service and health ports and honor the configured stop budget."
  }
  assert {
    condition     = !contains(keys(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0]), "healthCheck")
    error_message = "Readiness must be checked through the NLB, not an untrusted loopback probe."
  }
  assert {
    condition     = strcontains(base64decode(aws_launch_template.capacity[0].user_data), "ECS_CONTAINER_STOP_TIMEOUT=2460s") && strcontains(base64decode(aws_launch_template.capacity[0].user_data), "ECS_RESERVED_MEMORY=512") && aws_launch_template.capacity[0].metadata_options[0].http_tokens == "required" && length(aws_security_group.capacity[0].ingress) == 0
    error_message = "Capacity hosts must reserve memory, honor stop budget, require IMDSv2 and expose no application ingress."
  }
}

run "secrets_and_roles" {
  command = plan
  assert {
    condition     = toset([for s in data.aws_iam_policy_document.execution_secrets.statement : s.resources if s.sid == "ReadParameters"][0]) == toset(concat(var.config_parameter_arns, local.secret_parameter_arns)) && contains([for s in data.aws_iam_policy_document.execution_secrets.statement : s.sid], "DecryptSecrets")
    error_message = "Execution role must read only configured parameters and decrypt the supplied key."
  }
  assert {
    condition     = [for e in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].environment : e.value if e.name == "GANG_SECRET_FILES"][0] == "0=a\n1=z" && [for s in jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].secrets : s.name] == ["GANG_CONFIG", "GANG_SECRET_0", "GANG_SECRET_1"]
    error_message = "Secret filenames and injected values must share deterministic order."
  }
  assert {
    condition     = one(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].systemControls).namespace == "net.ipv4.ip_unprivileged_port_start" && one(jsondecode(aws_ecs_task_definition.controller[0].container_definitions)[0].systemControls).value == "0"
    error_message = "The unprivileged controller must be able to bind port 443 in its task namespace."
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
