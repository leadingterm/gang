resource "aws_ecr_repository" "controller" {
  count                = local.service_shape ? 1 : 0
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"
  encryption_configuration {
    encryption_type = "AES256"
  }
  image_scanning_configuration {
    scan_on_push = true
  }
  tags = var.tags
}

data "aws_ssm_parameter" "ecs_ami" {
  count = local.service_shape && var.ecs_ami_id == null ? 1 : 0
  name  = "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id"
}

data "aws_ami" "ecs" {
  count = local.service_shape ? 1 : 0
  filter {
    name   = "image-id"
    values = [var.ecs_ami_id != null ? var.ecs_ami_id : data.aws_ssm_parameter.ecs_ami[0].value]
  }
}

data "aws_security_group" "task" {
  count = local.service_shape ? 1 : 0
  id    = var.security_group_id
}

resource "aws_security_group" "capacity" {
  count       = local.service_shape ? 1 : 0
  name_prefix = "${var.name}-ecs-"
  description = "ECS capacity host egress"
  vpc_id      = data.aws_security_group.task[0].vpc_id
  ingress     = []
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = merge(var.tags, { Name = "${var.name}-ecs-host" })
}

data "aws_iam_policy_document" "ecs_host_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_host" {
  count                = local.service_shape ? 1 : 0
  name                 = "${var.iam_name_prefix}${var.name}-ecs-host"
  assume_role_policy   = data.aws_iam_policy_document.ecs_host_assume.json
  permissions_boundary = var.iam_permissions_boundary_arn
  tags                 = var.tags
}

resource "aws_iam_role_policy_attachment" "ecs_host" {
  count      = local.service_shape ? 1 : 0
  role       = aws_iam_role.ecs_host[0].name
  policy_arn = "arn:${data.aws_partition.this.partition}:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ecs_host" {
  count = local.service_shape ? 1 : 0
  name  = "${var.iam_name_prefix}${var.name}-ecs-host"
  role  = aws_iam_role.ecs_host[0].name
  tags  = var.tags
}

resource "aws_iam_role" "execution" {
  count                = local.service_shape ? 1 : 0
  name                 = "${var.iam_name_prefix}${var.name}-ecs-execution"
  assume_role_policy   = data.aws_iam_policy_document.ecs_task_assume.json
  permissions_boundary = var.iam_permissions_boundary_arn
  tags                 = var.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  count      = local.service_shape ? 1 : 0
  role       = aws_iam_role.execution[0].name
  policy_arn = "arn:${data.aws_partition.this.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    sid       = "ReadParameters"
    effect    = "Allow"
    actions   = ["ssm:GetParameters"]
    resources = concat(var.config_parameter_arns, local.secret_parameter_arns)
  }
  dynamic "statement" {
    for_each = length(var.secret_parameters) > 0 && var.secret_parameters_kms_key_arn != null ? [1] : []
    content {
      sid       = "DecryptSecrets"
      effect    = "Allow"
      actions   = ["kms:Decrypt"]
      resources = [var.secret_parameters_kms_key_arn]
      condition {
        test     = "StringEquals"
        variable = "kms:ViaService"
        values   = ["ssm.${data.aws_region.this.region}.amazonaws.com"]
      }
    }
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  count  = local.service_shape ? 1 : 0
  role   = aws_iam_role.execution[0].id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

resource "aws_ecs_cluster" "controller" {
  count = local.service_shape ? 1 : 0
  name  = var.name
  tags  = var.tags
}

resource "aws_launch_template" "capacity" {
  count                  = local.service_shape ? 1 : 0
  name_prefix            = "${var.name}-ecs-"
  image_id               = data.aws_ami.ecs[0].id
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.capacity[0].id]
  user_data = base64encode(join("\n", [
    "#!/bin/bash",
    "echo 'ECS_CLUSTER=${aws_ecs_cluster.controller[0].name}' >> /etc/ecs/ecs.config",
    "echo 'ECS_RESERVED_MEMORY=512' >> /etc/ecs/ecs.config",
    "echo 'ECS_CONTAINER_STOP_TIMEOUT=${var.stop_timeout_seconds}s' >> /etc/ecs/ecs.config",
    "echo 'ECS_AWSVPC_BLOCK_IMDS=true' >> /etc/ecs/ecs.config",
  ]))
  iam_instance_profile { name = aws_iam_instance_profile.ecs_host[0].name }
  dynamic "credit_specification" {
    for_each = data.aws_ec2_instance_type.this.burstable_performance_supported ? [1] : []
    content {
      cpu_credits = "unlimited"
    }
  }
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  block_device_mappings {
    device_name = data.aws_ami.ecs[0].root_device_name
    ebs {
      volume_type           = "gp3"
      volume_size           = var.root_volume_gib
      encrypted             = true
      delete_on_termination = true
    }
  }
  lifecycle {
    precondition {
      condition     = contains(data.aws_ec2_instance_type.this.supported_architectures, "arm64")
      error_message = "ECS capacity requires an arm64 instance type."
    }
  }
  tags = var.tags
}

resource "aws_autoscaling_group" "capacity" {
  count               = local.service_shape ? 1 : 0
  name                = "${var.name}-ecs-capacity"
  vpc_zone_identifier = var.subnet_ids
  min_size            = var.controller_count
  max_size            = var.controller_count
  desired_capacity    = var.controller_count
  health_check_type   = "EC2"
  launch_template {
    id      = aws_launch_template.capacity[0].id
    version = aws_launch_template.capacity[0].latest_version
  }
  dynamic "tag" {
    for_each = merge(var.tags, { Name = "${var.name}-ecs-host", AmazonECSManaged = "true" })
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }
}

resource "aws_ecs_capacity_provider" "controller" {
  count = local.service_shape ? 1 : 0
  name  = "${var.name}-capacity"
  auto_scaling_group_provider {
    auto_scaling_group_arn = aws_autoscaling_group.capacity[0].arn
    managed_scaling { status = "DISABLED" }
  }
  tags = var.tags
}

resource "aws_ecs_cluster_capacity_providers" "controller" {
  count              = local.service_shape ? 1 : 0
  cluster_name       = aws_ecs_cluster.controller[0].name
  capacity_providers = [aws_ecs_capacity_provider.controller[0].name]
}

resource "aws_cloudwatch_log_group" "controller" {
  count             = local.service_shape ? 1 : 0
  name              = "/ecs/${var.name}"
  retention_in_days = 30
  tags              = var.tags
}

resource "aws_ecs_task_definition" "controller" {
  count                    = local.service_shape ? 1 : 0
  family                   = var.name
  network_mode             = "awsvpc"
  requires_compatibilities = ["EC2"]
  cpu                      = "512"
  memory                   = "1536"
  task_role_arn            = module.instance_role.role_arn
  execution_role_arn       = aws_iam_role.execution[0].arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name           = "controller"
    image          = "${aws_ecr_repository.controller[0].repository_url}@${var.controller_image_digest}"
    essential      = true
    cpu            = 512
    memory         = 1536
    stopTimeout    = var.stop_timeout_seconds
    systemControls = [{ namespace = "net.ipv4.ip_unprivileged_port_start", value = "0" }]
    portMappings = [for port in distinct(concat([8080], [for group in values(var.target_groups) : group.port])) : {
      containerPort = port
      hostPort      = port
      protocol      = "tcp"
    }]
    environment = [
      { name = "AWS_REGION", value = data.aws_region.this.region },
      { name = "GANG_CONFIG_DIGEST", value = var.config_digest },
      { name = "GANG_SECRET_FILES", value = join("\n", [for i, file in sort(keys(var.secret_parameters)) : "${i}=${file}"]) },
    ]
    secrets = concat(
      [{ name = "GANG_CONFIG", valueFrom = "arn:${data.aws_partition.this.partition}:ssm:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:parameter${var.config_parameter_reference}" }],
      [for i, file in sort(keys(var.secret_parameters)) : { name = "GANG_SECRET_${i}", valueFrom = "arn:${data.aws_partition.this.partition}:ssm:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:parameter${var.secret_parameters[file]}" }]
    )
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.controller[0].name
        awslogs-region        = data.aws_region.this.region
        awslogs-stream-prefix = "controller"
      }
    }
  }])
  tags = var.tags
}

resource "aws_ecs_service" "controller" {
  count                              = local.service_shape ? 1 : 0
  name                               = var.name
  cluster                            = aws_ecs_cluster.controller[0].arn
  task_definition                    = aws_ecs_task_definition.controller[0].arn
  desired_count                      = var.controller_count
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  wait_for_steady_state              = true
  timeouts {
    create = "${max(1200, var.stop_timeout_seconds + 600)}s"
    update = "${max(1200, var.stop_timeout_seconds + 600)}s"
    delete = "${var.stop_timeout_seconds + 600}s"
  }
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.controller[0].name
    weight            = 1
  }
  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.security_group_id]
    assign_public_ip = false
  }
  dynamic "load_balancer" {
    for_each = var.target_groups
    content {
      target_group_arn = load_balancer.value.arn
      container_name   = "controller"
      container_port   = load_balancer.value.port
    }
  }
  depends_on = [module.instance_role, aws_ecs_cluster_capacity_providers.controller, aws_iam_role_policy_attachment.execution, aws_iam_role_policy.execution_secrets, aws_iam_role_policy_attachment.ecs_host]
  tags       = var.tags
}
