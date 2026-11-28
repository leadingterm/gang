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

resource "aws_iam_role_policy_attachments_exclusive" "ecs_host" {
  count       = local.service_shape ? 1 : 0
  role_name   = aws_iam_role.ecs_host[0].name
  policy_arns = []
}

data "aws_iam_policy_document" "ecs_host" {
  # ECS container agent permissions; image pulls and logging use the execution role.
  statement {
    sid    = "EcsAgent"
    effect = "Allow"
    actions = [
      "ec2:DescribeTags",
      "ecs:DeregisterContainerInstance",
      "ecs:DiscoverPollEndpoint",
      "ecs:Poll",
      "ecs:RegisterContainerInstance",
      "ecs:StartTelemetrySession",
      "ecs:UpdateContainerInstancesState",
      "ecs:Submit*",
    ]
    resources = ["*"]
  }
  statement {
    sid       = "ListEcsTags"
    actions   = ["ecs:ListTagsForResource"]
    resources = ["arn:${data.aws_partition.this.partition}:ecs:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:task/${var.name}/*", "arn:${data.aws_partition.this.partition}:ecs:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:container-instance/${var.name}/*"]
  }
  statement {
    sid       = "TagEcsRegistration"
    actions   = ["ecs:TagResource"]
    resources = ["arn:${data.aws_partition.this.partition}:ecs:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:container-instance/${var.name}/*"]
    condition {
      test     = "StringEquals"
      variable = "ecs:CreateAction"
      values   = ["RegisterContainerInstance"]
    }
  }

  # Match the instance-role SSM core without parameter reads.
  statement {
    sid    = "SsmCore"
    effect = "Allow"
    actions = [
      "ssm:DescribeAssociation",
      "ssm:DescribeDocument",
      "ssm:GetDeployablePatchSnapshotForInstance",
      "ssm:GetDocument",
      "ssm:GetManifest",
      "ssm:ListAssociations",
      "ssm:ListInstanceAssociations",
      "ssm:PutComplianceItems",
      "ssm:PutConfigurePackageResult",
      "ssm:PutInventory",
      "ssm:UpdateAssociationStatus",
      "ssm:UpdateInstanceAssociationStatus",
      "ssm:UpdateInstanceInformation",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
      "ec2messages:AcknowledgeMessage",
      "ec2messages:DeleteMessage",
      "ec2messages:FailMessage",
      "ec2messages:GetEndpoint",
      "ec2messages:GetMessages",
      "ec2messages:SendReply",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ecs_host" {
  count  = local.service_shape ? 1 : 0
  role   = aws_iam_role.ecs_host[0].id
  policy = data.aws_iam_policy_document.ecs_host.json
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

resource "aws_iam_role_policy_attachments_exclusive" "execution" {
  count       = local.service_shape ? 1 : 0
  role_name   = aws_iam_role.execution[0].name
  policy_arns = []
}

data "aws_iam_policy_document" "execution" {
  count = local.service_shape ? 1 : 0

  statement {
    sid       = "EcrAuthorization"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid       = "PullControllerImage"
    actions   = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"]
    resources = [aws_ecr_repository.controller[0].arn]
  }
  statement {
    sid       = "WriteControllerLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.controller[0].arn}:*"]
  }
}

resource "aws_iam_role_policy" "execution" {
  count  = local.service_shape ? 1 : 0
  role   = aws_iam_role.execution[0].id
  policy = data.aws_iam_policy_document.execution[0].json
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
  vpc_security_group_ids = [var.security_group_id]
  user_data = base64encode(<<-USERDATA
    #!/bin/bash
    set -euo pipefail
    systemctl mask --now ecs.service
    dnf install -y dnsmasq amazon-ssm-agent iptables
    systemctl enable --now amazon-ssm-agent.service
    id gang-dns >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin gang-dns
    install -d -m 0755 /etc/gang-dns
    # systemd-resolved's stub is not an upstream; use its real resolver file if present.
    upstream=/etc/resolv.conf
    if [ -s /run/systemd/resolve/resolv.conf ]; then
      upstream=/run/systemd/resolve/resolv.conf
    fi
    awk '$1 == "nameserver" && $2 !~ /^127[.]/ && $2 != "::1" && $2 != "0.0.0.0" { print "nameserver " $2 }' "$upstream" > /etc/gang-dns/upstream.conf
    test -s /etc/gang-dns/upstream.conf
    chmod 0644 /etc/gang-dns/upstream.conf
    echo 'net.ipv4.ip_unprivileged_port_start=0' > /etc/sysctl.d/99-gang-host.conf
    sysctl -p /etc/sysctl.d/99-gang-host.conf
    cat > /etc/systemd/system/gang-dns.service <<'UNIT'
    [Unit]
    Description=Gang host DNS cache
    After=network-online.target
    Wants=network-online.target

    [Service]
    User=gang-dns
    Group=gang-dns
    ExecStart=/usr/sbin/dnsmasq --conf-file=/dev/null --keep-in-foreground --pid-file= --listen-address=127.0.0.1 --bind-interfaces --resolv-file=/etc/gang-dns/upstream.conf
    Restart=always

    [Install]
    WantedBy=multi-user.target
    UNIT
    cat > /etc/systemd/system/gang-host-network.service <<'UNIT'
    [Unit]
    Description=Prevent the controller from accessing instance credentials
    Before=ecs.service

    [Service]
    Type=oneshot
    RemainAfterExit=yes
    ExecStart=/bin/sh -c '/usr/sbin/iptables -C OUTPUT -d 169.254.169.254/32 -m owner --uid-owner 65532 -j REJECT || /usr/sbin/iptables -I OUTPUT 1 -d 169.254.169.254/32 -m owner --uid-owner 65532 -j REJECT'

    [Install]
    WantedBy=multi-user.target
    UNIT
    mkdir -p /etc/systemd/system/ecs.service.d
    cat > /etc/systemd/system/ecs.service.d/gang-dns.conf <<'UNIT'
    [Unit]
    Requires=gang-dns.service gang-host-network.service
    After=gang-dns.service gang-host-network.service
    UNIT
    systemctl daemon-reload
    systemctl enable --now gang-dns.service
    systemctl is-active --quiet gang-dns.service
    systemctl enable --now gang-host-network.service
    systemctl is-active --quiet gang-host-network.service
    cat >> /etc/ecs/ecs.config <<'CONFIG'
    ECS_CLUSTER=${aws_ecs_cluster.controller[0].name}
    ECS_RESERVED_MEMORY=512
    ECS_CONTAINER_STOP_TIMEOUT=${var.stop_timeout_seconds}s
    ECS_ENABLE_TASK_IAM_ROLE_NETWORK_HOST=true
    ECS_ENABLE_AWSLOGS_EXECUTIONROLE_OVERRIDE=true
    CONFIG
    systemctl unmask ecs.service
    systemctl enable ecs.service
    systemctl start --no-block ecs.service
  USERDATA
  )
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
  count                 = local.service_shape ? 1 : 0
  name                  = "${var.name}-ecs-capacity"
  vpc_zone_identifier   = var.subnet_ids
  min_size              = var.controller_count
  max_size              = var.controller_count + 1
  desired_capacity      = var.controller_count
  health_check_type     = "EC2"
  protect_from_scale_in = true
  launch_template {
    id      = aws_launch_template.capacity[0].id
    version = aws_launch_template.capacity[0].latest_version
  }
  instance_refresh {
    strategy = "Rolling"
    preferences {
      min_healthy_percentage       = 100
      max_healthy_percentage       = 200
      scale_in_protected_instances = "Refresh"
      skip_matching                = true
    }
  }
  dynamic "tag" {
    for_each = merge(var.tags, { Name = "${var.name}-ecs-host", AmazonECSManaged = "true" })
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }
  depends_on = [aws_iam_role_policy.ecs_host, aws_iam_role_policy_attachments_exclusive.ecs_host]

  # ECS managed scaling owns the desired capacity after creation.
  lifecycle {
    ignore_changes = [desired_capacity]
  }
}

resource "aws_ecs_capacity_provider" "controller" {
  count = local.service_shape ? 1 : 0
  name  = "${var.name}-capacity"
  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.capacity[0].arn
    managed_termination_protection = "ENABLED"
    managed_draining               = "ENABLED"
    managed_scaling {
      status                    = "ENABLED"
      target_capacity           = 100
      minimum_scaling_step_size = 1
      maximum_scaling_step_size = 1
    }
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
  network_mode             = "host"
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
    name      = "controller"
    image     = "${aws_ecr_repository.controller[0].repository_url}@${var.controller_image_digest}"
    essential = true
    cpu       = 512
    memory    = 1536
    # EC2 uses ECS_CONTAINER_STOP_TIMEOUT, which supports drain budgets above 120 seconds.
    dnsServers = ["127.0.0.1"]
    portMappings = [for port in distinct(concat([8080], [for group in values(var.target_groups) : group.port])) : {
      containerPort = port
      hostPort      = port
      protocol      = "tcp"
    }]
    environment = [
      { name = "AWS_REGION", value = data.aws_region.this.region },
      { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
      { name = "GANG_CONFIG_DIGEST", value = var.config_digest },
      { name = "GANG_SECRET_FILES", value = join("\n", [for i, file in sort(keys(var.secret_parameters)) : "${i}=${file}"]) },
    ]
    secrets = concat(
      [{ name = "GANG_CONFIG", valueFrom = "arn:${data.aws_partition.this.partition}:ssm:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:parameter${split(":", var.config_parameter_reference)[0]}" }],
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
  health_check_grace_period_seconds  = 300
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  # The deployment verifier owns convergence; migration must stop the old host first.
  wait_for_steady_state = false
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
  dynamic "load_balancer" {
    for_each = var.target_groups
    content {
      target_group_arn = load_balancer.value.arn
      container_name   = "controller"
      container_port   = load_balancer.value.port
    }
  }
  depends_on = [module.instance_role, aws_ecs_cluster_capacity_providers.controller, aws_iam_role_policy.execution, aws_iam_role_policy.execution_secrets, aws_iam_role_policy.ecs_host, aws_iam_role_policy_attachments_exclusive.ecs_host, aws_iam_role_policy_attachments_exclusive.execution]
  tags       = var.tags
}
