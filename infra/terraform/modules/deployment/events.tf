data "aws_caller_identity" "events" {
  count = var.aws_events_enabled ? 1 : 0
}

data "aws_region" "events" {
  count = var.aws_events_enabled ? 1 : 0
}

resource "aws_sqs_queue" "events_dlq" {
  count = var.aws_events_enabled ? 1 : 0

  name                      = "${var.name}-events-dlq"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = var.aws_events_dead_letter_retention_seconds
  tags                      = merge(var.tags, { Name = "${var.name}-events-dlq" })
}

resource "aws_sqs_queue" "events" {
  count = var.aws_events_enabled ? 1 : 0

  name                      = "${var.name}-events"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = var.aws_events_retention_seconds
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.events_dlq[0].arn
    maxReceiveCount     = var.aws_events_max_receive_count
  })
  tags = merge(var.tags, { Name = "${var.name}-events" })
}

resource "aws_cloudwatch_event_rule" "spot" {
  count = var.aws_events_enabled ? 1 : 0

  name = "${var.name}-spot-events"
  event_pattern = jsonencode({
    source        = ["aws.ec2"]
    account       = [data.aws_caller_identity.events[0].account_id]
    region        = [data.aws_region.events[0].region]
    "detail-type" = ["EC2 Instance Rebalance Recommendation", "EC2 Spot Instance Interruption Warning"]
  })
  tags = var.tags
}

resource "aws_cloudwatch_event_rule" "fleet" {
  count = var.aws_events_enabled ? 1 : 0

  name = "${var.name}-fleet-events"
  event_pattern = jsonencode({
    source        = ["aws.autoscaling"]
    account       = [data.aws_caller_identity.events[0].account_id]
    region        = [data.aws_region.events[0].region]
    "detail-type" = ["EC2 Instance Launch Successful", "EC2 Instance Launch Unsuccessful", "EC2 Instance Terminate Successful"]
    detail        = { AutoScalingGroupName = module.worker_fleet.asg_names }
  })
  tags = var.tags
}

resource "aws_sqs_queue_policy" "events" {
  count = var.aws_events_enabled ? 1 : 0

  queue_url = aws_sqs_queue.events[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowEventBridge"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.events[0].arn
        Condition = {
          ArnEquals    = { "aws:SourceArn" = [aws_cloudwatch_event_rule.spot[0].arn, aws_cloudwatch_event_rule.fleet[0].arn] }
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.events[0].account_id }
        }
      },
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "sqs:*"
        Resource  = aws_sqs_queue.events[0].arn
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
    ]
  })
}

resource "aws_sqs_queue_policy" "events_dlq" {
  count = var.aws_events_enabled ? 1 : 0

  queue_url = aws_sqs_queue.events_dlq[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "sqs:*"
      Resource  = aws_sqs_queue.events_dlq[0].arn
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_cloudwatch_event_target" "spot" {
  count = var.aws_events_enabled ? 1 : 0

  rule       = aws_cloudwatch_event_rule.spot[0].name
  arn        = aws_sqs_queue.events[0].arn
  depends_on = [aws_sqs_queue_policy.events]
}

resource "aws_cloudwatch_event_target" "fleet" {
  count = var.aws_events_enabled ? 1 : 0

  rule       = aws_cloudwatch_event_rule.fleet[0].name
  arn        = aws_sqs_queue.events[0].arn
  depends_on = [aws_sqs_queue_policy.events]
}
