# CodeDeploy Application
resource "aws_codedeploy_app" "ecs" {
  name             = "${var.name_prefix}-app"
  compute_platform = "ECS"
}

# CodeDeploy IAM Role
resource "aws_iam_role" "codedeploy" {
  name_prefix = "${var.name_prefix}-codedeploy-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "codedeploy.amazonaws.com"
      }
    }]
  })
}

# Attach AWS managed policy for ECS deployments
resource "aws_iam_role_policy_attachment" "codedeploy_ecs" {
  role       = aws_iam_role.codedeploy.name
  policy_arn = "arn:aws:iam::aws:policy/AWSCodeDeployRoleForECS"
}

# CodeDeploy Deployment Group
resource "aws_codedeploy_deployment_group" "ecs" {
  app_name               = aws_codedeploy_app.ecs.name
  deployment_group_name  = "${var.name_prefix}-ecs-dg"
  deployment_config_name = "CodeDeployDefault.ECSLinear10PercentEvery1Minutes"
  service_role_arn       = aws_iam_role.codedeploy.arn

  # ECS specific configuration
  ecs_service {
    cluster_name = var.ecs_cluster_name
    service_name = var.ecs_service_name
  }

  # Blue/Green deployment configuration
  blue_green_deployment_config {
    terminate_blue_instances_on_deployment_success {
      action                                          = "TERMINATE"
      termination_wait_time_in_minutes               = 5
    }

    deployment_ready_option {
      action_on_timeout = "CONTINUE_DEPLOYMENT"
    }

    green_fleet_provisioning_option {
      action = "COPY_AUTO_SCALING_GROUP"
    }
  }

  # Load balancer info for traffic shifting
  load_balancer_info {
    target_group_pair_info {
      prod_traffic_route {
        listener_arns = var.alb_listener_arns
      }

      target_group {
        name = var.blue_target_group_name
      }

      target_group {
        name = var.green_target_group_name
      }
    }
  }

  # Auto rollback configuration
  auto_rollback_configuration {
    enabled = true
    events  = ["DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_ALARM"]
  }

  # Deployment style
  deployment_style {
    deployment_option = "WITH_TRAFFIC_CONTROL"
    deployment_type   = "BLUE_GREEN"
  }
}

# CloudWatch Alarms for deployment monitoring
resource "aws_cloudwatch_metric_alarm" "target_response_time" {
  alarm_name          = "${var.name_prefix}-target-response-time"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "TargetResponseTime"
  namespace           = "AWS/ApplicationELB"
  period              = "60"
  statistic           = "Average"
  threshold           = "1"
  alarm_description   = "This metric monitors target response time"
  treat_missing_data  = "notBreaching"

  dimensions = {
    LoadBalancer = var.alb_arn_suffix
    TargetGroup  = var.blue_target_group_arn_suffix
  }
}

resource "aws_cloudwatch_metric_alarm" "unhealthy_hosts" {
  alarm_name          = "${var.name_prefix}-unhealthy-hosts"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "UnHealthyHostCount"
  namespace           = "AWS/ApplicationELB"
  period              = "60"
  statistic           = "Average"
  threshold           = "0"
  alarm_description   = "This metric monitors unhealthy hosts"
  treat_missing_data  = "breaching"

  dimensions = {
    LoadBalancer = var.alb_arn_suffix
    TargetGroup  = var.blue_target_group_arn_suffix
  }
}

# SNS Topic for deployment notifications
resource "aws_sns_topic" "deployments" {
  name = "${var.name_prefix}-deployments"
}

resource "aws_sns_topic_subscription" "deployment_email" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.deployments.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# Add CloudWatch alarms to deployment group
resource "aws_codedeploy_deployment_group" "ecs_with_alarms" {
  count = var.enable_deployment_alarms ? 1 : 0
  
  app_name               = aws_codedeploy_app.ecs.name
  deployment_group_name  = "${var.name_prefix}-ecs-dg-with-alarms"
  deployment_config_name = "CodeDeployDefault.ECSLinear10PercentEvery1Minutes"
  service_role_arn       = aws_iam_role.codedeploy.arn

  # Same configuration as above...
  ecs_service {
    cluster_name = var.ecs_cluster_name
    service_name = var.ecs_service_name
  }

  blue_green_deployment_config {
    terminate_blue_instances_on_deployment_success {
      action                                          = "TERMINATE"
      termination_wait_time_in_minutes               = 5
    }

    deployment_ready_option {
      action_on_timeout = "CONTINUE_DEPLOYMENT"
    }

    green_fleet_provisioning_option {
      action = "COPY_AUTO_SCALING_GROUP"
    }
  }

  load_balancer_info {
    target_group_pair_info {
      prod_traffic_route {
        listener_arns = var.alb_listener_arns
      }

      target_group {
        name = var.blue_target_group_name
      }

      target_group {
        name = var.green_target_group_name
      }
    }
  }

  auto_rollback_configuration {
    enabled = true
    events  = ["DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_ALARM", "DEPLOYMENT_STOP_ON_REQUEST"]
  }

  deployment_style {
    deployment_option = "WITH_TRAFFIC_CONTROL"
    deployment_type   = "BLUE_GREEN"
  }

  # Alarm configuration
  alarm_configuration {
    alarms  = [
      aws_cloudwatch_metric_alarm.target_response_time.alarm_name,
      aws_cloudwatch_metric_alarm.unhealthy_hosts.alarm_name
    ]
    enabled = true
  }

  # Trigger configuration for notifications
  trigger_configuration {
    trigger_events     = ["DeploymentStart", "DeploymentSuccess", "DeploymentFailure", "DeploymentStop", "DeploymentRollback"]
    trigger_name       = "${var.name_prefix}-deployment-trigger"
    trigger_target_arn = aws_sns_topic.deployments.arn
  }
}