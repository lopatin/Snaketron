# Network Load Balancer for SnakeTron
resource "aws_lb" "nlb" {
  name               = "${var.name_prefix}-nlb"
  internal           = false
  load_balancer_type = "network"
  subnets            = var.subnet_ids
  
  enable_deletion_protection = false
  enable_cross_zone_load_balancing = true
  
  tags = {
    Name        = "${var.name_prefix}-nlb"
    Environment = var.environment
  }
}

# Target Group for WebSocket traffic (port 8080)
resource "aws_lb_target_group" "websocket" {
  name                 = "${var.name_prefix}-ws-tg"
  port                 = 8080
  protocol             = "TCP"
  vpc_id               = var.vpc_id
  target_type          = "ip"
  deregistration_delay = 30
  
  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    protocol            = "TCP"
    port                = "8080"
  }
  
  tags = {
    Name        = "${var.name_prefix}-ws-tg"
    Environment = var.environment
  }
}

# Target Group for API/Static traffic (port 3001)
resource "aws_lb_target_group" "api" {
  name                 = "${var.name_prefix}-api-tg"
  port                 = 3001
  protocol             = "TCP"
  vpc_id               = var.vpc_id
  target_type          = "ip"
  deregistration_delay = 30
  
  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    protocol            = "HTTP"
    port                = "3001"
    path                = "/api/health"
    matcher             = "200"
  }
  
  tags = {
    Name        = "${var.name_prefix}-api-tg"
    Environment = var.environment
  }
}

# Listener for HTTPS traffic (port 443)
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.nlb.arn
  port              = "443"
  protocol          = "TLS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn
  
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# Listener for HTTP traffic (port 80) - forwards to API/static
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.nlb.arn
  port              = "80"
  protocol          = "TCP"
  
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# Listener for WebSocket traffic (port 8080)
resource "aws_lb_listener" "websocket" {
  load_balancer_arn = aws_lb.nlb.arn
  port              = "8080"
  protocol          = "TCP"
  
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.websocket.arn
  }
}

# CloudWatch alarms for NLB
resource "aws_cloudwatch_metric_alarm" "nlb_unhealthy_hosts" {
  alarm_name          = "${var.name_prefix}-nlb-unhealthy-hosts"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "UnHealthyHostCount"
  namespace           = "AWS/NetworkELB"
  period              = "60"
  statistic           = "Average"
  threshold           = "0"
  alarm_description   = "This metric monitors unhealthy NLB targets"
  treat_missing_data  = "breaching"

  dimensions = {
    TargetGroup  = aws_lb_target_group.websocket.arn_suffix
    LoadBalancer = aws_lb.nlb.arn_suffix
  }

  alarm_actions = var.alarm_actions
}

# Route53 DNS record (optional)
resource "aws_route53_record" "nlb" {
  count = var.domain_name != "" && var.zone_id != "" ? 1 : 0
  
  zone_id = var.zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_lb.nlb.dns_name
    zone_id                = aws_lb.nlb.zone_id
    evaluate_target_health = true
  }
}