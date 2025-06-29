output "alb_dns_name" {
  description = "DNS name of the load balancer"
  value       = aws_lb.main.dns_name
}

output "alb_zone_id" {
  description = "Zone ID of the load balancer"
  value       = aws_lb.main.zone_id
}

output "blue_target_group_arn" {
  description = "ARN of the blue target group"
  value       = aws_lb_target_group.websocket_blue.arn
}

output "green_target_group_arn" {
  description = "ARN of the green target group"
  value       = aws_lb_target_group.websocket_green.arn
}

output "blue_target_group_name" {
  description = "Name of the blue target group"
  value       = aws_lb_target_group.websocket_blue.name
}

output "green_target_group_name" {
  description = "Name of the green target group"
  value       = aws_lb_target_group.websocket_green.name
}

output "blue_target_group_arn_suffix" {
  description = "ARN suffix of the blue target group"
  value       = aws_lb_target_group.websocket_blue.arn_suffix
}

output "alb_arn_suffix" {
  description = "ARN suffix of the ALB"
  value       = aws_lb.main.arn_suffix
}

output "listener_arns" {
  description = "List of listener ARNs"
  value       = concat(
    [aws_lb_listener.http.arn],
    var.certificate_arn != "" ? [aws_lb_listener.https[0].arn] : []
  )
}