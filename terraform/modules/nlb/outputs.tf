output "nlb_arn" {
  description = "ARN of the Network Load Balancer"
  value       = aws_lb.nlb.arn
}

output "nlb_dns_name" {
  description = "DNS name of the Network Load Balancer"
  value       = aws_lb.nlb.dns_name
}

output "nlb_zone_id" {
  description = "Zone ID of the Network Load Balancer"
  value       = aws_lb.nlb.zone_id
}

output "websocket_target_group_arn" {
  description = "ARN of the WebSocket target group"
  value       = aws_lb_target_group.websocket.arn
}

output "api_target_group_arn" {
  description = "ARN of the API target group"
  value       = aws_lb_target_group.api.arn
}

output "websocket_target_group_name" {
  description = "Name of the WebSocket target group"
  value       = aws_lb_target_group.websocket.name
}

output "api_target_group_name" {
  description = "Name of the API target group"
  value       = aws_lb_target_group.api.name
}