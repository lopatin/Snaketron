output "vpc_id" {
  description = "VPC ID"
  value       = data.aws_vpc.default.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = local.public_subnets
}

output "private_subnet_ids" {
  description = "Private subnet IDs (or public if no private exist)"
  value       = local.selected_private_subnets
}

output "ecs_security_group_id" {
  description = "Security group ID for ECS tasks"
  value       = aws_security_group.ecs_tasks.id
}

output "alb_security_group_id" {
  description = "Security group ID for ALB"
  value       = aws_security_group.alb.id
}

output "rds_security_group_id" {
  description = "Security group ID for RDS"
  value       = aws_security_group.rds.id
}