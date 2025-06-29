output "cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.server.name
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.server.repository_url
}

output "task_definition_arn" {
  description = "Task definition ARN"
  value       = aws_ecs_task_definition.server.arn
}