variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS tasks"
  type        = list(string)
}

variable "security_groups" {
  description = "Security groups for ECS tasks"
  type        = list(string)
}

variable "websocket_target_group_arn" {
  description = "Target group ARN for WebSocket traffic"
  type        = string
}

variable "api_target_group_arn" {
  description = "Target group ARN for API/static traffic"
  type        = string
}

variable "task_cpu" {
  description = "Fargate task CPU units"
  type        = string
}

variable "task_memory" {
  description = "Fargate task memory in MB"
  type        = string
}

variable "min_capacity" {
  description = "Minimum number of tasks"
  type        = number
}

variable "max_capacity" {
  description = "Maximum number of tasks"
  type        = number
}

variable "database_url" {
  description = "Database connection URL"
  type        = string
  sensitive   = true
}

variable "jwt_secret" {
  description = "JWT secret"
  type        = string
  sensitive   = true
}