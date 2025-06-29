variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "ecs_cluster_name" {
  description = "ECS cluster name"
  type        = string
}

variable "ecs_service_name" {
  description = "ECS service name"
  type        = string
}

variable "alb_listener_arns" {
  description = "ALB listener ARNs"
  type        = list(string)
}

variable "blue_target_group_name" {
  description = "Blue target group name"
  type        = string
}

variable "green_target_group_name" {
  description = "Green target group name"
  type        = string
}

variable "blue_target_group_arn_suffix" {
  description = "Blue target group ARN suffix for CloudWatch"
  type        = string
}

variable "alb_arn_suffix" {
  description = "ALB ARN suffix for CloudWatch"
  type        = string
}

variable "notification_email" {
  description = "Email for deployment notifications"
  type        = string
  default     = ""
}

variable "enable_deployment_alarms" {
  description = "Enable CloudWatch alarms for deployments"
  type        = bool
  default     = true
}