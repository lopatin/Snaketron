variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "snaketron"
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_storage_size" {
  description = "RDS storage size in GB"
  type        = number
  default     = 20
}

variable "task_cpu" {
  description = "Fargate task CPU units (256 = 0.25 vCPU)"
  type        = string
  default     = "256"
}

variable "task_memory" {
  description = "Fargate task memory in MB"
  type        = string
  default     = "512"
}

variable "min_capacity" {
  description = "Minimum number of tasks"
  type        = number
  default     = 3
}

variable "max_capacity" {
  description = "Maximum number of tasks"
  type        = number
  default     = 9
}

variable "jwt_secret" {
  description = "JWT secret for authentication"
  type        = string
  sensitive   = true
}

variable "certificate_arn" {
  description = "ACM certificate ARN for NLB HTTPS"
  type        = string
  default     = ""
}

variable "server_domain_name" {
  description = "Domain name for server (e.g., api.snaketron.io)"
  type        = string
  default     = ""
}

variable "route53_zone_id" {
  description = "Route53 hosted zone ID"
  type        = string
  default     = ""
}