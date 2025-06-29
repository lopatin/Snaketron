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
  description = "ACM certificate ARN for ALB HTTPS"
  type        = string
  default     = ""
}

variable "client_domain_name" {
  description = "Domain name for web client"
  type        = string
  default     = "play.snaketron.io"
}

variable "cloudfront_certificate_arn" {
  description = "ACM certificate ARN for CloudFront (must be in us-east-1)"
  type        = string
  default     = ""
}

variable "deployment_notification_email" {
  description = "Email address for deployment notifications"
  type        = string
  default     = ""
}

variable "enable_deployment_alarms" {
  description = "Enable CloudWatch alarms for CodeDeploy"
  type        = bool
  default     = true
}