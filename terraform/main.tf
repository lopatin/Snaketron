terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "snaketron-terraform-state"
    key    = "prod/terraform.tfstate"
    region = "us-east-1"
    encrypt = true
    dynamodb_table = "snaketron-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "SnakeTron"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

module "networking" {
  source = "./modules/networking"
  
  name_prefix = local.name_prefix
  environment = var.environment
}

module "rds" {
  source = "./modules/rds"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  vpc_id          = module.networking.vpc_id
  subnet_ids      = module.networking.private_subnet_ids
  security_groups = [module.networking.rds_security_group_id]
  
  db_instance_class = var.db_instance_class
  db_storage_size   = var.db_storage_size
}

module "load_balancing" {
  source = "./modules/load-balancing"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  vpc_id          = module.networking.vpc_id
  subnet_ids      = module.networking.public_subnet_ids
  security_groups = [module.networking.alb_security_group_id]
  
  certificate_arn = var.certificate_arn
}

module "ecs" {
  source = "./modules/ecs"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  vpc_id          = module.networking.vpc_id
  subnet_ids      = module.networking.private_subnet_ids
  security_groups = [module.networking.ecs_security_group_id]
  
  blue_target_group_arn = module.load_balancing.blue_target_group_arn
  
  task_cpu    = var.task_cpu
  task_memory = var.task_memory
  
  min_capacity = var.min_capacity
  max_capacity = var.max_capacity
  
  database_url = module.rds.connection_string
  jwt_secret   = var.jwt_secret
}

module "codedeploy" {
  source = "./modules/codedeploy"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  
  ecs_cluster_name = module.ecs.cluster_name
  ecs_service_name = module.ecs.service_name
  
  alb_listener_arns = module.load_balancing.listener_arns
  blue_target_group_name = module.load_balancing.blue_target_group_name
  green_target_group_name = module.load_balancing.green_target_group_name
  blue_target_group_arn_suffix = module.load_balancing.blue_target_group_arn_suffix
  alb_arn_suffix = module.load_balancing.alb_arn_suffix
  
  notification_email = var.deployment_notification_email
  enable_deployment_alarms = var.enable_deployment_alarms
}

module "s3_cloudfront" {
  source = "./modules/s3-cloudfront"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  domain_name     = var.client_domain_name
  certificate_arn = var.cloudfront_certificate_arn
}