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

module "nlb" {
  source = "./modules/nlb"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  vpc_id          = module.networking.vpc_id
  subnet_ids      = module.networking.public_subnet_ids
  
  certificate_arn = var.certificate_arn
  domain_name     = var.server_domain_name
  zone_id         = var.route53_zone_id
}

module "ecs" {
  source = "./modules/ecs"
  
  name_prefix     = local.name_prefix
  environment     = var.environment
  vpc_id          = module.networking.vpc_id
  subnet_ids      = module.networking.private_subnet_ids
  security_groups = [module.networking.ecs_security_group_id]
  
  websocket_target_group_arn = module.nlb.websocket_target_group_arn
  api_target_group_arn      = module.nlb.api_target_group_arn
  
  task_cpu    = var.task_cpu
  task_memory = var.task_memory
  
  min_capacity = var.min_capacity
  max_capacity = var.max_capacity
  
  database_url = module.rds.connection_string
  jwt_secret   = var.jwt_secret
}

# CodeDeploy module removed - using simple ECS deployments with NLB

# S3/CloudFront module removed - static files served from Rust server