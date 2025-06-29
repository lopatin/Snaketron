# DB subnet group
resource "aws_db_subnet_group" "main" {
  name_prefix = "${var.name_prefix}-"
  subnet_ids  = var.subnet_ids

  tags = {
    Name = "${var.name_prefix}-db-subnet-group"
  }
}

# DB parameter group
resource "aws_db_parameter_group" "postgres" {
  name_prefix = "${var.name_prefix}-postgres-"
  family      = "postgres15"

  parameter {
    name  = "shared_preload_libraries"
    value = "pg_stat_statements"
  }

  parameter {
    name  = "log_statement"
    value = "all"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# RDS instance
resource "aws_db_instance" "postgres" {
  identifier     = "${var.name_prefix}-db"
  engine         = "postgres"
  engine_version = "15.4"
  
  instance_class    = var.db_instance_class
  allocated_storage = var.db_storage_size
  storage_type      = "gp3"
  storage_encrypted = true
  
  db_name  = "snaketron"
  username = "snaketron"
  password = random_password.db_password.result
  
  vpc_security_group_ids = var.security_groups
  db_subnet_group_name   = aws_db_subnet_group.main.name
  parameter_group_name   = aws_db_parameter_group.postgres.name
  
  skip_final_snapshot = var.environment != "prod"
  deletion_protection = var.environment == "prod"
  
  backup_retention_period = 1  # Cost optimization
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  enabled_cloudwatch_logs_exports = ["postgresql"]
  
  performance_insights_enabled = false  # Cost optimization
  
  tags = {
    Name = "${var.name_prefix}-db"
  }
}

# Generate random password
resource "random_password" "db_password" {
  length  = 32
  special = true
}

# Store password in Secrets Manager
resource "aws_secretsmanager_secret" "db_password" {
  name_prefix = "${var.name_prefix}-db-password-"
  description = "RDS PostgreSQL password for ${var.name_prefix}"
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = random_password.db_password.result
}