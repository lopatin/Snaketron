output "endpoint" {
  description = "RDS endpoint"
  value       = aws_db_instance.postgres.endpoint
}

output "connection_string" {
  description = "PostgreSQL connection string"
  value       = "postgres://snaketron:${random_password.db_password.result}@${aws_db_instance.postgres.endpoint}/snaketron"
  sensitive   = true
}

output "password_secret_arn" {
  description = "ARN of the secret containing the database password"
  value       = aws_secretsmanager_secret.db_password.arn
}