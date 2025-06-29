#!/bin/bash
set -euo pipefail

# CodeDeploy deployment script for SnakeTron
# This script creates a deployment using AWS CodeDeploy

# Configuration
REGION="${AWS_REGION:-us-east-1}"
APP_NAME="${CODEDEPLOY_APP_NAME:-snaketron-prod-app}"
DEPLOYMENT_GROUP="${CODEDEPLOY_DEPLOYMENT_GROUP:-snaketron-prod-ecs-dg}"
DEPLOYMENT_CONFIG="${DEPLOYMENT_CONFIG:-CodeDeployDefault.ECSLinear10PercentEvery1Minutes}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
    exit 1
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! command -v aws &> /dev/null; then
        error "AWS CLI is not installed"
    fi
    
    if ! command -v jq &> /dev/null; then
        error "jq is not installed"
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured"
    fi
    
    log "Prerequisites check passed"
}

# Get current task definition
get_current_task_definition() {
    local cluster_name=$(aws ecs list-clusters --region "$REGION" | jq -r '.clusterArns[0]' | cut -d'/' -f2)
    local service_name=$(aws ecs list-services --cluster "$cluster_name" --region "$REGION" | jq -r '.serviceArns[0]' | cut -d'/' -f3)
    
    aws ecs describe-services \
        --cluster "$cluster_name" \
        --services "$service_name" \
        --region "$REGION" \
        --query 'services[0].taskDefinition' \
        --output text
}

# Create appspec content
create_appspec_content() {
    local task_definition_arn=$1
    local container_name=$2
    local container_port=$3
    
    cat <<EOF
{
  "version": 0.0,
  "Resources": [
    {
      "TargetService": {
        "Type": "AWS::ECS::Service",
        "Properties": {
          "TaskDefinition": "${task_definition_arn}",
          "LoadBalancerInfo": {
            "ContainerName": "${container_name}",
            "ContainerPort": ${container_port}
          }
        }
      }
    }
  ]
}
EOF
}

# Create deployment
create_deployment() {
    local task_definition_arn=$1
    local description="${2:-CodeDeploy deployment triggered by script}"
    
    log "Creating CodeDeploy deployment..."
    
    # Create appspec content
    local appspec_content=$(create_appspec_content "$task_definition_arn" "snaketron-server" 8080)
    
    # Create deployment
    local deployment_id=$(aws deploy create-deployment \
        --application-name "$APP_NAME" \
        --deployment-group-name "$DEPLOYMENT_GROUP" \
        --deployment-config-name "$DEPLOYMENT_CONFIG" \
        --description "$description" \
        --region "$REGION" \
        --revision "{
            \"revisionType\": \"AppSpecContent\",
            \"appSpecContent\": {
                \"content\": $(echo "$appspec_content" | jq -Rs .)
            }
        }" \
        --query 'deploymentId' \
        --output text)
    
    if [ -z "$deployment_id" ]; then
        error "Failed to create deployment"
    fi
    
    log "Created deployment: $deployment_id"
    echo "$deployment_id"
}

# Monitor deployment
monitor_deployment() {
    local deployment_id=$1
    local start_time=$(date +%s)
    local timeout=1800  # 30 minutes
    
    log "Monitoring deployment $deployment_id..."
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -gt $timeout ]; then
            error "Deployment timed out after $timeout seconds"
        fi
        
        # Get deployment status
        local deployment_info=$(aws deploy get-deployment \
            --deployment-id "$deployment_id" \
            --region "$REGION" \
            --query 'deploymentInfo')
        
        local status=$(echo "$deployment_info" | jq -r '.status')
        local deployment_overview=$(echo "$deployment_info" | jq -r '.deploymentOverview')
        
        # Display progress
        info "Status: $status"
        info "Overview: $(echo "$deployment_overview" | jq -c .)"
        
        case "$status" in
            "Succeeded")
                log "Deployment completed successfully!"
                return 0
                ;;
            "Failed")
                error "Deployment failed!"
                ;;
            "Stopped")
                error "Deployment was stopped!"
                ;;
            "Ready")
                warn "Deployment is ready but requires manual promotion"
                return 0
                ;;
        esac
        
        # Get deployment target info for ECS
        local target_info=$(aws deploy list-deployment-targets \
            --deployment-id "$deployment_id" \
            --region "$REGION" \
            --query 'targetIds[0]' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$target_info" ]; then
            local lifecycle_events=$(aws deploy get-deployment-target \
                --deployment-id "$deployment_id" \
                --target-id "$target_info" \
                --region "$REGION" \
                --query 'deploymentTarget.ecsTarget.lifecycleEvents' 2>/dev/null || echo "[]")
            
            if [ "$lifecycle_events" != "[]" ]; then
                info "Lifecycle events:"
                echo "$lifecycle_events" | jq -r '.[] | "\(.lifecycleEventName): \(.status)"'
            fi
        fi
        
        sleep 10
    done
}

# Main execution
main() {
    check_prerequisites
    
    # Parse command line arguments
    local task_definition_arn=""
    local description="Manual CodeDeploy deployment"
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --task-definition)
                task_definition_arn="$2"
                shift 2
                ;;
            --description)
                description="$2"
                shift 2
                ;;
            --app-name)
                APP_NAME="$2"
                shift 2
                ;;
            --deployment-group)
                DEPLOYMENT_GROUP="$2"
                shift 2
                ;;
            --deployment-config)
                DEPLOYMENT_CONFIG="$2"
                shift 2
                ;;
            *)
                error "Unknown option: $1"
                ;;
        esac
    done
    
    # If no task definition provided, use the current one
    if [ -z "$task_definition_arn" ]; then
        log "No task definition specified, using current task definition"
        task_definition_arn=$(get_current_task_definition)
        if [ -z "$task_definition_arn" ]; then
            error "Could not determine current task definition"
        fi
    fi
    
    info "Application: $APP_NAME"
    info "Deployment Group: $DEPLOYMENT_GROUP"
    info "Deployment Config: $DEPLOYMENT_CONFIG"
    info "Task Definition: $task_definition_arn"
    info "Description: $description"
    
    # Create deployment
    local deployment_id=$(create_deployment "$task_definition_arn" "$description")
    
    # Monitor deployment
    monitor_deployment "$deployment_id"
    
    log "Deployment process completed"
}

# Show usage
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Deploy ECS service using AWS CodeDeploy

Options:
    --task-definition ARN    Task definition ARN to deploy (optional, uses current if not specified)
    --description TEXT       Deployment description (default: "Manual CodeDeploy deployment")
    --app-name NAME          CodeDeploy application name (default: from environment or snaketron-prod-app)
    --deployment-group NAME  Deployment group name (default: from environment or snaketron-prod-ecs-dg)
    --deployment-config NAME Deployment configuration (default: CodeDeployDefault.ECSLinear10PercentEvery1Minutes)

Environment Variables:
    AWS_REGION               AWS region (default: us-east-1)
    CODEDEPLOY_APP_NAME      CodeDeploy application name
    CODEDEPLOY_DEPLOYMENT_GROUP  Deployment group name

Examples:
    # Deploy using current task definition
    $0

    # Deploy specific task definition
    $0 --task-definition arn:aws:ecs:us-east-1:123456789012:task-definition/snaketron-server:42

    # Deploy with custom description
    $0 --description "Deploy version 2.0.1 with bug fixes"
EOF
}

# Handle help flag
if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

# Run main function
main "$@"