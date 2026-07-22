#!/usr/bin/env bash
# Deterministic local resilience suite plus an explicitly opted-in staging run:
# policy scale-out, forced ownership 1 -> 10 -> 1, then policy scale-in.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mode="${1:-local}"
staging_redis_control_url=""
staging_traefik_metrics_control_url=""
# The staging EXIT trap runs after Bash has unwound run_staging_suite on a
# set -e failure. Keep the state needed by that trap at script scope; function
# locals are no longer available by the time an EXIT trap runs.
report_dir=""
scaling_resource=""
original_desired=""
scaling_state=""
load_pid=""
capacity_pid=""
admission_population_pid=""
idle_population_pid=""
lobby_population_pid=""
matchmaking_population_pid=""
traefik_monitor_pid=""
traefik_monitor_dir=""

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

unix_time_ms() {
  jq -nr 'now * 1000 | floor'
}

sanitize_task_definition_evidence() {
  jq '
    {
      taskDefinition: {
        taskDefinitionArn: .taskDefinition.taskDefinitionArn,
        family: .taskDefinition.family,
        revision: .taskDefinition.revision,
        status: .taskDefinition.status,
        networkMode: .taskDefinition.networkMode,
        requiresCompatibilities: .taskDefinition.requiresCompatibilities,
        cpu: .taskDefinition.cpu,
        memory: .taskDefinition.memory,
        runtimePlatform: .taskDefinition.runtimePlatform,
        ephemeralStorage: .taskDefinition.ephemeralStorage,
        containerDefinitions: [
          .taskDefinition.containerDefinitions[]
          | {
              name,
              image,
              essential,
              linuxParameters: (
                if .linuxParameters == null
                then null
                else {initProcessEnabled: .linuxParameters.initProcessEnabled}
                end
              )
            }
        ]
      }
    }
  '
}

assert_task_definition_evidence_sanitized() {
  jq -e '
    (.taskDefinition.taskDefinitionArn | type == "string" and length > 0)
    and (.taskDefinition.containerDefinitions | type == "array" and length > 0)
    and all(.taskDefinition.containerDefinitions[];
      (.name | type == "string" and length > 0)
      and (.image | type == "string" and length > 0)
      and (.essential | type == "boolean")
      and ([keys[]
        | select(
            . != "name"
            and . != "image"
            and . != "essential"
            and . != "linuxParameters")
      ] | length == 0)
      and (
        .linuxParameters == null
        or ([.linuxParameters | keys[]
          | select(. != "initProcessEnabled")]
          | length == 0)
      )
      and (has("environment") | not)
      and (has("environmentFiles") | not)
      and (has("secrets") | not)
      and (has("repositoryCredentials") | not)
      and (has("logConfiguration") | not)
      and (has("dockerLabels") | not)
      and (has("healthCheck") | not)
      and (has("command") | not)
      and (has("entryPoint") | not))
  ' "$@"
}

select_verified_task_service_name() {
  local environment="$1"
  local region="$2"
  local aws_region="$3"
  local origin="$4"
  local redis_url="$5"
  local router_service_key="$6"
  jq -er \
    --arg environment "$environment" \
    --arg region "$region" \
    --arg aws_region "$aws_region" \
    --arg origin "$origin" \
    --arg redis_url "$redis_url" \
    --arg router_service_key "$router_service_key" '
      ([.taskDefinition.containerDefinitions[]
          | select(.name == "snaketron-server")
          | .environment[]
          | {key: .name, value: .value}
        ] | from_entries) as $server_environment
      | select(
          $server_environment.SNAKETRON_ENVIRONMENT == $environment
          and $server_environment.SNAKETRON_REGION == $region
          and $server_environment.SNAKETRON_AWS_REGION == $aws_region
          and $server_environment.SNAKETRON_ORIGIN == $origin
          and $server_environment.SNAKETRON_REDIS_URL == $redis_url
          and $server_environment.AWS_REGION == "us-east-1"
          and $server_environment.DYNAMODB_TABLE_PREFIX == ("snaketron-" + $environment)
          and $server_environment.DYNAMODB_ENDPOINT == "")
      | .taskDefinition.containerDefinitions[]
      | select(.name == "snaketron-server")
      | .dockerLabels[$router_service_key]
      | select(type == "string" and length > 0)
    '
}

test_task_definition_evidence_sanitizer() {
  local sensitive="fixture-sensitive-value-do-not-persist"
  local sanitized
  sanitized="$(
    jq -n --arg sensitive "$sensitive" '
      {
        taskDefinition: {
          taskDefinitionArn: "arn:aws:ecs:us-east-1:111111111111:task-definition/fixture:7",
          family: "fixture",
          revision: 7,
          cpu: "512",
          memory: "1024",
          containerDefinitions: [
            {
              name: "server",
              image: "111111111111.dkr.ecr.us-east-1.amazonaws.com/fixture:commit",
              essential: true,
              linuxParameters: {initProcessEnabled: true},
              environment: [{name: "SECRET", value: $sensitive}],
              environmentFiles: [{type: "s3", value: $sensitive}],
              secrets: [{name: "SECRET", valueFrom: $sensitive}],
              repositoryCredentials: {credentialsParameter: $sensitive},
              dockerLabels: {"unrelated.label": $sensitive},
              healthCheck: {command: ["CMD-SHELL", $sensitive]},
              logConfiguration: {
                options: {token: $sensitive},
                secretOptions: [{name: "token", valueFrom: $sensitive}]
              },
              command: [$sensitive],
              entryPoint: [$sensitive]
            },
            {name: "sidecar", image: "fixture-sidecar:latest", essential: false}
          ]
        }
      }
    ' | sanitize_task_definition_evidence
  )" || {
    echo "Task-definition evidence sanitizer could not process its fixture" >&2
    return 1
  }

  if [[ "$sanitized" == *"$sensitive"* ]]; then
    echo "Task-definition evidence retained forbidden fixture data" >&2
    return 1
  fi

  if ! printf '%s\n' "$sanitized" \
    | assert_task_definition_evidence_sanitized >/dev/null; then
    echo "Task-definition evidence sanitizer produced an unsafe shape" >&2
    return 1
  fi
  if ! printf '%s\n' "$sanitized" \
    | jq -e '
        .taskDefinition.family == "fixture"
        and .taskDefinition.revision == 7
        and .taskDefinition.cpu == "512"
        and .taskDefinition.memory == "1024"
        and (.taskDefinition.containerDefinitions | length) == 2
        and .taskDefinition.containerDefinitions[0].essential == true
        and .taskDefinition.containerDefinitions[0].linuxParameters.initProcessEnabled == true
        and .taskDefinition.containerDefinitions[1].essential == false
      ' >/dev/null; then
    echo "Task-definition evidence sanitizer removed required structural fields" >&2
    return 1
  fi
}

test_live_task_definition_gate() {
  local router_service_key="traefik.http.routers.snaketron-dev.service"
  local fixture
  fixture="$(jq -n --arg router_service_key "$router_service_key" '
    {
      taskDefinition: {
        containerDefinitions: [{
          name: "snaketron-server",
          environment: [
            {name: "SNAKETRON_ENVIRONMENT", value: "dev"},
            {name: "SNAKETRON_REGION", value: "use1"},
            {name: "SNAKETRON_AWS_REGION", value: "us-east-1"},
            {name: "SNAKETRON_ORIGIN", value: "https://stg-123-1.snaketron.io"},
            {name: "SNAKETRON_REDIS_URL", value: "redis://fixture.cache.amazonaws.com:6379/"},
            {name: "AWS_REGION", value: "us-east-1"},
            {name: "DYNAMODB_TABLE_PREFIX", value: "snaketron-dev"},
            {name: "DYNAMODB_ENDPOINT", value: ""}
          ],
          dockerLabels: {($router_service_key): "snaketron-dev-use1"}
        }]
      }
    }
  ')"

  local service_name
  service_name="$(printf '%s\n' "$fixture" \
    | select_verified_task_service_name \
      dev use1 us-east-1 \
      https://stg-123-1.snaketron.io \
      redis://fixture.cache.amazonaws.com:6379/ \
      "$router_service_key")" || {
    echo "Live task-definition gate rejected its safe fixture" >&2
    return 1
  }
  if [[ "$service_name" != "snaketron-dev-use1" ]]; then
    echo "Live task-definition gate returned the wrong Traefik service" >&2
    return 1
  fi

  local mutation
  for mutation in AWS_REGION DYNAMODB_TABLE_PREFIX DYNAMODB_ENDPOINT; do
    local unsafe
    unsafe="$(printf '%s\n' "$fixture" | jq --arg mutation "$mutation" '
      .taskDefinition.containerDefinitions[0].environment |= map(
        if .name == $mutation then .value = "unsafe" else . end)
    ')"
    if printf '%s\n' "$unsafe" \
      | select_verified_task_service_name \
        dev use1 us-east-1 \
        https://stg-123-1.snaketron.io \
        redis://fixture.cache.amazonaws.com:6379/ \
        "$router_service_key" >/dev/null 2>&1; then
      echo "Live task-definition gate accepted unsafe $mutation" >&2
      return 1
    fi
  done
}

test_evidence_safety_helpers() {
  test_task_definition_evidence_sanitizer
  test_live_task_definition_gate
}

run_offline_cdk_synth() {
  local development_synth_dir
  local production_synth_dir
  development_synth_dir="$(mktemp -d)"
  production_synth_dir="$(mktemp -d)"
  if [[ -z "$development_synth_dir" || ! -d "$development_synth_dir" \
    || -z "$production_synth_dir" || ! -d "$production_synth_dir" ]]; then
    echo "Could not create a temporary CDK assembly directory" >&2
    return 1
  fi
  local expires_at_epoch=$(( $(date -u +%s) + 3600 ))
  local lookup_context='"availability-zones:account=111111111111:region=us-east-1":["us-east-1a","us-east-1b"],"availability-zones:account=111111111111:region=eu-west-1":["eu-west-1a","eu-west-1b"],"hosted-zone:account=111111111111:domainName=snaketron.io:region=us-east-1":{"Id":"/hostedzone/ZDUMMYIO","Name":"snaketron.io."},"hosted-zone:account=111111111111:domainName=snaketron.com:region=us-east-1":{"Id":"/hostedzone/ZDUMMYCOM","Name":"snaketron.com."},"hosted-zone:account=111111111111:domainName=snaketron.io:region=eu-west-1":{"Id":"/hostedzone/ZDUMMYIO","Name":"snaketron.io."},"hosted-zone:account=111111111111:domainName=snaketron.com:region=eu-west-1":{"Id":"/hostedzone/ZDUMMYCOM","Name":"snaketron.com."}'
  local development_context
  local production_context
  development_context="{\"environment\":\"development\",\"ephemeral\":\"true\",\"ephemeralRunId\":\"1-1\",\"expiresAtEpoch\":\"$expires_at_epoch\",\"imageTag\":\"0000000000000000000000000000000000000000\",$lookup_context}"
  production_context="{\"environment\":\"production\",$lookup_context}"

  if ! (
    cd "$repo_dir/../cdk"
    CDK_DEFAULT_ACCOUNT=111111111111 \
    CDK_DEFAULT_REGION=us-east-1 \
    CDK_CONTEXT_JSON="$development_context" \
    CDK_OUTDIR="$development_synth_dir" \
    SNAKETRON_JWT_SECRET=offline-synth-only-not-a-real-secret \
    AWS_EC2_METADATA_DISABLED=true \
      npm exec -- ts-node --prefer-ts-exts bin/snaketron-stack.ts >/dev/null
  ) || [[ ! -f "$development_synth_dir/manifest.json" ]]; then
    rm -rf -- "$development_synth_dir" "$production_synth_dir"
    echo "Offline development CDK synth failed" >&2
    return 1
  fi
  if ! (
    cd "$repo_dir/../cdk"
    CDK_DEFAULT_ACCOUNT=111111111111 \
    CDK_DEFAULT_REGION=us-east-1 \
    CDK_CONTEXT_JSON="$production_context" \
    CDK_OUTDIR="$production_synth_dir" \
    SNAKETRON_JWT_SECRET=offline-synth-only-not-a-real-secret \
    AWS_EC2_METADATA_DISABLED=true \
      npm exec -- ts-node --prefer-ts-exts bin/snaketron-stack.ts >/dev/null
  ) || [[ ! -f "$production_synth_dir/manifest.json" ]]; then
    rm -rf -- "$development_synth_dir" "$production_synth_dir"
    echo "Offline production CDK synth failed" >&2
    return 1
  fi
  rm -rf -- "$development_synth_dir" "$production_synth_dir"
}

run_local_suite() {
  require_command npm
  require_command cargo
  require_command wasm-pack
  require_command curl
  require_command jq
  test_evidence_safety_helpers
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -n 1 PING | grep -qx PONG || {
      echo "Redis database 1 is required at 127.0.0.1:6379" >&2
      exit 1
    }
  else
    require_command docker
    local valkey_container="${SNAKETRON_VALKEY_CONTAINER:-snaketron-valkey}"
    docker inspect "$valkey_container" >/dev/null 2>&1 || {
      echo "redis-cli is absent and Valkey container $valkey_container was not found" >&2
      exit 1
    }
    docker exec "$valkey_container" valkey-cli -n 1 PING | grep -qx PONG || {
      echo "Valkey database 1 is not reachable in container $valkey_container" >&2
      exit 1
    }
  fi
  curl -fsS http://127.0.0.1:4566/_localstack/health >/dev/null || {
    echo "LocalStack is required at 127.0.0.1:4566" >&2
    exit 1
  }

  export SNAKETRON_REDIS_URL="redis://127.0.0.1:6379/1?protocol=resp3"
  export AWS_ENDPOINT_URL="http://127.0.0.1:4566"
  export AWS_REGION="us-east-1"
  export AWS_ACCESS_KEY_ID="test"
  export AWS_SECRET_ACCESS_KEY="test"

  cd "$repo_dir"
  cargo test --workspace -- --test-threads=1

  (cd client && wasm-pack build --target web --out-dir pkg)
  npm --prefix client/web run type-check
  npm --prefix client/web run test:unit
  npm --prefix client/web run test:drain
  npm --prefix client/web run build:prod
  npm --prefix ../cdk test
  run_offline_cdk_synth
}

require_staging_environment() {
  local required=(
    SNAKETRON_STAGING_CONFIRM
    SNAKETRON_STAGING_TARGET
    SNAKETRON_STAGING_ACCOUNT_ID
    SNAKETRON_STAGING_ENVIRONMENT
    SNAKETRON_ECS_CLUSTER
    SNAKETRON_ECS_SERVICE
    SNAKETRON_AWS_REGION
    SNAKETRON_REGION_CODE
    SNAKETRON_STAGING_REDIS_URL
    SNAKETRON_VALKEY_REPLICATION_GROUP_ID
    SNAKETRON_TRAEFIK_INSTANCE_ID
    SNAKETRON_TRAEFIK_METRICS_URL
  )
  for name in "${required[@]}"; do
    if [[ -z "${!name:-}" ]]; then
      echo "$name is required for --staging" >&2
      exit 1
    fi
  done
  if [[ "$SNAKETRON_STAGING_CONFIRM" != "RUN_SNAKETRON_STAGING_CHAOS" ]]; then
    echo "Set SNAKETRON_STAGING_CONFIRM=RUN_SNAKETRON_STAGING_CHAOS" >&2
    exit 1
  fi
  if [[ "$SNAKETRON_STAGING_ENVIRONMENT" == "prod" || "$SNAKETRON_STAGING_ENVIRONMENT" == "production" ]]; then
    echo "The staging runner refuses a production Environment tag" >&2
    exit 1
  fi
  if [[ "$SNAKETRON_STAGING_TARGET" != https://* ]]; then
    echo "SNAKETRON_STAGING_TARGET must be an HTTPS origin" >&2
    exit 1
  fi
  local target_host="${SNAKETRON_STAGING_TARGET#*://}"
  target_host="${target_host%%/*}"
  target_host="${target_host%%:*}"
  case "$target_host" in
    snaketron.io|api.snaketron.io|use1.snaketron.io|euw1.snaketron.io)
      echo "The staging runner refuses the production host $target_host" >&2
      exit 1
      ;;
  esac
  if [[ "$SNAKETRON_ECS_CLUSTER" == *prod* || "$SNAKETRON_ECS_SERVICE" == *prod* ]]; then
    echo "The staging runner refuses prod-labeled ECS cluster or service identifiers" >&2
    exit 1
  fi
}

url_host() {
  local authority="${1#*://}"
  authority="${authority%%/*}"
  authority="${authority##*@}"
  if [[ "$authority" == \[* ]]; then
    authority="${authority#\[}"
    printf '%s\n' "${authority%%\]*}"
  else
    printf '%s\n' "${authority%%:*}"
  fi
}

is_loopback_url() {
  local host
  host="$(url_host "$1")"
  case "$host" in
    localhost|127.*|::1) return 0 ;;
    *) return 1 ;;
  esac
}

configure_staging_control_urls() {
  staging_redis_control_url="${SNAKETRON_STAGING_REDIS_CONTROL_URL:-$SNAKETRON_STAGING_REDIS_URL}"
  staging_traefik_metrics_control_url="${SNAKETRON_TRAEFIK_METRICS_CONTROL_URL:-$SNAKETRON_TRAEFIK_METRICS_URL}"

  local tunneled=false
  if [[ "$staging_redis_control_url" != "$SNAKETRON_STAGING_REDIS_URL" ]]; then
    is_loopback_url "$staging_redis_control_url" || {
      echo "A differing SNAKETRON_STAGING_REDIS_CONTROL_URL must use a loopback tunnel" >&2
      return 1
    }
    tunneled=true
  fi
  if [[ "$staging_traefik_metrics_control_url" != "$SNAKETRON_TRAEFIK_METRICS_URL" ]]; then
    is_loopback_url "$staging_traefik_metrics_control_url" || {
      echo "A differing SNAKETRON_TRAEFIK_METRICS_CONTROL_URL must use a loopback tunnel" >&2
      return 1
    }
    tunneled=true
  fi
  if [[ "$tunneled" == true \
    && "${SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID:-}" != "$SNAKETRON_TRAEFIK_INSTANCE_ID" ]]; then
    echo "Tunneled controls require SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID to equal the verified Traefik instance ID" >&2
    return 1
  fi
}

retry_command() {
  local attempts="$1"
  shift
  local attempt=1
  local delay=2
  while ! "$@"; do
    if (( attempt >= attempts )); then
      return 1
    fi
    sleep "$delay"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
  done
}

assert_ecs_tags() {
  local file="$1"
  local resource="$2"
  jq -e \
    --arg environment "$SNAKETRON_STAGING_ENVIRONMENT" \
    --arg region "$SNAKETRON_REGION_CODE" '
      ([.tags[] | {key: .key, value: .value}] | from_entries) as $tags
      | $tags.Project == "Snaketron"
        and $tags.Environment == $environment
        and $tags.Region == $region
        and $tags.ManagedBy == "CDK"
    ' "$file" >/dev/null || {
      echo "$resource does not have the confirmed non-production deployment tags" >&2
      return 1
    }
}

assert_aws_tags() {
  local file="$1"
  local array_path="$2"
  local resource="$3"
  jq -e \
    --arg array_path "$array_path" \
    --arg environment "$SNAKETRON_STAGING_ENVIRONMENT" \
    --arg region "$SNAKETRON_REGION_CODE" '
      (if $array_path == "TagList" then .TagList else .Tags end
        | [.[] | {key: .Key, value: .Value}] | from_entries) as $tags
      | $tags.Project == "Snaketron"
        and $tags.Environment == $environment
        and $tags.Region == $region
        and $tags.ManagedBy == "CDK"
    ' "$file" >/dev/null || {
      echo "$resource does not have the confirmed non-production deployment tags" >&2
      return 1
    }
}

verify_staging_identity() {
  local evidence_dir="$1"
  mkdir -p "$evidence_dir/identity"
  local identity_dir="$evidence_dir/identity"

  local outer_repo_dir="$repo_dir/.."
  if [[ -n "$(git -C "$outer_repo_dir" status --porcelain --untracked-files=all)" ]]; then
    echo "The outer snaketron-io checkout has tracked or untracked changes; staging evidence must run from the exact deployed commit" >&2
    return 1
  fi
  if [[ -n "$(git -C "$repo_dir" status --porcelain --untracked-files=all)" ]]; then
    echo "The Snaketron submodule checkout has tracked or untracked changes; staging tools must match the committed gitlink" >&2
    return 1
  fi
  local runner_submodule_commit
  local expected_submodule_commit
  runner_submodule_commit="$(git -C "$repo_dir" rev-parse HEAD)"
  expected_submodule_commit="$(git -C "$outer_repo_dir" rev-parse HEAD:snaketron)" || {
    echo "The outer checkout does not contain the expected Snaketron gitlink" >&2
    return 1
  }
  if [[ ! "$expected_submodule_commit" =~ ^[0-9a-f]{40}$ ]] \
    || [[ "$runner_submodule_commit" != "$expected_submodule_commit" ]]; then
    echo "Snaketron checkout $runner_submodule_commit does not match outer gitlink $expected_submodule_commit" >&2
    return 1
  fi

  local caller_account
  caller_account="$(aws sts get-caller-identity --query Account --output text)"
  if [[ "$caller_account" != "$SNAKETRON_STAGING_ACCOUNT_ID" ]]; then
    echo "AWS caller account $caller_account does not match confirmed staging account $SNAKETRON_STAGING_ACCOUNT_ID" >&2
    return 1
  fi

  aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" \
    --include TAGS >"$identity_dir/ecs-service.json"
  jq -e '(.failures | length) == 0 and (.services | length) == 1' \
    "$identity_dir/ecs-service.json" >/dev/null || {
      echo "The named ECS service was not found exactly once" >&2
      return 1
    }
  staging_service_arn="$(jq -r '.services[0].serviceArn' "$identity_dir/ecs-service.json")"
  staging_task_definition_arn="$(jq -r '.services[0].taskDefinition' "$identity_dir/ecs-service.json")"
  jq -e --arg task_definition "$staging_task_definition_arn" '
    .services[0] as $service
    | ($service.deployments | length) == 1
      and $service.deployments[0].status == "PRIMARY"
      and $service.deployments[0].rolloutState == "COMPLETED"
      and $service.deployments[0].taskDefinition == $task_definition
      and $service.deployments[0].runningCount == $service.runningCount
      and $service.deployments[0].pendingCount == 0
      and $service.deployments[0].failedTasks == 0
  ' "$identity_dir/ecs-service.json" >/dev/null || {
    echo "The ECS service is not bound to one completed primary deployment" >&2
    return 1
  }
  aws ecs list-tags-for-resource \
    --region "$SNAKETRON_AWS_REGION" \
    --resource-arn "$staging_service_arn" >"$identity_dir/ecs-service-tags.json"
  assert_ecs_tags "$identity_dir/ecs-service-tags.json" "ECS service"

  aws ecs describe-clusters \
    --region "$SNAKETRON_AWS_REGION" \
    --clusters "$SNAKETRON_ECS_CLUSTER" \
    --include TAGS SETTINGS >"$identity_dir/ecs-cluster.json"
  jq -e '(.failures | length) == 0 and (.clusters | length) == 1' \
    "$identity_dir/ecs-cluster.json" >/dev/null || {
      echo "The named ECS cluster was not found exactly once" >&2
      return 1
    }
  staging_cluster_arn="$(jq -r '.clusters[0].clusterArn' "$identity_dir/ecs-cluster.json")"
  aws ecs list-tags-for-resource \
    --region "$SNAKETRON_AWS_REGION" \
    --resource-arn "$staging_cluster_arn" >"$identity_dir/ecs-cluster-tags.json"
  assert_ecs_tags "$identity_dir/ecs-cluster-tags.json" "ECS cluster"
  jq -e '.clusters[0].settings[]
    | select(.name == "containerInsights" and .value == "enabled")' \
    "$identity_dir/ecs-cluster.json" >/dev/null || {
      echo "Staging ECS cluster must have Container Insights enabled" >&2
      return 1
    }

  aws ecs describe-task-definition \
    --region "$SNAKETRON_AWS_REGION" \
    --task-definition "$staging_task_definition_arn" \
    | sanitize_task_definition_evidence >"$identity_dir/task-definition.json"
  assert_task_definition_evidence_sanitized \
    "$identity_dir/task-definition.json" >/dev/null || {
      echo "The saved task-definition evidence retained sensitive fields or lost its required shape" >&2
      return 1
    }
  staging_image_uri="$(jq -er '
    .taskDefinition.containerDefinitions[]
    | select(.name == "snaketron-server")
    | .image
    | select(type == "string" and length > 0)
  ' "$identity_dir/task-definition.json")" || {
    echo "The verified task definition lacks the Snaketron server image" >&2
    return 1
  }
  if [[ ! "$staging_image_uri" =~ ^([^/]+)/(.+):([^:/]+)$ ]]; then
    echo "The staging server image is not a tagged ECR image" >&2
    return 1
  fi
  local staging_image_registry="${BASH_REMATCH[1]}"
  local staging_image_repository="${BASH_REMATCH[2]}"
  local staging_image_tag="${BASH_REMATCH[3]}"
  local staging_image_registry_id="${staging_image_registry%%.*}"
  if [[ "$staging_image_registry_id" != "$caller_account" ]]; then
    echo "The staging server image is not hosted in the confirmed staging account" >&2
    return 1
  fi
  aws ecr describe-images \
    --region "$SNAKETRON_AWS_REGION" \
    --registry-id "$staging_image_registry_id" \
    --repository-name "$staging_image_repository" \
    --image-ids imageTag="$staging_image_tag" \
    >"$identity_dir/server-image.json"
  staging_image_digest="$(jq -er '
    select((.imageDetails | length) == 1)
    | .imageDetails[0].imageDigest
    | select(test("^sha256:[0-9a-f]{64}$"))
  ' "$identity_dir/server-image.json")" || {
    echo "The task-definition image tag did not resolve to one ECR digest" >&2
    return 1
  }
  staging_image_commit="$(jq -er '
    [.imageDetails[0].imageTags[]
      | select(test("^[0-9a-f]{40}$"))]
    | unique
    | select(length == 1)
    | .[0]
  ' "$identity_dir/server-image.json")" || {
    echo "The deployed image digest is not bound to exactly one full commit tag" >&2
    return 1
  }
  local runner_checkout_commit
  runner_checkout_commit="$(git -C "$repo_dir/.." rev-parse HEAD)"
  if [[ "$runner_checkout_commit" != "$staging_image_commit" ]]; then
    echo "The staging image commit $staging_image_commit does not match runner checkout $runner_checkout_commit" >&2
    return 1
  fi

  aws elasticache describe-replication-groups \
    --region "$SNAKETRON_AWS_REGION" \
    --replication-group-id "$SNAKETRON_VALKEY_REPLICATION_GROUP_ID" \
    >"$identity_dir/valkey.json"
  staging_valkey_arn="$(jq -r '.ReplicationGroups[0].ARN' "$identity_dir/valkey.json")"
  staging_valkey_host="$(jq -r '.ReplicationGroups[0].NodeGroups[0].PrimaryEndpoint.Address' "$identity_dir/valkey.json")"
  staging_valkey_port="$(jq -r '.ReplicationGroups[0].NodeGroups[0].PrimaryEndpoint.Port' "$identity_dir/valkey.json")"
  staging_valkey_cluster_id="$(jq -r '.ReplicationGroups[0].MemberClusters[0]' "$identity_dir/valkey.json")"
  jq -e '
    (.ReplicationGroups | length) == 1
    and .ReplicationGroups[0].Status == "available"
    and (.ReplicationGroups[0].MemberClusters | length) == 1
  ' "$identity_dir/valkey.json" >/dev/null || {
    echo "The named Valkey replication group is not one available single-node group" >&2
    return 1
  }
  aws elasticache list-tags-for-resource \
    --region "$SNAKETRON_AWS_REGION" \
    --resource-name "$staging_valkey_arn" >"$identity_dir/valkey-tags.json"
  assert_aws_tags "$identity_dir/valkey-tags.json" TagList "Valkey replication group"
  aws elasticache describe-cache-clusters \
    --region "$SNAKETRON_AWS_REGION" \
    --cache-cluster-id "$staging_valkey_cluster_id" \
    --show-cache-node-info >"$identity_dir/valkey-cache-cluster.json"
  staging_valkey_parameter_group="$(jq -r '.CacheClusters[0].CacheParameterGroup.CacheParameterGroupName' "$identity_dir/valkey-cache-cluster.json")"
  aws elasticache describe-cache-parameters \
    --region "$SNAKETRON_AWS_REGION" \
    --cache-parameter-group-name "$staging_valkey_parameter_group" \
    --source user >"$identity_dir/valkey-parameters.json"
  jq -e '.Parameters[] | select(.ParameterName == "maxmemory-policy" and .ParameterValue == "noeviction")' \
    "$identity_dir/valkey-parameters.json" >/dev/null || {
      echo "Staging Valkey must explicitly use maxmemory-policy=noeviction" >&2
      return 1
    }

  local expected_redis_url="redis://$staging_valkey_host:$staging_valkey_port/"
  if [[ "$SNAKETRON_STAGING_REDIS_URL" != "$expected_redis_url"* ]]; then
    echo "SNAKETRON_STAGING_REDIS_URL does not name the tagged Valkey primary" >&2
    return 1
  fi
  local target_origin
  target_origin="$(printf '%s' "$SNAKETRON_STAGING_TARGET" | sed 's:/*$::')"
  local router_service_key="traefik.http.routers.snaketron-${SNAKETRON_STAGING_ENVIRONMENT}.service"
  local task_service_name
  # Verify the live immutable task-definition revision without writing its raw
  # environment or arbitrary labels into the evidence tree. The selected
  # routing label is constrained below and recorded in verified-deployment.json.
  task_service_name="$(
    aws ecs describe-task-definition \
      --region "$SNAKETRON_AWS_REGION" \
      --task-definition "$staging_task_definition_arn" \
    | select_verified_task_service_name \
      "$SNAKETRON_STAGING_ENVIRONMENT" \
      "$SNAKETRON_REGION_CODE" \
      "$SNAKETRON_AWS_REGION" \
      "$target_origin" \
      "$expected_redis_url" \
      "$router_service_key"
  )" || {
    echo "The ECS task definition does not match the confirmed environment, DynamoDB/Valkey targets, and Traefik route" >&2
    return 1
  }
  if [[ ! "$task_service_name" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "The task definition's Traefik service name is malformed" >&2
    return 1
  fi
  staging_traefik_service_label="${task_service_name}@ecs"

  aws ec2 describe-instances \
    --region "$SNAKETRON_AWS_REGION" \
    --instance-ids "$SNAKETRON_TRAEFIK_INSTANCE_ID" >"$identity_dir/traefik-instance.json"
  jq '.Reservations[0].Instances[0]' "$identity_dir/traefik-instance.json" \
    >"$identity_dir/traefik-instance-flat.json"
  assert_aws_tags "$identity_dir/traefik-instance-flat.json" Tags "Traefik instance"
  staging_traefik_ip="$(jq -r '.PublicIpAddress' "$identity_dir/traefik-instance-flat.json")"
  staging_traefik_private_ip="$(jq -r '.PrivateIpAddress' "$identity_dir/traefik-instance-flat.json")"
  jq -e '.State.Name == "running" and (.PublicIpAddress | type == "string")' \
    "$identity_dir/traefik-instance-flat.json" >/dev/null || {
      echo "The tagged Traefik instance is not running with a public IP" >&2
      return 1
    }

  local target_host
  target_host="$(printf '%s' "$target_origin" | sed -E 's#^https://##; s#:[0-9]+$##')"
  if ! dig +short A "$target_host" | grep -Fx "$staging_traefik_ip" >/dev/null; then
    echo "$target_host does not resolve to tagged Traefik instance $staging_traefik_ip" >&2
    return 1
  fi
  curl -fsS --max-time 5 "$target_origin/api/health" \
    | jq -e '.status == "ok"' >/dev/null || {
      echo "The staging target health probe failed" >&2
      return 1
    }
  curl -fsS --max-time 5 \
    --resolve "$target_host:443:$staging_traefik_ip" \
    "$target_origin/api/health" | jq -e '.status == "ok"' >/dev/null || {
      echo "A direct TLS probe through the tagged Traefik instance failed" >&2
      return 1
    }

  local metrics_host
  metrics_host="$(printf '%s' "$SNAKETRON_TRAEFIK_METRICS_URL" \
    | sed -E 's#^https?://##; s#[:/].*$##')"
  local metrics_addresses
  metrics_addresses="$(dig +short A "$metrics_host" || true)"
  if [[ "$metrics_host" != "$staging_traefik_ip" && "$metrics_host" != "$staging_traefik_private_ip" ]] \
    && ! grep -Fx "$staging_traefik_ip" <<<"$metrics_addresses" >/dev/null \
    && ! grep -Fx "$staging_traefik_private_ip" <<<"$metrics_addresses" >/dev/null; then
    echo "Traefik metrics URL does not resolve to the tagged Traefik instance" >&2
    return 1
  fi
  curl -fsS --max-time 5 "$staging_traefik_metrics_control_url" \
    >"$identity_dir/traefik-metrics.prom" || {
      echo "The configured Traefik metrics control URL must be reachable from the staging runner" >&2
      return 1
    }
  grep -F "service=\"$staging_traefik_service_label\"" "$identity_dir/traefik-metrics.prom" \
    | grep -q '^traefik_' || {
      echo "Traefik metrics lack derived service label $staging_traefik_service_label" >&2
      return 1
    }

  jq -n \
    --arg account "$caller_account" \
    --arg environment "$SNAKETRON_STAGING_ENVIRONMENT" \
    --arg region "$SNAKETRON_REGION_CODE" \
    --arg service_arn "$staging_service_arn" \
    --arg cluster_arn "$staging_cluster_arn" \
    --arg task_definition_arn "$staging_task_definition_arn" \
    --arg image_uri "$staging_image_uri" \
    --arg image_digest "$staging_image_digest" \
    --arg commit "$staging_image_commit" \
    --arg runner_checkout_commit "$runner_checkout_commit" \
    --arg runner_submodule_commit "$runner_submodule_commit" \
    --arg expected_submodule_commit "$expected_submodule_commit" \
    --arg valkey_arn "$staging_valkey_arn" \
    --arg valkey_primary_host "$staging_valkey_host" \
    --argjson valkey_primary_port "$staging_valkey_port" \
    --arg traefik_instance_id "$SNAKETRON_TRAEFIK_INSTANCE_ID" \
    --arg traefik_private_ip "$staging_traefik_private_ip" \
    --arg traefik_service_label "$staging_traefik_service_label" \
    --arg target_origin "$target_origin" \
    --arg control_tunnel_instance_id "${SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID:-}" \
    '{
      account: $account,
      deployment_tags: {
        Project: "Snaketron",
        Environment: $environment,
        Region: $region,
        ManagedBy: "CDK"
      },
      ecs_service_arn: $service_arn,
      ecs_cluster_arn: $cluster_arn,
      task_definition_arn: $task_definition_arn,
      image_uri: $image_uri,
      image_digest: $image_digest,
      commit: $commit,
      runner_checkout_commit: $runner_checkout_commit,
      runner_submodule_commit: $runner_submodule_commit,
      expected_submodule_commit: $expected_submodule_commit,
      valkey_arn: $valkey_arn,
      valkey_primary: {
        host: $valkey_primary_host,
        port: $valkey_primary_port
      },
      traefik_instance_id: $traefik_instance_id,
      traefik_private_ip: $traefik_private_ip,
      traefik_service_label: $traefik_service_label,
      target_origin: $target_origin,
      control_tunnel_instance_id: (
        if $control_tunnel_instance_id == "" then null
        else $control_tunnel_instance_id
        end
      )
    }' >"$identity_dir/verified-deployment.json"
}

verify_scaling_policies() {
  local identity_dir="$1/identity"
  aws application-autoscaling describe-scaling-policies \
    --region "$SNAKETRON_AWS_REGION" \
    --service-namespace ecs \
    --resource-id "$scaling_resource" \
    --scalable-dimension ecs:service:DesiredCount \
    >"$identity_dir/scaling-policies.json"
  jq -e '
    def target($metric; $value):
      [.ScalingPolicies[]
        | select(
            .PolicyType == "TargetTrackingScaling"
            and .TargetTrackingScalingPolicyConfiguration.PredefinedMetricSpecification.PredefinedMetricType == $metric
            and .TargetTrackingScalingPolicyConfiguration.TargetValue == $value
            and .TargetTrackingScalingPolicyConfiguration.ScaleInCooldown == 60
            and .TargetTrackingScalingPolicyConfiguration.ScaleOutCooldown == 60
            and (.TargetTrackingScalingPolicyConfiguration.DisableScaleIn // false) == false
          )]
      | length == 1;
    (.ScalingPolicies | length) == 2
    and target("ECSServiceAverageCPUUtilization"; 70)
    and target("ECSServiceAverageMemoryUtilization"; 80)
  ' "$identity_dir/scaling-policies.json" >/dev/null || {
    echo "Staging must have only CPU=70% and memory=80% target tracking with 60-second cooldowns" >&2
    return 1
  }
}

wait_for_running_count() {
  local wanted="$1"
  local deadline=$((SECONDS + 600))
  while (( SECONDS < deadline )); do
    local counts
    counts="$(aws ecs describe-services \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --services "$SNAKETRON_ECS_SERVICE" \
      --query 'services[0].[desiredCount,runningCount,pendingCount]' \
      --output text)"
    if [[ "$counts" == "$wanted"$'\t'"$wanted"$'\t'"0" ]]; then
      return 0
    fi
    sleep 5
  done
  echo "ECS did not converge to $wanted running tasks within ten minutes" >&2
  return 1
}

wait_for_policy_activity() {
  local started_at_epoch="$1"
  local output="$2"
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    aws application-autoscaling describe-scaling-activities \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --resource-id "$scaling_resource" \
      --scalable-dimension ecs:service:DesiredCount \
      --max-results 50 >"$output"
    if jq -e --argjson started "$started_at_epoch" '
      [.ScalingActivities[] |
        select(
          .StatusCode == "Successful"
          and ((.StartTime
            | sub("\\.[0-9]+\\+00:00$"; "Z")
            | sub("\\.[0-9]+Z$"; "Z")
            | sub("\\+00:00$"; "Z")
            | fromdateiso8601) >= $started)
          and (.Cause | test("alarm|target.tracking"; "i"))
        )
      ] | length > 0
    ' "$output" >/dev/null; then
      return 0
    fi
    sleep 5
  done
  echo "No successful CPU/memory target-tracking scaling activity appeared after the observation began" >&2
  return 1
}

wait_for_automatic_scale_out() {
  local report_dir="$1"
  local started_at_epoch="$2"
  local observed_pid="${3:-}"
  # Target tracking needs three one-minute alarm periods, may begin just after
  # a bucket boundary, and is not useful until the added Fargate task is
  # RUNNING. Eight minutes avoids racing that normal observation pipeline.
  local deadline=$((SECONDS + 480))
  while (( SECONDS < deadline )); do
    if [[ -n "$observed_pid" ]] && ! kill -0 "$observed_pid" 2>/dev/null; then
      local load_exit=0
      wait "$observed_pid" || load_exit=$?
      echo "Scale-out load runner exited with status $load_exit before target tracking added capacity" >&2
      return 1
    fi
    local candidate="$report_dir/automatic-scale-out.pending.json"
    aws ecs describe-services \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --services "$SNAKETRON_ECS_SERVICE" >"$candidate"
    local desired
    local running
    desired="$(jq -r '.services[0].desiredCount' "$candidate")"
    running="$(jq -r '.services[0].runningCount' "$candidate")"
    if (( desired > 1 && running > 1 )); then
      mv "$candidate" "$report_dir/automatic-scale-out.json"
      wait_for_policy_activity "$started_at_epoch" \
        "$report_dir/automatic-scale-out-activities.json"
      return 0
    fi
    sleep 5
  done
  echo "CPU/memory autoscaling did not scale the staging service above one task" >&2
  return 1
}

wait_for_automatic_scale_in() {
  local report_dir="$1"
  local started_at_epoch="$2"
  # Application Auto Scaling's managed low alarms require fifteen one-minute
  # datapoints. Leave time for bucket alignment, alarm/action propagation, and
  # the final ECS task stop after that observation window.
  local deadline=$((SECONDS + 1200))
  local decrease_observed=false
  while (( SECONDS < deadline )); do
    local candidate="$report_dir/automatic-scale-in.pending.json"
    aws ecs describe-services \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --services "$SNAKETRON_ECS_SERVICE" >"$candidate"
    local desired
    local running
    local pending
    desired="$(jq -r '.services[0].desiredCount' "$candidate")"
    running="$(jq -r '.services[0].runningCount' "$candidate")"
    pending="$(jq -r '.services[0].pendingCount' "$candidate")"
    if (( desired < 10 )); then
      decrease_observed=true
    fi
    if [[ "$decrease_observed" == true && "$desired" == "1" && "$running" == "1" && "$pending" == "0" ]]; then
      mv "$candidate" "$report_dir/automatic-scale-in.json"
      wait_for_policy_activity "$started_at_epoch" \
        "$report_dir/automatic-scale-in-activities.json"
      return 0
    fi
    sleep 5
  done
  echo "After load removal, CPU/memory autoscaling did not reduce the ten-task service to one within twenty minutes" >&2
  return 1
}

start_traefik_monitor() {
  traefik_monitor_dir="$1/traefik"
  mkdir -p "$traefik_monitor_dir"
  (
    local sequence=0
    while true; do
      sequence=$((sequence + 1))
      local sample
      printf -v sample '%s/%06d.prom' "$traefik_monitor_dir" "$sequence"
      if ! curl -fsS --max-time 3 "$staging_traefik_metrics_control_url" >"$sample"; then
        mv "$sample" "$sample.error"
      fi
      sleep 2
    done
  ) &
  traefik_monitor_pid=$!
}

stop_traefik_monitor() {
  local monitor_pid="${traefik_monitor_pid:-}"
  if [[ -n "$monitor_pid" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi
  traefik_monitor_pid=""
}

assert_traefik_monitor() {
  local report_dir="$1"
  local sample_count=0
  local zero_ready_count=0
  local error_count=0
  local sample
  for sample in "$traefik_monitor_dir"/*.prom.error; do
    [[ -e "$sample" ]] || continue
    error_count=$((error_count + 1))
  done
  for sample in "$traefik_monitor_dir"/*.prom; do
    [[ -e "$sample" ]] || continue
    sample_count=$((sample_count + 1))
    if ! grep -F "service=\"$staging_traefik_service_label\"" "$sample" \
      | awk '
          /^traefik_service_server_up{/ && ($NF + 0) > 0 { healthy = 1 }
          END { exit(healthy ? 0 : 1) }
        '; then
      zero_ready_count=$((zero_ready_count + 1))
    fi
  done
  jq -n \
    --argjson samples "$sample_count" \
    --argjson scrape_errors "$error_count" \
    --argjson zero_healthy_backend_samples "$zero_ready_count" \
    '{
      samples: $samples,
      scrape_errors: $scrape_errors,
      zero_healthy_backend_samples: $zero_healthy_backend_samples
    }' >"$report_dir/traefik-summary.json"
  if (( sample_count < 10 || error_count > 0 || zero_ready_count > 0 )); then
    echo "Traefik evidence was incomplete or observed a zero-healthy-backend sample; see traefik-summary.json" >&2
    return 1
  fi
}

capture_ecs_health() {
  local report_dir="$1"
  local label="$2"
  local expected="$3"
  local phase_dir="$report_dir/ecs-$label"
  mkdir -p "$phase_dir"
  aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" >"$phase_dir/service.json"
  local task_arns
  task_arns="$(aws ecs list-tasks \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service-name "$SNAKETRON_ECS_SERVICE" \
    --desired-status RUNNING \
    --query 'taskArns[]' --output text)"
  if [[ -z "$task_arns" ]]; then
    echo "ECS phase $label has no running tasks" >&2
    return 1
  fi
  # Task ARNs contain no whitespace; intentional splitting supplies AWS CLI's
  # variadic --tasks argument.
  aws ecs describe-tasks \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --tasks $task_arns >"$phase_dir/tasks.json"
  local health_observed_at_ms=$(( $(date -u +%s) * 1000 ))
  local first_health_observations="$phase_dir/healthy-first-observations.jsonl"
  [[ -f "$first_health_observations" ]] || : >"$first_health_observations"
  while IFS=$'\t' read -r task_id private_ipv4; do
    if grep -Fq "\"task_id\":\"$task_id\"" "$first_health_observations"; then
      continue
    fi
    jq -cn \
      --arg task_id "$task_id" \
      --arg private_ipv4 "$private_ipv4" \
      --argjson healthy_observed_at_unix_ms "$health_observed_at_ms" '
        {
          task_id: $task_id,
          private_ipv4: $private_ipv4,
          healthy_observed_at_unix_ms: $healthy_observed_at_unix_ms
        }
      ' >>"$first_health_observations"
  done < <(jq -r \
    --arg task_definition "$staging_task_definition_arn" \
    --arg image "$staging_image_uri" \
    --arg digest "$staging_image_digest" '
      .tasks[]
      | select(
          .lastStatus == "RUNNING"
          and .healthStatus == "HEALTHY"
          and .taskDefinitionArn == $task_definition
          and any(.containers[];
            .name == "snaketron-server"
            and .image == $image
            and .imageDigest == $digest)
        )
      | [
          (.taskArn | split("/")[-1]),
          ([.attachments[].details[]
            | select(.name == "privateIPv4Address")
            | .value][0])
        ]
      | @tsv
    ' "$phase_dir/tasks.json")
  jq -e \
    --argjson expected "$expected" \
    --arg task_definition "$staging_task_definition_arn" '
    .services[0] as $service
    | $service.desiredCount == $expected
      and $service.runningCount == $expected
      and $service.pendingCount == 0
      and ($service.deployments | length) == 1
      and $service.deployments[0].status == "PRIMARY"
      and $service.deployments[0].rolloutState == "COMPLETED"
      and $service.deployments[0].taskDefinition == $task_definition
  ' "$phase_dir/service.json" >/dev/null \
    && jq -e \
      --argjson expected "$expected" \
      --arg task_definition "$staging_task_definition_arn" \
      --arg image "$staging_image_uri" \
      --arg digest "$staging_image_digest" '
      (.failures | length) == 0
      and (.tasks | length) == $expected
      and all(.tasks[];
        .lastStatus == "RUNNING"
        and .healthStatus == "HEALTHY"
        and .taskDefinitionArn == $task_definition
        and ([.containers[]
          | select(
              .name == "snaketron-server"
              and .image == $image
              and .imageDigest == $digest
            )] | length) == 1
        and ([.attachments[].details[]
          | select(.name == "privateIPv4Address")
          | .value
          | select(type == "string" and length > 0)] | length) == 1)
    ' "$phase_dir/tasks.json" >/dev/null || {
      echo "ECS phase $label was not exactly $expected healthy tasks on the verified deployment and image" >&2
      return 1
    }
  jq -s '{tasks: (sort_by(.task_id))}' "$first_health_observations" \
    >"$phase_dir/healthy-observation.json"
  jq -e \
    --argjson expected "$expected" \
    --slurpfile described "$phase_dir/tasks.json" '
      (.tasks | length) == $expected
      and ([.tasks[].task_id] | unique | sort)
        == ([$described[0].tasks[].taskArn | split("/")[-1]] | unique | sort)
    ' "$phase_dir/healthy-observation.json" >/dev/null || {
      echo "ECS phase $label lacks a first-health timestamp for every task" >&2
      return 1
    }
}

wait_for_ecs_health() {
  local report_dir="$1"
  local label="$2"
  local expected="$3"
  local phase_dir="$report_dir/ecs-$label"
  mkdir -p "$phase_dir"
  : >"$phase_dir/healthy-first-observations.jsonl"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if capture_ecs_health "$report_dir" "$label" "$expected" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  capture_ecs_health "$report_dir" "$label" "$expected" || true
  echo "ECS phase $label did not become fully healthy within two minutes" >&2
  return 1
}

wait_for_traefik_task_readiness() {
  local report_dir="$1"
  local label="$2"
  local phase_dir="$report_dir/ecs-$label"
  local healthy_observation="$phase_dir/healthy-observation.json"
  local readiness_dir="$phase_dir/traefik-readiness"
  mkdir -p "$readiness_dir"
  local observations="$readiness_dir/observations.jsonl"
  : >"$observations"
  local deadline_ms
  deadline_ms="$(jq -r '[.tasks[].healthy_observed_at_unix_ms] | max + 10000' \
    "$healthy_observation")"
  local sequence=0

  while true; do
    sequence=$((sequence + 1))
    local observed_at_ms
    local sample
    printf -v sample '%s/%03d.prom' "$readiness_dir" "$sequence"
    if curl -fsS --max-time 2 "$staging_traefik_metrics_control_url" >"$sample"; then
      observed_at_ms=$(( $(date -u +%s) * 1000 ))
      while IFS=$'\t' read -r task_id private_ipv4 healthy_observed_at_ms; do
        if grep -Fq "\"task_id\":\"$task_id\"" "$observations"; then
          continue
        fi
        if awk \
          -v service="$staging_traefik_service_label" \
          -v ip="$private_ipv4" '
            index($0, "traefik_service_server_up{") == 1
              && index($0, "service=\"" service "\"")
              && index($0, "url=\"http://" ip ":8080")
              && ($NF + 0) > 0 { found = 1 }
            END { exit(found ? 0 : 1) }
          ' "$sample"; then
          jq -cn \
            --arg task_id "$task_id" \
            --arg private_ipv4 "$private_ipv4" \
            --argjson healthy_observed_at_unix_ms "$healthy_observed_at_ms" \
            --argjson server_up_observed_at_unix_ms "$observed_at_ms" '
              {
                task_id: $task_id,
                private_ipv4: $private_ipv4,
                healthy_observed_at_unix_ms: $healthy_observed_at_unix_ms,
                server_up_observed_at_unix_ms: $server_up_observed_at_unix_ms,
                propagation_upper_bound_ms:
                  ($server_up_observed_at_unix_ms - $healthy_observed_at_unix_ms)
              }
            ' >>"$observations"
        fi
      done < <(jq -r '
        .tasks[]
        | [.task_id, .private_ipv4, .healthy_observed_at_unix_ms]
        | @tsv
      ' "$healthy_observation")
    else
      observed_at_ms=$(( $(date -u +%s) * 1000 ))
      mv "$sample" "$sample.error"
    fi

    local observed_count
    observed_count="$(wc -l <"$observations" | tr -d ' ')"
    local expected_count
    expected_count="$(jq -r '.tasks | length' "$healthy_observation")"
    if [[ "$observed_count" == "$expected_count" ]]; then
      break
    fi
    if (( observed_at_ms >= deadline_ms )); then
      break
    fi
    sleep 2
  done

  jq -s '{tasks: .}' "$observations" >"$readiness_dir/summary.json"
  jq -e \
    --slurpfile healthy "$healthy_observation" '
      ([.tasks[].task_id] | unique | sort)
        == ([$healthy[0].tasks[].task_id] | unique | sort)
      and all(.tasks[];
        .propagation_upper_bound_ms >= 0
        and .propagation_upper_bound_ms <= 10000)
    ' "$readiness_dir/summary.json" >/dev/null || {
      echo "Traefik did not expose every healthy $label task as server_up within ten seconds" >&2
      return 1
    }
}

collect_ecs_runtime_evidence() {
  local report_dir="$1"
  local ecs_dir="$report_dir/ecs-runtime"
  mkdir -p "$ecs_dir"
  aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" >"$ecs_dir/service.json"
  local stopped_arns
  stopped_arns="$(aws ecs list-tasks \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service-name "$SNAKETRON_ECS_SERVICE" \
    --desired-status STOPPED \
    --query 'taskArns[]' --output text)"
  if [[ -n "$stopped_arns" ]]; then
    # See capture_ecs_health: ARN whitespace splitting is intentional.
    aws ecs describe-tasks \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --tasks $stopped_arns >"$ecs_dir/stopped-tasks.json"
  else
    jq -n '{tasks: [], failures: []}' >"$ecs_dir/stopped-tasks.json"
  fi

  jq -e --argjson started "$evidence_started_epoch" '
    def epoch:
      sub("\\.[0-9]+\\+00:00$"; "Z")
      | sub("\\.[0-9]+Z$"; "Z")
      | sub("\\+00:00$"; "Z")
      | fromdateiso8601;
    [.tasks[] |
      select((.stoppedAt | epoch) >= $started)
      | select(
          .stopCode == "EssentialContainerExited"
          or ((.stoppedReason // "") | test("unhealthy|out.of.memory|failed"; "i"))
        )
    ] | length == 0
  ' "$ecs_dir/stopped-tasks.json" >/dev/null \
    && jq -e \
      --argjson started "$evidence_started_epoch" \
      --arg task_definition "$staging_task_definition_arn" '
      def epoch:
        sub("\\.[0-9]+\\+00:00$"; "Z")
        | sub("\\.[0-9]+Z$"; "Z")
        | sub("\\+00:00$"; "Z")
        | fromdateiso8601;
      .services[0] as $service
      | ($service.deployments | length) == 1
      and $service.deployments[0].status == "PRIMARY"
      and $service.deployments[0].rolloutState == "COMPLETED"
      and $service.deployments[0].taskDefinition == $task_definition
      and ([.services[0].events[] |
        select((.createdAt | epoch) >= $started)
        | select(.message | test("unhealthy|failed to|was unable|insufficient"; "i"))
      ] | length) == 0
    ' "$ecs_dir/service.json" >/dev/null || {
      echo "ECS recorded an unhealthy/failed task or scheduler failure during the measured run" >&2
      return 1
    }

  jq \
    --argjson started "$evidence_started_epoch" \
    --arg task_definition "$staging_task_definition_arn" '
      def epoch:
        sub("\\.[0-9]+\\+00:00$"; "Z")
        | sub("\\.[0-9]+Z$"; "Z")
        | sub("\\+00:00$"; "Z")
        | fromdateiso8601;
      [.tasks[]
        | select(
            .taskDefinitionArn == $task_definition
            and .stopCode == "ServiceSchedulerInitiated"
            and .stoppingAt != null
            and .stoppedAt != null
            and (.stoppedAt | epoch) >= $started
          )
        | {
            task_id: (.taskArn | split("/")[-1]),
            stopping_at: .stoppingAt,
            stopped_at: .stoppedAt,
            shutdown_ms: (((.stoppedAt | epoch) - (.stoppingAt | epoch)) * 1000)
          }]
    ' "$ecs_dir/stopped-tasks.json" >"$ecs_dir/task-shutdown-durations.json"
  jq -e '
    length >= 9
    and all(.[]; .shutdown_ms >= 0 and .shutdown_ms <= 45000)
  ' "$ecs_dir/task-shutdown-durations.json" >/dev/null || {
    echo "ECS did not prove every measured scheduler shutdown completed within 45 seconds" >&2
    return 1
  }
}

cloudwatch_metric() {
  local output="$1"
  local namespace="$2"
  local metric_name="$3"
  local statistic="$4"
  shift 4
  aws cloudwatch get-metric-statistics \
    --region "$SNAKETRON_AWS_REGION" \
    --namespace "$namespace" \
    --metric-name "$metric_name" \
    --start-time "$evidence_started_at" \
    --end-time "$evidence_finished_at" \
    --period 60 \
    --statistics "$statistic" \
    --dimensions "$@" >"$output"
  local maximum_gap_seconds=90
  if [[ "$namespace" == "AWS/EC2" ]]; then
    # Basic EC2 monitoring emits five-minute buckets. The application, ECS,
    # and ElastiCache metrics above are one-minute or finer.
    maximum_gap_seconds=360
  fi
  jq -e \
    --arg started_at "$evidence_started_at" \
    --arg finished_at "$evidence_finished_at" \
    --argjson maximum_gap_seconds "$maximum_gap_seconds" '
      def epoch:
        sub("\\.[0-9]+\\+00:00$"; "Z")
        | sub("\\.[0-9]+Z$"; "Z")
        | sub("\\+00:00$"; "Z")
        | fromdateiso8601;
      ($started_at | epoch) as $started
      | ($finished_at | epoch) as $finished
      | ([.Datapoints[].Timestamp | epoch] | sort) as $timestamps
      | ($timestamps | length) > 0
        and $timestamps[0] <= ($started + $maximum_gap_seconds)
        and $timestamps[-1] >= ($finished - $maximum_gap_seconds)
        and all(range(1; ($timestamps | length));
          ($timestamps[.] - $timestamps[. - 1]) <= $maximum_gap_seconds)
    ' "$output" >/dev/null || {
    echo "CloudWatch $namespace/$metric_name buckets do not cover the measured run" >&2
    return 1
  }
}

collect_cloudwatch_evidence() {
  local report_dir="$1"
  local cloudwatch_dir="$report_dir/cloudwatch"
  mkdir -p "$cloudwatch_dir"

  cloudwatch_metric "$cloudwatch_dir/ready-tasks.json" \
    Snaketron/Resilience ReadyTasks Minimum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/fingerprint-divergences.json" \
    Snaketron/Resilience RecoveryFingerprintDivergences Sum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/owner-mismatches.json" \
    Snaketron/Resilience PartitionOwnerMismatches Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/active-index-mismatches.json" \
    Snaketron/Resilience ActiveGameIndexMismatches Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/planned-drain-failures.json" \
    Snaketron/Resilience PlannedDrainFailures Sum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/partition-unowned-ms.json" \
    Snaketron/Resilience PartitionUnownedMs Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/assignment-imbalance.json" \
    Snaketron/Resilience AssignmentImbalance Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/oldest-pending-command-ms.json" \
    Snaketron/Resilience OldestPendingCommandMs Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/pending-commands.json" \
    Snaketron/Resilience PendingCommands Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/pending-completions.json" \
    Snaketron/Resilience PendingCompletions Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/checkpoint-age-ms.json" \
    Snaketron/Resilience CheckpointAgeMs Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/checkpoint-bytes.json" \
    Snaketron/Resilience CheckpointBytes Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/checkpoint-writes.json" \
    Snaketron/Resilience CheckpointWrites Sum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/checkpoint-failures.json" \
    Snaketron/Resilience CheckpointFailures Sum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/fenced-write-rejections.json" \
    Snaketron/Resilience FencedWriteRejections Sum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/quarantined-commands.json" \
    Snaketron/Resilience QuarantinedCommands Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"
  cloudwatch_metric "$cloudwatch_dir/active-websockets.json" \
    Snaketron/Resilience ActiveWebSockets Maximum \
    Name=Environment,Value="$SNAKETRON_STAGING_ENVIRONMENT"

  cloudwatch_metric "$cloudwatch_dir/ecs-cpu.json" \
    AWS/ECS CPUUtilization Maximum \
    Name=ClusterName,Value="$cluster_name" \
    Name=ServiceName,Value="$service_name"
  cloudwatch_metric "$cloudwatch_dir/ecs-memory.json" \
    AWS/ECS MemoryUtilization Maximum \
    Name=ClusterName,Value="$cluster_name" \
    Name=ServiceName,Value="$service_name"

  cloudwatch_metric "$cloudwatch_dir/valkey-cpu.json" \
    AWS/ElastiCache CPUUtilization Maximum \
    Name=CacheClusterId,Value="$staging_valkey_cluster_id"
  cloudwatch_metric "$cloudwatch_dir/valkey-memory.json" \
    AWS/ElastiCache DatabaseMemoryUsagePercentage Maximum \
    Name=CacheClusterId,Value="$staging_valkey_cluster_id"
  cloudwatch_metric "$cloudwatch_dir/valkey-connections.json" \
    AWS/ElastiCache CurrConnections Maximum \
    Name=CacheClusterId,Value="$staging_valkey_cluster_id"
  cloudwatch_metric "$cloudwatch_dir/valkey-read-latency.json" \
    AWS/ElastiCache GetTypeCmdsLatency Average \
    Name=CacheClusterId,Value="$staging_valkey_cluster_id"
  cloudwatch_metric "$cloudwatch_dir/valkey-write-latency.json" \
    AWS/ElastiCache SetTypeCmdsLatency Average \
    Name=CacheClusterId,Value="$staging_valkey_cluster_id"
  cloudwatch_metric "$cloudwatch_dir/valkey-evictions.json" \
    AWS/ElastiCache Evictions Sum \
    Name=CacheClusterId,Value="$staging_valkey_cluster_id"

  cloudwatch_metric "$cloudwatch_dir/traefik-cpu.json" \
    AWS/EC2 CPUUtilization Maximum \
    Name=InstanceId,Value="$SNAKETRON_TRAEFIK_INSTANCE_ID"
  cloudwatch_metric "$cloudwatch_dir/traefik-network-in.json" \
    AWS/EC2 NetworkIn Sum \
    Name=InstanceId,Value="$SNAKETRON_TRAEFIK_INSTANCE_ID"
  cloudwatch_metric "$cloudwatch_dir/traefik-network-out.json" \
    AWS/EC2 NetworkOut Sum \
    Name=InstanceId,Value="$SNAKETRON_TRAEFIK_INSTANCE_ID"

  jq -e 'all(.Datapoints[]; .Minimum > 0)' \
    "$cloudwatch_dir/ready-tasks.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/fingerprint-divergences.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) == 0' \
      "$cloudwatch_dir/owner-mismatches.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) == 0' \
      "$cloudwatch_dir/active-index-mismatches.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/planned-drain-failures.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) < 5000' \
      "$cloudwatch_dir/partition-unowned-ms.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) <= 1' \
      "$cloudwatch_dir/assignment-imbalance.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) < 10000' \
      "$cloudwatch_dir/oldest-pending-command-ms.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) < 5000' \
      "$cloudwatch_dir/checkpoint-age-ms.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) > 0' \
      "$cloudwatch_dir/checkpoint-bytes.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) > 0' \
      "$cloudwatch_dir/checkpoint-writes.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/checkpoint-failures.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/fenced-write-rejections.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) == 0' \
      "$cloudwatch_dir/quarantined-commands.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) >= 295' \
      "$cloudwatch_dir/active-websockets.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/valkey-evictions.json" >/dev/null \
    && jq -e '([.Datapoints[].Maximum] | max) < 90' \
      "$cloudwatch_dir/valkey-memory.json" >/dev/null || {
      echo "CloudWatch acceptance failed: readiness, recovery, ownership, checkpoint, drain, socket-envelope, eviction, or Valkey headroom evidence is outside bounds" >&2
      return 1
  }
}

collect_container_insights_evidence() {
  local report_dir="$1"
  local insights_dir="$report_dir/container-insights"
  local control_plane="$report_dir/control-plane-capacity-10.json"
  local scale_window="$report_dir/capacity-window.json"
  mkdir -p "$insights_dir"

  local task_ids_json
  task_ids_json="$(jq -ce '
    [.live_members[]
      | select(.lifecycle == "ACTIVE")
      | .ecs_task_id]
    | unique | sort
  ' "$control_plane")"
  if [[ "$(jq 'length' <<<"$task_ids_json")" != "10" ]]; then
    echo "Fresh control-plane snapshot does not contain ten unique ECS task IDs" >&2
    return 1
  fi

  local query_start_epoch
  local query_end_epoch
  query_start_epoch="$(jq -r '(.started_at_unix_ms / 1000 | floor)' "$scale_window")"
  query_end_epoch="$(jq -r '(.finished_at_unix_ms / 1000 | ceil)' "$scale_window")"
  local query_string
  query_string="fields TaskId, CpuUtilized, MemoryUtilized | filter Type = \"Task\" and TaskId in $task_ids_json | stats count(*) as samples, avg(CpuUtilized) as avg_cpu_utilized, max(CpuUtilized) as max_cpu_utilized, avg(MemoryUtilized) as avg_memory_utilized, max(MemoryUtilized) as max_memory_utilized by TaskId | sort TaskId asc"
  local log_group="/aws/ecs/containerinsights/$cluster_name/performance"

  jq -n \
    --arg log_group "$log_group" \
    --arg query "$query_string" \
    --argjson start_time "$query_start_epoch" \
    --argjson end_time "$query_end_epoch" \
    --argjson expected_task_ids "$task_ids_json" '
      {
        log_group: $log_group,
        start_time: $start_time,
        end_time: $end_time,
        expected_task_ids: $expected_task_ids,
        query: $query
      }
    ' >"$insights_dir/request.json"
  aws logs start-query \
    --region "$SNAKETRON_AWS_REGION" \
    --log-group-name "$log_group" \
    --start-time "$query_start_epoch" \
    --end-time "$query_end_epoch" \
    --query-string "$query_string" >"$insights_dir/start-query.json"
  local query_id
  query_id="$(jq -r '.queryId' "$insights_dir/start-query.json")"
  if [[ -z "$query_id" || "$query_id" == "null" ]]; then
    echo "CloudWatch Logs did not return a Container Insights query ID" >&2
    return 1
  fi

  local deadline=$((SECONDS + 180))
  local query_status=""
  while (( SECONDS < deadline )); do
    aws logs get-query-results \
      --region "$SNAKETRON_AWS_REGION" \
      --query-id "$query_id" >"$insights_dir/results.pending.json"
    query_status="$(jq -r '.status' "$insights_dir/results.pending.json")"
    case "$query_status" in
      Complete)
        mv "$insights_dir/results.pending.json" "$insights_dir/results.json"
        break
        ;;
      Failed|Cancelled|Timeout|Unknown)
        mv "$insights_dir/results.pending.json" "$insights_dir/results.json"
        echo "Container Insights query ended with status $query_status" >&2
        return 1
        ;;
    esac
    sleep 3
  done
  if [[ "$query_status" != "Complete" ]]; then
    [[ -f "$insights_dir/results.pending.json" ]] \
      && mv "$insights_dir/results.pending.json" "$insights_dir/results.json"
    echo "Container Insights query did not complete within three minutes" >&2
    return 1
  fi

  jq -e \
    --slurpfile control "$control_plane" '
      ([$control[0].live_members[]
        | select(.lifecycle == "ACTIVE")
        | .ecs_task_id]
        | unique | sort) as $expected
      | ([.results[] | map({key: .field, value: .value}) | from_entries]) as $rows
      | ($rows | map(.TaskId) | unique | sort) == $expected
        and ($rows | length) == 10
        and all($rows[];
          (.samples | tonumber) >= 4
          and (.avg_cpu_utilized | tonumber) >= 0
          and (.max_cpu_utilized | tonumber) >= 0
          and (.avg_memory_utilized | tonumber) >= 0
          and (.max_memory_utilized | tonumber) >= 0)
    ' "$insights_dir/results.json" >/dev/null || {
      echo "Container Insights lacks CPU/memory samples for every fresh ten-task member" >&2
      return 1
    }
}

verify_crash_exec_configuration() {
  local report_dir="$1"
  local phase="$2"
  jq -e '
    .services[0].enableExecuteCommand == true
  ' "$report_dir/identity/ecs-service.json" >/dev/null \
    && jq -e '
      [.taskDefinition.containerDefinitions[]
        | select(
            .name == "snaketron-server"
            and .essential == true
            and .linuxParameters.initProcessEnabled == true)]
      | length == 1
    ' "$report_dir/identity/task-definition.json" >/dev/null \
    && jq -e '
      (.failures | length) == 0
      and (.tasks | length) > 0
      and all(.tasks[];
        .enableExecuteCommand == true
        and any(.containers[];
          .name == "snaketron-server"
          and any(.managedAgents[]?;
            .name == "ExecuteCommandAgent"
            and .lastStatus == "RUNNING")))
    ' "$report_dir/ecs-$phase/tasks.json" >/dev/null || {
      echo "Hard-crash certification requires ECS Exec, tini, and a RUNNING execute-command agent on every task" >&2
      return 1
    }
}

capture_control_status() {
  local output="$1"
  SNAKETRON_REDIS_URL="$staging_redis_control_url" \
    "$resilience_admin" status \
    --region-key "$SNAKETRON_REGION_CODE" >"$output"
}

inject_hard_crash_and_prove_takeover() {
  local report_dir="$1"
  local pre="$report_dir/control-plane-pre-crash-10.json"
  local candidate="$pre.pending"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if capture_control_status "$candidate" 2>/dev/null \
      && jq -e '
        any(.runtime_partitions[];
          .owner_matches
          and .active_games > 0
          and .pending_count > 0
          and (.lease_token as $token
            | any(.pending_entry_sample[]; .consumer == $token)))
      ' "$candidate" >/dev/null; then
      mv "$candidate" "$pre"
      break
    fi
    sleep 0.2
  done
  if [[ ! -f "$pre" ]]; then
    echo "No owned partition had both an active game and pending command work" >&2
    return 1
  fi

  local partition_json
  local member_json
  local killed_partition
  local killed_boot_id
  local killed_task_id
  local killed_task_arn
  local killed_task_boot_id
  local killed_lease_token
  partition_json="$(jq -ce '
    [.runtime_partitions[]
      | select(
          .owner_matches
          and .active_games > 0
          and .pending_count > 0
          and (.lease_token as $token
            | any(.pending_entry_sample[]; .consumer == $token)))]
    | sort_by(-.pending_count, -.active_games, .partition)
    | .[0]
  ' "$pre")"
  killed_partition="$(jq -r '.partition' <<<"$partition_json")"
  killed_boot_id="$(jq -r '.active_owner' <<<"$partition_json")"
  killed_lease_token="$(jq -r '.lease_token' <<<"$partition_json")"
  member_json="$(jq -ce --arg boot_id "$killed_boot_id" '
    [.live_members[]
      | select(.boot_id == $boot_id and .lifecycle == "ACTIVE")]
    | select(length == 1)
    | .[0]
  ' "$pre")" || {
    echo "Selected executor owner did not map to exactly one ACTIVE member" >&2
    return 1
  }
  killed_task_id="$(jq -r '.ecs_task_id' <<<"$member_json")"
  killed_task_boot_id="$(jq -r '"\(.server_id):\(.boot_id)"' <<<"$member_json")"
  killed_task_arn="$(jq -er --arg task_id "$killed_task_id" '
    [.tasks[] | select((.taskArn | split("/")[-1]) == $task_id)]
    | select(length == 1)
    | .[0].taskArn
  ' "$report_dir/ecs-crash-baseline-10/tasks.json")" || {
    echo "Selected executor member did not map to exactly one healthy ECS task ARN" >&2
    return 1
  }

  jq -n \
    --argjson selected_partition "$partition_json" \
    --argjson selected_member "$member_json" \
    --arg task_arn "$killed_task_arn" \
    --arg task_boot_id "$killed_task_boot_id" \
    --argjson ecs_exec_attempts 1 '
      {
        selected_partition: $selected_partition,
        selected_member: $selected_member,
        task_arn: $task_arn,
        task_boot_id: $task_boot_id,
        ecs_exec_attempts: $ecs_exec_attempts
      }
    ' >"$report_dir/hard-crash-manifest.json"

  # One non-retried ECS Exec session discovers exactly one non-PID-1 `server`
  # child, records the timestamp used by every deadline, SIGKILLs the child,
  # and only then emits the marker. A marker therefore proves that the kill
  # syscall succeeded before the PEL observation begins.
  local hard_kill_command
  hard_kill_command='/bin/sh -c '\''set -eu; count=0; server_pid=; for comm_file in /proc/[0-9]*/comm; do IFS= read -r comm < "$comm_file" || continue; [ "$comm" = server ] || continue; server_pid=${comm_file#/proc/}; server_pid=${server_pid%/comm}; count=$((count + 1)); done; [ "$count" -eq 1 ]; [ "$server_pid" -ne 1 ]; kill_at_ms=$(date +%s%3N); kill -KILL "$server_pid"; printf "SNAKETRON_HARD_KILL_AT_MS=%s SERVER_PID=%s\\n" "$kill_at_ms" "$server_pid"'\'''
  local exec_output="$report_dir/hard-crash-ecs-exec.log"
  aws ecs execute-command \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --task "$killed_task_arn" \
    --container snaketron-server \
    --interactive \
    --command "$hard_kill_command" >"$exec_output" 2>&1 &
  local ecs_exec_pid=$!
  local marker_deadline=$((SECONDS + 30))
  local kill_at_ms=""
  while (( SECONDS < marker_deadline )); do
    kill_at_ms="$(sed -n -E 's/.*SNAKETRON_HARD_KILL_AT_MS=([0-9]+).*/\1/p' "$exec_output" | tail -1)"
    [[ "$kill_at_ms" =~ ^[0-9]{13}$ ]] && break
    kill -0 "$ecs_exec_pid" 2>/dev/null || break
    sleep 0.05
  done
  if [[ ! "$kill_at_ms" =~ ^[0-9]{13}$ ]]; then
    wait "$ecs_exec_pid" 2>/dev/null || true
    echo "The single ECS Exec injection did not emit its hard-kill marker" >&2
    return 1
  fi

  # Capture the Redis PEL immediately after the fail-stop marker and before
  # polling for successor ownership. This ties the takeover proof to exact
  # command IDs that the killed lease could no longer acknowledge.
  local pending_after_kill="$report_dir/control-plane-immediate-post-kill.json"
  local pending_candidate="$pending_after_kill.pending"
  local pending_deadline=$((SECONDS + 2))
  while (( SECONDS < pending_deadline )); do
    if capture_control_status "$pending_candidate" 2>/dev/null \
      && jq -e \
        --arg killed_lease_token "$killed_lease_token" \
        --argjson partition "$killed_partition" \
        --argjson kill_at_ms "$kill_at_ms" '
          .captured_at_ms >= $kill_at_ms
          and ([.runtime_partitions[]
            | select(.partition == $partition)
            | .pending_entry_sample[]
            | select(.consumer == $killed_lease_token)] | length) > 0
        ' "$pending_candidate" >/dev/null; then
      mv "$pending_candidate" "$pending_after_kill"
      break
    fi
    sleep 0.05
  done
  if [[ ! -f "$pending_after_kill" ]]; then
    [[ -f "$pending_candidate" ]] && mv "$pending_candidate" "$pending_after_kill"
    wait "$ecs_exec_pid" 2>/dev/null || true
    echo "No exact pending command remained under the killed lease immediately after SIGKILL" >&2
    return 1
  fi

  local manifest_pending="$report_dir/hard-crash-manifest.pending.json"
  jq \
    --arg killed_lease_token "$killed_lease_token" \
    --argjson partition "$killed_partition" \
    --slurpfile observed "$pending_after_kill" '
      . + {
        pending_after_kill: {
          captured_at_unix_ms: $observed[0].captured_at_ms,
          partition: $partition,
          killed_lease_token: $killed_lease_token,
          entries: [$observed[0].runtime_partitions[]
            | select(.partition == $partition)
            | .pending_entry_sample[]
            | select(.consumer == $killed_lease_token)]
        }
      }
    ' "$report_dir/hard-crash-manifest.json" >"$manifest_pending"
  mv "$manifest_pending" "$report_dir/hard-crash-manifest.json"

  local owner_ready="$report_dir/control-plane-hard-crash-owner-ready.json"
  local owner_candidate="$owner_ready.pending"
  local poll_deadline=$((SECONDS + 8))
  while (( SECONDS < poll_deadline )); do
    if capture_control_status "$owner_candidate" 2>/dev/null \
      && jq -e \
        --arg killed_boot_id "$killed_boot_id" \
        --arg killed_task_id "$killed_task_id" \
        --argjson partition "$killed_partition" \
        --argjson kill_at_ms "$kill_at_ms" \
        --slurpfile pre "$pre" '
          ($pre[0].runtime_partitions[] | select(.partition == $partition)) as $old
          | (.runtime_partitions[] | select(.partition == $partition)) as $new
          | ([.live_members[].boot_id] | index($killed_boot_id)) == null
          and .assignment.version > $pre[0].assignment.version
          and .captured_at_ms >= $kill_at_ms
          and .captured_at_ms <= ($kill_at_ms + 5000)
          and $new.owner_matches
          and $new.desired_owner != $killed_boot_id
          and $new.active_owner == $new.desired_owner
          and $new.lease_token != $old.lease_token
          and ($new.desired_owner as $owner
            | [.live_members[]
                | select(.boot_id == $owner and .lifecycle == "ACTIVE")] as $current
            | ($current | length) == 1
              and $current[0].ecs_task_id != $killed_task_id
              and ($current[0].ecs_task_id as $owner_task_id
                | any($pre[0].live_members[];
                    .boot_id == $owner and .ecs_task_id == $owner_task_id)))
        ' "$owner_candidate" >/dev/null; then
      mv "$owner_candidate" "$owner_ready"
      break
    fi
    sleep 0.2
  done
  set +e
  wait "$ecs_exec_pid"
  local ecs_exec_exit_code=$?
  set -e
  if [[ ! -f "$owner_ready" ]]; then
    [[ -f "$owner_candidate" ]] && mv "$owner_candidate" "$owner_ready"
    echo "Killed membership and fenced partition ownership did not fail over to a pre-existing survivor within five seconds" >&2
    return 1
  fi
  jq \
    --argjson kill_at_unix_ms "$kill_at_ms" \
    --argjson ecs_exec_exit_code "$ecs_exec_exit_code" \
    --slurpfile ready "$owner_ready" '
      . + {
        kill_at_unix_ms: $kill_at_unix_ms,
        ecs_exec_exit_code: $ecs_exec_exit_code,
        owner_ready_at_unix_ms: $ready[0].captured_at_ms,
        assignment_version_after: $ready[0].assignment.version
      }
    ' "$report_dir/hard-crash-manifest.json" >"$manifest_pending"
  mv "$manifest_pending" "$report_dir/hard-crash-manifest.json"
}

collect_crash_ecs_runtime_evidence() {
  local report_dir="$1"
  local ecs_dir="$report_dir/ecs-runtime"
  mkdir -p "$ecs_dir"
  aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" >"$ecs_dir/service.json"
  local stopped_arns
  stopped_arns="$(aws ecs list-tasks \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service-name "$SNAKETRON_ECS_SERVICE" \
    --desired-status STOPPED \
    --query 'taskArns[]' --output text)"
  if [[ -n "$stopped_arns" ]]; then
    aws ecs describe-tasks \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --tasks $stopped_arns >"$ecs_dir/stopped-tasks.json"
  else
    jq -n '{tasks: [], failures: []}' >"$ecs_dir/stopped-tasks.json"
  fi
  jq -e \
    --arg task_definition "$staging_task_definition_arn" \
    --slurpfile manifest "$report_dir/hard-crash-manifest.json" '
      def epoch:
        sub("\\.[0-9]+\\+00:00$"; "Z")
        | sub("\\.[0-9]+Z$"; "Z")
        | sub("\\+00:00$"; "Z")
        | fromdateiso8601;
      ($manifest[0].kill_at_unix_ms / 1000 | floor) as $kill_epoch
      | [.tasks[]
          | select(.taskArn == $manifest[0].task_arn)
          | select((.stoppedAt | epoch) >= $kill_epoch)] as $expected
      | [.tasks[]
          | select((.stoppedAt | epoch) >= $kill_epoch)
          | select(.taskArn != $manifest[0].task_arn)
          | select(
              .stopCode == "EssentialContainerExited"
              or ((.stoppedReason // "") | test("unhealthy|out.of.memory|failed"; "i"))
            )] as $unexpected
      | ($expected | length) == 1
        and $expected[0].taskDefinitionArn == $task_definition
        and $expected[0].stopCode == "EssentialContainerExited"
        and ([ $expected[0].containers[]
          | select(
              .name == "snaketron-server"
              and .exitCode == 137)] | length) == 1
        and ($unexpected | length) == 0
    ' "$ecs_dir/stopped-tasks.json" >/dev/null \
    && jq -e \
      --arg task_definition "$staging_task_definition_arn" \
      --argjson started "$evidence_started_epoch" \
      --slurpfile manifest "$report_dir/hard-crash-manifest.json" '
        def epoch:
          sub("\\.[0-9]+\\+00:00$"; "Z")
          | sub("\\.[0-9]+Z$"; "Z")
          | sub("\\+00:00$"; "Z")
          | fromdateiso8601;
        .services[0] as $service
        | ($service.deployments | length) == 1
        and $service.deployments[0].status == "PRIMARY"
        and $service.deployments[0].rolloutState == "COMPLETED"
        and $service.deployments[0].taskDefinition == $task_definition
        and ([.services[0].events[]
          | select((.createdAt | epoch) >= $started)
          | select(.message | test("unhealthy|failed to|was unable|insufficient"; "i"))
          | select(
              (.message | contains($manifest[0].selected_member.ecs_task_id))
              | not)]
          | length) == 0
      ' "$ecs_dir/service.json" >/dev/null || {
        echo "ECS evidence was not exactly one expected exit-137 server crash with no unrelated runtime failures" >&2
        return 1
      }
}

assert_hard_crash_report() {
  local report_dir="$1"
  local summary="$2"
  jq -n \
    --slurpfile report "$summary" \
    --slurpfile manifest "$report_dir/hard-crash-manifest.json" \
    --slurpfile pending_after_kill "$report_dir/control-plane-immediate-post-kill.json" \
    --slurpfile owner_ready "$report_dir/control-plane-hard-crash-owner-ready.json" \
    --slurpfile final "$report_dir/control-plane-hard-crash-final-10.json" '
      def p99:
        sort as $values
        | if ($values | length) == 0 then null
          else $values[(((((($values | length) * 99) + 99) / 100) | floor) - 1)]
          end;
      def fully_joined_duels_at($value; $midpoint):
        ([$value.sessions[] | select(.game_id != null)]
          | group_by(.game_id)
          | map(select(
              length == 2
              and all(.[];
                .playing_at_unix_ms != null
                and .game_finished_at_unix_ms != null
                and .playing_at_unix_ms <= $midpoint
                and .game_finished_at_unix_ms > $midpoint)))
          | length);
      $report[0] as $r
      | $manifest[0] as $m
      | $m.kill_at_unix_ms as $kill
      | $m.selected_partition.partition as $partition
      | ($m.pending_after_kill.entries | map(.id) | unique | sort) as $pending_ids
      | ([$pending_after_kill[0].runtime_partitions[]
          | select(.partition == $partition)
          | .pending_entry_sample[]
          | select(.consumer == $m.selected_partition.lease_token)]) as $observed_pending
      # Exclude any bucket that began before the observer proved the successor
      # lease. The selected bucket end remains the conservative output bound.
      | ($owner_ready[0].captured_at_ms / 1000 | ceil) as $first_post_second
      | (($kill / 1000 | floor) - 30) as $stable_first_second
      | ($kill / 1000 | floor) as $stable_after_last_second
      | [$r.sessions[].hard_recoveries[]?
          | select(
              .from_task_boot_id == $m.task_boot_id
              and .detected_at_unix_ms >= $kill
              and .ready_at_unix_ms >= .detected_at_unix_ms)] as $affected
      | ([$affected[].ready_at_unix_ms - $kill] | p99) as $kill_to_ready_p99_ms
      | ([$r.metrics.scheduled_command_counts_by_partition_and_unix_second
            [($partition | tostring)]
            | to_entries[]
            | select((.key | tonumber) >= $first_post_second and .value > 0)
            | (.key | tonumber)] | min // null) as $first_output_second
      | {
          affected_sessions: ($affected | length),
          ambiguous_commands_after_initial_barrier: (
            [$affected[].pending_commands_after_outcome_barrier] | add // 0),
          pending_commands_at_finish:
            $r.metrics.planned_handoffs.pending_commands_at_finish,
          pending_ids_observed_after_kill: $pending_ids,
          pending_observed_at_unix_ms: $m.pending_after_kill.captured_at_unix_ms,
          kill_to_ready_p99_ms: $kill_to_ready_p99_ms,
          first_authoritative_output_second: $first_output_second,
          first_authoritative_output_upper_bound_ms: (
            if $first_output_second == null then null
            else (($first_output_second + 1) * 1000) - $kill
            end),
          passed: (
            $r.schema_version >= 9
            and $r.metadata.threshold_result == "passed"
            and $r.configured_max_concurrency == 272
            and $r.metadata.mode == "duel"
            and $r.metadata.command_profile == "every-tick"
            and $r.metadata.spawn_rate_per_second == "4"
            and $r.session_counts.peak_authenticated_concurrency == 272
            and $r.session_counts.peak_active_game_concurrency >= 136
            and $r.session_counts.failed == 0
            and $r.session_counts.cancelled == 0
            and $r.session_counts.incomplete == 0
            and $r.games.pairing_violations == 0
            and all($r.sessions[]; .outcome == "completed" and .failure_phase == null)
            and ($stable_after_last_second - $stable_first_second) == 30
            and all(range($stable_first_second; $stable_after_last_second);
              . as $second
              | (($second * 1000) + 500) as $midpoint
              | ([$r.sessions[]
                  | select(
                      .authenticated_at_unix_ms != null
                      and .authenticated_at_unix_ms <= $midpoint
                      and .finished_at_unix_ms > $midpoint)] | length) >= 256
                and fully_joined_duels_at($r; $midpoint) >= 128
                and (($r.metrics.command_counts_by_unix_second
                      [($second | tostring)] // 0) >= 1280))
            and ($affected | length) > 0
            and $m.pending_after_kill.partition == $partition
            and $m.pending_after_kill.killed_lease_token == $m.selected_partition.lease_token
            and $m.pending_after_kill.captured_at_unix_ms >= $kill
            and $m.pending_after_kill.captured_at_unix_ms
              == $pending_after_kill[0].captured_at_ms
            and ($pending_ids | length) > 0
            and ($pending_ids | length) == ($m.pending_after_kill.entries | length)
            and $m.pending_after_kill.entries == $observed_pending
            and $kill_to_ready_p99_ms != null
            and $kill_to_ready_p99_ms <= 10000
            and all($affected[];
              .to_task_boot_id != .from_task_boot_id
              and .fresh_snapshot_received)
            and $r.metrics.planned_handoffs.pending_commands_at_finish == 0
            and $first_output_second != null
            and ((($first_output_second + 1) * 1000) - $kill) <= 5000
            and all($final[0].runtime_partitions[];
              .consumer_group_exists
              and .owner_matches
              and .pending_count == 0
              and .pending_completion_count == 0
              and .quarantined_command_count == 0)
          )
        }
    ' >"$report_dir/hard-crash-acceptance.json"
  jq -e '.passed' "$report_dir/hard-crash-acceptance.json" >/dev/null || {
    echo "Hard-crash recovery failed its session, command, ownership, or five-second output gates" >&2
    return 1
  }
}

run_staging_suite() {
  local certification_mode="${1:-planned}"
  local crash_mode=false
  if [[ "$certification_mode" == "crash" ]]; then
    crash_mode=true
  elif [[ "$certification_mode" != "planned" ]]; then
    echo "Unknown staging certification mode: $certification_mode" >&2
    return 2
  fi
  require_command aws
  require_command cargo
  require_command curl
  require_command dig
  require_command git
  require_command jq
  if [[ "$crash_mode" == true ]]; then
    require_command session-manager-plugin
  fi
  require_staging_environment
  configure_staging_control_urls

  local run_id
  run_id="autoscaling-${certification_mode}-$(date -u +%Y%m%dT%H%M%SZ)"
  report_dir="$repo_dir/test-results/$run_id"
  mkdir -p "$report_dir"

  # This is the final read-only gate before any load or cloud mutation. It
  # proves that target ingress, ECS, task configuration, and Valkey all share
  # one explicitly tagged non-production deployment in the confirmed account.
  local staging_service_arn=""
  local staging_cluster_arn=""
  local staging_task_definition_arn=""
  local staging_image_uri=""
  local staging_image_digest=""
  local staging_image_commit=""
  local staging_valkey_arn=""
  local staging_valkey_host=""
  local staging_valkey_port=""
  local staging_valkey_cluster_id=""
  local staging_valkey_parameter_group=""
  local staging_traefik_ip=""
  local staging_traefik_private_ip=""
  # Populated from the verified task definition's router label. Callers cannot
  # select an unrelated healthy Traefik service.
  local staging_traefik_service_label=""
  local cluster_name="${SNAKETRON_ECS_CLUSTER##*/}"
  local service_name="${SNAKETRON_ECS_SERVICE##*/}"
  scaling_resource="service/$cluster_name/$service_name"
  verify_staging_identity "$report_dir"
  verify_scaling_policies "$report_dir"
  if [[ "$crash_mode" == true ]]; then
    jq -e '.services[0].enableExecuteCommand == true' \
      "$report_dir/identity/ecs-service.json" >/dev/null \
      && jq -e '
        [.taskDefinition.containerDefinitions[]
          | select(
              .name == "snaketron-server"
              and .essential == true
              and .linuxParameters.initProcessEnabled == true)]
        | length == 1
      ' "$report_dir/identity/task-definition.json" >/dev/null || {
        echo "Hard-crash mode requires ECS Exec and initProcessEnabled on the verified deployment" >&2
        return 1
      }
  fi

  original_desired="$(aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" \
    --query 'services[0].desiredCount' \
    --output text)"
  if [[ "$original_desired" != "1" ]]; then
    echo "Staging service must begin at desiredCount=1; found $original_desired" >&2
    exit 1
  fi

  scaling_state="$(aws application-autoscaling describe-scalable-targets \
    --region "$SNAKETRON_AWS_REGION" \
    --service-namespace ecs \
    --scalable-dimension ecs:service:DesiredCount \
    --resource-ids "$scaling_resource" \
    --query 'ScalableTargets[0].[MinCapacity,MaxCapacity,SuspendedState.DynamicScalingInSuspended,SuspendedState.DynamicScalingOutSuspended,SuspendedState.ScheduledScalingSuspended]' \
    --output text)"
  if [[ "$scaling_state" != "1"$'\t'"10"$'\t'"False"$'\t'"False"$'\t'"False" ]]; then
    echo "Staging autoscaling must be min=1, max=10, and fully enabled; found: $scaling_state" >&2
    exit 1
  fi

  load_pid=""
  capacity_pid=""
  admission_population_pid=""
  idle_population_pid=""
  lobby_population_pid=""
  matchmaking_population_pid=""
  traefik_monitor_pid=""
  traefik_monitor_dir=""

  set_scaling_suspended() {
    local value="$1"
    aws application-autoscaling register-scalable-target \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --resource-id "$scaling_resource" \
      --scalable-dimension ecs:service:DesiredCount \
      --min-capacity 1 \
      --max-capacity 10 \
      --suspended-state \
"DynamicScalingInSuspended=$value,DynamicScalingOutSuspended=$value,ScheduledScalingSuspended=$value" \
      >/dev/null
  }

  restore_and_verify() {
    local exit_code="$?"
    trap - EXIT
    set +e
    local cleanup_ok=true
    stop_traefik_monitor
    local population_pid
    for population_pid in \
      "$load_pid" \
      "$capacity_pid" \
      "$admission_population_pid" \
      "$idle_population_pid" \
      "$lobby_population_pid" \
      "$matchmaking_population_pid"; do
      if [[ -n "$population_pid" ]] && kill -0 "$population_pid" 2>/dev/null; then
        kill -TERM "$population_pid" 2>/dev/null || true
        wait "$population_pid" 2>/dev/null || true
      fi
    done
    # Suspend policy writes while restoring the exact count, then restore the
    # original fully enabled policy state. Every step retries and is verified.
    retry_command 5 set_scaling_suspended true || cleanup_ok=false
    retry_command 5 aws ecs update-service \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --service "$SNAKETRON_ECS_SERVICE" \
      --desired-count "$original_desired" >/dev/null || cleanup_ok=false
    wait_for_running_count "$original_desired" || cleanup_ok=false
    retry_command 5 set_scaling_suspended false || cleanup_ok=false
    local restored_scaling_state
    restored_scaling_state="$(aws application-autoscaling describe-scalable-targets \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --scalable-dimension ecs:service:DesiredCount \
      --resource-ids "$scaling_resource" \
      --query 'ScalableTargets[0].[MinCapacity,MaxCapacity,SuspendedState.DynamicScalingInSuspended,SuspendedState.DynamicScalingOutSuspended,SuspendedState.ScheduledScalingSuspended]' \
      --output text 2>/dev/null)"
    if [[ "$restored_scaling_state" != "$scaling_state" ]]; then
      cleanup_ok=false
    fi
    local restored_counts
    restored_counts="$(aws ecs describe-services \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --services "$SNAKETRON_ECS_SERVICE" \
      --query 'services[0].[desiredCount,runningCount,pendingCount]' \
      --output text 2>/dev/null)"
    if [[ "$restored_counts" != "$original_desired"$'\t'"$original_desired"$'\t'"0" ]]; then
      cleanup_ok=false
    fi
    jq -n \
      --argjson restored "$cleanup_ok" \
      --arg scaling_state "$restored_scaling_state" \
      --arg counts "$restored_counts" \
      '{restored: $restored, scaling_state: $scaling_state, counts: $counts}' \
      >"$report_dir/cleanup.json"
    if [[ "$cleanup_ok" != true ]]; then
      echo "Staging cleanup could not verify restoration of desired count and autoscaling policy; inspect cleanup.json" >&2
      exit_code=1
    fi
    exit "$exit_code"
  }
  trap restore_and_verify EXIT

  cd "$repo_dir"
  cargo build -p server --release --bin resilience_admin
  cargo build -p loadtest --release --bin snaketron-loadtest
  local resilience_admin="$repo_dir/target/release/resilience_admin"
  local loadtest_runner="$repo_dir/target/release/snaketron-loadtest"

  wait_for_control_plane() {
    local label="$1"
    local expected_tasks="$2"
    local snapshot="$report_dir/control-plane-$label.json"
    local candidate="$snapshot.pending"
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
      if SNAKETRON_REDIS_URL="$staging_redis_control_url" \
        "$resilience_admin" status \
        --region-key "$SNAKETRON_REGION_CODE" >"$candidate" 2>/dev/null \
        && jq -e --argjson expected "$expected_tasks" '
          ([.live_members[] | select(.lifecycle == "ACTIVE") | .boot_id]
            | unique | sort) as $active_boot_ids
          | .assignment != null
          and ([.live_members[] | select(.lifecycle == "ACTIVE")] | length) == $expected
          and all(.live_members[] | select(.lifecycle == "ACTIVE");
            (.ecs_task_id // "") | length > 0)
          and (.assignment.eligible_members | unique | sort) == $active_boot_ids
          and ([.assignment.owners[]] | unique | sort) == $active_boot_ids
          and (.assignment.owners | length) == 10
          and (
            [.assignment.eligible_members[] as $member
              | [.assignment.owners[] | select(. == $member)] | length
            ] as $owner_counts
            | (($owner_counts | max) - ($owner_counts | min)) <= 1
          )
          and ([.runtime_partitions[] |
            select(
              .desired_owner == null
              or .active_owner == null
              or (.owner_matches | not)
              or .lease_ttl_ms <= 0
              or (.consumer_group_exists | not)
            )
          ] | length) == 0
          and ([.runtime_partitions[].lease_token] | unique | length) == 10
        ' "$candidate" >/dev/null; then
        mv "$candidate" "$snapshot"
        return 0
      fi
      sleep 2
    done
    echo "Executor control plane did not settle at $expected_tasks ACTIVE tasks" >&2
    [[ -f "$candidate" ]] && mv "$candidate" "$snapshot"
    return 1
  }

  wait_for_control_plane initial 1
  wait_for_ecs_health "$report_dir" initial 1
  if [[ "$crash_mode" == true ]]; then
    verify_crash_exec_configuration "$report_dir" initial
  fi
  local evidence_started_at
  local evidence_started_epoch
  evidence_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  evidence_started_epoch="$(date -u +%s)"
  start_traefik_monitor "$report_dir"
  # Keep the one-task continuity/staircase load separate from the supported
  # 272-session capacity load. Target-tracking latency must never leave the
  # complete capacity envelope on the initial 0.5-vCPU task.
  require_runner_running() {
    local label="$1"
    local pid="$2"
    if ! kill -0 "$pid" 2>/dev/null; then
      local runner_exit=0
      wait "$pid" || runner_exit=$?
      echo "$label exited with status $runner_exit before its measured phase completed" >&2
      return 1
    fi
  }

  require_load_running() {
    require_runner_running "Continuity load runner" "$load_pid"
  }

  require_population_running() {
    local population="$1"
    local pid="$2"
    if ! kill -0 "$pid" 2>/dev/null; then
      if ! wait "$pid"; then
        echo "$population population probe failed before scale-in completed" >&2
      else
        echo "$population population probe exited before scale-in completed" >&2
      fi
      exit 1
    fi
  }

  regional_socket_count() {
    curl -fsS --max-time 3 \
      "${SNAKETRON_STAGING_TARGET%/}/api/regions/user-counts" \
      | jq -er --arg region "$SNAKETRON_REGION_CODE" \
        '(.[$region] // 0) | select(type == "number" and . >= 0 and floor == .)'
  }

  wait_for_region_socket_floor() {
    local label="$1"
    local required_floor="$2"
    local observed_pid="${3:-}"
    local samples="$report_dir/region-sockets-$label.jsonl"
    local summary="$report_dir/region-sockets-$label.json"
    local deadline=$((SECONDS + 60))
    : >"$samples"
    while (( SECONDS < deadline )); do
      if [[ -n "$observed_pid" ]]; then
        require_runner_running "$label admission runner" "$observed_pid"
      fi
      local observed=0
      if observed="$(regional_socket_count 2>/dev/null)"; then
        jq -cn \
          --argjson observed_at_unix_ms "$(unix_time_ms)" \
          --argjson raw_websockets "$observed" '
            {
              observed_at_unix_ms: $observed_at_unix_ms,
              raw_websockets: $raw_websockets
            }
          ' >>"$samples"
        if (( observed >= required_floor )); then
          jq -s \
            --argjson required_raw_websockets "$required_floor" \
            '{
              passed: true,
              required_raw_websockets: $required_raw_websockets,
              samples: .
            }' "$samples" >"$summary"
          return 0
        fi
      fi
      sleep 1
    done
    jq -s \
      --argjson required_raw_websockets "$required_floor" \
      '{
        passed: false,
        required_raw_websockets: $required_raw_websockets,
        samples: .
      }' "$samples" >"$summary"
    echo "$label did not expose at least $required_floor regional WebSockets within one minute" >&2
    return 1
  }

  wait_for_zero_certification_load() {
    local label="$1"
    local samples="$report_dir/zero-load-$label.jsonl"
    local summary="$report_dir/zero-load-$label.json"
    local control_candidate="$report_dir/zero-load-$label.control.pending.json"
    local deadline=$((SECONDS + 120))
    local consecutive=0
    : >"$samples"
    while (( SECONDS < deadline )); do
      local sockets=0
      local games=0
      if sockets="$(regional_socket_count 2>/dev/null)" \
        && capture_control_status "$control_candidate" 2>/dev/null; then
        games="$(jq -r '[.runtime_partitions[].active_games] | add // 0' \
          "$control_candidate")"
        jq -cn \
          --argjson observed_at_unix_ms "$(unix_time_ms)" \
          --argjson raw_websockets "$sockets" \
          --argjson active_games "$games" '
            {
              observed_at_unix_ms: $observed_at_unix_ms,
              raw_websockets: $raw_websockets,
              active_games: $active_games
            }
          ' >>"$samples"
        if (( sockets == 0 && games == 0 )); then
          consecutive=$((consecutive + 1))
        else
          consecutive=0
        fi
        if (( consecutive >= 3 )); then
          jq -s '{passed: true, required_consecutive_samples: 3, samples: .}' \
            "$samples" >"$summary"
          rm -f "$control_candidate"
          return 0
        fi
      else
        consecutive=0
      fi
      sleep 1
    done
    jq -s '{passed: false, required_consecutive_samples: 3, samples: .}' \
      "$samples" >"$summary"
    [[ -f "$control_candidate" ]] \
      && mv "$control_candidate" "$report_dir/zero-load-$label.control.json"
    echo "$label retained regional WebSockets or authoritative games after load removal" >&2
    return 1
  }

  wait_for_certification_envelope() {
    local label="$1"
    local observed_pid="$2"
    local stable_seconds="$3"
    local baseline_control="$4"
    local evidence_dir="$report_dir/envelope-$label"
    local samples="$evidence_dir/samples.jsonl"
    local control_candidate="$evidence_dir/control.pending.json"
    local deadline=$((SECONDS + 600))
    local consecutive=0
    # N qualifying samples contain only N-1 inter-sample intervals. Requiring
    # one extra sample makes the label a real minimum duration, not a count.
    local required_samples=$((stable_seconds + 1))
    mkdir -p "$evidence_dir"
    : >"$samples"
    while (( SECONDS < deadline )); do
      require_runner_running "$label load runner" "$observed_pid"
      local users=0
      local games=0
      local user_candidate="$evidence_dir/users.pending.json"
      if curl -fsS --max-time 3 \
        "${SNAKETRON_STAGING_TARGET%/}/api/regions/user-counts" \
        >"$user_candidate" \
        && capture_control_status "$control_candidate" 2>/dev/null; then
        users="$(jq -r --arg region "$SNAKETRON_REGION_CODE" \
          '.[$region] // 0' "$user_candidate")"
        games="$(jq -r \
          '[.runtime_partitions[].active_games] | add // 0' \
          "$control_candidate")"
        if ! jq -e --slurpfile baseline "$baseline_control" '
          ([.live_members[]
            | select(.lifecycle == "ACTIVE")
            | "\(.server_id):\(.boot_id)"] | unique | sort)
          == ([$baseline[0].live_members[]
            | select(.lifecycle == "ACTIVE")
            | "\(.server_id):\(.boot_id)"] | unique | sort)
          and all(.runtime_partitions[]; .owner_matches)
        ' "$control_candidate" >/dev/null; then
          echo "$label lost or replaced a verified task before its envelope gate completed" >&2
          return 1
        fi
      fi
      jq -cn \
        --argjson observed_at_unix_ms "$(unix_time_ms)" \
        --argjson raw_websockets "$users" \
        --argjson active_games "$games" \
        '{
          observed_at_unix_ms: $observed_at_unix_ms,
          raw_websockets: $raw_websockets,
          active_games: $active_games
        }' >>"$samples"
      if (( users >= 256 && games >= 128 )); then
        consecutive=$((consecutive + 1))
      else
        consecutive=0
      fi
      if (( consecutive >= required_samples )); then
        mv "$control_candidate" "$evidence_dir/control.json"
        jq -s \
          --argjson required_stable_seconds "$stable_seconds" \
          --argjson required_qualifying_samples "$required_samples" \
          '{
            required_stable_seconds: $required_stable_seconds,
            required_qualifying_samples: $required_qualifying_samples,
            samples: .
          }' \
          "$samples" >"$evidence_dir/summary.json"
        return 0
      fi
      sleep 1
    done
    jq -s \
      --argjson required_stable_seconds "$stable_seconds" \
      --argjson required_qualifying_samples "$required_samples" \
      '{
        required_stable_seconds: $required_stable_seconds,
        required_qualifying_samples: $required_qualifying_samples,
        samples: .
      }' \
      "$samples" >"$evidence_dir/summary.json"
    echo "$label did not hold at least 256 public WebSockets and 128 active games for $stable_seconds seconds" >&2
    return 1
  }

  if [[ "$crash_mode" == true ]]; then
    # Crash certification is capacity testing, not a scale-out trigger. Reach
    # ten verified ready tasks before creating the first synthetic user.
    retry_command 5 set_scaling_suspended true
    retry_command 5 aws ecs update-service \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --service "$SNAKETRON_ECS_SERVICE" \
      --desired-count 10 >/dev/null
    wait_for_running_count 10
    wait_for_ecs_health "$report_dir" crash-baseline-10 10
    wait_for_traefik_task_readiness "$report_dir" crash-baseline-10
    wait_for_control_plane crash-baseline-10 10
    verify_crash_exec_configuration "$report_dir" crash-baseline-10
    local crash_command=(
      "$loadtest_runner"
      --target "$SNAKETRON_STAGING_TARGET" \
      --confirm-production \
      --require-same-origin \
      --region "$SNAKETRON_REGION_CODE" \
      --mode duel \
      --stages 272@8m \
      --spawn-rate 4 \
      --max-total-sessions 8192 \
      --command-profile every-tick \
      --run-id "$run_id" \
      --report-dir "$report_dir"
    )
    "${crash_command[@]}" &
    load_pid=$!
    # Require the public and authoritative views to hold the supported
    # envelope for thirty consecutive seconds before selecting the kill.
    wait_for_certification_envelope hard-crash "$load_pid" 30 \
      "$report_dir/control-plane-crash-baseline-10.json"
    inject_hard_crash_and_prove_takeover "$report_dir"
    wait_for_running_count 10
    wait_for_ecs_health "$report_dir" hard-crash-replacement-10 10
    wait_for_traefik_task_readiness "$report_dir" hard-crash-replacement-10
    require_load_running
    wait "$load_pid"
    load_pid=""

    local final_control="$report_dir/control-plane-hard-crash-final-10.json"
    local final_candidate="$final_control.pending"
    local final_deadline=$((SECONDS + 60))
    while (( SECONDS < final_deadline )); do
      if capture_control_status "$final_candidate" 2>/dev/null \
        && jq -e '
            ([.live_members[] | select(.lifecycle == "ACTIVE")] | length) == 10
            and .assignment != null
            and all(.runtime_partitions[];
              .consumer_group_exists
              and .owner_matches
              and .pending_count == 0
              and .pending_completion_count == 0
              and .quarantined_command_count == 0)
          ' "$final_candidate" >/dev/null; then
        mv "$final_candidate" "$final_control"
        break
      fi
      sleep 0.2
    done
    if [[ ! -f "$final_control" ]]; then
      echo "Executor partitions did not fully drain after hard-crash load completion" >&2
      return 1
    fi

    local load_summary="$report_dir/$run_id/summary.json"
    local evidence_finished_at
    evidence_finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    collect_crash_ecs_runtime_evidence "$report_dir"
    stop_traefik_monitor
    assert_traefik_monitor "$report_dir"
    assert_hard_crash_report "$report_dir" "$load_summary"
    echo "Hard-crash staging evidence written to $report_dir"
    return 0
  fi

  local continuity_run_id="${run_id}-continuity"
  local capacity_run_id="${run_id}-capacity"
  local idle_run_id="${run_id}-idle"
  local lobby_run_id="${run_id}-lobby"
  local matchmaking_run_id="${run_id}-matchmaking"
  local admission_run_id="${run_id}-admission"
  local continuity_command=(
    "$loadtest_runner"
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --mode duel \
    --stages 64@20m \
    --spawn-rate 4 \
    --max-total-sessions 4096 \
    --command-profile every-tick \
    --require-planned-handoff \
    --run-id "$continuity_run_id" \
    --report-dir "$report_dir"
  )
  "${continuity_command[@]}" &
  load_pid=$!
  # Sixty-four sessions are the first evidence-backed calibration. Failure to
  # trigger the configured CPU/memory policy is a certification failure; never
  # escalate the initial task to the full capacity envelope.
  wait_for_automatic_scale_out \
    "$report_dir" "$evidence_started_epoch" "$load_pid"

  # Freeze policy writes only for the deterministic ownership staircase. This
  # keeps the autoscaler from undoing the forced ten-to-one leg while commands
  # remain under load; the trap restores the original enabled state.
  retry_command 5 set_scaling_suspended true
  local automatic_scale_out_count
  automatic_scale_out_count="$(aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" \
    --query 'services[0].desiredCount' \
    --output text)"
  if [[ ! "$automatic_scale_out_count" =~ ^[0-9]+$ ]] \
    || (( automatic_scale_out_count < 2 || automatic_scale_out_count > 10 )); then
    echo "Target tracking did not leave a valid added-capacity count: $automatic_scale_out_count" >&2
    exit 1
  fi
  local automatic_scale_out_label="automatic-scale-out-$automatic_scale_out_count"
  wait_for_running_count "$automatic_scale_out_count"
  wait_for_ecs_health \
    "$report_dir" "$automatic_scale_out_label" "$automatic_scale_out_count"
  wait_for_traefik_task_readiness "$report_dir" "$automatic_scale_out_label"
  wait_for_control_plane "$automatic_scale_out_label" "$automatic_scale_out_count"
  require_load_running
  # Automatic scale-out may have stopped at any count from two through ten.
  # Return to a measured one-task baseline before calling the next phase a
  # deterministic 1 -> 10 -> 1 staircase.
  retry_command 5 aws ecs update-service \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service "$SNAKETRON_ECS_SERVICE" \
    --desired-count 1 >/dev/null
  wait_for_running_count 1
  wait_for_control_plane forced-initial-1 1
  wait_for_ecs_health "$report_dir" forced-initial-1 1
  require_load_running
  local scale_out_started_ms
  scale_out_started_ms="$(unix_time_ms)"
  retry_command 5 aws ecs update-service \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service "$SNAKETRON_ECS_SERVICE" \
    --desired-count 10 >/dev/null
  wait_for_running_count 10
  wait_for_control_plane scale-10 10
  wait_for_ecs_health "$report_dir" scale-10 10
  wait_for_traefik_task_readiness "$report_dir" scale-10
  local scale_out_finished_ms
  scale_out_finished_ms="$(unix_time_ms)"
  jq -n \
    --argjson started_at_unix_ms "$scale_out_started_ms" \
    --argjson finished_at_unix_ms "$scale_out_finished_ms" \
    '{
      started_at_unix_ms: $started_at_unix_ms,
      finished_at_unix_ms: $finished_at_unix_ms,
      duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
    }' >"$report_dir/scale-out-window.json"
  jq -e '.quickmatch_two_v_two_queued_lobbies == 0' \
    "$report_dir/control-plane-scale-10.json" >/dev/null || {
      echo "Dedicated staging 2v2 quickmatch queue is not empty; refusing a nondeterministic waiter cohort" >&2
      exit 1
    }
  # Start durable context cohorts only after all ten tasks are ready. Three 2v2
  # queue entrants cannot form a four-player match, so that cohort stays a
  # waiter without any production-path test hook.
  "$loadtest_runner" \
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --population idle \
    --mode duel \
    --stages 10@15m \
    --spawn-rate 4 \
    --max-total-sessions 10 \
    --untimed-play-duration 15m \
    --drain-timeout 1m \
    --require-planned-handoff \
    --run-id "$idle_run_id" \
    --report-dir "$report_dir" &
  idle_population_pid=$!
  "$loadtest_runner" \
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --population lobby \
    --mode duel \
    --stages 10@15m \
    --spawn-rate 4 \
    --max-total-sessions 10 \
    --untimed-play-duration 15m \
    --drain-timeout 1m \
    --require-planned-handoff \
    --run-id "$lobby_run_id" \
    --report-dir "$report_dir" &
  lobby_population_pid=$!
  "$loadtest_runner" \
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --population matchmaking \
    --mode 2v2 \
    --stages 3@15m \
    --spawn-rate 3 \
    --max-total-sessions 3 \
    --untimed-play-duration 15m \
    --drain-timeout 1m \
    --require-planned-handoff \
    --run-id "$matchmaking_run_id" \
    --report-dir "$report_dir" &
  matchmaking_population_pid=$!
  # Allow one complete duel time limit so replacement sockets and the three
  # context cohorts exercise the settled gateways before scale-in.
  sleep 120
  require_load_running
  require_population_running idle "$idle_population_pid"
  require_population_running lobby "$lobby_population_pid"
  require_population_running matchmaking "$matchmaking_population_pid"
  # Refresh both views immediately before the measured transition. Membership
  # carries the ECS task ID, so this also proves that the control-plane members
  # and the ten healthy ECS tasks are the same exact set.
  wait_for_control_plane pre-scale-in-10 10
  wait_for_ecs_health "$report_dir" pre-scale-in-10 10
  jq -e \
    --slurpfile control "$report_dir/control-plane-pre-scale-in-10.json" '
      ([.tasks[].taskArn | split("/")[-1]] | unique | sort)
      == ([$control[0].live_members[]
            | select(.lifecycle == "ACTIVE")
            | .ecs_task_id]
          | unique | sort)
    ' "$report_dir/ecs-pre-scale-in-10/tasks.json" >/dev/null || {
      echo "Fresh executor membership does not match the ten healthy ECS task IDs" >&2
      exit 1
    }
  # Generate four new idle admissions per second through the direct ten-to-one
  # action. Two hundred eight low-CPU sockets cover the 45-second application
  # drain deadline, including the heartbeat used to observe the seed wave,
  # without putting the full game-command envelope on one task.
  # The public count is a heartbeat-delayed raw WebSocket count. It proves the
  # candidate sockets exist; the finished admission report proves auth/readiness.
  wait_for_region_socket_floor pre-admission 87
  "$loadtest_runner" \
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --population idle \
    --mode duel \
    --stages 208@2m \
    --spawn-rate 4 \
    --max-total-sessions 208 \
    --untimed-play-duration 2m \
    --drain-timeout 1m \
    --require-planned-handoff \
    --run-id "$admission_run_id" \
    --report-dir "$report_dir" &
  admission_population_pid=$!
  wait_for_region_socket_floor admission-seed 91 "$admission_population_pid"
  local scale_in_started_ms
  scale_in_started_ms="$(unix_time_ms)"
  retry_command 5 aws ecs update-service \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service "$SNAKETRON_ECS_SERVICE" \
    --desired-count 1 >/dev/null
  wait_for_running_count 1
  wait_for_control_plane final-1 1
  wait_for_ecs_health "$report_dir" final-1 1
  local scale_in_finished_ms
  scale_in_finished_ms="$(unix_time_ms)"
  jq -n \
    --argjson started_at_unix_ms "$scale_in_started_ms" \
    --argjson finished_at_unix_ms "$scale_in_finished_ms" \
    '{
      started_at_unix_ms: $started_at_unix_ms,
      finished_at_unix_ms: $finished_at_unix_ms,
      duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
    }' >"$report_dir/scale-in-window.json"

  jq -n \
    --slurpfile initial "$report_dir/control-plane-forced-initial-1.json" \
    --slurpfile ten "$report_dir/control-plane-scale-10.json" \
    --slurpfile final "$report_dir/control-plane-final-1.json" '
      def moved($left; $right):
        [range(0; 10) as $partition
          | ($partition | tostring) as $key
          | select($left.assignment.owners[$key] != $right.assignment.owners[$key])]
        | length;
      {
        initial_version: $initial[0].assignment.version,
        scale_10_version: $ten[0].assignment.version,
        final_version: $final[0].assignment.version,
        scale_out_moved_partitions: moved($initial[0]; $ten[0]),
        scale_in_moved_partitions: moved($ten[0]; $final[0])
      }
    ' >"$report_dir/assignment-movement.json"
  jq -e '
    .initial_version < .scale_10_version
    and .scale_10_version < .final_version
    and .scale_out_moved_partitions == 9
    and .scale_in_moved_partitions == 9
  ' "$report_dir/assignment-movement.json" >/dev/null || {
    echo "Assignment versions or minimum 1 -> 10 -> 1 movement invariants failed" >&2
    exit 1
  }

  wait "$load_pid"
  load_pid=""
  wait "$idle_population_pid"
  idle_population_pid=""
  wait "$lobby_population_pid"
  lobby_population_pid=""
  wait "$matchmaking_population_pid"
  matchmaking_population_pid=""
  wait "$admission_population_pid"
  admission_population_pid=""
  local continuity_summary="$report_dir/$continuity_run_id/summary.json"
  local idle_summary="$report_dir/$idle_run_id/summary.json"
  local lobby_summary="$report_dir/$lobby_run_id/summary.json"
  local matchmaking_summary="$report_dir/$matchmaking_run_id/summary.json"
  local admission_summary="$report_dir/$admission_run_id/summary.json"
  jq -e \
    --slurpfile scale_out "$report_dir/scale-out-window.json" \
    --slurpfile scale_in "$report_dir/scale-in-window.json" '
      def scheduled_at($report; $second):
        [range(0; 10) as $partition
          | ($report.metrics.scheduled_command_counts_by_partition_and_unix_second
              [($partition | tostring)][($second | tostring)] // 0)]
        | add // 0;
      . as $report
      | (($scale_out[0].started_at_unix_ms / 1000) | ceil) as $scale_out_first_second
      | (($scale_out[0].finished_at_unix_ms / 1000) | floor) as $scale_out_after_last_second
      | .schema_version >= 9
      and .metadata.threshold_result == "passed"
      and .configured_max_concurrency == 64
      and .metadata.mode == "duel"
      and .metadata.command_profile == "every-tick"
      and .metadata.spawn_rate_per_second == "4"
      and .session_counts.peak_authenticated_concurrency == 64
      and .session_counts.peak_active_game_concurrency >= 32
      and .session_counts.failed == 0
      and .session_counts.cancelled == 0
      and .session_counts.incomplete == 0
      and .session_counts.completed == .session_counts.total
      and all(.sessions[]; .outcome == "completed" and .failure_phase == null)
      and .games.pairing_violations == 0
      and (.ramp_stages | length) == 1
      and .ramp_stages[0].target_reached
      and .metrics.traffic.disconnects == 0
      and .metrics.traffic.reconnects == 0
      and ([.metrics.command_counts_by_unix_second[]] | add)
        == .metrics.traffic.commands_sent
      and (.metrics.usable_session_gap_ms.max_ms // 0) == 0
      and .metrics.planned_handoffs.attempts > 0
      and .metrics.planned_handoffs.failures == 0
      and .metrics.planned_handoffs.successes == .metrics.planned_handoffs.attempts
      and .metrics.planned_handoffs.outcome_barriers > 0
      and .metrics.planned_handoff_duration_ms.max_ms <= 20000
      and .metrics.planned_handoffs.pending_commands_at_finish == 0
      # Scale-up moves executor authority but shuts down no gateway. Prove
      # continuous submitted and authoritative command flow; do not invent a
      # WebSocket Drain requirement for a task that remains alive.
      and ($scale_out_after_last_second - $scale_out_first_second) >= 1
      and all(range($scale_out_first_second; $scale_out_after_last_second);
        . as $second
        | (($report.metrics.command_counts_by_unix_second
              [($second | tostring)] // 0) > 0)
          and scheduled_at($report; $second) > 0)
      and any(.sessions[];
        any(.planned_game_handoff_at_unix_ms[];
          . >= $scale_in[0].started_at_unix_ms
          and . <= $scale_in[0].finished_at_unix_ms))
      and ($scale_in[0].duration_ms >= 1000 and $scale_in[0].duration_ms <= 45000)
      and (.metrics.scheduled_command_counts_by_partition_and_unix_second | length) == 10
      and ([.metrics.scheduled_command_counts_by_partition_and_unix_second[] | .[]] | add) > 0
    ' "$continuity_summary" >/dev/null || {
      echo "Continuity load did not prove the active-game 1 -> 10 -> 1 ownership and zero-gap handoff path" >&2
      exit 1
    }

  jq -e --slurpfile scale_in "$report_dir/scale-in-window.json" '
    def p99:
      sort as $values
      | if ($values | length) == 0 then null
        else $values[(((((($values | length) * 99) + 99) / 100) | floor) - 1)]
        end;
    [.sessions[]
      | select(
          .started_at_unix_ms >= $scale_in[0].started_at_unix_ms
          and .started_at_unix_ms <= $scale_in[0].finished_at_unix_ms)]
      as $scale_in_sessions
    | ($scale_in_sessions
        | group_by(.wave_index)
        | map({
            wave_index: .[0].wave_index,
            started_at_unix_ms: (map(.started_at_unix_ms) | min),
            sessions: .
          })
        | sort_by(.started_at_unix_ms)) as $admission_waves
    | .schema_version >= 9
    and .metadata.threshold_result == "passed"
    and .metadata.population == "idle"
    and .configured_max_concurrency == 208
    and .metadata.spawn_rate_per_second == "4"
    and .session_counts.peak_authenticated_concurrency == 208
    and .session_counts.failed == 0
    and .session_counts.cancelled == 0
    and .session_counts.incomplete == 0
    and .session_counts.completed == .session_counts.total
    and .games.expected == 0
    and .games.observed == 0
    and all(.sessions[]; .outcome == "completed" and .failure_phase == null)
    and (.ramp_stages | length) == 1
    and .ramp_stages[0].target_reached
    and .metrics.traffic.disconnects == 0
    and .metrics.traffic.reconnects == 0
    and (.metrics.usable_session_gap_ms.max_ms // 0) == 0
    and .metrics.planned_handoffs.failures == 0
    and .metrics.planned_handoffs.successes == .metrics.planned_handoffs.attempts
    and .metrics.planned_handoff_duration_ms.max_ms <= 20000
    and ($scale_in[0].duration_ms >= 1000 and $scale_in[0].duration_ms <= 45000)
    and ($admission_waves | length) >= 2
    and $admission_waves[0].started_at_unix_ms
      <= ($scale_in[0].started_at_unix_ms + 1100)
    and $admission_waves[-1].started_at_unix_ms
      >= ($scale_in[0].finished_at_unix_ms - 1100)
    and all($admission_waves[];
      (.sessions | length) == 4
      and all(.sessions[];
        .outcome == "completed"
        and .failure_phase == null
        and .initial_admission_ready_ms != null
        and .initial_admission_ready_ms <= 10000))
    and all(range(1; ($admission_waves | length));
      . as $index
      | ($admission_waves[$index].started_at_unix_ms
        - $admission_waves[$index - 1].started_at_unix_ms) <= 1100)
    and all($scale_in_sessions[];
      .outcome == "completed"
      and .failure_phase == null
      and .initial_admission_ready_ms != null)
    and ([$scale_in_sessions[].initial_admission_ready_ms] | p99) <= 10000
    and (.metrics.initial_admission_ready_ms.p99_ms // 10001) <= 10000
  ' "$admission_summary" >/dev/null || {
    echo "Planned scale-in did not preserve four-per-second admission, ten-second readiness, and zero-gap handoff" >&2
    exit 1
  }

  assert_population_summary() {
    local summary="$1"
    local population="$2"
    local expected_concurrency="$3"
    local ready_field="$4"
    jq -e \
      --arg population "$population" \
      --arg ready_field "$ready_field" \
      --argjson expected_concurrency "$expected_concurrency" \
      --slurpfile scale_in "$report_dir/scale-in-window.json" \
      --slurpfile pre_scale_in "$report_dir/control-plane-pre-scale-in-10.json" '
        ($pre_scale_in[0].live_members
          | map(select(.lifecycle == "ACTIVE") | "\(.server_id):\(.boot_id)")) as $eligible_boot_ids
        | .schema_version >= 9
        and .metadata.threshold_result == "passed"
        and .metadata.population == $population
        and .configured_max_concurrency == $expected_concurrency
        and .session_counts.peak_authenticated_concurrency == $expected_concurrency
        and .games.expected == 0
        and .games.observed == 0
        and .metrics.traffic.disconnects == 0
        and .metrics.traffic.reconnects == 0
        and (.metrics.usable_session_gap_ms.max_ms // 0) == 0
        and .metrics.planned_handoffs.attempts > 0
        and .metrics.planned_handoffs.failures == 0
        and .metrics.planned_handoffs.successes == .metrics.planned_handoffs.attempts
        and .metrics.planned_handoffs.continuity_proofs == .metrics.planned_handoffs.successes
        and .metrics.planned_handoff_duration_ms.max_ms <= 20000
        and .ramp_stages[0].target_reached
        and .ramp_stages[0].target_reached_at_unix_ms <= $scale_in[0].started_at_unix_ms
        and all(.sessions[];
          .outcome == "completed"
          and .failure_phase == null
          and .initial_admission_ready_ms != null
          and .initial_admission_ready_ms <= 10000
          and .initial_task_boot_id != null
          and (.initial_task_boot_id as $boot_id
            | ($eligible_boot_ids | index($boot_id)) != null)
          and .[$ready_field] != null
          and .[$ready_field] <= $scale_in[0].started_at_unix_ms
          and .finished_at_unix_ms >= $scale_in[0].finished_at_unix_ms
          and (
            if $population == "idle" then
              .lobby_code == null and .game_id == null
            elif $population == "lobby" then
              .lobby_code != null and .game_id == null
            else
              .lobby_code != null and .game_id == null
              and .matchmaking_at_unix_ms != null
            end
          )
        )
      ' "$summary" >/dev/null || {
        echo "$population population did not remain healthy and correctly positioned through scale-in" >&2
        exit 1
      }
  }

  assert_population_summary "$idle_summary" idle 10 authenticated_at_unix_ms
  assert_population_summary "$lobby_summary" lobby 10 lobby_ready_at_unix_ms
  assert_population_summary \
    "$matchmaking_summary" matchmaking 3 queued_at_unix_ms

  # Reconstruct the WebSocket `<database server id>:<executor boot UUID>` task
  # ID from the same two fields in membership and compare the complete exact
  # identity; connection count alone is not distribution evidence.
  jq -n \
    --slurpfile ten_start "$report_dir/control-plane-scale-10.json" \
    --slurpfile pre_scale_in "$report_dir/control-plane-pre-scale-in-10.json" \
    --slurpfile scale_in "$report_dir/scale-in-window.json" \
    --slurpfile game "$continuity_summary" \
    --slurpfile idle "$idle_summary" \
    --slurpfile lobby "$lobby_summary" \
    --slurpfile matchmaking "$matchmaking_summary" '
      ($pre_scale_in[0].live_members
        | map(select(.lifecycle == "ACTIVE") | "\(.server_id):\(.boot_id)")
        | unique
        | sort) as $expected
      | def ids($report):
          [$report[0].sessions[]
            | select(
                .authenticated_at_unix_ms != null
                and .authenticated_at_unix_ms >= $ten_start[0].captured_at_ms
                and .authenticated_at_unix_ms <= $scale_in[0].started_at_unix_ms
                and .finished_at_unix_ms >= $scale_in[0].started_at_unix_ms
                and .initial_task_boot_id != null
              )
            | .initial_task_boot_id
            | select(. as $id | ($expected | index($id)) != null)]
          | unique
          | sort;
      def counts($report):
        reduce (
          [$report[0].sessions[]
            | select(
                .authenticated_at_unix_ms != null
                and .authenticated_at_unix_ms >= $ten_start[0].captured_at_ms
                and .authenticated_at_unix_ms <= $scale_in[0].started_at_unix_ms
                and .finished_at_unix_ms >= $scale_in[0].started_at_unix_ms
                and .initial_task_boot_id != null
              )
            | .initial_task_boot_id
            | select(. as $id | ($expected | index($id)) != null)]
          | group_by(.)[]
          | {key: .[0], value: length}
        ) as $entry ({}; . + {($entry.key): $entry.value});
      def event_counts($report):
        reduce (
          [$report[0].sessions[]
            | select(
                .authenticated_at_unix_ms != null
                and .authenticated_at_unix_ms >= $ten_start[0].captured_at_ms
                and .authenticated_at_unix_ms <= $scale_in[0].started_at_unix_ms
                and .finished_at_unix_ms >= $scale_in[0].started_at_unix_ms
                and .playing_at_unix_ms != null
                and .playing_at_unix_ms <= $scale_in[0].started_at_unix_ms
                and .first_game_event_at_unix_ms != null
                and .first_game_event_at_unix_ms <= $scale_in[0].started_at_unix_ms
                and .initial_task_boot_id != null
                and .game_events_received > 0
                and (.initial_task_boot_id as $id | ($expected | index($id)) != null)
              )
            | {task_boot_id: .initial_task_boot_id, events: .game_events_received}]
          | group_by(.task_boot_id)[]
          | {key: .[0].task_boot_id, value: (map(.events) | add)}
        ) as $entry ({}; . + {($entry.key): $entry.value});
      (counts($game)) as $game_counts
      | (event_counts($game)) as $game_event_counts
      | {
        ten_task_admission_started_at_ms: $ten_start[0].captured_at_ms,
        fresh_membership_captured_at_ms: $pre_scale_in[0].captured_at_ms,
        expected_task_boot_ids: $expected,
        game_task_boot_ids: ids($game),
        idle_task_boot_ids: ids($idle),
        lobby_task_boot_ids: ids($lobby),
        matchmaking_task_boot_ids: ids($matchmaking),
        game_task_counts: $game_counts,
        game_event_counts: $game_event_counts,
        idle_task_counts: counts($idle),
        lobby_task_counts: counts($lobby),
        matchmaking_task_counts: counts($matchmaking),
        transition: {
          configured_game_websockets: 64,
          companion_websockets: 23,
          configured_total_websockets: 87,
          observed_game_websockets: ([$game_counts[]] | add // 0),
          observed_total_websockets:
            (([$game_counts[]] | add // 0)
              + ([counts($idle)[]] | add // 0)
              + ([counts($lobby)[]] | add // 0)
              + ([counts($matchmaking)[]] | add // 0))
        },
        covered_task_boot_ids: (
          (ids($game) + ids($idle) + ids($lobby) + ids($matchmaking))
          | unique
          | sort
        )
      }
    ' >"$report_dir/population-distribution.json"
  jq -e '
    .covered_task_boot_ids == .expected_task_boot_ids
    and .game_task_boot_ids == .expected_task_boot_ids
    and ([.game_task_counts | keys[]] | sort) == .expected_task_boot_ids
    and ([.game_event_counts | keys[]] | sort) == .expected_task_boot_ids
    and all(.game_event_counts[]; . > 0)
    and (.idle_task_boot_ids | length) > 1
    and (.lobby_task_boot_ids | length) > 1
    and (.matchmaking_task_boot_ids | length) >= 1
    and .transition.configured_game_websockets == 64
    and .transition.companion_websockets == 23
    and .transition.configured_total_websockets == 87
    and .transition.observed_game_websockets == 64
    and .transition.observed_total_websockets == 87
  ' "$report_dir/population-distribution.json" >/dev/null || {
    echo "Exact TaskBootId transition WebSocket/event-forwarding distribution was not proven" >&2
    exit 1
  }

  # Run B is the supported capacity envelope. Re-establish ten tasks without
  # clients and verify ECS, Traefik, membership, assignment, and leases before
  # creating the first capacity session.
  wait_for_zero_certification_load before-capacity
  retry_command 5 aws ecs update-service \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service "$SNAKETRON_ECS_SERVICE" \
    --desired-count 10 >/dev/null
  wait_for_running_count 10
  wait_for_control_plane capacity-10 10
  wait_for_ecs_health "$report_dir" capacity-10 10
  wait_for_traefik_task_readiness "$report_dir" capacity-10

  local capacity_command=(
    "$loadtest_runner"
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --mode duel \
    --stages 272@8m \
    --spawn-rate 4 \
    --max-total-sessions 8192 \
    --command-profile every-tick \
    --run-id "$capacity_run_id" \
    --report-dir "$report_dir"
  )
  "${capacity_command[@]}" &
  capacity_pid=$!
  wait_for_certification_envelope capacity "$capacity_pid" 1 \
    "$report_dir/control-plane-capacity-10.json"
  wait "$capacity_pid"
  capacity_pid=""
  local capacity_summary="$report_dir/$capacity_run_id/summary.json"

  jq -e '
    def fully_joined_duels_at($report; $midpoint):
      ([$report.sessions[] | select(.game_id != null)]
        | group_by(.game_id)
        | map(select(
            length == 2
            and all(.[];
              .playing_at_unix_ms != null
              and .game_finished_at_unix_ms != null
              and .playing_at_unix_ms <= $midpoint
              and .game_finished_at_unix_ms > $midpoint)))
        | length);
    . as $report
    | .ramp_stages[0].target_reached_at_unix_ms as $hold_started_at_ms
    | .ramp_stages[0].finished_at_unix_ms as $hold_finished_at_ms
    | (($hold_started_at_ms / 1000) | ceil) as $hold_first_second
    | (($hold_finished_at_ms / 1000) | floor) as $hold_after_last_second
    | 1280 as $minimum_commands_per_second
    | .schema_version >= 9
    and .metadata.threshold_result == "passed"
    and .configured_max_concurrency == 272
    and .metadata.mode == "duel"
    and .metadata.command_profile == "every-tick"
    and .metadata.spawn_rate_per_second == "4"
    and .session_counts.peak_authenticated_concurrency == 272
    and .session_counts.peak_active_game_concurrency >= 136
    and .session_counts.failed == 0
    and .session_counts.cancelled == 0
    and .session_counts.incomplete == 0
    and .session_counts.completed == .session_counts.total
    and all(.sessions[]; .outcome == "completed" and .failure_phase == null)
    and .games.pairing_violations == 0
    and (.ramp_stages | length) == 1
    and .ramp_stages[0].target_reached
    and ($hold_finished_at_ms - $hold_started_at_ms) >= 300000
    and ($hold_after_last_second - $hold_first_second) >= 299
    and all(range($hold_first_second; $hold_after_last_second);
      . as $second
      | (($second * 1000) + 500) as $midpoint
      | ([$report.sessions[]
          | select(
              .authenticated_at_unix_ms != null
              and .authenticated_at_unix_ms <= $midpoint
              and .finished_at_unix_ms > $midpoint)] | length) >= 256
        and fully_joined_duels_at($report; $midpoint) >= 128
        and (($report.metrics.command_counts_by_unix_second[($second | tostring)] // 0)
          >= $minimum_commands_per_second)
        and all(range(0; 10);
          . as $partition
          | (($report.metrics.scheduled_command_counts_by_partition_and_unix_second
                [($partition | tostring)][($second | tostring)] // 0) > 0)))
    and .metrics.traffic.disconnects == 0
    and .metrics.traffic.reconnects == 0
    and (.metrics.usable_session_gap_ms.max_ms // 0) == 0
    and ([.metrics.command_counts_by_unix_second[]] | add)
      == .metrics.traffic.commands_sent
    and .metrics.planned_handoffs.pending_commands_at_finish == 0
    and (.metrics.initial_admission_ready_ms.p99_ms // 10001) <= 10000
    and (.metrics.scheduled_command_counts_by_partition_and_unix_second | length) == 10
  ' "$capacity_summary" >/dev/null || {
    echo "Ten-task Run B did not hold the 256-session/128-duel every-tick envelope for five continuous minutes" >&2
    exit 1
  }
  jq -n --slurpfile capacity "$capacity_summary" '
    {
      started_at_unix_ms: $capacity[0].ramp_stages[0].target_reached_at_unix_ms,
      finished_at_unix_ms: $capacity[0].ramp_stages[0].finished_at_unix_ms,
      duration_ms: (
        $capacity[0].ramp_stages[0].finished_at_unix_ms
        - $capacity[0].ramp_stages[0].target_reached_at_unix_ms)
    }
  ' >"$report_dir/capacity-window.json"

  jq -n \
    --slurpfile control "$report_dir/control-plane-capacity-10.json" \
    --slurpfile game "$capacity_summary" '
      ($control[0].live_members
        | map(select(.lifecycle == "ACTIVE") | "\(.server_id):\(.boot_id)")
        | unique | sort) as $expected
      | ([$game[0].sessions[]
          | select(.initial_task_boot_id != null)
          | .initial_task_boot_id]
        | group_by(.)
        | map({key: .[0], value: length})
        | from_entries) as $session_counts
      | ([$game[0].sessions[]
          | select(.initial_task_boot_id != null and .game_events_received > 0)
          | {task_boot_id: .initial_task_boot_id, events: .game_events_received}]
        | group_by(.task_boot_id)
        | map({key: .[0].task_boot_id, value: (map(.events) | add)})
        | from_entries) as $event_counts
      | {
          expected_task_boot_ids: $expected,
          session_task_boot_ids: ($session_counts | keys | sort),
          event_task_boot_ids: ($event_counts | keys | sort),
          session_counts: $session_counts,
          event_counts: $event_counts,
          configured_game_websockets: 272,
          peak_authenticated_game_websockets:
            $game[0].session_counts.peak_authenticated_concurrency
        }
    ' >"$report_dir/capacity-distribution.json"
  jq -e '
    .session_task_boot_ids == .expected_task_boot_ids
    and .event_task_boot_ids == .expected_task_boot_ids
    and all(.event_counts[]; . > 0)
    and .configured_game_websockets == 272
    and .peak_authenticated_game_websockets == 272
  ' "$report_dir/capacity-distribution.json" >/dev/null || {
    echo "Run B did not distribute authenticated game sockets and events across every verified capacity task" >&2
    exit 1
  }

  # Only after all synthetic clients have exited do we re-enable target
  # tracking and require an AWS-observed automatic ten-to-one scale-in.
  wait_for_zero_certification_load before-automatic-scale-in
  wait_for_control_plane automatic-scale-in-baseline 10
  wait_for_ecs_health "$report_dir" automatic-scale-in-baseline 10
  local automatic_scale_in_started_epoch
  automatic_scale_in_started_epoch="$(date -u +%s)"
  retry_command 5 set_scaling_suspended false
  wait_for_automatic_scale_in "$report_dir" "$automatic_scale_in_started_epoch"
  wait_for_control_plane automatic-final-1 1
  wait_for_ecs_health "$report_dir" automatic-final-1 1
  collect_ecs_runtime_evidence "$report_dir"
  jq -n \
    --slurpfile automatic_out "$report_dir/automatic-scale-out.json" \
    --slurpfile forced "$report_dir/assignment-movement.json" \
    --slurpfile automatic_in "$report_dir/automatic-scale-in.json" '
      {
        automatic_scale_out: {
          desired: $automatic_out[0].services[0].desiredCount,
          running: $automatic_out[0].services[0].runningCount,
          evidence: "target-tracking activity"
        },
        deterministic_forced_staircase: $forced[0],
        fixed_capacity_envelope: {
          configured_sessions: 272,
          required_sessions: 256,
          required_duels: 128,
          held_seconds: 300,
          settled_tasks: 10
        },
        automatic_scale_in_after_load_removal: {
          desired: $automatic_in[0].services[0].desiredCount,
          running: $automatic_in[0].services[0].runningCount,
          evidence: "target-tracking activity"
        }
      }
    ' >"$report_dir/scaling-phases.json"

  local evidence_finished_at
  evidence_finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stop_traefik_monitor
  assert_traefik_monitor "$report_dir"
  # CloudWatch datapoints commonly arrive after their measurement timestamp.
  # Waiting changes no cloud state and prevents a false pass on partial data.
  sleep 120
  collect_cloudwatch_evidence "$report_dir"
  collect_container_insights_evidence "$report_dir"

  echo "Staging evidence written to $report_dir"
}

case "$mode" in
  local)
    run_local_suite
    ;;
  --staging)
    run_staging_suite planned
    ;;
  --staging-crash)
    run_staging_suite crash
    ;;
  --test-evidence-sanitizer)
    require_command jq
    test_evidence_safety_helpers
    echo "Evidence safety helper tests passed"
    ;;
  *)
    echo "Usage: $0 [local|--staging|--staging-crash|--test-evidence-sanitizer]" >&2
    exit 2
    ;;
esac
