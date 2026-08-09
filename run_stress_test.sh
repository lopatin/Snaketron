#!/usr/bin/env bash

# Run the bounded, isolated stress cohort used by the Stress Test workflow.
#
# This runner is intentionally read-only with respect to ECS and Application
# Auto Scaling. It proves that the configured target-tracking policy raised
# both desired and running capacity above the captured baseline; it never
# forces desired count, suspends scaling, executes into a task, or terminates a
# task.

set -euo pipefail
IFS=$'\n\t'

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AWS_PAGER=""

required_environment=(
  SNAKETRON_STRESS_ENVIRONMENT
  SNAKETRON_STRESS_TARGET
  SNAKETRON_STRESS_ACCOUNT_ID
  SNAKETRON_ECS_CLUSTER
  SNAKETRON_ECS_SERVICE
  SNAKETRON_AWS_REGION
  SNAKETRON_REGION_CODE
)
for name in "${required_environment[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required" >&2
    exit 1
  fi
done

for executable in aws cargo curl jq python3; do
  command -v "$executable" >/dev/null || {
    echo "$executable is required" >&2
    exit 1
  }
done

[[ "$SNAKETRON_STRESS_ACCOUNT_ID" =~ ^[0-9]{12}$ ]] || {
  echo "SNAKETRON_STRESS_ACCOUNT_ID must be a 12-digit AWS account ID" >&2
  exit 1
}

expected_target=""
expected_cluster=""
expected_service=""
jwt_secret_name=""
case "$SNAKETRON_STRESS_ENVIRONMENT" in
  dev)
    [[ "$SNAKETRON_AWS_REGION" == "us-east-1" \
      && "$SNAKETRON_REGION_CODE" == "use1" ]] || {
      echo "Development stress tests are restricted to use1/us-east-1" >&2
      exit 1
    }
    expected_target="https://dev.snaketron.io"
    expected_cluster="snaketron-cluster-dev"
    expected_service="snaketron-server-dev"
    jwt_secret_name="snaketron-jwt-secret-dev"
    ;;
  prod)
    [[ "${SNAKETRON_STRESS_CONFIRM:-}" == "RUN_PRODUCTION_STRESS" ]] || {
      echo "Production requires SNAKETRON_STRESS_CONFIRM=RUN_PRODUCTION_STRESS" >&2
      exit 1
    }
    case "$SNAKETRON_REGION_CODE:$SNAKETRON_AWS_REGION" in
      use1:us-east-1) expected_target="https://use1.snaketron.io" ;;
      euw1:eu-west-1) expected_target="https://euw1.snaketron.io" ;;
      *)
        echo "Production region must be use1/us-east-1 or euw1/eu-west-1" >&2
        exit 1
        ;;
    esac
    expected_cluster="snaketron-cluster-prod"
    expected_service="snaketron-server-prod"
    jwt_secret_name="snaketron-jwt-secret-prod"
    ;;
  *)
    echo "SNAKETRON_STRESS_ENVIRONMENT must be dev or prod" >&2
    exit 1
    ;;
esac

[[ "$SNAKETRON_STRESS_TARGET" == "$expected_target" ]] || {
  echo "Target must be the exact $SNAKETRON_STRESS_ENVIRONMENT regional origin $expected_target" >&2
  exit 1
}
[[ "$SNAKETRON_ECS_CLUSTER" == "$expected_cluster" \
  && "$SNAKETRON_ECS_SERVICE" == "$expected_service" ]] || {
  echo "ECS cluster/service do not match the selected environment" >&2
  exit 1
}

run_id="${SNAKETRON_STRESS_RUN_ID:-stress-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
[[ "$run_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
  echo "SNAKETRON_STRESS_RUN_ID contains unsupported characters" >&2
  exit 1
}
(( ${#run_id} <= 64 )) || {
  echo "SNAKETRON_STRESS_RUN_ID must not exceed 64 characters" >&2
  exit 1
}

report_dir="${SNAKETRON_STRESS_REPORT_DIR:-$script_dir/test-results/stress-test-$run_id}"
if [[ -d "$report_dir" && -n "$(find "$report_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "Stress report directory already contains evidence: $report_dir" >&2
  exit 1
fi
umask 077
mkdir -p "$report_dir/loadtest"

load_pid=""
web_canary_pid=""
stress_test_key=""

stop_child() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    kill -INT "$pid" 2>/dev/null || true
    for _ in {1..30}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -TERM "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local status="$?"
  trap - EXIT
  set +e
  stop_child "$load_pid"
  stop_child "$web_canary_pid"
  unset SNAKETRON_STRESS_TEST_KEY SNAKETRON_JWT_SECRET_VALUE jwt_secret stress_test_key
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

caller_identity="$report_dir/aws-caller-identity.json"
aws sts get-caller-identity >"$caller_identity"
caller_account="$(jq -er '.Account' "$caller_identity")"
[[ "$caller_account" == "$SNAKETRON_STRESS_ACCOUNT_ID" ]] || {
  echo "AWS caller account $caller_account does not match the configured account" >&2
  exit 1
}

echo "Building the isolated load generator"
cargo build \
  --locked \
  --release \
  --package loadtest \
  --manifest-path "$script_dir/Cargo.toml"
loadtest_binary="$script_dir/target/release/snaketron-loadtest"
[[ -x "$loadtest_binary" ]] || {
  echo "Loadtest binary was not produced at $loadtest_binary" >&2
  exit 1
}

# The load generator receives only a derived, narrowly scoped credential.
# Never put the JWT signing secret or derived value in a process argument or
# report.
jwt_secret="$(aws secretsmanager get-secret-value \
  --region "$SNAKETRON_AWS_REGION" \
  --secret-id "$jwt_secret_name" \
  --query SecretString \
  --output text)"
(( ${#jwt_secret} >= 32 )) || {
  unset jwt_secret
  echo "$jwt_secret_name did not contain a valid signing secret" >&2
  exit 1
}
stress_test_key="$(
  SNAKETRON_JWT_SECRET_VALUE="$jwt_secret" python3 - <<'PY'
import hashlib
import hmac
import os
import sys

key = os.environ.pop("SNAKETRON_JWT_SECRET_VALUE").encode()
sys.stdout.write(hmac.new(key, b"snaketron-stress-test-v1", hashlib.sha256).hexdigest())
PY
)"
unset jwt_secret
[[ "$stress_test_key" =~ ^[0-9a-f]{64}$ ]] || {
  echo "Could not derive the stress-test credential" >&2
  exit 1
}
if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  printf '::add-mask::%s\n' "$stress_test_key"
fi
export SNAKETRON_STRESS_TEST_KEY="$stress_test_key"

scaling_resource="service/$SNAKETRON_ECS_CLUSTER/$SNAKETRON_ECS_SERVICE"
aws application-autoscaling describe-scalable-targets \
  --region "$SNAKETRON_AWS_REGION" \
  --service-namespace ecs \
  --resource-ids "$scaling_resource" \
  >"$report_dir/scalable-targets.json"
jq -e '
  (.ScalableTargets | length) == 1
  and (.ScalableTargets[0] as $target
    | $target.MinCapacity == 1
    and $target.MaxCapacity == 25
    and ($target.SuspendedState.DynamicScalingInSuspended // false) == false
    and ($target.SuspendedState.DynamicScalingOutSuspended // false) == false
    and ($target.SuspendedState.ScheduledScalingSuspended // false) == false)
' "$report_dir/scalable-targets.json" >/dev/null || {
  echo "Stress testing requires enabled ECS autoscaling with bounds 1..25" >&2
  exit 1
}
max_capacity="$(jq -er '.ScalableTargets[0].MaxCapacity' \
  "$report_dir/scalable-targets.json")"

aws application-autoscaling describe-scaling-policies \
  --region "$SNAKETRON_AWS_REGION" \
  --service-namespace ecs \
  --resource-id "$scaling_resource" \
  --scalable-dimension ecs:service:DesiredCount \
  >"$report_dir/scaling-policies.json"
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
  and target("ECSServiceAverageCPUUtilization"; 15)
  and target("ECSServiceAverageMemoryUtilization"; 80)
' "$report_dir/scaling-policies.json" >/dev/null || {
  echo "Expected only CPU=15% and memory=80% target tracking with 60-second cooldowns" >&2
  exit 1
}

aws ecs describe-services \
  --region "$SNAKETRON_AWS_REGION" \
  --cluster "$SNAKETRON_ECS_CLUSTER" \
  --services "$SNAKETRON_ECS_SERVICE" \
  >"$report_dir/baseline-service.json"
jq -e --argjson max_capacity "$max_capacity" '
  (.failures | length) == 0
  and (.services | length) == 1
  and (.services[0] as $service
    | $service.status == "ACTIVE"
    and $service.desiredCount >= 1
    and $service.desiredCount < $max_capacity
    and $service.runningCount == $service.desiredCount
    and $service.pendingCount == 0
    and ($service.deployments | length) == 1
    and $service.deployments[0].status == "PRIMARY")
' "$report_dir/baseline-service.json" >/dev/null || {
  echo "ECS service must be stable below maximum capacity before stress load begins" >&2
  exit 1
}
baseline_desired="$(jq -er '.services[0].desiredCount' "$report_dir/baseline-service.json")"

curl --fail-with-body --silent --show-error \
  --connect-timeout 2 \
  --max-time 5 \
  "$SNAKETRON_STRESS_TARGET/api/health" \
  >"$report_dir/preflight-health.json"
jq -e '.status == "ok"' "$report_dir/preflight-health.json" >/dev/null || {
  echo "The selected regional health endpoint is not ready" >&2
  exit 1
}

quiescent_deadline=$((SECONDS + 180))
while true; do
  aws application-autoscaling describe-scaling-activities \
    --region "$SNAKETRON_AWS_REGION" \
    --service-namespace ecs \
    --resource-id "$scaling_resource" \
    --scalable-dimension ecs:service:DesiredCount \
    --max-results 50 \
    >"$report_dir/scaling-activities-before.json"
  if jq -e '
    [.ScalingActivities[]?
      | select(.StatusCode == "Pending" or .StatusCode == "InProgress")]
    | length == 0
  ' "$report_dir/scaling-activities-before.json" >/dev/null; then
    break
  fi
  if (( SECONDS >= quiescent_deadline )); then
    echo "Application Auto Scaling did not become quiescent within three minutes" >&2
    exit 1
  fi
  sleep 5
done

test_started_epoch="$(date -u +%s)"
jq -n \
  --arg run_id "$run_id" \
  --arg environment "$SNAKETRON_STRESS_ENVIRONMENT" \
  --arg target "$SNAKETRON_STRESS_TARGET" \
  --arg aws_region "$SNAKETRON_AWS_REGION" \
  --arg region "$SNAKETRON_REGION_CODE" \
  --arg cluster "$SNAKETRON_ECS_CLUSTER" \
  --arg service "$SNAKETRON_ECS_SERVICE" \
  --argjson baseline_desired "$baseline_desired" \
  --argjson started_at_epoch "$test_started_epoch" \
  '{
    run_id: $run_id,
    environment: $environment,
    target: $target,
    aws_region: $aws_region,
    region: $region,
    cluster: $cluster,
    service: $service,
    baseline_desired: $baseline_desired,
    started_at_epoch: $started_at_epoch,
    load: {
      stress_test: true,
      mode: "duel",
      sessions: 128,
      duration: "20m",
      spawn_rate: 4,
      command_profile: "every-tick",
      recovery_timeout: "10s"
    }
  }' >"$report_dir/configuration.json"

stress_ws_url="${SNAKETRON_STRESS_TARGET/https:/wss:}/ws"
export SNAKETRON_STRESS_WS_URL="$stress_ws_url"
export SNAKETRON_STRESS_REGION="$SNAKETRON_REGION_CODE"
export SNAKETRON_STRESS_CANARY_DURATION_MS="${SNAKETRON_STRESS_CANARY_DURATION_MS:-1200000}"
export SNAKETRON_STRESS_MAX_DISRUPTION_MS="${SNAKETRON_STRESS_MAX_DISRUPTION_MS:-10000}"
export REACT_APP_API_URL="$SNAKETRON_STRESS_TARGET"
export REACT_APP_WS_URL="$stress_ws_url"
export REACT_APP_ENVIRONMENT="$SNAKETRON_STRESS_ENVIRONMENT"

if [[ -n "${SNAKETRON_WEB_CANARY_COMMAND:-}" ]]; then
  command -v timeout >/dev/null || {
    echo "timeout is required when SNAKETRON_WEB_CANARY_COMMAND is configured" >&2
    exit 1
  }
  web_canary_timeout_seconds="${SNAKETRON_WEB_CANARY_TIMEOUT_SECONDS:-2400}"
  [[ "$web_canary_timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
    echo "SNAKETRON_WEB_CANARY_TIMEOUT_SECONDS must be a positive integer" >&2
    exit 1
  }
  (
    cd "$script_dir"
    exec timeout \
      --signal=INT \
      --kill-after=30s \
      "${web_canary_timeout_seconds}s" \
      bash -euo pipefail -c "$SNAKETRON_WEB_CANARY_COMMAND"
  ) >"$report_dir/web-canary.log" 2>&1 &
  web_canary_pid=$!
fi

loadtest_run_id="${run_id}-load"
loadtest_command=(
  "$loadtest_binary"
  --stress-test
  --stress-recovery-timeout 10s
  --target "$SNAKETRON_STRESS_TARGET"
  --confirm-production
  --require-scale-out
  --region "$SNAKETRON_REGION_CODE"
  --mode duel
  --queue-mode quickmatch
  --population game
  --stages 128@20m
  --spawn-rate 4
  --max-total-sessions 4096
  --drain-timeout 10m
  --command-profile every-tick
  --run-id "$loadtest_run_id"
  --report-dir "$report_dir/loadtest"
)
if [[ "$SNAKETRON_STRESS_ENVIRONMENT" == "dev" ]]; then
  loadtest_command+=(--require-same-origin)
fi

"${loadtest_command[@]}" >"$report_dir/loadtest.log" 2>&1 &
load_pid=$!

successful_target_tracking_after() {
  local activities_file="$1"
  jq -e --argjson started "$test_started_epoch" '
    [.ScalingActivities[]?
      | select(
          .StatusCode == "Successful"
          and ((.StartTime
            | sub("\\.[0-9]+\\+00:00$"; "Z")
            | sub("\\.[0-9]+Z$"; "Z")
            | sub("\\+00:00$"; "Z")
            | fromdateiso8601) >= $started)
          and (.Cause | test("alarm|target.tracking"; "i"))
        )]
    | length > 0
  ' "$activities_file" >/dev/null
}

observe_automatic_scale_out() {
  local deadline=$((SECONDS + 900))
  local service_candidate="$report_dir/scale-out-service.pending.json"
  local activity_candidate="$report_dir/scale-out-activities.pending.json"
  : >"$report_dir/scale-out-observations.jsonl"

  while (( SECONDS < deadline )); do
    if ! kill -0 "$load_pid" 2>/dev/null; then
      echo "The load generator exited before AWS scale-out was proven" >&2
      return 1
    fi

    aws ecs describe-services \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --services "$SNAKETRON_ECS_SERVICE" \
      >"$service_candidate"
    aws application-autoscaling describe-scaling-activities \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --resource-id "$scaling_resource" \
      --scalable-dimension ecs:service:DesiredCount \
      --max-results 50 \
      >"$activity_candidate"

    observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    jq -c --arg observed_at "$observed_at" '
      .services[0]
      | {
          observed_at: $observed_at,
          desired_count: .desiredCount,
          running_count: .runningCount,
          pending_count: .pendingCount,
          task_definition: .taskDefinition
        }
    ' "$service_candidate" >>"$report_dir/scale-out-observations.jsonl"

    desired="$(jq -er '.services[0].desiredCount' "$service_candidate")"
    running="$(jq -er '.services[0].runningCount' "$service_candidate")"
    if (( desired > baseline_desired \
      && running > baseline_desired )) \
      && successful_target_tracking_after "$activity_candidate"; then
      mv "$service_candidate" "$report_dir/scale-out-service.json"
      mv "$activity_candidate" "$report_dir/scale-out-activities.json"
      return 0
    fi
    sleep 10
  done

  [[ ! -f "$service_candidate" ]] || mv "$service_candidate" "$report_dir/scale-out-service.json"
  [[ ! -f "$activity_candidate" ]] || mv "$activity_candidate" "$report_dir/scale-out-activities.json"
  echo "Target tracking did not raise desired and running capacity above baseline $baseline_desired within fifteen minutes" >&2
  return 1
}

scale_out_status=0
observe_automatic_scale_out || scale_out_status=$?

set +e
wait "$load_pid"
loadtest_status=$?
set -e
load_pid=""

web_canary_status=0
if [[ -n "$web_canary_pid" ]]; then
  set +e
  wait "$web_canary_pid"
  web_canary_status=$?
  set -e
  web_canary_pid=""
fi

unset SNAKETRON_STRESS_TEST_KEY stress_test_key

aws ecs describe-services \
  --region "$SNAKETRON_AWS_REGION" \
  --cluster "$SNAKETRON_ECS_CLUSTER" \
  --services "$SNAKETRON_ECS_SERVICE" \
  >"$report_dir/final-service.json" || true
aws application-autoscaling describe-scaling-activities \
  --region "$SNAKETRON_AWS_REGION" \
  --service-namespace ecs \
  --resource-id "$scaling_resource" \
  --scalable-dimension ecs:service:DesiredCount \
  --max-results 50 \
  >"$report_dir/final-scaling-activities.json" || true

summary="$report_dir/loadtest/$loadtest_run_id/summary.json"
summary_status=0
if [[ ! -s "$summary" ]]; then
  echo "Loadtest did not produce $summary" >&2
  summary_status=1
elif ! jq -e '
  .schema_version == 14
  and .metadata.stress_test == "true"
  and .metadata.stress_matchmaking_pool == "stress"
  and .metadata.stress_recovery_timeout_ms == "10000"
  and .metadata.threshold_result == "passed"
' "$summary" >/dev/null; then
  echo "Loadtest summary did not contain passing stress-mode evidence" >&2
  summary_status=1
fi

if (( loadtest_status != 0 )); then
  echo "Loadtest exited with status $loadtest_status" >&2
  tail -n 200 "$report_dir/loadtest.log" >&2 || true
fi
if (( web_canary_status != 0 )); then
  echo "Web canary exited with status $web_canary_status" >&2
  tail -n 200 "$report_dir/web-canary.log" >&2 || true
fi
if (( scale_out_status != 0 \
  || loadtest_status != 0 \
  || web_canary_status != 0 \
  || summary_status != 0 )); then
  echo "Stress test failed; evidence is in $report_dir" >&2
  exit 1
fi

echo "Stress test passed: isolated clients completed and AWS capacity rose above baseline $baseline_desired"
echo "Evidence written to $report_dir"
