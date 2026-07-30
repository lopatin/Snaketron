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
canonical_scaling_state=$'1\t10\tFalse\tFalse\tFalse'
suspended_scaling_state=$'1\t10\tTrue\tTrue\tTrue'
gate_a_post_ready_required_full_seconds=60
load_pid=""
capacity_pid=""
admission_population_pid=""
idle_population_pid=""
lobby_population_pid=""
matchmaking_population_pid=""
traefik_monitor_pid=""
traefik_monitor_dir=""
ecs_runtime_monitor_pid=""
ecs_runtime_monitor_dir=""
hard_crash_control_observer_pid=""
hard_crash_control_observer_stop_file=""
hard_crash_ownership_observer_pid=""
hard_crash_ownership_observer_stop_file=""
hard_crash_ecs_exec_pid=""

hard_crash_envelope_jq='
  def hard_crash_required_authenticated_sessions: 256;
  def hard_crash_required_fully_joined_duels: 128;
  def hard_crash_required_commands_per_second: 1280;
  def hard_crash_required_report_seconds: 30;
  def hard_crash_required_online_samples:
    hard_crash_required_report_seconds + 1;
  def hard_crash_max_online_sample_gap_ms: 10000;
  def longest_qualifying_streak($samples):
    reduce $samples[] as $sample (
      {
        current_seconds: 0,
        current_started_at_second: null,
        longest_seconds: 0,
        longest_started_at_second: null,
        longest_finished_at_second: null
      };
      if $sample.qualifying then
        .current_seconds += 1
        | if .current_seconds == 1 then
            .current_started_at_second = $sample.unix_second
          else .
          end
        | if .current_seconds > .longest_seconds then
            .longest_seconds = .current_seconds
            | .longest_started_at_second = .current_started_at_second
            | .longest_finished_at_second = ($sample.unix_second + 1)
          else .
          end
      else
        .current_seconds = 0
        | .current_started_at_second = null
      end);
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
  def hard_crash_report_second_qualifies:
    .authenticated_sessions >= hard_crash_required_authenticated_sessions
    and .fully_joined_duels >= hard_crash_required_fully_joined_duels
    and .commands_sent >= hard_crash_required_commands_per_second;
  def hard_crash_pre_crash_seconds($value; $timing_origin):
    (($value.ramp_stages[0].target_reached_at_unix_ms / 1000) | ceil)
      as $first_second
    | ($timing_origin / 1000) as $after_last_second
    | [range($first_second; $after_last_second)
        | . as $second
        | (($second * 1000) + 500) as $midpoint
        | {
            unix_second: $second,
            authenticated_sessions: (
              [$value.sessions[]
                | select(
                    .authenticated_at_unix_ms != null
                    and .authenticated_at_unix_ms <= $midpoint
                    and .finished_at_unix_ms > $midpoint)]
              | length),
            fully_joined_duels:
              fully_joined_duels_at($value; $midpoint),
            commands_sent: (
              $value.metrics.command_counts_by_unix_second
                [($second | tostring)] // 0)
          }
        | .qualifying = hard_crash_report_second_qualifies];
  def nonnegative_integer:
    type == "number" and . >= 0 and . == floor;
  def bounded_sample_cadence($samples):
    all(
      range(1; ($samples | length));
      . as $index
      | (($samples[$index].observed_at_unix_ms
            - $samples[$index - 1].observed_at_unix_ms) > 0)
        and (($samples[$index].observed_at_unix_ms
            - $samples[$index - 1].observed_at_unix_ms)
              <= hard_crash_max_online_sample_gap_ms));
  def hard_crash_envelope_passes(
    $pre_crash_seconds;
    $envelope;
    $ecs_exec_invoked_at_unix_ms
  ):
    hard_crash_required_online_samples as $required_samples
    | (if ($envelope.samples | type) == "array" then
        $envelope.samples[-$required_samples:]
      else []
      end) as $online_tail
    | longest_qualifying_streak($pre_crash_seconds) as $report_streak
    | ($pre_crash_seconds | last) as $final_report_second
    | $report_streak.longest_seconds
        >= hard_crash_required_report_seconds
      and $final_report_second.qualifying
      and $envelope.required_stable_seconds
        == hard_crash_required_report_seconds
      and $envelope.required_qualifying_samples == $required_samples
      and ($online_tail | length) == $required_samples
      and all($online_tail[];
        (.observed_at_unix_ms | nonnegative_integer)
        and (.raw_websockets | nonnegative_integer)
        and (.active_games | nonnegative_integer)
        and .raw_websockets
          >= hard_crash_required_authenticated_sessions
        and .active_games >= hard_crash_required_fully_joined_duels)
      and bounded_sample_cadence($online_tail)
      and (($online_tail | last | .observed_at_unix_ms)
        - ($online_tail | first | .observed_at_unix_ms))
          >= (hard_crash_required_report_seconds * 1000)
      and ($ecs_exec_invoked_at_unix_ms | nonnegative_integer)
      and ($online_tail | last | .observed_at_unix_ms)
        <= $ecs_exec_invoked_at_unix_ms;
'

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

staging_entry_state_is_valid() {
  local certification_mode="$1"
  local desired_count="$2"
  local target_state="$3"
  case "$certification_mode" in
    planned)
      [[ "$desired_count" == "1" && "$target_state" == "$canonical_scaling_state" ]]
      ;;
    crash)
      [[ "$desired_count" == "1" && "$target_state" == "$suspended_scaling_state" ]]
      ;;
    *)
      return 1
      ;;
  esac
}

unix_time_ms() {
  jq -nr 'now * 1000 | floor'
}

ecs_timestamp_to_unix_ms() {
  local timestamp="$1"
  jq -en --arg timestamp "$timestamp" '
    $timestamp
    | capture(
        "^(?<second>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.(?<fraction>[0-9]+))?(?:Z|\\+00:00)$"
      )
    | ((.second + "Z" | fromdateiso8601) * 1000)
      + (((.fraction // "0") + "000")[0:3] | tonumber)
  '
}

select_pre_fault_pending_sample() {
  local partition="$1"
  local killed_lease_token="$2"
  shift 2
  (( $# > 0 )) || return 1
  jq -es \
    --argjson partition "$partition" \
    --arg killed_lease_token "$killed_lease_token" '
      [.[]
        | select(
            .observation_started_at_ms <= .captured_at_ms
            and .captured_at_ms <= .observation_completed_at_ms)
        | select(
            .partition == $partition
            and .requested_consumer == $killed_lease_token
            and .pending_entry != null
            and (.pending_entry.id | type == "string" and length > 0)
            and .pending_entry.consumer == $killed_lease_token)]
      | min_by(.observation_completed_at_ms)
  ' "$@"
}

select_post_kill_pending_sample() {
  local execution_stopped_at_ms="$1"
  local partition="$2"
  local killed_lease_token="$3"
  shift 3
  (( $# > 0 )) || return 1
  jq -es \
    --argjson execution_stopped_at_ms "$execution_stopped_at_ms" \
    --argjson partition "$partition" \
    --arg killed_lease_token "$killed_lease_token" '
      [.[]
        | select(
            .observation_started_at_ms >= $execution_stopped_at_ms
            and .captured_at_ms >= .observation_started_at_ms
            and .captured_at_ms <= .observation_completed_at_ms
            and .observation_completed_at_ms
              <= ($execution_stopped_at_ms + 2000))
        | select(
            .partition == $partition
            and .requested_consumer == $killed_lease_token
            and .pending_entry != null
            and (.pending_entry.id | type == "string" and length > 0)
            and .pending_entry.consumer == $killed_lease_token)]
      | min_by(.observation_completed_at_ms)
  ' "$@"
}

select_unexpected_crash_stops() {
  local evidence_started_epoch="$1"
  local expected_task_arn="$2"
  local baseline="$3"
  local observed="$4"
  jq -n \
    --argjson started "$evidence_started_epoch" \
    --arg expected_task_arn "$expected_task_arn" \
    --slurpfile baseline "$baseline" \
    --slurpfile observed "$observed" '
      def epoch:
        sub("\\.[0-9]+\\+00:00$"; "Z")
        | sub("\\.[0-9]+Z$"; "Z")
        | sub("\\+00:00$"; "Z")
        | fromdateiso8601;
      ($baseline[0].tasks
        | map(select(.desiredStatus == "STOPPED") | .taskArn)
        | unique) as $prior_stopping
      | [$observed[0].tasks[]
          | select(.taskArn != $expected_task_arn)
          | select(
              .taskArn as $arn
              | $prior_stopping
              | index($arn) == null)
          | (.stoppingAt // .executionStoppedAt // .stoppedAt) as $onset
          | select($onset == null or ($onset | epoch) >= $started)]
    '
}

select_pre_fault_ownership_sample() {
  local partition="$1"
  local killed_boot_id="$2"
  local killed_lease_token="$3"
  shift 3
  (( $# > 0 )) || return 1
  jq -es \
    --argjson partition "$partition" \
    --arg killed_boot_id "$killed_boot_id" \
    --arg killed_lease_token "$killed_lease_token" '
      [.[]
        | select(
            .observation_started_at_ms <= .captured_at_ms
            and .captured_at_ms <= .observation_completed_at_ms
            and .observation_started_at_ms <= .membership_observed_at_ms
            and .membership_observed_at_ms <= .observation_completed_at_ms
            and .observation_started_at_ms <= .authority_observed_at_ms
            and .authority_observed_at_ms <= .observation_completed_at_ms
            and (.authority_event_tail_id
              | type == "string"
              and test("^[0-9]+-[0-9]+$")))
        | (.runtime_partitions[]
            | select(.partition == $partition)) as $runtime
        | select(
            .authority_stable
            and .killed_member_live
            and $runtime.owner_matches
            and $runtime.desired_owner == $killed_boot_id
            and $runtime.active_owner == $killed_boot_id
            and $runtime.lease_token == $killed_lease_token
            and any(.live_members[];
              .boot_id == $killed_boot_id
              and .lifecycle == "ACTIVE"))]
      | min_by(.observation_completed_at_ms)
  ' "$@"
}

select_hard_crash_owner_ownership_sample() {
  local pre="$1"
  local execution_stopped_at_ms_json="$2"
  local partition="$3"
  local killed_boot_id="$4"
  local killed_task_id="$5"
  local killed_lease_token="$6"
  shift 6
  (( $# > 0 )) || return 1
  jq -es \
    --slurpfile pre "$pre" \
    --argjson execution_stopped_at_ms "$execution_stopped_at_ms_json" \
    --argjson partition "$partition" \
    --arg killed_boot_id "$killed_boot_id" \
    --arg killed_task_id "$killed_task_id" \
    --arg killed_lease_token "$killed_lease_token" '
      ($pre[0].runtime_partitions[]
        | select(.partition == $partition)) as $old
      | [.[]
          | select(
              .captured_at_ms >= .observation_started_at_ms
              and .captured_at_ms <= .observation_completed_at_ms
              and .membership_observed_at_ms >= .observation_started_at_ms
              and .membership_observed_at_ms <= .observation_completed_at_ms
              and .authority_observed_at_ms >= .observation_started_at_ms
              and .authority_observed_at_ms <= .observation_completed_at_ms
              and (.authority_event_tail_id
                | type == "string"
                and test("^[0-9]+-[0-9]+$"))
              and (
                $execution_stopped_at_ms == null
                or (
                  .observation_started_at_ms >= $execution_stopped_at_ms
                  and .observation_completed_at_ms
                    <= ($execution_stopped_at_ms + 5000))))
          | (.runtime_partitions[]
              | select(.partition == $partition)) as $new
          | select(
              .authority_stable
              and .killed_member_live == false
              and .assignment.version > $pre[0].assignment.version
              and (
                $execution_stopped_at_ms == null
                or .assignment.computed_at_ms >= $execution_stopped_at_ms)
              and $new.owner_matches
              and $new.desired_owner != $killed_boot_id
              and $new.active_owner == $new.desired_owner
              and ($new.lease_token | type == "string" and length > 0)
              and $new.lease_token != $killed_lease_token
              and $new.lease_token != $old.lease_token
              and ($new.desired_owner as $owner
                | [.live_members[]
                    | select(
                        .boot_id == $owner
                        and .lifecycle == "ACTIVE")] as $current
                | ($current | length) == 1
                  and $current[0].ecs_task_id != $killed_task_id
                  and ($current[0].ecs_task_id as $owner_task_id
                    | any($pre[0].live_members[];
                        .boot_id == $owner
                        and .ecs_task_id == $owner_task_id))))]
      | min_by(.observation_completed_at_ms)
  ' "$@"
}

select_hard_crash_owner_ready_ownership_sample() {
  select_hard_crash_owner_ownership_sample "$@"
}

select_hard_crash_owner_candidate_ownership_sample() {
  local pre="$1"
  local partition="$2"
  local killed_boot_id="$3"
  local killed_task_id="$4"
  local killed_lease_token="$5"
  shift 5
  select_hard_crash_owner_ownership_sample \
    "$pre" \
    null \
    "$partition" \
    "$killed_boot_id" \
    "$killed_task_id" \
    "$killed_lease_token" \
    "$@"
}

select_hard_crash_authoritative_output_sample() {
  local execution_stopped_at_ms="$1"
  local partition="$2"
  local authority_event_tail_id="$3"
  shift 3
  (( $# > 0 )) || return 1
  jq -es \
    --argjson execution_stopped_at_ms "$execution_stopped_at_ms" \
    --argjson partition "$partition" \
    --arg authority_event_tail_id "$authority_event_tail_id" '
      def parsed_stream_id:
        capture("^(?<milliseconds>[0-9]+)-(?<sequence>[0-9]+)$")
        | {
            milliseconds: (.milliseconds | tonumber),
            sequence: (.sequence | tonumber)
          };
      ($authority_event_tail_id | parsed_stream_id) as $anchor
      | [.[]
          | select(.first_scheduled_output != null)
          | (.first_scheduled_output.stream_id | parsed_stream_id) as $output
          | select(
              .observation_started_at_ms >= $execution_stopped_at_ms
              and .captured_at_ms >= .observation_started_at_ms
              and .captured_at_ms <= .observation_completed_at_ms
              and .partition == $partition
              and .after_stream_id == $authority_event_tail_id
              and ($output.milliseconds > $anchor.milliseconds
                or ($output.milliseconds == $anchor.milliseconds
                  and $output.sequence > $anchor.sequence))
              and .first_scheduled_output.stream_unix_ms
                == $output.milliseconds
              and .first_scheduled_output.stream_unix_ms
                >= $execution_stopped_at_ms
              and .first_scheduled_output.stream_unix_ms
                <= ($execution_stopped_at_ms + 5000)
              and .first_scheduled_output.stream_unix_ms <= .captured_at_ms
              and .first_scheduled_output.game_id % 10 == $partition
              and .first_scheduled_output.command_id.game_id
                == .first_scheduled_output.game_id
              and .first_scheduled_output.command_id.sequence > 0
              and (.first_scheduled_output.command_id.client_game_session_id
                | type == "string" and length > 0)
              and .first_scheduled_output.deduplicated_replay == false)]
      | min_by(.observation_completed_at_ms)
  ' "$@"
}

write_capacity_acceptance_report() {
  local summary="$1"
  local output="$2"
  local max_latency_ms=1000
  local required_continuous_seconds=300
  jq -n \
    --slurpfile report "$summary" \
    --argjson max_latency_ms "$max_latency_ms" \
    --argjson required_continuous_seconds "$required_continuous_seconds" '
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
      def longest_streak($samples):
        reduce $samples[] as $sample (
          {
            current_seconds: 0,
            current_started_at_second: null,
            longest_seconds: 0,
            longest_started_at_second: null,
            longest_finished_at_second: null
          };
          if $sample.qualifying then
            .current_seconds += 1
            | if .current_seconds == 1 then
                .current_started_at_second = $sample.unix_second
              else .
              end
            | if .current_seconds > .longest_seconds then
                .longest_seconds = .current_seconds
                | .longest_started_at_second = .current_started_at_second
                | .longest_finished_at_second = ($sample.unix_second + 1)
              else .
              end
          else
            .current_seconds = 0
            | .current_started_at_second = null
          end);
      $report[0] as $r
      | $r.ramp_stages[0].target_reached_at_unix_ms as $hold_started_at_ms
      | $r.ramp_stages[0].finished_at_unix_ms as $hold_finished_at_ms
      | (($hold_started_at_ms / 1000) | ceil) as $hold_first_second
      | (($hold_finished_at_ms / 1000) | floor) as $hold_after_last_second
      | 1280 as $minimum_commands_per_second
      | [range($hold_first_second; $hold_after_last_second)
          | . as $second
          | (($second * 1000) + 500) as $midpoint
          | ([$r.sessions[]
              | select(
                  .authenticated_at_unix_ms != null
                  and .authenticated_at_unix_ms <= $midpoint
                  and .finished_at_unix_ms > $midpoint)] | length)
              as $authenticated_sessions
          | fully_joined_duels_at($r; $midpoint) as $fully_joined_duels
          | ($r.metrics.command_counts_by_unix_second
              [($second | tostring)] // 0) as $commands_sent
          | ($r.metrics.command_outcome_counts_by_sent_unix_second
              [($second | tostring)] // 0) as $command_outcomes
          | ($r.metrics.command_outcome_max_latency_ms_by_sent_unix_second
              [($second | tostring)] // null) as $max_outcome_latency_ms
          | ([range(0; 10)
              | . as $partition
              | select(
                  ($r.metrics
                    .scheduled_command_counts_by_partition_and_unix_second
                    [($partition | tostring)][($second | tostring)] // 0) > 0)]
              | length) as $productive_partitions
          | {
              unix_second: $second,
              authenticated_sessions: $authenticated_sessions,
              fully_joined_duels: $fully_joined_duels,
              commands_sent: $commands_sent,
              command_outcomes: $command_outcomes,
              max_outcome_latency_ms: $max_outcome_latency_ms,
              productive_partitions: $productive_partitions,
              qualifying: (
                $authenticated_sessions >= 256
                and $fully_joined_duels >= 128
                and $commands_sent >= $minimum_commands_per_second
                and $command_outcomes == $commands_sent
                and $max_outcome_latency_ms != null
                and $max_outcome_latency_ms <= $max_latency_ms
                and $productive_partitions == 10)
            }] as $seconds
      | longest_streak($seconds) as $streak
      | {
          required_continuous_seconds: $required_continuous_seconds,
          max_outcome_latency_ms: $max_latency_ms,
          hold_started_at_unix_ms: $hold_started_at_ms,
          hold_finished_at_unix_ms: $hold_finished_at_ms,
          evaluated_first_second: $hold_first_second,
          evaluated_after_last_second: $hold_after_last_second,
          evaluated_seconds: ($seconds | length),
          longest_qualifying_streak: $streak,
          nonqualifying_seconds: [
            $seconds[]
            | select(.qualifying | not)
          ],
          global_checks: {
            schema: ($r.schema_version >= 10),
            load_threshold: ($r.metadata.threshold_result == "passed"),
            configured_concurrency: ($r.configured_max_concurrency == 272),
            mode: ($r.metadata.mode == "duel"),
            command_profile: ($r.metadata.command_profile == "every-tick"),
            spawn_rate: ($r.metadata.spawn_rate_per_second == "4"),
            exact_peak_authenticated:
              ($r.session_counts.peak_authenticated_concurrency == 272),
            peak_active_games:
              ($r.session_counts.peak_active_game_concurrency >= 136),
            sessions_clean: (
              $r.session_counts.failed == 0
              and $r.session_counts.cancelled == 0
              and $r.session_counts.incomplete == 0
              and $r.session_counts.completed == $r.session_counts.total
              and all($r.sessions[];
                .outcome == "completed" and .failure_phase == null)),
            pairing_clean: ($r.games.pairing_violations == 0),
            one_reached_stage: (
              ($r.ramp_stages | length) == 1
              and $r.ramp_stages[0].target_reached),
            socket_continuity: (
              $r.metrics.traffic.disconnects == 0
              and $r.metrics.traffic.reconnects == 0
              and ($r.metrics.usable_session_gap_ms.max_ms // 0) == 0),
            exact_command_accounting: (
              ([$r.metrics.command_counts_by_unix_second[]] | add // 0)
                == $r.metrics.traffic.commands_sent
              and ([
                  $r.metrics.command_outcome_counts_by_sent_unix_second[]
                ] | add // 0) == $r.metrics.traffic.commands_sent
              and $r.metrics.planned_handoffs.pending_commands_at_finish == 0),
            admission_budget:
              (($r.metrics.initial_admission_ready_ms.p99_ms // 10001) <= 10000)
          }
        }
      | .passed = (
          ([.global_checks[]] | all)
          and .longest_qualifying_streak.longest_seconds
            >= .required_continuous_seconds)
    ' >"$output"
}

traefik_sample_has_healthy_backend() {
  local sample="$1"
  awk '
    /^traefik_service_server_up{/ {
      if ($0 ~ /url="http:\/\/[^\"]+:8080"/ && ($NF + 0) > 0) {
        found = 1
      }
    }
    END { exit(found ? 0 : 1) }
  ' "$sample"
}

traefik_sample_has_healthy_task() {
  local sample="$1"
  local private_ipv4="$2"
  awk -v ip="$private_ipv4" '
    index($0, "traefik_service_server_up{") == 1 {
      if (index($0, "url=\"http://" ip ":8080\"") && ($NF + 0) > 0) {
        found = 1
      }
    }
    END { exit(found ? 0 : 1) }
  ' "$sample"
}

traefik_sample_has_healthy_fleet() {
  local sample="$1"
  local healthy_observation="$2"
  local expected_count
  expected_count="$(jq -er '.tasks | length | select(. > 0)' "$healthy_observation")" \
    || return 1
  local observed_count=0
  while IFS= read -r private_ipv4; do
    [[ -n "$private_ipv4" ]] || return 1
    traefik_sample_has_healthy_task "$sample" "$private_ipv4" || return 1
    observed_count=$((observed_count + 1))
  done < <(jq -er '
    .tasks[]
    | .private_ipv4
    | select(type == "string" and length > 0)
  ' "$healthy_observation")
  (( observed_count == expected_count ))
}

command_outcome_window_diagnostics() {
  local summary="$1"
  local window="$2"
  local max_latency_ms="${3:-1000}"
  local required_partition_count="${4:-0}"
  local required_full_seconds="${5:-1}"
  jq \
    --argjson max_latency_ms "$max_latency_ms" \
    --argjson required_partition_count "$required_partition_count" \
    --argjson required_full_seconds "$required_full_seconds" \
    --slurpfile window "$window" '
      . as $report
      | (($window[0].started_at_unix_ms / 1000) | ceil) as $first_second
      | (($window[0].finished_at_unix_ms / 1000) | floor) as $after_last_second
      | [range($first_second; $after_last_second) as $second
          | ($second | tostring) as $second_key
          | ($report.metrics.command_counts_by_unix_second[$second_key] // 0)
            as $sent
          | ($report.metrics.command_outcome_counts_by_sent_unix_second
              [$second_key] // 0) as $outcomes
          | ($report.metrics.command_outcome_max_latency_ms_by_sent_unix_second
              [$second_key] // ($max_latency_ms + 1)) as $outcome_max_latency_ms
          | ($report.metrics.scheduled_command_counts_by_sent_unix_second
              [$second_key] // 0) as $scheduled
          | [range(0; $required_partition_count) as $partition
              | select(
                  ($report.metrics
                    .scheduled_command_counts_by_partition_and_unix_second
                    [($partition | tostring)][$second_key] // 0) <= 0)
              | $partition] as $missing_partitions
          | {
              unix_second: $second,
              sent: $sent,
              outcomes: $outcomes,
              outcome_max_latency_ms: $outcome_max_latency_ms,
              scheduled: $scheduled,
              missing_partitions: $missing_partitions
            }
          | select(
              .sent <= 0
              or .outcomes != .sent
              or .outcome_max_latency_ms > $max_latency_ms
              or .scheduled <= 0
              or (.missing_partitions | length) > 0)
        ] as $failed_seconds
      | {
          max_latency_ms: $max_latency_ms,
          required_partition_count: $required_partition_count,
          required_full_seconds: $required_full_seconds,
          first_full_second: $first_second,
          after_last_full_second: $after_last_second,
          full_second_count: ($after_last_second - $first_second),
          failed_seconds: $failed_seconds,
          passed:
            (($after_last_second - $first_second) >= $required_full_seconds
              and ($failed_seconds | length) == 0)
        }
    ' "$summary"
}

command_outcomes_meet_window_budget() {
  command_outcome_window_diagnostics "$@" | jq -e '.passed' >/dev/null
}

write_gate_a_post_ready_window() {
  local summary="$1"
  local started_at_unix_ms="$2"
  jq \
    --argjson started_at_unix_ms "$started_at_unix_ms" '
      (.ramp_stages[0].finished_at_unix_ms // $started_at_unix_ms)
        as $finished_at_unix_ms
      | {
          started_at_unix_ms: $started_at_unix_ms,
          finished_at_unix_ms: $finished_at_unix_ms,
          duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
        }
    ' "$summary"
}

write_gate_a_acceptance_report() {
  local summary="$1"
  local baseline_diagnostics="$2"
  local movement_diagnostics="$3"
  local post_ready_steady_diagnostics="$4"
  local zero_load_summary="$5"
  local runner_exit_status="$6"
  local output="$7"
  jq -n \
    --argjson runner_exit_status "$runner_exit_status" \
    --slurpfile summary "$summary" \
    --slurpfile baseline "$baseline_diagnostics" \
    --slurpfile movement "$movement_diagnostics" \
    --slurpfile post_ready_steady "$post_ready_steady_diagnostics" \
    --slurpfile zero_load "$zero_load_summary" '
      ($summary[0] // {}) as $report
      | {
          configuration:
            (($report.schema_version // 0) >= 10
              and ($report.metadata.threshold_result // null) == "passed"
              and ($report.configured_max_concurrency // 0) == 128
              and ($report.metadata.mode // null) == "duel"
              and ($report.metadata.command_profile // null) == "every-tick"
              and ($report.metadata.spawn_rate_per_second // null) == "4"),
          population_completion:
            (($report.session_counts.peak_authenticated_concurrency // 0) == 128
              and ($report.session_counts.peak_active_game_concurrency // 0) >= 64
              and ($report.session_counts.failed // -1) == 0
              and ($report.session_counts.cancelled // -1) == 0
              and ($report.session_counts.incomplete // -1) == 0
              and ($report.session_counts.completed // -1)
                == ($report.session_counts.total // -2)
              and all(($report.sessions // [])[];
                .outcome == "completed" and .failure_phase == null)
              and ($report.games.pairing_violations // -1) == 0
              and (($report.ramp_stages // []) | length) == 1
              and ($report.ramp_stages[0].target_reached // false)),
          websocket_continuity:
            (($report.metrics.traffic.disconnects // -1) == 0
              and ($report.metrics.traffic.reconnects // -1) == 0
              and ($report.metrics.usable_session_gap_ms.max_ms // 0) == 0),
          command_accounting_and_partitions:
            (([($report.metrics.command_counts_by_unix_second // {})[]] | add)
              == ($report.metrics.traffic.commands_sent // -1)
              and ([($report.metrics
                  .command_outcome_counts_by_sent_unix_second // {})[]] | add)
                == ($report.metrics.traffic.commands_sent // -1)
              and (($report.metrics
                .scheduled_command_counts_by_partition_and_unix_second // {})
                | length) == 10)
        } as $outcomes
      | (($report.missing // false) | not) as $summary_available
      | {
          schema_version: 2,
          gate: "natural-scale-out",
          runner: {
            exit_status: $runner_exit_status,
            passed: ($runner_exit_status == 0)
          },
          envelope: {
            summary_available: $summary_available,
            passed: ($summary_available and ([$outcomes[]] | all)),
            outcomes: $outcomes,
            observed: {
              configured_max_concurrency:
                ($report.configured_max_concurrency // null),
              peak_authenticated_concurrency:
                ($report.session_counts.peak_authenticated_concurrency // null),
              peak_active_game_concurrency:
                ($report.session_counts.peak_active_game_concurrency // null),
              sessions_total: ($report.session_counts.total // null),
              sessions_completed: ($report.session_counts.completed // null),
              commands_sent: ($report.metrics.traffic.commands_sent // null)
            }
          },
          zero_load: {
            passed: ($zero_load[0].passed // false),
            sample_count: (($zero_load[0].samples // []) | length),
            final_sample: (($zero_load[0].samples // []) | last // null)
          },
          baseline: ($baseline[0] // {passed: false}),
          movement: ($movement[0] // {passed: false}),
          post_ready_steady:
            ($post_ready_steady[0] // {passed: false})
        }
      | .passed = (
          .runner.passed
          and .envelope.passed
          and .zero_load.passed
          and .baseline.passed
          and .movement.passed
          and .post_ready_steady.passed
        )
    ' >"$output"
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
              cpu,
              memory,
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
            and . != "cpu"
            and . != "memory"
            and . != "linuxParameters")
      ] | length == 0)
      and (.cpu == null or (.cpu | type == "number"))
      and (.memory == null or (.memory | type == "number"))
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

select_exact_tag_image_digest() {
  local expected_tag="$1"
  local checkout_commit="$2"
  [[ "$expected_tag" =~ ^[0-9a-f]{40}$ \
    && "$checkout_commit" =~ ^[0-9a-f]{40}$ \
    && "$expected_tag" == "$checkout_commit" ]] || return 1
  jq -er --arg expected_tag "$expected_tag" '
    select((.imageDetails | length) == 1)
    | .imageDetails[0]
    | select((.imageTags // []) | index($expected_tag) != null)
    | .imageDigest
    | select(test("^sha256:[0-9a-f]{64}$"))
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
              cpu: 512,
              memory: 1024,
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
        and .taskDefinition.containerDefinitions[0].cpu == 512
        and .taskDefinition.containerDefinitions[0].memory == 1024
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
            {name: "SNAKETRON_ORIGIN", value: "https://dev.snaketron.io"},
            {name: "SNAKETRON_REDIS_URL", value: "rediss://fixture.serverless.use1.cache.amazonaws.com:6379/?protocol=resp3&cluster=true"},
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
      https://dev.snaketron.io \
      'rediss://fixture.serverless.use1.cache.amazonaws.com:6379/?protocol=resp3&cluster=true' \
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
        https://dev.snaketron.io \
        'rediss://fixture.serverless.use1.cache.amazonaws.com:6379/?protocol=resp3&cluster=true' \
        "$router_service_key" >/dev/null 2>&1; then
      echo "Live task-definition gate accepted unsafe $mutation" >&2
      return 1
    fi
  done
}

test_exact_tag_image_digest_gate() {
  local expected_tag="1111111111111111111111111111111111111111"
  local previous_tag="2222222222222222222222222222222222222222"
  local expected_digest="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  local fixture
  fixture="$(jq -n \
    --arg expected_tag "$expected_tag" \
    --arg previous_tag "$previous_tag" \
    --arg expected_digest "$expected_digest" '{
      imageDetails: [{
        imageDigest: $expected_digest,
        imageTags: [$previous_tag, $expected_tag]
      }]
    }')"

  local selected_digest
  selected_digest="$(printf '%s\n' "$fixture" \
    | select_exact_tag_image_digest "$expected_tag" "$expected_tag")" || {
    echo "Exact image-tag gate rejected a reused digest carrying the deployed commit tag" >&2
    return 1
  }
  if [[ "$selected_digest" != "$expected_digest" ]]; then
    echo "Exact image-tag gate returned the wrong digest" >&2
    return 1
  fi
  if printf '%s\n' "$fixture" \
    | select_exact_tag_image_digest \
      "$previous_tag" "$expected_tag" >/dev/null 2>&1; then
    echo "Exact image-tag gate accepted a task-definition tag from a different commit" >&2
    return 1
  fi
  if printf '%s\n' "$fixture" \
    | select_exact_tag_image_digest \
      "3333333333333333333333333333333333333333" \
      "3333333333333333333333333333333333333333" >/dev/null 2>&1; then
    echo "Exact image-tag gate accepted a digest without the deployed commit tag" >&2
    return 1
  fi
  if jq '.imageDetails += [.imageDetails[0]]' <<<"$fixture" \
    | select_exact_tag_image_digest "$expected_tag" "$expected_tag" \
      >/dev/null 2>&1; then
    echo "Exact image-tag gate accepted multiple image details" >&2
    return 1
  fi
  if jq '.imageDetails[0].imageDigest = "sha256:bad"' <<<"$fixture" \
    | select_exact_tag_image_digest "$expected_tag" "$expected_tag" \
      >/dev/null 2>&1; then
    echo "Exact image-tag gate accepted a malformed image digest" >&2
    return 1
  fi
}

test_traefik_server_up_parser() {
  local fixture_dir
  fixture_dir="$(mktemp -d)"
  local fixture="$fixture_dir/partial-a.prom"
  local partial_b="$fixture_dir/partial-b.prom"
  local complete="$fixture_dir/complete.prom"
  local healthy="$fixture_dir/healthy.json"
  printf '%s\n' \
    '# TYPE traefik_service_server_up gauge' \
    'traefik_service_server_up{service="opaque-task-a",url="http://10.0.1.10:8080"} 1' \
    'traefik_service_server_up{service="opaque-task-b",url="http://10.0.2.20:8080"} 0' \
    >"$fixture"
  printf '%s\n' \
    '# TYPE traefik_service_server_up gauge' \
    'traefik_service_server_up{service="opaque-task-a",url="http://10.0.1.10:8080"} 0' \
    'traefik_service_server_up{service="opaque-task-b",url="http://10.0.2.20:8080"} 1' \
    >"$partial_b"
  printf '%s\n' \
    '# TYPE traefik_service_server_up gauge' \
    'traefik_service_server_up{service="opaque-task-a",url="http://10.0.1.10:8080"} 1' \
    'traefik_service_server_up{service="opaque-task-b",url="http://10.0.2.20:8080"} 1' \
    >"$complete"
  jq -n '{
    tasks: [
      {task_id: "task-a", private_ipv4: "10.0.1.10"},
      {task_id: "task-b", private_ipv4: "10.0.2.20"}
    ]
  }' >"$healthy"

  local result=0
  traefik_sample_has_healthy_backend "$fixture" || result=1
  traefik_sample_has_healthy_task "$fixture" 10.0.1.10 || result=1
  if traefik_sample_has_healthy_task "$fixture" 10.0.2.20; then
    result=1
  fi
  if traefik_sample_has_healthy_task "$fixture" 10.0.3.30; then
    result=1
  fi
  if traefik_sample_has_healthy_fleet "$fixture" "$healthy"; then
    result=1
  fi
  if traefik_sample_has_healthy_fleet "$partial_b" "$healthy"; then
    result=1
  fi
  traefik_sample_has_healthy_fleet "$complete" "$healthy" || result=1
  rm -rf "$fixture_dir"

  if (( result != 0 )); then
    echo "Traefik server-up parser accepted partial fleet coverage or rejected a healthy opaque service" >&2
    return 1
  fi
}

test_command_outcome_window_gate() {
  local fixture_dir
  fixture_dir="$(mktemp -d)"
  local summary="$fixture_dir/summary.json"
  local window="$fixture_dir/window.json"
  local invalid="$fixture_dir/invalid.json"
  jq -n '{
    schema_version: 10,
    finished_at_unix_ms: 13000,
    configured_max_concurrency: 128,
    metadata: {
      threshold_result: "passed",
      mode: "duel",
      command_profile: "every-tick",
      spawn_rate_per_second: "4"
    },
    session_counts: {
      total: 1,
      completed: 1,
      failed: 0,
      cancelled: 0,
      incomplete: 0,
      peak_authenticated_concurrency: 128,
      peak_active_game_concurrency: 64
    },
    sessions: [{outcome: "completed", failure_phase: null}],
    games: {pairing_violations: 0},
    ramp_stages: [{
      target_reached: true,
      finished_at_unix_ms: 12500
    }],
    metrics: {
      traffic: {disconnects: 0, reconnects: 0, commands_sent: 11},
      usable_session_gap_ms: {max_ms: 0},
      command_counts_by_unix_second: {"10": 5, "11": 6},
      scheduled_command_counts_by_sent_unix_second: {"10": 4, "11": 5},
      scheduled_command_counts_by_partition_and_unix_second:
        (reduce range(0; 10) as $partition
          ({}; .[($partition | tostring)] = {"10": 1, "11": 1})),
      command_outcome_counts_by_sent_unix_second: {"10": 5, "11": 6},
      command_outcome_max_latency_ms_by_sent_unix_second: {"10": 999, "11": 1000}
    }
  }' >"$summary"
  jq -n '{started_at_unix_ms: 9500, finished_at_unix_ms: 12000}' >"$window"

  local result=0
  command_outcomes_meet_window_budget "$summary" "$window" || result=1
  jq '.metrics.command_outcome_max_latency_ms_by_sent_unix_second["11"] = 1001' \
    "$summary" >"$invalid"
  if command_outcomes_meet_window_budget "$invalid" "$window"; then
    result=1
  fi
  jq '.metrics.command_outcome_counts_by_sent_unix_second["10"] = 4' \
    "$summary" >"$invalid"
  if command_outcomes_meet_window_budget "$invalid" "$window"; then
    result=1
  fi
  jq '.metrics.command_counts_by_unix_second["11"] = 0' \
    "$summary" >"$invalid"
  if command_outcomes_meet_window_budget "$invalid" "$window"; then
    result=1
  fi
  jq '.metrics.scheduled_command_counts_by_sent_unix_second["11"] = 0' \
    "$summary" >"$invalid"
  if command_outcomes_meet_window_budget "$invalid" "$window"; then
    result=1
  fi
  jq 'del(
    .metrics.scheduled_command_counts_by_partition_and_unix_second["1"]["11"]
  )' "$summary" >"$invalid"
  if command_outcomes_meet_window_budget "$invalid" "$window" 1000 2; then
    result=1
  fi
  if ! command_outcome_window_diagnostics \
    "$invalid" "$window" 1000 2 \
    | jq -e '
        .passed == false
        and .full_second_count == 2
        and (.failed_seconds | length) == 1
        and .failed_seconds[0].unix_second == 11
        and .failed_seconds[0].missing_partitions == [1]
      ' >/dev/null; then
    result=1
  fi
  jq -n '{started_at_unix_ms: 8500, finished_at_unix_ms: 12000}' >"$invalid"
  if command_outcomes_meet_window_budget "$summary" "$invalid"; then
    result=1
  fi

  # The baseline ends before the scaling activity's containing second. Keep
  # that whole second in the movement gate so immediate disruption cannot hide
  # inside a fractional control-plane timestamp.
  jq -n '{started_at_unix_ms: 8500, finished_at_unix_ms: 10000}' >"$window"
  command_outcome_window_diagnostics "$summary" "$window" 1000 0 \
    | jq -e '
        .first_full_second == 9
        and .after_last_full_second == 10
      ' >/dev/null || result=1
  jq -n '{started_at_unix_ms: 10000, finished_at_unix_ms: 12000}' >"$window"
  command_outcome_window_diagnostics "$summary" "$window" 1000 0 \
    | jq -e '
        .first_full_second == 10
        and .after_last_full_second == 12
      ' >/dev/null || result=1

  local baseline="$fixture_dir/baseline.json"
  local movement="$fixture_dir/movement.json"
  local post_ready_window="$fixture_dir/post-ready-window.json"
  local post_ready_steady="$fixture_dir/post-ready-steady.json"
  local insufficient_post_ready_steady="$fixture_dir/insufficient-post-ready-steady.json"
  local failing_post_ready_steady="$fixture_dir/failing-post-ready-steady.json"
  local zero_load="$fixture_dir/zero-load.json"
  local output="$fixture_dir/gate-a-acceptance.json"
  jq -n '{passed: true, failed_seconds: []}' >"$baseline"
  jq -n '{passed: true, failed_seconds: []}' >"$movement"
  write_gate_a_post_ready_window "$summary" 10500 >"$post_ready_window"
  jq -e '
    .started_at_unix_ms == 10500
    and .finished_at_unix_ms == 12500
    and .duration_ms == 2000
  ' "$post_ready_window" >/dev/null || result=1
  command_outcome_window_diagnostics \
    "$summary" "$post_ready_window" 1000 10 >"$post_ready_steady"
  jq -e '
    .passed
    and .required_full_seconds == 1
    and .first_full_second == 11
    and .after_last_full_second == 12
    and .full_second_count == 1
  ' "$post_ready_steady" >/dev/null || result=1
  command_outcome_window_diagnostics \
    "$summary" "$post_ready_window" 1000 10 2 \
    >"$insufficient_post_ready_steady"
  jq -e '
    .passed == false
    and .required_full_seconds == 2
    and .full_second_count == 1
    and (.failed_seconds | length) == 0
  ' "$insufficient_post_ready_steady" >/dev/null || result=1
  if (( gate_a_post_ready_required_full_seconds != 60 )); then
    result=1
  fi
  jq -n '{
    passed: true,
    samples: [
      {observed_at_unix_ms: 1, raw_websockets: 0, active_games: 0}
    ]
  }' >"$zero_load"

  write_gate_a_acceptance_report \
    "$summary" "$baseline" "$movement" "$post_ready_steady" \
    "$zero_load" 0 "$output"
  jq -e '
    .schema_version == 2
    and .passed
    and .runner.passed
    and .envelope.passed
    and .zero_load.passed
    and .baseline.passed
    and .movement.passed
    and .post_ready_steady.passed
  ' "$output" >/dev/null || result=1

  jq '
    .passed = false
    | .failed_seconds = [{
        unix_second: 11,
        sent: 6,
        outcomes: 6,
        outcome_max_latency_ms: 1001,
        scheduled: 5,
        missing_partitions: []
      }]
  ' "$post_ready_steady" >"$failing_post_ready_steady"
  write_gate_a_acceptance_report \
    "$summary" "$baseline" "$movement" "$failing_post_ready_steady" \
    "$zero_load" 0 "$output"
  jq -e '
    .passed == false
    and .baseline.passed
    and .movement.passed
    and .post_ready_steady.passed == false
  ' "$output" >/dev/null || result=1

  jq '
    .metrics.traffic.commands_sent = 0
    | .metrics.command_counts_by_unix_second = {}
    | .metrics.command_outcome_counts_by_sent_unix_second = {}
  ' "$summary" >"$invalid"
  write_gate_a_acceptance_report \
    "$invalid" "$baseline" "$movement" "$post_ready_steady" \
    "$zero_load" 0 "$output"
  jq -e '
    .passed == false
    and .envelope.outcomes.command_accounting_and_partitions == false
  ' "$output" >/dev/null || result=1

  jq -n '{missing: true}' >"$invalid"
  write_gate_a_acceptance_report \
    "$invalid" "$baseline" "$movement" "$post_ready_steady" \
    "$zero_load" 1 "$output"
  jq -e '
    .passed == false
    and .envelope.summary_available == false
  ' "$output" >/dev/null || result=1
  rm -rf "$fixture_dir"

  if (( result != 0 )); then
    echo "Command-window or Gate A evidence contract failed" >&2
    return 1
  fi
}

test_staging_entry_state_contract() {
  local result=0
  staging_entry_state_is_valid \
    planned 1 "$canonical_scaling_state" || result=1
  staging_entry_state_is_valid \
    crash 1 "$suspended_scaling_state" || result=1
  if staging_entry_state_is_valid planned 2 "$canonical_scaling_state"; then
    result=1
  fi
  if staging_entry_state_is_valid planned 1 "$suspended_scaling_state"; then
    result=1
  fi
  if staging_entry_state_is_valid crash 2 "$suspended_scaling_state"; then
    result=1
  fi
  if staging_entry_state_is_valid crash 1 "$canonical_scaling_state"; then
    result=1
  fi
  if staging_entry_state_is_valid unknown 1 "$canonical_scaling_state"; then
    result=1
  fi
  if (( result != 0 )); then
    echo "Staging phase-entry state contract accepted a non-isolated baseline" >&2
    return 1
  fi
}

test_capacity_continuous_window_contract() {
  local result=0
  local fixture_dir
  fixture_dir="$(mktemp -d)"
  local summary="$fixture_dir/summary.json"
  local acceptance="$fixture_dir/acceptance.json"
  jq -n '
    def per_second($value):
      [range(1; 602)
        | {key: (. | tostring), value: $value}]
      | from_entries;
    def per_partition:
      [range(0; 10)
        | . as $partition
        | {
            key: ($partition | tostring),
            value: per_second(128)
          }]
      | from_entries;
    {
      schema_version: 10,
      configured_max_concurrency: 272,
      metadata: {
        threshold_result: "passed",
        mode: "duel",
        command_profile: "every-tick",
        spawn_rate_per_second: "4"
      },
      session_counts: {
        total: 272,
        completed: 272,
        failed: 0,
        cancelled: 0,
        incomplete: 0,
        peak_authenticated_concurrency: 272,
        peak_active_game_concurrency: 136
      },
      games: {pairing_violations: 0},
      ramp_stages: [{
        target_reached: true,
        target_reached_at_unix_ms: 1000,
        finished_at_unix_ms: 602000
      }],
      sessions: [
        range(0; 272)
        | {
            game_id: ((. / 2) | floor),
            authenticated_at_unix_ms: 0,
            playing_at_unix_ms: 0,
            game_finished_at_unix_ms: 1000000000,
            finished_at_unix_ms: 1000000000,
            outcome: "completed",
            failure_phase: null
          }
      ],
      metrics: {
        traffic: {
          commands_sent: (601 * 1280),
          disconnects: 0,
          reconnects: 0
        },
        command_counts_by_unix_second: per_second(1280),
        command_outcome_counts_by_sent_unix_second: per_second(1280),
        command_outcome_max_latency_ms_by_sent_unix_second: per_second(100),
        scheduled_command_counts_by_partition_and_unix_second: per_partition,
        planned_handoffs: {pending_commands_at_finish: 0},
        usable_session_gap_ms: {max_ms: 0},
        initial_admission_ready_ms: {p99_ms: 100}
      }
    }
  ' >"$summary"

  # A nonqualifying extra observation second must not invalidate either
  # adjacent 300-second qualifying window.
  jq '
    .metrics.command_outcome_max_latency_ms_by_sent_unix_second["301"] = 1001
  ' "$summary" >"$fixture_dir/one-gap.json"
  write_capacity_acceptance_report \
    "$fixture_dir/one-gap.json" "$acceptance"
  jq -e '
    .passed
    and .required_continuous_seconds == 300
    and .max_outcome_latency_ms == 1000
    and .evaluated_seconds == 601
    and .longest_qualifying_streak.longest_seconds == 300
    and (.nonqualifying_seconds | map(.unix_second)) == [301]
  ' "$acceptance" >/dev/null || result=1

  # Three separated gaps leave no five-minute continuous interval.
  jq '
    .metrics.command_outcome_max_latency_ms_by_sent_unix_second["150"] = 1001
    | .metrics.command_outcome_max_latency_ms_by_sent_unix_second["450"] = 1001
  ' "$fixture_dir/one-gap.json" >"$fixture_dir/three-gaps.json"
  write_capacity_acceptance_report \
    "$fixture_dir/three-gaps.json" "$acceptance"
  jq -e '
    (.passed | not)
    and .longest_qualifying_streak.longest_seconds < 300
    and (.nonqualifying_seconds | map(.unix_second)) == [150, 301, 450]
  ' "$acceptance" >/dev/null || result=1

  rm -rf "$fixture_dir"
  if (( result != 0 )); then
    echo "Capacity continuous-window evidence contract failed" >&2
    return 1
  fi
}

test_hard_crash_envelope_contract() {
  jq -en "$hard_crash_envelope_jq"'
    def command_counts($first_second; $after_last_second; $value):
      [range($first_second; $after_last_second)
        | {key: (. | tostring), value: $value}]
      | from_entries;
    def passes($seconds; $envelope; $exec_invoked_at):
      hard_crash_envelope_passes(
        $seconds;
        $envelope;
        $exec_invoked_at
      );
    hard_crash_required_report_seconds as $required_seconds
    | hard_crash_required_authenticated_sessions as $required_sessions
    | hard_crash_required_fully_joined_duels as $required_duels
    | hard_crash_required_commands_per_second as $required_commands
    | hard_crash_required_online_samples as $required_samples
    | 11 as $first_second
    | ($first_second + $required_seconds) as $gap_second
    | ($gap_second + 1) as $final_second
    | (($final_second + 1) * 1000) as $timing_origin
    | ([$required_sessions, ($required_duels * 2)] | max)
        as $minimum_session_count
    | (((($minimum_session_count + 1) / 2) | floor) * 2)
        as $session_count
    | ({
        ramp_stages: [{
          target_reached_at_unix_ms: (($first_second * 1000) - 500)
        }],
        sessions: [
          range(0; $session_count)
          | {
              game_id: ((. / 2) | floor),
              authenticated_at_unix_ms: 0,
              playing_at_unix_ms: 0,
              game_finished_at_unix_ms: ($timing_origin + 60000),
              finished_at_unix_ms: ($timing_origin + 60000)
            }
        ],
        metrics: {
          command_counts_by_unix_second: command_counts(
            $first_second;
            ($final_second + 1);
            $required_commands
          )
        }
      }
      | .metrics.command_counts_by_unix_second[
          ($gap_second | tostring)
        ] = 0) as $report
    | hard_crash_pre_crash_seconds($report; $timing_origin)
        as $positive_seconds
    | ($report
        | .metrics.command_counts_by_unix_second[
            (($gap_second - 1) | tostring)
          ] = 0
        | hard_crash_pre_crash_seconds(.; $timing_origin))
        as $short_streak_seconds
    | ($report
        | .metrics.command_counts_by_unix_second[
            ($final_second | tostring)
          ] = 0
        | hard_crash_pre_crash_seconds(.; $timing_origin))
        as $bad_final_seconds
    | {
        required_stable_seconds: $required_seconds,
        required_qualifying_samples: $required_samples,
        samples: [
          range(0; $required_samples)
          | {
              observed_at_unix_ms: (100000 + (. * 5000)),
              raw_websockets: $required_sessions,
              active_games: $required_duels
            }
        ]
      } as $online
    | (($online.samples | last | .observed_at_unix_ms) + 1)
        as $exec_invoked_at
    | [
        (($positive_seconds | first | .unix_second) == $first_second),
        (($positive_seconds | last | .unix_second) == $final_second),
        (longest_qualifying_streak($positive_seconds).longest_seconds
          == $required_seconds),
        ($positive_seconds | last | .qualifying),
        passes($positive_seconds; $online; $exec_invoked_at),
        (passes(
          $short_streak_seconds;
          $online;
          $exec_invoked_at
        ) | not),
        (passes(
          $bad_final_seconds;
          $online;
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          ($online | .samples = .samples[:-1]);
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          ($online
            | .samples[0].raw_websockets = ($required_sessions | tostring));
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          ($online | .samples[0].active_games += 0.5);
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          ($online | .samples[0].observed_at_unix_ms += 0.5);
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          ($online
            | .samples[1].observed_at_unix_ms =
                .samples[0].observed_at_unix_ms);
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          ($online
            | .samples |= (
                to_entries
                | map(
                    .value.observed_at_unix_ms +=
                      (.key * (hard_crash_max_online_sample_gap_ms - 4999))
                  )
                | map(.value)
              ));
          $exec_invoked_at
        ) | not),
        (passes(
          $positive_seconds;
          $online;
          ($online.samples | last | .observed_at_unix_ms) - 1
        ) | not)
      ]
    | all
  ' >/dev/null || {
    echo "Hard-crash envelope evidence contract failed" >&2
    return 1
  }
}

test_hard_crash_evidence_selectors() {
  local result=0
  local fixture_dir
  fixture_dir="$(mktemp -d)"
  local execution_stopped_at_ms=1785340236421
  local partition=3
  local killed_boot_id="11111111-1111-4111-8111-111111111111"
  local survivor_boot_id="22222222-2222-4222-8222-222222222222"
  local replacement_boot_id="33333333-3333-4333-8333-333333333333"
  local killed_task_id="task-killed"
  local survivor_task_id="task-survivor"
  local killed_lease_token="${killed_boot_id}:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  local survivor_lease_token="${survivor_boot_id}:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
  local pre="$fixture_dir/pre.json"
  local ownership_pre="$fixture_dir/ownership-pre.json"
  local pending_pre="$fixture_dir/pending-pre.json"
  local post_pending="$fixture_dir/post-pending.json"
  local owner_valid="$fixture_dir/owner-valid.json"
  local output_valid="$fixture_dir/output-valid.json"
  local selected="$fixture_dir/selected.json"

  jq -n \
    --argjson stopped "$execution_stopped_at_ms" \
    --argjson partition "$partition" \
    --arg killed_boot "$killed_boot_id" \
    --arg survivor_boot "$survivor_boot_id" \
    --arg killed_task "$killed_task_id" \
    --arg survivor_task "$survivor_task_id" \
    --arg killed_token "$killed_lease_token" '
      {
        observation_started_at_ms: ($stopped - 2),
        captured_at_ms: ($stopped - 1),
        observation_completed_at_ms: ($stopped - 1),
        live_members: [
          {
            boot_id: $killed_boot,
            ecs_task_id: $killed_task,
            lifecycle: "ACTIVE"
          },
          {
            boot_id: $survivor_boot,
            ecs_task_id: $survivor_task,
            lifecycle: "ACTIVE"
          }
        ],
        assignment: {
          version: 41,
          computed_at_ms: ($stopped - 100)
        },
        runtime_partitions: [{
          partition: $partition,
          desired_owner: $killed_boot,
          active_owner: $killed_boot,
          owner_matches: true,
          lease_token: $killed_token,
          pending_entry_sample: [{id: "1-0", consumer: $killed_token}]
        }]
      }
    ' >"$pre"
  jq \
    --arg killed_boot "$killed_boot_id" \
    --arg killed_task "$killed_task_id" '
      .authority_stable = true
      | .killed_member_live = true
      | .membership_observed_at_ms = .captured_at_ms
      | .authority_observed_at_ms = .captured_at_ms
      | .authority_event_tail_id = "1785340236000-1"
      | .live_members = [{
          boot_id: $killed_boot,
          ecs_task_id: $killed_task,
          lifecycle: "ACTIVE"
        }]
    ' "$pre" >"$ownership_pre"
  jq -n \
    --argjson stopped "$execution_stopped_at_ms" \
    --argjson partition "$partition" \
    --arg killed_token "$killed_lease_token" '
      {
        observation_started_at_ms: ($stopped - 2),
        captured_at_ms: ($stopped - 1),
        observation_completed_at_ms: ($stopped - 1),
        partition: $partition,
        requested_consumer: $killed_token,
        pending_entry: {
          id: "1-0",
          consumer: $killed_token,
          idle_ms: 42,
          delivery_count: 1
        }
      }
    ' >"$pending_pre"
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .observation_started_at_ms = ($stopped + 500)
      | .captured_at_ms = ($stopped + 1000)
      | .observation_completed_at_ms = ($stopped + 2000)
    ' "$pending_pre" >"$post_pending"
  jq \
    --argjson stopped "$execution_stopped_at_ms" \
    --arg survivor_boot "$survivor_boot_id" \
    --arg survivor_task "$survivor_task_id" \
    --arg survivor_token "$survivor_lease_token" '
      .observation_started_at_ms = ($stopped + 500)
      | .captured_at_ms = ($stopped + 2500)
      | .observation_completed_at_ms = ($stopped + 5000)
      | .membership_observed_at_ms = ($stopped + 2400)
      | .authority_observed_at_ms = ($stopped + 2450)
      | .authority_event_tail_id = (($stopped + 2450 | tostring) + "-1")
      | .authority_stable = true
      | .killed_member_live = false
      | .live_members = [{
          boot_id: $survivor_boot,
          ecs_task_id: $survivor_task,
          lifecycle: "ACTIVE"
      }]
      | .assignment.version = 42
      | .assignment.computed_at_ms = ($stopped + 100)
      | .runtime_partitions[0].desired_owner = $survivor_boot
      | .runtime_partitions[0].active_owner = $survivor_boot
      | .runtime_partitions[0].owner_matches = true
      | .runtime_partitions[0].lease_token = $survivor_token
    ' "$ownership_pre" >"$owner_valid"
  jq -n \
    --argjson stopped "$execution_stopped_at_ms" \
    --argjson partition "$partition" \
    --arg anchor "$((execution_stopped_at_ms + 2450))-1" '
      {
        observation_started_at_ms: ($stopped + 6000),
        captured_at_ms: ($stopped + 6500),
        observation_completed_at_ms: ($stopped + 7000),
        partition: $partition,
        after_stream_id: $anchor,
        first_scheduled_output: {
          stream_id: (($stopped + 3000 | tostring) + "-0"),
          stream_unix_ms: ($stopped + 3000),
          game_id: (10 + $partition),
          command_id: {
            game_id: (10 + $partition),
            user_id: 77,
            client_game_session_id: "fixture-session",
            sequence: 1
          },
          deduplicated_replay: false
        }
      }
    ' >"$output_valid"

  select_pre_fault_pending_sample \
    "$partition" "$killed_lease_token" \
    "$pending_pre" >"$selected" || result=1
  jq -e \
    --argjson expected "$((execution_stopped_at_ms - 1))" '
      .observation_completed_at_ms == $expected
    ' "$selected" >/dev/null || result=1
  jq '
    .observation_completed_at_ms = (.observation_started_at_ms - 1)
  ' "$pending_pre" >"$fixture_dir/pre-invalid-interval.json"
  if select_pre_fault_pending_sample \
    "$partition" "$killed_lease_token" \
    "$fixture_dir/pre-invalid-interval.json" >/dev/null 2>&1; then
    result=1
  fi
  select_pre_fault_ownership_sample \
    "$partition" "$killed_boot_id" "$killed_lease_token" \
    "$ownership_pre" >"$selected" || result=1
  jq -e '.authority_stable and .killed_member_live' \
    "$selected" >/dev/null || result=1

  [[ "$(ecs_timestamp_to_unix_ms \
    "2026-07-29T15:50:36.421000+00:00")" == "$execution_stopped_at_ms" ]] \
    || result=1
  [[ "$(ecs_timestamp_to_unix_ms \
    "2026-07-29T15:50:36Z")" == "1785340236000" ]] || result=1
  if ecs_timestamp_to_unix_ms \
    "2026-07-29T15:50:36.421000+01:00" >/dev/null 2>&1; then
    result=1
  fi
  if ecs_timestamp_to_unix_ms "not-a-timestamp" >/dev/null 2>&1; then
    result=1
  fi

  select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$pending_pre" "$post_pending" >"$selected" || result=1
  jq -e \
    --argjson expected "$((execution_stopped_at_ms + 2000))" '
      .observation_completed_at_ms == $expected
    ' "$selected" >/dev/null || result=1

  select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$owner_valid" >"$selected" || result=1
  jq -e \
    --argjson expected "$((execution_stopped_at_ms + 5000))" \
    --arg survivor "$survivor_boot_id" '
      .observation_completed_at_ms == $expected
      and .runtime_partitions[0].active_owner == $survivor
    ' "$selected" >/dev/null || result=1
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .assignment.computed_at_ms = ($stopped + 600)
    ' "$owner_valid" >"$fixture_dir/owner-computed-during-observation.json"
  select_hard_crash_owner_candidate_ownership_sample \
    "$pre" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-computed-during-observation.json" \
    >"$selected" || result=1
  select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-computed-during-observation.json" \
    >"$selected" || result=1
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .observation_started_at_ms = ($stopped - 1)
      | .assignment.computed_at_ms = ($stopped + 1)
    ' "$owner_valid" >"$fixture_dir/owner-spans-stop.json"
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .observation_started_at_ms = ($stopped + 500)
    ' "$fixture_dir/owner-spans-stop.json" \
    >"$fixture_dir/owner-after-spanning-sample.json"
  select_hard_crash_owner_candidate_ownership_sample \
    "$pre" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-spans-stop.json" >"$selected" || result=1
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-spans-stop.json" >/dev/null 2>&1; then
    result=1
  fi
  select_hard_crash_owner_candidate_ownership_sample \
    "$pre" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-after-spanning-sample.json" \
    >"$selected" || result=1
  select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-after-spanning-sample.json" \
    >"$selected" || result=1
  select_hard_crash_authoritative_output_sample \
    "$execution_stopped_at_ms" "$partition" \
    "$((execution_stopped_at_ms + 2450))-1" \
    "$output_valid" >"$selected" || result=1
  jq -e \
    --argjson expected "$((execution_stopped_at_ms + 3000))" '
      .first_scheduled_output.stream_unix_ms == $expected
    ' "$selected" >/dev/null || result=1

  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$pending_pre" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --argjson stopped "$execution_stopped_at_ms" \
    '.observation_completed_at_ms = ($stopped + 2001)' \
    "$post_pending" >"$fixture_dir/pending-late.json"
  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$fixture_dir/pending-late.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .observation_started_at_ms = ($stopped - 1)
    ' "$post_pending" >"$fixture_dir/pending-started-early.json"
  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$fixture_dir/pending-started-early.json" >/dev/null 2>&1; then
    result=1
  fi
  jq --arg token "$survivor_lease_token" \
    '.pending_entry.consumer = $token' \
    "$post_pending" >"$fixture_dir/pending-wrong-consumer.json"
  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$fixture_dir/pending-wrong-consumer.json" >/dev/null 2>&1; then
    result=1
  fi
  jq --arg token "$survivor_lease_token" \
    '.requested_consumer = $token' \
    "$post_pending" >"$fixture_dir/pending-wrong-request.json"
  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$fixture_dir/pending-wrong-request.json" >/dev/null 2>&1; then
    result=1
  fi
  jq '.pending_entry = null' \
    "$post_pending" >"$fixture_dir/pending-empty.json"
  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    "$fixture_dir/pending-empty.json" >/dev/null 2>&1; then
    result=1
  fi

  jq \
    --argjson stopped "$execution_stopped_at_ms" \
    --arg killed_token "$killed_lease_token" '
      .captured_at_ms = ($stopped + 100)
      | .assignment.version = 41
      | .runtime_partitions[0].lease_token = $killed_token
    ' "$owner_valid" >"$fixture_dir/owner-unfenced.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-unfenced.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .assignment.computed_at_ms = ($stopped - 1)
    ' "$owner_valid" >"$fixture_dir/owner-assigned-before-stop.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-assigned-before-stop.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    '.authority_stable = false' \
    "$owner_valid" >"$fixture_dir/owner-incoherent.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-incoherent.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    '.killed_member_live = true' \
    "$owner_valid" >"$fixture_dir/owner-killed-member-live.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-killed-member-live.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --arg replacement_boot "$replacement_boot_id" '
      .live_members = [{
        boot_id: $replacement_boot,
        ecs_task_id: "task-replacement",
        lifecycle: "ACTIVE"
      }]
      | .runtime_partitions[0].desired_owner = $replacement_boot
      | .runtime_partitions[0].active_owner = $replacement_boot
      | .runtime_partitions[0].lease_token =
          ($replacement_boot + ":cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    ' "$owner_valid" >"$fixture_dir/owner-replacement.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-replacement.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --argjson stopped "$execution_stopped_at_ms" \
    '.observation_completed_at_ms = ($stopped + 5001)' \
    "$owner_valid" >"$fixture_dir/owner-late.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-late.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .observation_started_at_ms = ($stopped - 1)
    ' "$owner_valid" >"$fixture_dir/owner-started-early.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-started-early.json" >/dev/null 2>&1; then
    result=1
  fi
  jq '
    .authority_observed_at_ms = (.observation_completed_at_ms + 1)
  ' "$owner_valid" >"$fixture_dir/owner-authority-outside-interval.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-authority-outside-interval.json" >/dev/null 2>&1; then
    result=1
  fi
  jq '.authority_event_tail_id = "not-a-stream-id"' \
    "$owner_valid" >"$fixture_dir/owner-invalid-tail.json"
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    "$fixture_dir/owner-invalid-tail.json" >/dev/null 2>&1; then
    result=1
  fi

  jq \
    --argjson stopped "$execution_stopped_at_ms" '
      .first_scheduled_output.stream_id = (($stopped + 5001 | tostring) + "-0")
      | .first_scheduled_output.stream_unix_ms = ($stopped + 5001)
    ' "$output_valid" >"$fixture_dir/output-late.json"
  if select_hard_crash_authoritative_output_sample \
    "$execution_stopped_at_ms" "$partition" \
    "$((execution_stopped_at_ms + 2450))-1" \
    "$fixture_dir/output-late.json" >/dev/null 2>&1; then
    result=1
  fi
  jq \
    --arg anchor "$((execution_stopped_at_ms + 2450))-1" '
      .first_scheduled_output.stream_id = $anchor
      | .first_scheduled_output.stream_unix_ms =
          ($anchor | split("-")[0] | tonumber)
    ' "$output_valid" >"$fixture_dir/output-not-after-anchor.json"
  if select_hard_crash_authoritative_output_sample \
    "$execution_stopped_at_ms" "$partition" \
    "$((execution_stopped_at_ms + 2450))-1" \
    "$fixture_dir/output-not-after-anchor.json" >/dev/null 2>&1; then
    result=1
  fi
  jq '.after_stream_id = "1-0"' \
    "$output_valid" >"$fixture_dir/output-wrong-anchor.json"
  if select_hard_crash_authoritative_output_sample \
    "$execution_stopped_at_ms" "$partition" \
    "$((execution_stopped_at_ms + 2450))-1" \
    "$fixture_dir/output-wrong-anchor.json" >/dev/null 2>&1; then
    result=1
  fi
  jq '.first_scheduled_output.deduplicated_replay = true' \
    "$output_valid" >"$fixture_dir/output-deduplicated-replay.json"
  if select_hard_crash_authoritative_output_sample \
    "$execution_stopped_at_ms" "$partition" \
    "$((execution_stopped_at_ms + 2450))-1" \
    "$fixture_dir/output-deduplicated-replay.json" >/dev/null 2>&1; then
    result=1
  fi

  if select_post_kill_pending_sample \
    "$execution_stopped_at_ms" "$partition" "$killed_lease_token" \
    >/dev/null 2>&1; then
    result=1
  fi
  if select_pre_fault_pending_sample \
    "$partition" "$killed_lease_token" \
    >/dev/null 2>&1; then
    result=1
  fi
  if select_hard_crash_owner_ready_ownership_sample \
    "$pre" "$execution_stopped_at_ms" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    >/dev/null 2>&1; then
    result=1
  fi
  if select_hard_crash_owner_candidate_ownership_sample \
    "$pre" "$partition" \
    "$killed_boot_id" "$killed_task_id" "$killed_lease_token" \
    >/dev/null 2>&1; then
    result=1
  fi
  if select_hard_crash_authoritative_output_sample \
    "$execution_stopped_at_ms" "$partition" \
    "$((execution_stopped_at_ms + 2450))-1" \
    >/dev/null 2>&1; then
    result=1
  fi

  rm -rf "$fixture_dir"
  if (( result != 0 )); then
    echo "Hard-crash evidence selector contract failed" >&2
    return 1
  fi
}

test_unexpected_crash_stop_selector() {
  local result=0
  local fixture_dir
  fixture_dir="$(mktemp -d)"
  local started=1785340300
  local expected_task_arn="arn:aws:ecs:region:account:task/expected"
  local prior_task_arn="arn:aws:ecs:region:account:task/prior"
  local unexpected_task_arn="arn:aws:ecs:region:account:task/unexpected"
  local unknown_onset_task_arn="arn:aws:ecs:region:account:task/unknown-onset"
  local baseline_survivor_arn="arn:aws:ecs:region:account:task/survivor"
  local old_nonbaseline_task_arn="arn:aws:ecs:region:account:task/old"

  jq -n \
    --arg prior "$prior_task_arn" \
    --arg survivor "$baseline_survivor_arn" '
    {
      tasks: [
        {
          taskArn: $prior,
          desiredStatus: "STOPPED",
          lastStatus: "STOPPING",
          stoppingAt: "2026-07-29T15:50:30.969Z"
        },
        {
          taskArn: $survivor,
          desiredStatus: "RUNNING",
          lastStatus: "RUNNING"
        }
      ],
      failures: []
    }
  ' >"$fixture_dir/baseline.json"
  jq -n \
    --arg prior "$prior_task_arn" \
    --arg expected "$expected_task_arn" \
    --arg unexpected "$unexpected_task_arn" \
    --arg unknown_onset "$unknown_onset_task_arn" \
    --arg survivor "$baseline_survivor_arn" \
    --arg old "$old_nonbaseline_task_arn" '
      {
        tasks: [
          {
            taskArn: $prior,
            stoppingAt: "2026-07-29T15:50:30.969Z",
            executionStoppedAt: "2026-07-29T15:51:41.511Z",
            stoppedAt: "2026-07-29T15:52:17.437Z"
          },
          {
            taskArn: $expected,
            stoppingAt: "2026-07-29T15:51:45Z",
            stoppedAt: "2026-07-29T15:51:50Z"
          },
          {
            taskArn: $unexpected,
            stoppingAt: "2026-07-29T15:51:46Z",
            stoppedAt: "2026-07-29T15:51:51Z"
          },
          {
            taskArn: $unknown_onset
          },
          {
            taskArn: $survivor,
            stoppingAt: "2026-07-29T15:51:47Z",
            stoppedAt: "2026-07-29T15:51:53Z"
          },
          {
            taskArn: $old,
            stoppingAt: "2026-07-29T15:50:20Z",
            stoppedAt: "2026-07-29T15:51:52Z"
          }
        ],
        failures: []
      }
    ' >"$fixture_dir/observed.json"

  select_unexpected_crash_stops \
    "$started" \
    "$expected_task_arn" \
    "$fixture_dir/baseline.json" \
    "$fixture_dir/observed.json" >"$fixture_dir/selected.json" || result=1
  jq -e \
    --arg unexpected "$unexpected_task_arn" \
    --arg unknown_onset "$unknown_onset_task_arn" \
    --arg survivor "$baseline_survivor_arn" '
    (map(.taskArn) | sort)
      == ([$unexpected, $unknown_onset, $survivor] | sort)
  ' "$fixture_dir/selected.json" >/dev/null || result=1

  rm -rf "$fixture_dir"
  if (( result != 0 )); then
    echo "Unexpected crash-stop selector contract failed" >&2
    return 1
  fi
}

test_evidence_safety_helpers() {
  test_task_definition_evidence_sanitizer
  test_live_task_definition_gate
  test_exact_tag_image_digest_gate
  test_traefik_server_up_parser
  test_command_outcome_window_gate
  test_staging_entry_state_contract
  test_capacity_continuous_window_contract
  test_hard_crash_envelope_contract
  test_hard_crash_evidence_selectors
  test_unexpected_crash_stop_selector
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
  local shared_vpc_id="vpc-0123456789abcdef0"
  local lookup_context='"availability-zones:account=111111111111:region=us-east-1":["us-east-1a","us-east-1b"],"availability-zones:account=111111111111:region=eu-west-1":["eu-west-1a","eu-west-1b"],"hosted-zone:account=111111111111:domainName=snaketron.io:region=us-east-1":{"Id":"/hostedzone/ZDUMMYIO","Name":"snaketron.io."},"hosted-zone:account=111111111111:domainName=snaketron.com:region=us-east-1":{"Id":"/hostedzone/ZDUMMYCOM","Name":"snaketron.com."},"hosted-zone:account=111111111111:domainName=snaketron.io:region=eu-west-1":{"Id":"/hostedzone/ZDUMMYIO","Name":"snaketron.io."},"hosted-zone:account=111111111111:domainName=snaketron.com:region=eu-west-1":{"Id":"/hostedzone/ZDUMMYCOM","Name":"snaketron.com."},"vpc-provider:account=111111111111:filter.vpc-id=vpc-0123456789abcdef0:region=us-east-1:returnAsymmetricSubnets=true":{"vpcId":"vpc-0123456789abcdef0","vpcCidrBlock":"10.1.0.0/16","ownerAccountId":"111111111111","availabilityZones":[],"subnetGroups":[{"name":"Public","type":"Public","subnets":[{"subnetId":"subnet-public-a","cidr":"10.1.0.0/24","availabilityZone":"us-east-1a","routeTableId":"rtb-public-a"}]},{"name":"Private","type":"Private","subnets":[{"subnetId":"subnet-private-a","cidr":"10.1.2.0/24","availabilityZone":"us-east-1a","routeTableId":"rtb-private-a"},{"subnetId":"subnet-private-b","cidr":"10.1.3.0/24","availabilityZone":"us-east-1b","routeTableId":"rtb-private-b"}]}]}'
  local development_context
  local production_context
  development_context="{\"environment\":\"development\",\"ephemeral\":\"true\",\"sharedVpcId\":\"$shared_vpc_id\",\"ephemeralRunId\":\"1-1\",\"expiresAtEpoch\":\"$expires_at_epoch\",\"imageTag\":\"0000000000000000000000000000000000000000\",$lookup_context}"
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
    SNAKETRON_VALKEY_SERVERLESS_CACHE_NAME
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
    echo "SNAKETRON_STAGING_REDIS_CONTROL_URL must preserve the Serverless cache hostname for TLS and cluster discovery" >&2
    return 1
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
  jq -e '
    .taskDefinition.cpu == "2048"
    and .taskDefinition.memory == "4096"
    and ([.taskDefinition.containerDefinitions[]
      | select(
          .name == "snaketron-server"
          and .cpu == 2048
          and .memory == 4096)]
      | length) == 1
  ' "$identity_dir/task-definition.json" >/dev/null || {
    echo "Staging must use the certified two-vCPU, four-GiB task and server-container size" >&2
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
  local runner_checkout_commit
  runner_checkout_commit="$(git -C "$repo_dir/.." rev-parse HEAD)"
  staging_image_digest="$(select_exact_tag_image_digest \
    "$staging_image_tag" "$runner_checkout_commit" \
    <"$identity_dir/server-image.json")" || {
    echo "The task-definition tag must match the runner checkout and resolve to one valid ECR digest" >&2
    return 1
  }
  staging_image_commit="$staging_image_tag"

  aws elasticache describe-serverless-caches \
    --region "$SNAKETRON_AWS_REGION" \
    --serverless-cache-name "$SNAKETRON_VALKEY_SERVERLESS_CACHE_NAME" \
    >"$identity_dir/valkey.json"
  jq -e --arg expected_name "$SNAKETRON_VALKEY_SERVERLESS_CACHE_NAME" '
    (.ServerlessCaches | length) == 1
    and .ServerlessCaches[0].ServerlessCacheName == $expected_name
    and (.ServerlessCaches[0].Status | ascii_downcase) == "available"
    and (.ServerlessCaches[0].Engine | ascii_downcase) == "valkey"
    and .ServerlessCaches[0].MajorEngineVersion == "8"
    and (.ServerlessCaches[0].FullEngineVersion | startswith("8."))
    and (.ServerlessCaches[0].Endpoint.Address | type == "string" and length > 0)
    and .ServerlessCaches[0].Endpoint.Port == 6379
  ' "$identity_dir/valkey.json" >/dev/null || {
    echo "The named Serverless Valkey 8 cache is not one available TLS endpoint" >&2
    return 1
  }
  staging_valkey_arn="$(jq -r '.ServerlessCaches[0].ARN' "$identity_dir/valkey.json")"
  staging_valkey_name="$(jq -r '.ServerlessCaches[0].ServerlessCacheName' "$identity_dir/valkey.json")"
  staging_valkey_host="$(jq -r '.ServerlessCaches[0].Endpoint.Address' "$identity_dir/valkey.json")"
  staging_valkey_port="$(jq -r '.ServerlessCaches[0].Endpoint.Port' "$identity_dir/valkey.json")"
  aws elasticache list-tags-for-resource \
    --region "$SNAKETRON_AWS_REGION" \
    --resource-name "$staging_valkey_arn" >"$identity_dir/valkey-tags.json"
  assert_aws_tags "$identity_dir/valkey-tags.json" TagList "Serverless Valkey cache"

  local expected_redis_url="rediss://$staging_valkey_host:$staging_valkey_port/?protocol=resp3&cluster=true"
  if [[ "$SNAKETRON_STAGING_REDIS_URL" != "$expected_redis_url" ]]; then
    echo "SNAKETRON_STAGING_REDIS_URL must name the tagged Serverless cache with TLS, RESP3, and cluster mode" >&2
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
    --arg valkey_name "$staging_valkey_name" \
    --arg valkey_endpoint_host "$staging_valkey_host" \
    --argjson valkey_endpoint_port "$staging_valkey_port" \
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
      valkey_serverless_cache_name: $valkey_name,
      valkey_endpoint: {
        host: $valkey_endpoint_host,
        port: $valkey_endpoint_port
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
    and target("ECSServiceAverageCPUUtilization"; 15)
    and target("ECSServiceAverageMemoryUtilization"; 80)
  ' "$identity_dir/scaling-policies.json" >/dev/null || {
    echo "Staging must have only CPU=15% and memory=80% target tracking with 60-second cooldowns" >&2
    return 1
  }
}

wait_for_running_count() {
  local wanted="$1"
  local stable_samples="${2:-1}"
  local consecutive=0
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
      consecutive=$((consecutive + 1))
      if (( consecutive >= stable_samples )); then
        return 0
      fi
    else
      consecutive=0
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

wait_for_no_active_scaling_activity() {
  local output="$1"
  local candidate="$output.pending"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    aws application-autoscaling describe-scaling-activities \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --resource-id "$scaling_resource" \
      --scalable-dimension ecs:service:DesiredCount \
      --max-results 50 >"$candidate"
    if jq -e '
      [.ScalingActivities[]
        | select(.StatusCode == "Pending" or .StatusCode == "InProgress")]
      | length == 0
    ' "$candidate" >/dev/null; then
      mv "$candidate" "$output"
      return 0
    fi
    sleep 2
  done
  [[ -f "$candidate" ]] && mv "$candidate" "$output"
  echo "Application Auto Scaling still has an active service write after three minutes" >&2
  return 1
}

wait_for_automatic_scale_out() {
  local report_dir="$1"
  local started_at_epoch="$2"
  local observed_pid="${3:-}"
  # Target tracking needs three one-minute alarm periods and may begin just
  # after a bucket boundary. Bound the managed policy decision here; the
  # caller separately waits for the added Fargate task and every readiness
  # view, so cold task startup does not consume this fixed observation budget.
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
    desired="$(jq -r '.services[0].desiredCount' "$candidate")"
    if (( desired > 1 )); then
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
  # datapoints. A ten-to-one contraction can then require eight further
  # cooldown/evaluation cycles plus the final ECS task stop. Forty minutes is
  # an observation ceiling, not a product scale-in SLO; healthy runs return as
  # soon as the service reaches one.
  local deadline=$((SECONDS + 2400))
  local decrease_observed=false
  local next_control_probe=0
  while (( SECONDS < deadline )); do
    # The public workflow reaches Valkey through two SSM port-forwarding
    # sessions. Keep both sessions active during this deliberately zero-load
    # AWS observation, and retain a semantic control-plane read as evidence.
    # Each fresh cluster client bootstraps and PINGs both advertised endpoints.
    # This is certification plumbing only; it does not influence assignment,
    # leases, desired count, or target tracking.
    if (( SECONDS >= next_control_probe )); then
      local port
      for port in "$staging_valkey_port" "${VALKEY_READ_PORT:-6380}"; do
        if ! retry_command 3 nc -z -w 3 "$staging_valkey_host" "$port"; then
          echo "Valkey certification path on port $port became unavailable during automatic scale-in" >&2
          return 1
        fi
      done

      local control_candidate="$report_dir/automatic-scale-in-control.pending.json"
      local control_probe_ok=false
      local control_attempt
      for control_attempt in 1 2 3; do
        if SNAKETRON_REDIS_URL="$staging_redis_control_url" \
          timeout --signal=TERM --kill-after=1s 10s \
            "$resilience_admin" status \
            --region-key "$SNAKETRON_REGION_CODE" \
            >"$control_candidate" 2>/dev/null \
          && jq -e '
            type == "object"
            and (.live_members | type == "array")
            and (.runtime_partitions | type == "array")
          ' "$control_candidate" >/dev/null; then
          control_probe_ok=true
          break
        fi
        sleep 2
      done
      if [[ "$control_probe_ok" != true ]]; then
        echo "Valkey executor control path became unavailable during automatic scale-in" >&2
        return 1
      fi
      mv "$control_candidate" "$report_dir/automatic-scale-in-control.json"
      next_control_probe=$((SECONDS + 60))
    fi

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
  echo "After load removal, CPU/memory autoscaling did not reduce the ten-task service to one within forty minutes" >&2
  return 1
}

capture_stopped_tasks_snapshot() {
  local output="$1"
  local pending="$output.pending"
  local task_list="$output.task-list.pending"
  local stopped_arns
  aws ecs list-tasks \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service-name "$SNAKETRON_ECS_SERVICE" \
    --desired-status STOPPED >"$task_list" || {
      rm -f "$task_list" "$pending"
      return 1
    }
  stopped_arns="$(jq -r '.taskArns[]?' "$task_list" | tr '\n' ' ')"
  rm -f "$task_list"
  if [[ -n "$stopped_arns" ]]; then
    # ECS task ARNs contain no whitespace. Intentional splitting supplies the
    # AWS CLI's variadic --tasks argument.
    aws ecs describe-tasks \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --tasks $stopped_arns >"$pending" || {
        rm -f "$pending"
        return 1
      }
  else
    jq -n '{tasks: [], failures: []}' >"$pending"
  fi
  mv "$pending" "$output"
}

start_ecs_runtime_monitor() {
  ecs_runtime_monitor_dir="$1/ecs-runtime-observations"
  mkdir -p "$ecs_runtime_monitor_dir"
  jq -n '{tasks: [], failures: []}' \
    >"$ecs_runtime_monitor_dir/000000.json"
  (
    local sequence=0
    while true; do
      sequence=$((sequence + 1))
      local sample
      printf -v sample '%s/%06d.json' "$ecs_runtime_monitor_dir" "$sequence"
      capture_stopped_tasks_snapshot "$sample" || true
      sleep 30
    done
  ) &
  ecs_runtime_monitor_pid=$!
}

stop_ecs_runtime_monitor() {
  local monitor_pid="${ecs_runtime_monitor_pid:-}"
  if [[ -n "$monitor_pid" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi
  ecs_runtime_monitor_pid=""
}

collect_observed_stopped_tasks() {
  local output="$1"
  local final_sample="$ecs_runtime_monitor_dir/final.json"
  capture_stopped_tasks_snapshot "$final_sample"
  jq -s '
    ([.[] | .tasks[]?]
      | group_by(.taskArn)
      | map(. as $versions
        | (($versions | map(select(.stoppedAt != null)) | last)
          // ($versions | last)))) as $tasks
    | {
        tasks: $tasks,
        failures: ([.[] | .failures[]?]
          | unique_by([.arn, .reason, .detail])
          | map(select(.arn as $arn
            | ($tasks | any(.taskArn == $arn) | not))))
      }
  ' "$ecs_runtime_monitor_dir"/*.json >"$output"
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
    # Traefik's per-server metric uses an opaque service id even though its
    # router metrics use the configured service name. This dedicated proxy has
    # only the Snaketron ECS provider, so any healthy :8080 backend proves that
    # public routing retained an available server.
    if ! traefik_sample_has_healthy_backend "$sample"; then
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
  local health_observed_at_ms
  health_observed_at_ms="$(unix_time_ms)"
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
  local expected_count
  expected_count="$(jq -er '.tasks | length | select(. > 0)' "$healthy_observation")"
  local fully_healthy_observed_at_ms
  fully_healthy_observed_at_ms="$(jq -er \
    '[.tasks[].healthy_observed_at_unix_ms] | max' "$healthy_observation")"
  local polling_started_at_ms
  polling_started_at_ms="$(unix_time_ms)"
  # This is an operational bound for the staging probe, not a per-task
  # readiness-transition SLA. User-visible readiness timing is certified by
  # the admission load; this probe only decides when fleet capacity may count.
  local deadline=$((SECONDS + 10))
  local sequence=0

  while true; do
    sequence=$((sequence + 1))
    local observed_at_ms
    local sample
    local sample_name
    local scrape_succeeded=false
    local all_tasks_server_up=false
    printf -v sample '%s/%03d.prom' "$readiness_dir" "$sequence"
    sample_name="$(basename "$sample")"
    if curl -fsS --max-time 2 "$staging_traefik_metrics_control_url" >"$sample"; then
      observed_at_ms="$(unix_time_ms)"
      scrape_succeeded=true
      if traefik_sample_has_healthy_fleet "$sample" "$healthy_observation"; then
        all_tasks_server_up=true
      fi
    else
      observed_at_ms="$(unix_time_ms)"
      mv "$sample" "$sample.error"
      sample_name="${sample_name}.error"
    fi
    jq -cn \
      --arg sample_file "$sample_name" \
      --argjson sequence "$sequence" \
      --argjson observed_at_unix_ms "$observed_at_ms" \
      --argjson scrape_succeeded "$scrape_succeeded" \
      --argjson expected_task_count "$expected_count" \
      --argjson all_tasks_server_up "$all_tasks_server_up" '
        {
          sequence: $sequence,
          observed_at_unix_ms: $observed_at_unix_ms,
          sample_file: $sample_file,
          scrape_succeeded: $scrape_succeeded,
          expected_task_count: $expected_task_count,
          all_tasks_server_up: $all_tasks_server_up
        }
      ' >>"$observations"

    # Capacity counts only when one fresh scrape sees the complete fleet at
    # once. Accumulating per-task observations across scrapes could certify a
    # fleet that was never simultaneously routable.
    if [[ "$all_tasks_server_up" == true ]]; then
      break
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 2
  done

  local polling_finished_at_ms
  polling_finished_at_ms="$(unix_time_ms)"
  jq -s \
    --argjson fully_healthy_observed_at_unix_ms "$fully_healthy_observed_at_ms" \
    --argjson polling_started_at_unix_ms "$polling_started_at_ms" \
    --argjson polling_finished_at_unix_ms "$polling_finished_at_ms" \
    --slurpfile healthy "$healthy_observation" '
      . as $observations
      | ([$observations[] | select(.all_tasks_server_up)] | first // null)
        as $ready_sample
      | {
          schema_version: 1,
          fully_healthy_observed_at_unix_ms:
            $fully_healthy_observed_at_unix_ms,
          polling_started_at_unix_ms: $polling_started_at_unix_ms,
          polling_finished_at_unix_ms: $polling_finished_at_unix_ms,
          all_tasks_server_up_observed_at_unix_ms:
            ($ready_sample.observed_at_unix_ms // null),
          ready_sample_file: ($ready_sample.sample_file // null),
          expected_tasks: $healthy[0].tasks,
          ready_sample: $ready_sample,
          observations: $observations
        }
    ' "$observations" >"$readiness_dir/summary.json"
  jq -e \
    --argjson expected_count "$expected_count" '
      .ready_sample != null
      and .ready_sample.scrape_succeeded
      and .ready_sample.all_tasks_server_up
      and .ready_sample.expected_task_count == $expected_count
      and (.expected_tasks | length) == $expected_count
      and .ready_sample_file == .ready_sample.sample_file
      and .all_tasks_server_up_observed_at_unix_ms
        == .ready_sample.observed_at_unix_ms
    ' "$readiness_dir/summary.json" >/dev/null || {
      echo "Traefik did not expose every healthy $label task simultaneously as server_up during bounded readiness polling" >&2
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
  # ECS can report desired=running=1 while the final scale-in task is still
  # STOPPING and its stoppedAt field is null. Wait for complete task records
  # so the checks below prove every observed shutdown instead of racing ECS.
  local stopped_tasks_deadline=$((SECONDS + 180))
  while true; do
    collect_observed_stopped_tasks "$ecs_dir/stopped-tasks.json"
    if jq -e '
      (.failures | length) == 0
      and all(.tasks[]; .stoppedAt != null)
    ' "$ecs_dir/stopped-tasks.json" >/dev/null; then
      break
    fi
    if (( SECONDS >= stopped_tasks_deadline )); then
      echo "ECS did not publish complete records for every observed task stop within three minutes" >&2
      return 1
    fi
    sleep 2
  done

  jq -e --argjson started "$evidence_started_epoch" '
    def epoch:
      sub("\\.[0-9]+\\+00:00$"; "Z")
      | sub("\\.[0-9]+Z$"; "Z")
      | sub("\\+00:00$"; "Z")
      | fromdateiso8601;
    (.failures | length) == 0
    and ([.tasks[] |
      select(.stoppedAt == null or (.stoppedAt | epoch) >= $started)
      | select(
          .stopCode != "ServiceSchedulerInitiated"
          or ((.stoppedReason // "") | test("unhealthy|out.of.memory|failed"; "i"))
        )
    ] | length == 0)
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
      echo "ECS recorded an unexpected task stop or scheduler failure during the measured run" >&2
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
            and .stoppedAt != null
            and (.stoppedAt | epoch) >= $started
          )
        | {
            task_id: (.taskArn | split("/")[-1]),
            stopping_at: .stoppingAt,
            execution_stopped_at: .executionStoppedAt,
            application_shutdown_ms:
              (if .stoppingAt == null or .executionStoppedAt == null
               then null
               else (((.executionStoppedAt | epoch) - (.stoppingAt | epoch)) * 1000)
               end)
          }]
    ' "$ecs_dir/stopped-tasks.json" >"$ecs_dir/task-shutdown-durations.json"
  jq -e '
    length >= 9
    and all(.[];
      .stopping_at != null
      and .execution_stopped_at != null
      and .application_shutdown_ms >= 0
      and .application_shutdown_ms <= 45000)
  ' "$ecs_dir/task-shutdown-durations.json" >/dev/null || {
    echo "ECS did not prove every measured application shutdown completed within 45 seconds" >&2
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
  cloudwatch_metric "$cloudwatch_dir/regional-collection-failures.json" \
    Snaketron/Resilience RegionalCollectionFailures Sum \
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
    AWS/ECS CPUUtilization Average \
    Name=ClusterName,Value="$cluster_name" \
    Name=ServiceName,Value="$service_name"
  cloudwatch_metric "$cloudwatch_dir/ecs-memory.json" \
    AWS/ECS MemoryUtilization Average \
    Name=ClusterName,Value="$cluster_name" \
    Name=ServiceName,Value="$service_name"

  cloudwatch_metric "$cloudwatch_dir/valkey-ecpu.json" \
    AWS/ElastiCache ElastiCacheProcessingUnits Maximum \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-data-storage.json" \
    AWS/ElastiCache BytesUsedForCache Maximum \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-connections.json" \
    AWS/ElastiCache CurrConnections Maximum \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-read-latency.json" \
    AWS/ElastiCache SuccessfulReadRequestLatency Average \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-write-latency.json" \
    AWS/ElastiCache SuccessfulWriteRequestLatency Average \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-evictions.json" \
    AWS/ElastiCache Evictions Sum \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-throttled-commands.json" \
    AWS/ElastiCache ThrottledCmds Sum \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-network-in.json" \
    AWS/ElastiCache NetworkBytesIn Sum \
    Name=clusterId,Value="$staging_valkey_name"
  cloudwatch_metric "$cloudwatch_dir/valkey-network-out.json" \
    AWS/ElastiCache NetworkBytesOut Sum \
    Name=clusterId,Value="$staging_valkey_name"

  cloudwatch_metric "$cloudwatch_dir/traefik-cpu.json" \
    AWS/EC2 CPUUtilization Maximum \
    Name=InstanceId,Value="$SNAKETRON_TRAEFIK_INSTANCE_ID"
  cloudwatch_metric "$cloudwatch_dir/traefik-network-in.json" \
    AWS/EC2 NetworkIn Sum \
    Name=InstanceId,Value="$SNAKETRON_TRAEFIK_INSTANCE_ID"
  cloudwatch_metric "$cloudwatch_dir/traefik-network-out.json" \
    AWS/EC2 NetworkOut Sum \
    Name=InstanceId,Value="$SNAKETRON_TRAEFIK_INSTANCE_ID"

  # ActiveWebSockets is emitted by each task into one environment-level
  # metric stream, so Maximum is a per-task peak rather than a fleet sum.
  # This run-wide series only corroborates a 128-socket per-task peak. Gate A's
  # phase-scoped report and region samples prove its exact cohort; the separate
  # reports below prove the Gate B and Gate C fleet envelopes.
  jq -e 'all(.Datapoints[]; .Minimum > 0)' \
    "$cloudwatch_dir/ready-tasks.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/regional-collection-failures.json" >/dev/null \
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
    && jq -e '([.Datapoints[].Maximum] | max) >= 128' \
      "$cloudwatch_dir/active-websockets.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/valkey-evictions.json" >/dev/null \
    && jq -e '([.Datapoints[].Sum] | add) == 0' \
      "$cloudwatch_dir/valkey-throttled-commands.json" >/dev/null || {
      echo "CloudWatch acceptance failed: readiness, recovery, ownership, checkpoint, drain, socket-envelope, Serverless Valkey eviction, or throttling evidence is outside bounds" >&2
      return 2
  }
}

collect_cloudwatch_evidence_with_retry() {
  local report_dir="$1"
  local attempt
  local status
  for attempt in $(seq 1 8); do
    # Run in an errexit-enabled subshell. Calling a function directly as an
    # `if` condition disables Bash errexit inside that function and could let a
    # missing metric fall through to the correctness gate.
    set +e
    ( set -e; collect_cloudwatch_evidence "$report_dir" )
    status=$?
    set -e
    if (( status == 0 )); then
      return 0
    fi
    # Complete buckets with a failed correctness gate will not improve with
    # ingestion time. Only missing/partial evidence receives a bounded retry.
    if (( status == 2 || attempt == 8 )); then
      return "$status"
    fi
    echo "CloudWatch evidence is not complete yet; retrying in 60 seconds ($attempt/8)" >&2
    sleep 60
  done
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
  local phase_dir="$report_dir/ecs-$phase"
  local tasks_path="$report_dir/ecs-$phase/tasks.json"
  local exec_ready_path="$phase_dir/exec-ready-tasks.json"
  local candidate="$exec_ready_path.pending"

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
    ' "$report_dir/identity/task-definition.json" >/dev/null || {
      echo "Hard-crash certification requires ECS Exec and tini" >&2
      return 1
    }

  local expected_task_arns
  expected_task_arns="$(jq -c '[.tasks[].taskArn] | sort' "$tasks_path")"
  local task_arns
  task_arns="$(jq -r '.tasks[].taskArn' "$tasks_path" | tr '\n' ' ')"
  [[ -n "$task_arns" ]] || {
    echo "Hard-crash certification has no verified ECS tasks" >&2
    return 1
  }

  # A task can be application-healthy before ECS starts its managed exec
  # agent. Poll the exact already-verified task cohort so test setup does not
  # mistake that short initialization window for a product failure.
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if aws ecs describe-tasks \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --tasks $task_arns >"$candidate" \
      && jq -e \
        --argjson expected_task_arns "$expected_task_arns" \
        --arg task_definition "$staging_task_definition_arn" \
        --arg image "$staging_image_uri" \
        --arg digest "$staging_image_digest" '
          (.failures | length) == 0
          and ([.tasks[].taskArn] | sort) == $expected_task_arns
          and all(.tasks[];
            .desiredStatus == "RUNNING"
            and .lastStatus == "RUNNING"
            and .healthStatus == "HEALTHY"
            and .taskDefinitionArn == $task_definition
            and .enableExecuteCommand == true
            and any(.containers[];
              .name == "snaketron-server"
              and .image == $image
              and .imageDigest == $digest
              and .lastStatus == "RUNNING"
              and .healthStatus == "HEALTHY"
              and any(.managedAgents[]?;
                .name == "ExecuteCommandAgent"
                and .lastStatus == "RUNNING")))
        ' "$candidate" >/dev/null; then
      mv "$candidate" "$exec_ready_path"
      return 0
    fi
    sleep 2
  done

  [[ -f "$candidate" ]] \
    && mv "$candidate" "$phase_dir/exec-ready-tasks-final.json"
  echo "Hard-crash certification requires a RUNNING execute-command agent on every verified healthy task" >&2
  return 1
}

capture_control_status() {
  local output="$1"
  if [[ $# -eq 2 ]]; then
    SNAKETRON_REDIS_URL="$staging_redis_control_url" \
      timeout --signal=TERM --kill-after=1s 3s \
        "$resilience_admin" status \
        --region-key "$SNAKETRON_REGION_CODE" \
        --partition "$2" >"$output"
    return
  fi
  SNAKETRON_REDIS_URL="$staging_redis_control_url" \
    "$resilience_admin" status \
    --region-key "$SNAKETRON_REGION_CODE" >"$output"
}

capture_ownership_status() {
  local output="$1"
  local partition="$2"
  local killed_boot_id="$3"
  SNAKETRON_REDIS_URL="$staging_redis_control_url" \
    timeout --signal=TERM --kill-after=1s 3s \
      "$resilience_admin" ownership \
      --region-key "$SNAKETRON_REGION_CODE" \
      --partition "$partition" \
      --killed-boot-id "$killed_boot_id" >"$output"
}

capture_pending_status() {
  local output="$1"
  local partition="$2"
  local consumer="$3"
  SNAKETRON_REDIS_URL="$staging_redis_control_url" \
    timeout --signal=TERM --kill-after=1s 3s \
      "$resilience_admin" pending \
      --region-key "$SNAKETRON_REGION_CODE" \
      --partition "$partition" \
      --consumer "$consumer" >"$output"
}

capture_authoritative_output_status() {
  local output="$1"
  local partition="$2"
  local after_stream_id="$3"
  SNAKETRON_REDIS_URL="$staging_redis_control_url" \
    timeout --signal=TERM --kill-after=1s 3s \
      "$resilience_admin" output \
      --region-key "$SNAKETRON_REGION_CODE" \
      --partition "$partition" \
      --after-stream-id "$after_stream_id" >"$output"
}

stop_hard_crash_control_observer() {
  if [[ -n "$hard_crash_control_observer_stop_file" ]]; then
    if ! touch "$hard_crash_control_observer_stop_file" 2>/dev/null \
      && [[ -n "$hard_crash_control_observer_pid" ]]; then
      kill -TERM "$hard_crash_control_observer_pid" 2>/dev/null || true
    fi
  fi
  if [[ -n "$hard_crash_control_observer_pid" ]]; then
    wait "$hard_crash_control_observer_pid" 2>/dev/null || true
  fi
  if [[ -n "$hard_crash_control_observer_stop_file" ]]; then
    rm -f "$hard_crash_control_observer_stop_file"
  fi
  if [[ -n "$hard_crash_ownership_observer_stop_file" ]]; then
    if ! touch "$hard_crash_ownership_observer_stop_file" 2>/dev/null \
      && [[ -n "$hard_crash_ownership_observer_pid" ]]; then
      kill -TERM "$hard_crash_ownership_observer_pid" 2>/dev/null || true
    fi
  fi
  if [[ -n "$hard_crash_ownership_observer_pid" ]]; then
    wait "$hard_crash_ownership_observer_pid" 2>/dev/null || true
  fi
  if [[ -n "$hard_crash_ownership_observer_stop_file" ]]; then
    rm -f "$hard_crash_ownership_observer_stop_file"
  fi
  hard_crash_control_observer_pid=""
  hard_crash_control_observer_stop_file=""
  hard_crash_ownership_observer_pid=""
  hard_crash_ownership_observer_stop_file=""
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
    --arg task_boot_id "$killed_task_boot_id" '
      {
        selected_partition: $selected_partition,
        selected_member: $selected_member,
        task_arn: $task_arn,
        task_boot_id: $task_boot_id
      }
    ' >"$report_dir/hard-crash-manifest.json"

  # The repeating two-second proof is one partition-local Lua snapshot:
  # filtered XPENDING followed by Redis TIME. The rich status above remains
  # sufficient for partition selection and the final executor-drain proof.
  local control_observation_dir="$report_dir/hard-crash-control-samples"
  mkdir -p "$control_observation_dir"
  hard_crash_control_observer_stop_file="$control_observation_dir/stop-requested"
  rm -f "$hard_crash_control_observer_stop_file"
  local control_observer_stop_file="$hard_crash_control_observer_stop_file"
  (
    local observer_stop_requested=false
    trap 'observer_stop_requested=true' TERM INT
    local observation_sequence=0
    while [[ "$observer_stop_requested" != true \
      && ! -e "$control_observer_stop_file" ]]; do
      observation_sequence=$((observation_sequence + 1))
      local observation_started_at_ms
      local observation_completed_at_ms
      local observation_raw
      local observation_pending
      local observation_sample
      printf -v observation_raw \
        '%s/%06d.raw.pending' "$control_observation_dir" "$observation_sequence"
      printf -v observation_pending \
        '%s/%06d.json.pending' "$control_observation_dir" "$observation_sequence"
      printf -v observation_sample \
        '%s/%06d.json' "$control_observation_dir" "$observation_sequence"
      observation_started_at_ms="$(unix_time_ms)"
      if capture_pending_status \
        "$observation_raw" \
        "$killed_partition" \
        "$killed_lease_token" 2>/dev/null; then
        observation_completed_at_ms="$(unix_time_ms)"
        if jq -e \
          --argjson partition "$killed_partition" \
          --arg killed_lease_token "$killed_lease_token" \
          --argjson observation_started_at_ms "$observation_started_at_ms" \
          --argjson observation_completed_at_ms "$observation_completed_at_ms" '
            . + {
              observation_started_at_ms: $observation_started_at_ms,
              observation_completed_at_ms: $observation_completed_at_ms
            }
            | select(
                .partition == $partition
                and .requested_consumer == $killed_lease_token
                and .pending_entry != null
                and (.pending_entry.id | type == "string" and length > 0)
                and .pending_entry.consumer == $killed_lease_token
                and .observation_started_at_ms <= .captured_at_ms
                and .captured_at_ms <= .observation_completed_at_ms)
          ' "$observation_raw" >"$observation_pending"; then
          mv "$observation_pending" "$observation_sample"
        fi
      fi
      rm -f "$observation_raw" "$observation_pending"
      sleep 0.05
    done
  ) &
  hard_crash_control_observer_pid=$!

  # Ownership has a separate lightweight observer. Its command sandwiches one
  # atomic membership read between two atomic partition-local assignment/lease
  # reads, avoiding PEL enumeration on the five-second takeover path. Once it
  # sees a possible successor, it also captures the first authoritative output
  # after that exact snapshot's event-stream tail. Saving the pair immediately
  # prevents the bounded event stream from trimming recovery evidence while ECS
  # exposes the exact container-stop timestamp.
  local ownership_observation_dir="$report_dir/hard-crash-ownership-samples"
  local takeover_output_observation_dir="$report_dir/hard-crash-takeover-output-samples"
  mkdir -p "$ownership_observation_dir"
  mkdir -p "$takeover_output_observation_dir"
  hard_crash_ownership_observer_stop_file="$ownership_observation_dir/stop-requested"
  rm -f "$hard_crash_ownership_observer_stop_file"
  local ownership_observer_stop_file="$hard_crash_ownership_observer_stop_file"
  (
    local observer_stop_requested=false
    trap 'observer_stop_requested=true' TERM INT
    local observation_sequence=0
    local captured_ambiguous_authority_identity=""
    local captured_definitive_authority_identity=""
    while [[ "$observer_stop_requested" != true \
      && ! -e "$ownership_observer_stop_file" ]]; do
      observation_sequence=$((observation_sequence + 1))
      local observation_started_at_ms
      local observation_completed_at_ms
      local observation_raw
      local observation_pending
      local observation_sample
      local candidate_assignment_computed_at_ms
      local candidate_authority_identity
      local output_raw
      local output_pending
      local output_sample
      printf -v observation_raw \
        '%s/%06d.raw.pending' "$ownership_observation_dir" "$observation_sequence"
      printf -v observation_pending \
        '%s/%06d.json.pending' "$ownership_observation_dir" "$observation_sequence"
      printf -v observation_sample \
        '%s/%06d.json' "$ownership_observation_dir" "$observation_sequence"
      printf -v output_raw \
        '%s/%06d.raw.pending' "$takeover_output_observation_dir" "$observation_sequence"
      printf -v output_pending \
        '%s/%06d.json.pending' "$takeover_output_observation_dir" "$observation_sequence"
      printf -v output_sample \
        '%s/%06d.json' "$takeover_output_observation_dir" "$observation_sequence"
      observation_started_at_ms="$(unix_time_ms)"
      if capture_ownership_status \
        "$observation_raw" \
        "$killed_partition" \
        "$killed_boot_id" 2>/dev/null; then
        observation_completed_at_ms="$(unix_time_ms)"
        if jq -e \
          --argjson partition "$killed_partition" \
          --argjson observation_started_at_ms "$observation_started_at_ms" \
          --argjson observation_completed_at_ms "$observation_completed_at_ms" '
            {
              observation_started_at_ms: $observation_started_at_ms,
              observation_completed_at_ms: $observation_completed_at_ms,
              captured_at_ms,
              membership_observed_at_ms,
              authority_observed_at_ms,
              authority_event_tail_id,
              authority_stable,
              killed_member_live,
              assignment,
              live_members: (
                if .owner_member == null then [] else [.owner_member] end),
              runtime_partitions: [
                .runtime_partition
                | select(.partition == $partition)
              ]
            }
            | select(
                (.runtime_partitions | length) == 1
                and .observation_started_at_ms <= .captured_at_ms
                and .captured_at_ms <= .observation_completed_at_ms
                and .observation_started_at_ms
                  <= .membership_observed_at_ms
                and .membership_observed_at_ms
                  <= .observation_completed_at_ms
                and .observation_started_at_ms
                  <= .authority_observed_at_ms
                and .authority_observed_at_ms
                  <= .observation_completed_at_ms
                and (.authority_event_tail_id
                  | type == "string"
                  and test("^[0-9]+-[0-9]+$")))
          ' "$observation_raw" >"$observation_pending"; then
          mv "$observation_pending" "$observation_sample"
          if select_hard_crash_owner_candidate_ownership_sample \
              "$pre" \
              "$killed_partition" \
              "$killed_boot_id" \
              "$killed_task_id" \
              "$killed_lease_token" \
              "$observation_sample" >/dev/null 2>&1 \
            && candidate_authority_identity="$(
              jq -er --argjson partition "$killed_partition" '
                (.runtime_partitions[]
                  | select(.partition == $partition)) as $runtime
                | [
                    .assignment.version,
                    $runtime.lease_token
                  ]
                | @json
              ' "$observation_sample"
            )" \
            && candidate_assignment_computed_at_ms="$(
              jq -er '
                .assignment.computed_at_ms
                | select(type == "number")
              ' "$observation_sample"
            )" \
            && [[ "$candidate_authority_identity" \
              != "$captured_definitive_authority_identity" ]] \
            && [[ "$observation_started_at_ms" \
                -ge "$candidate_assignment_computed_at_ms" \
              || "$candidate_authority_identity" \
                != "$captured_ambiguous_authority_identity" ]]; then
            local authority_event_tail_id
            local output_observation_started_at_ms
            local output_observation_completed_at_ms
            authority_event_tail_id="$(
              jq -er '.authority_event_tail_id' "$observation_sample"
            )"
            output_observation_started_at_ms="$(unix_time_ms)"
            if capture_authoritative_output_status \
              "$output_raw" \
              "$killed_partition" \
              "$authority_event_tail_id" 2>/dev/null; then
              output_observation_completed_at_ms="$(unix_time_ms)"
              if jq -e \
                --slurpfile ownership "$observation_sample" \
                --argjson partition "$killed_partition" \
                --arg authority_event_tail_id "$authority_event_tail_id" \
                --argjson observation_started_at_ms \
                  "$output_observation_started_at_ms" \
                --argjson observation_completed_at_ms \
                  "$output_observation_completed_at_ms" '
                  . + {
                    observation_started_at_ms: $observation_started_at_ms,
                    observation_completed_at_ms:
                      $observation_completed_at_ms
                  }
                  | select(
                      .partition == $partition
                      and .after_stream_id == $authority_event_tail_id
                      and .first_scheduled_output != null
                      and .observation_started_at_ms <= .captured_at_ms
                      and .captured_at_ms <= .observation_completed_at_ms)
                  | {
                      ownership: $ownership[0],
                      output: .
                    }
                ' "$output_raw" >"$output_pending"; then
                mv "$output_pending" "$output_sample"
                if (( observation_started_at_ms \
                  >= candidate_assignment_computed_at_ms )); then
                  captured_definitive_authority_identity="$candidate_authority_identity"
                else
                  captured_ambiguous_authority_identity="$candidate_authority_identity"
                fi
              fi
            fi
          fi
        fi
      fi
      rm -f \
        "$observation_raw" \
        "$observation_pending" \
        "$output_raw" \
        "$output_pending"
      sleep 0.05
    done
  ) &
  hard_crash_ownership_observer_pid=$!

  # Do not start the external fault until both observers have published valid
  # old-consumer and old-owner/token samples. This closes the
  # fork-before-first-sample race for both independently selected proofs.
  local observer_ready="$report_dir/control-plane-observer-ready-before-exec.json"
  local observer_ready_candidate="$observer_ready.pending"
  local observer_ready_deadline=$((SECONDS + 10))
  local -a observer_startup_samples=()
  while (( SECONDS < observer_ready_deadline )); do
    observer_startup_samples=( "$control_observation_dir"/*.json )
    if [[ -e "${observer_startup_samples[0]}" ]]; then
      if select_pre_fault_pending_sample \
        "$killed_partition" \
        "$killed_lease_token" \
        "${observer_startup_samples[@]}" >"$observer_ready_candidate" \
        2>/dev/null \
        && jq -e 'type == "object"' "$observer_ready_candidate" >/dev/null; then
        mv "$observer_ready_candidate" "$observer_ready"
        break
      fi
      rm -f "$observer_ready_candidate"
    fi
    sleep 0.1
  done
  if [[ ! -f "$observer_ready" ]]; then
    stop_hard_crash_control_observer
    rm -f "$observer_ready_candidate"
    echo "The hard-crash observer did not establish a valid pre-fault sample" >&2
    return 1
  fi
  local ownership_observer_ready="$report_dir/ownership-observer-ready-before-exec.json"
  local ownership_observer_ready_candidate="$ownership_observer_ready.pending"
  local ownership_observer_ready_deadline=$((SECONDS + 10))
  local -a ownership_observer_startup_samples=()
  while (( SECONDS < ownership_observer_ready_deadline )); do
    ownership_observer_startup_samples=( "$ownership_observation_dir"/*.json )
    if [[ -e "${ownership_observer_startup_samples[0]}" ]]; then
      if select_pre_fault_ownership_sample \
        "$killed_partition" \
        "$killed_boot_id" \
        "$killed_lease_token" \
        "${ownership_observer_startup_samples[@]}" \
        >"$ownership_observer_ready_candidate" 2>/dev/null \
        && jq -e 'type == "object"' \
          "$ownership_observer_ready_candidate" >/dev/null; then
        mv "$ownership_observer_ready_candidate" "$ownership_observer_ready"
        break
      fi
      rm -f "$ownership_observer_ready_candidate"
    fi
    sleep 0.1
  done
  if [[ ! -f "$ownership_observer_ready" ]]; then
    stop_hard_crash_control_observer
    rm -f "$ownership_observer_ready_candidate"
    echo "The ownership observer did not establish a valid pre-fault sample" >&2
    return 1
  fi

  # One non-retried ECS Exec session discovers exactly one non-PID-1 `server`
  # child and sends that process SIGKILL directly. There is no SIGSTOP,
  # in-container marker, delay, graceful cleanup, or second fault action.
  local hard_kill_command
  hard_kill_command='/bin/sh -c '\''set -eu; count=0; server_pid=; for comm_file in /proc/[0-9]*/comm; do IFS= read -r comm < "$comm_file" || continue; [ "$comm" = server ] || continue; server_pid=${comm_file#/proc/}; server_pid=${server_pid%/comm}; count=$((count + 1)); done; [ "$count" -eq 1 ]; [ "$server_pid" -ne 1 ]; kill -KILL "$server_pid"'\'''
  local exec_output="$report_dir/hard-crash-ecs-exec.log"
  local ecs_exec_invoked_at_ms
  local ecs_exec_max_attempts=1
  ecs_exec_invoked_at_ms="$(unix_time_ms)"
  AWS_MAX_ATTEMPTS="$ecs_exec_max_attempts" \
    timeout --signal=TERM --kill-after=2s 40s aws ecs execute-command \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --task "$killed_task_arn" \
    --container snaketron-server \
    --interactive \
    --command "$hard_kill_command" >"$exec_output" 2>&1 &
  hard_crash_ecs_exec_pid=$!
  local ecs_exec_pid="$hard_crash_ecs_exec_pid"

  # Poll only the selected task ARN. executionStoppedAt is the ECS control
  # plane's exact container-exit time; stoppedAt can lag it by many seconds and
  # is therefore unsuitable as a recovery origin.
  local task_stop="$report_dir/hard-crash-task-stop.json"
  local task_stop_candidate="$task_stop.pending"
  local task_stop_deadline=$((SECONDS + 45))
  local task_stop_observed_at_ms=""
  while (( SECONDS < task_stop_deadline )); do
    if aws ecs describe-tasks \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --tasks "$killed_task_arn" >"$task_stop_candidate" 2>/dev/null \
      && jq -e --arg task_arn "$killed_task_arn" '
        (.failures | length) == 0
        and (.tasks | length) == 1
        and .tasks[0].taskArn == $task_arn
        and (.tasks[0].executionStoppedAt | type) == "string"
      ' "$task_stop_candidate" >/dev/null; then
      mv "$task_stop_candidate" "$task_stop"
      task_stop_observed_at_ms="$(unix_time_ms)"
      break
    fi
    sleep 1
  done

  local execution_stopped_at=""
  local execution_stopped_at_ms=""
  local recovery_timing_origin_ms=""
  local failure_reason=""
  if [[ ! -f "$task_stop" ]]; then
    failure_reason="The selected ECS task never exposed an authoritative executionStoppedAt after the single SIGKILL attempt"
  else
    execution_stopped_at="$(jq -er '.tasks[0].executionStoppedAt' "$task_stop")"
    if ! execution_stopped_at_ms="$(ecs_timestamp_to_unix_ms "$execution_stopped_at")" \
      || [[ ! "$execution_stopped_at_ms" =~ ^[0-9]{13}$ ]]; then
      failure_reason="ECS returned an executionStoppedAt value that could not be parsed at millisecond precision"
    else
      recovery_timing_origin_ms=$(( (execution_stopped_at_ms / 1000) * 1000 ))
      if (( execution_stopped_at_ms < ecs_exec_invoked_at_ms \
        || execution_stopped_at_ms > task_stop_observed_at_ms )); then
        failure_reason="ECS executionStoppedAt did not fall between the ECS Exec invocation and its control-plane observation"
      fi
    fi
  fi

  # Select the PEL proof from the atomic partition-local stream and ownership
  # from the lightweight ownership stream. The PEL proof requires an exact
  # old-consumer command ID no later than two seconds after the stop. Ownership
  # must advance to a new fenced token on a member that was already present
  # before the crash. The observer immediately paired that same-slot ownership
  # snapshot's event-stream tail with a later CommandScheduledV2; after ECS
  # exposes its timestamp, both halves of one saved pair must fit the five-second
  # bound.
  local pending_after_kill="$report_dir/control-plane-immediate-post-kill.json"
  local pending_candidate="$pending_after_kill.pending"
  local owner_ready="$report_dir/control-plane-hard-crash-owner-ready.json"
  local owner_raw_candidate="$owner_ready.raw.pending"
  local owner_candidate="$owner_ready.pending"
  local authoritative_output="$report_dir/control-plane-hard-crash-output.json"
  local authoritative_output_raw="$authoritative_output.raw.pending"
  local authoritative_output_candidate="$authoritative_output.pending"
  if [[ -z "$failure_reason" ]]; then
    local proof_deadline=$((SECONDS + 10))
    local -a control_samples=()
    local -a takeover_output_samples=()
    while (( SECONDS < proof_deadline )); do
      control_samples=( "$control_observation_dir"/*.json )
      if [[ -e "${control_samples[0]}" ]]; then
        if [[ ! -f "$pending_after_kill" ]]; then
          if select_post_kill_pending_sample \
            "$execution_stopped_at_ms" \
            "$killed_partition" \
            "$killed_lease_token" \
            "${control_samples[@]}" >"$pending_candidate" 2>/dev/null \
            && jq -e 'type == "object"' "$pending_candidate" >/dev/null; then
            mv "$pending_candidate" "$pending_after_kill"
          else
            rm -f "$pending_candidate"
          fi
        fi
      fi
      takeover_output_samples=( "$takeover_output_observation_dir"/*.json )
      if [[ -e "${takeover_output_samples[0]}" \
        && ! -f "$authoritative_output" ]]; then
        local takeover_output_sample
        for takeover_output_sample in "${takeover_output_samples[@]}"; do
          if jq -e '.ownership' "$takeover_output_sample" \
            >"$owner_raw_candidate" \
            && select_hard_crash_owner_ready_ownership_sample \
              "$pre" \
              "$execution_stopped_at_ms" \
              "$killed_partition" \
              "$killed_boot_id" \
              "$killed_task_id" \
              "$killed_lease_token" \
              "$owner_raw_candidate" >"$owner_candidate" 2>/dev/null \
            && jq -e '.output' "$takeover_output_sample" \
              >"$authoritative_output_raw"; then
            local authority_event_tail_id
            authority_event_tail_id="$(
              jq -er '.authority_event_tail_id' "$owner_candidate"
            )"
            if select_hard_crash_authoritative_output_sample \
              "$execution_stopped_at_ms" \
              "$killed_partition" \
              "$authority_event_tail_id" \
              "$authoritative_output_raw" \
              >"$authoritative_output_candidate" 2>/dev/null; then
              mv "$owner_candidate" "$owner_ready"
              mv "$authoritative_output_candidate" "$authoritative_output"
              break
            fi
          fi
          rm -f \
            "$owner_raw_candidate" \
            "$owner_candidate" \
            "$authoritative_output_raw" \
            "$authoritative_output_candidate"
        done
      fi
      if [[ -f "$pending_after_kill" \
        && -f "$owner_ready" \
        && -f "$authoritative_output" ]]; then
        break
      fi
      sleep 0.1
    done
  fi

  stop_hard_crash_control_observer
  set +e
  wait "$ecs_exec_pid"
  local ecs_exec_exit_code=$?
  set -e
  hard_crash_ecs_exec_pid=""
  local ecs_exec_session_started=false
  if grep -Eq \
    'Starting session with SessionId: ecs-execute-command-' "$exec_output"; then
    ecs_exec_session_started=true
  fi

  rm -f \
    "$task_stop_candidate" \
    "$pending_candidate" \
    "$owner_raw_candidate" \
    "$owner_candidate" \
    "$authoritative_output_raw" \
    "$authoritative_output_candidate" \
    "$ownership_observer_ready_candidate"
  if [[ -z "$failure_reason" && "$ecs_exec_session_started" != true ]]; then
    failure_reason="ECS Exec did not establish the single non-retried session before the selected task stopped"
  fi
  if [[ -z "$failure_reason" && ! -f "$pending_after_kill" ]]; then
    failure_reason="No exact pending command remained under the killed lease within two seconds after ECS executionStoppedAt"
  fi
  if [[ -z "$failure_reason" && ! -f "$owner_ready" ]]; then
    failure_reason="Killed membership and fenced partition ownership did not fail over to a pre-existing survivor within five seconds"
  fi
  if [[ -z "$failure_reason" && ! -f "$authoritative_output" ]]; then
    failure_reason="The fenced successor did not produce authoritative scheduled output within five seconds"
  fi
  if [[ -n "$failure_reason" ]]; then
    echo "$failure_reason" >&2
    return 1
  fi

  local manifest_pending="$report_dir/hard-crash-manifest.pending.json"
  jq \
    --arg execution_stopped_at "$execution_stopped_at" \
    --arg killed_lease_token "$killed_lease_token" \
    --argjson execution_stopped_at_unix_ms "$execution_stopped_at_ms" \
    --argjson recovery_timing_origin_unix_ms "$recovery_timing_origin_ms" \
    --argjson task_stop_observed_at_unix_ms "$task_stop_observed_at_ms" \
    --argjson ecs_exec_invoked_at_unix_ms "$ecs_exec_invoked_at_ms" \
    --argjson ecs_exec_max_attempts "$ecs_exec_max_attempts" \
    --argjson ecs_exec_exit_code "$ecs_exec_exit_code" \
    --argjson ecs_exec_session_started "$ecs_exec_session_started" \
    --argjson partition "$killed_partition" \
    --slurpfile observer_ready "$observer_ready" \
    --slurpfile ownership_observer_ready "$ownership_observer_ready" \
    --slurpfile observed "$pending_after_kill" \
    --slurpfile ready "$owner_ready" \
    --slurpfile output "$authoritative_output" '
      . + {
        execution_stopped_at: $execution_stopped_at,
        execution_stopped_at_unix_ms: $execution_stopped_at_unix_ms,
        recovery_timing_origin_unix_ms: $recovery_timing_origin_unix_ms,
        task_stop_observed_at_unix_ms: $task_stop_observed_at_unix_ms,
        ecs_exec_invoked_at_unix_ms: $ecs_exec_invoked_at_unix_ms,
        ecs_exec_max_attempts: $ecs_exec_max_attempts,
        ecs_exec_exit_code: $ecs_exec_exit_code,
        ecs_exec_session_started: $ecs_exec_session_started,
        observer_ready_before_exec_at_unix_ms:
          $observer_ready[0].observation_completed_at_ms,
        ownership_observer_ready_before_exec_at_unix_ms:
          $ownership_observer_ready[0].observation_completed_at_ms,
        pending_after_kill: {
          captured_at_unix_ms: $observed[0].captured_at_ms,
          observation_started_at_unix_ms:
            $observed[0].observation_started_at_ms,
          observation_completed_at_unix_ms:
            $observed[0].observation_completed_at_ms,
          partition: $partition,
          killed_lease_token: $killed_lease_token,
          entries: [$observed[0].pending_entry]
        },
        owner_observation_started_at_unix_ms:
          $ready[0].observation_started_at_ms,
        owner_sample_captured_at_unix_ms: $ready[0].captured_at_ms,
        owner_membership_observed_at_unix_ms:
          $ready[0].membership_observed_at_ms,
        owner_authority_observed_at_unix_ms:
          $ready[0].authority_observed_at_ms,
        owner_authority_event_tail_id:
          $ready[0].authority_event_tail_id,
        owner_ready_at_unix_ms: $ready[0].observation_completed_at_ms,
        assignment_version_after: $ready[0].assignment.version,
        authoritative_output: {
          observation_started_at_unix_ms:
            $output[0].observation_started_at_ms,
          captured_at_unix_ms: $output[0].captured_at_ms,
          observation_completed_at_unix_ms:
            $output[0].observation_completed_at_ms,
          partition: $output[0].partition,
          after_stream_id: $output[0].after_stream_id,
          first_scheduled_output: $output[0].first_scheduled_output
        }
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
  collect_observed_stopped_tasks "$ecs_dir/stopped-tasks.json"
  select_unexpected_crash_stops \
    "$evidence_started_epoch" \
    "$(jq -r '.task_arn' "$report_dir/hard-crash-manifest.json")" \
    "$report_dir/ecs-runtime-baseline-before-evidence.json" \
    "$ecs_dir/stopped-tasks.json" \
    >"$ecs_dir/unexpected-stopped-tasks.json"
  jq -e \
    --arg task_definition "$staging_task_definition_arn" \
    --slurpfile manifest "$report_dir/hard-crash-manifest.json" \
    --slurpfile baseline \
      "$report_dir/ecs-runtime-baseline-before-evidence.json" \
    --slurpfile injected_stop "$report_dir/hard-crash-task-stop.json" \
    --slurpfile unexpected "$ecs_dir/unexpected-stopped-tasks.json" '
      [.tasks[]
          | select(.taskArn == $manifest[0].task_arn)
          | select(
              .executionStoppedAt
                == $manifest[0].execution_stopped_at)] as $expected
      | (.failures | length) == 0
        and ($baseline[0].failures | length) == 0
        and ($expected | length) == 1
        and ($injected_stop[0].failures | length) == 0
        and ($injected_stop[0].tasks | length) == 1
        and $injected_stop[0].tasks[0].taskArn == $manifest[0].task_arn
        and $injected_stop[0].tasks[0].executionStoppedAt
          == $manifest[0].execution_stopped_at
        and $manifest[0].ecs_exec_max_attempts == 1
        and $manifest[0].ecs_exec_session_started == true
        and $manifest[0].observer_ready_before_exec_at_unix_ms
          <= $manifest[0].ecs_exec_invoked_at_unix_ms
        and $manifest[0].ownership_observer_ready_before_exec_at_unix_ms
          <= $manifest[0].ecs_exec_invoked_at_unix_ms
        and $manifest[0].ecs_exec_invoked_at_unix_ms
          <= $manifest[0].execution_stopped_at_unix_ms
        and $manifest[0].execution_stopped_at_unix_ms
          <= $manifest[0].task_stop_observed_at_unix_ms
        and $expected[0].taskDefinitionArn == $task_definition
        and $expected[0].stopCode == "EssentialContainerExited"
        and (($expected[0].stoppedReason // "")
          | test("out.?of.?memory|oom|unhealthy|failed"; "i") | not)
        and all($expected[0].containers[];
          ((.reason // "")
            | test("out.?of.?memory|oom|unhealthy|failed"; "i") | not))
        and ([ $expected[0].containers[]
          | select(
              .name == "snaketron-server"
              and .exitCode == 137)] | length) == 1
        and ($unexpected[0] | length) == 0
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
    --slurpfile pre "$report_dir/control-plane-pre-crash-10.json" \
    --slurpfile observer_ready "$report_dir/control-plane-observer-ready-before-exec.json" \
    --slurpfile ownership_observer_ready "$report_dir/ownership-observer-ready-before-exec.json" \
    --slurpfile pending_after_kill "$report_dir/control-plane-immediate-post-kill.json" \
    --slurpfile owner_ready "$report_dir/control-plane-hard-crash-owner-ready.json" \
    --slurpfile authoritative_output "$report_dir/control-plane-hard-crash-output.json" \
    --slurpfile envelope "$report_dir/envelope-hard-crash/summary.json" \
    --slurpfile final "$report_dir/control-plane-hard-crash-final-10.json" \
    "$hard_crash_envelope_jq"'
      def p99:
        sort as $values
        | if ($values | length) == 0 then null
          else $values[(((((($values | length) * 99) + 99) / 100) | floor) - 1)]
          end;
      def parsed_stream_id:
        capture("^(?<milliseconds>[0-9]+)-(?<sequence>[0-9]+)$")
        | {
            milliseconds: (.milliseconds | tonumber),
            sequence: (.sequence | tonumber)
          };
      $report[0] as $r
      | $manifest[0] as $m
      | $m.execution_stopped_at_unix_ms as $exact_stop
      | $m.recovery_timing_origin_unix_ms as $timing_origin
      | $m.selected_partition.partition as $partition
      | ($pre[0].runtime_partitions[]
          | select(.partition == $partition)) as $old
      | ($owner_ready[0].runtime_partitions[]
          | select(.partition == $partition)) as $new
      | ($ownership_observer_ready[0].runtime_partitions[]
          | select(.partition == $partition)) as $ownership_observer_partition
      | ($m.pending_after_kill.entries | map(.id) | unique | sort) as $pending_ids
      | ([$pending_after_kill[0].pending_entry]) as $observed_pending
      | ($owner_ready[0].authority_event_tail_id | parsed_stream_id)
          as $output_anchor
      | ($authoritative_output[0].first_scheduled_output.stream_id
          | parsed_stream_id) as $output_id
      | (($r.ramp_stages[0].target_reached_at_unix_ms / 1000) | ceil)
          as $stable_first_second
      | ($timing_origin / 1000) as $stable_after_last_second
      | hard_crash_pre_crash_seconds($r; $timing_origin)
          as $pre_crash_seconds
      | longest_qualifying_streak($pre_crash_seconds)
          as $pre_crash_streak
      | ($pre_crash_seconds | last) as $final_pre_crash_second
      | hard_crash_required_online_samples as $required_online_samples
      | ($envelope[0].samples[-$required_online_samples:] // [])
          as $online_envelope_tail
      | [$r.sessions[].hard_recoveries[]?] as $all_recoveries
      | [$all_recoveries[]
          | select(
              .from_task_boot_id == $m.task_boot_id
              and .detected_at_unix_ms >= $timing_origin
              and .detected_at_unix_ms >= $m.ecs_exec_invoked_at_unix_ms
              and .ready_at_unix_ms >= $exact_stop
              and .ready_at_unix_ms >= .detected_at_unix_ms)] as $affected
      | ([$affected[].ready_at_unix_ms - $timing_origin] | p99)
          as $crash_to_ready_p99_upper_bound_ms
      | {
          affected_sessions: ($affected | length),
          ambiguous_commands_after_initial_barrier: (
            [$affected[].pending_commands_after_outcome_barrier] | add // 0),
          pending_commands_at_finish:
            $r.metrics.planned_handoffs.pending_commands_at_finish,
          pending_ids_observed_after_kill: $pending_ids,
          pending_observed_at_unix_ms: $m.pending_after_kill.captured_at_unix_ms,
          pending_observation_completed_at_unix_ms:
            $m.pending_after_kill.observation_completed_at_unix_ms,
          execution_stopped_at_unix_ms: $exact_stop,
          recovery_timing_origin_unix_ms: $timing_origin,
          owner_ready_at_unix_ms:
            $owner_ready[0].observation_completed_at_ms,
          crash_to_ready_p99_upper_bound_ms:
            $crash_to_ready_p99_upper_bound_ms,
          first_authoritative_output_stream_id:
            $authoritative_output[0].first_scheduled_output.stream_id,
          first_authoritative_output_at_unix_ms:
            $authoritative_output[0].first_scheduled_output.stream_unix_ms,
          first_authoritative_output_ms:
            ($authoritative_output[0].first_scheduled_output.stream_unix_ms
              - $exact_stop),
          pre_crash_capacity: {
            evaluated_first_second: $stable_first_second,
            evaluated_after_last_second: $stable_after_last_second,
            longest_qualifying_streak: $pre_crash_streak,
            final_complete_second: $final_pre_crash_second,
            online_envelope: {
              required_stable_seconds:
                $envelope[0].required_stable_seconds,
              required_qualifying_samples:
                $envelope[0].required_qualifying_samples,
              qualifying_tail_samples: ($online_envelope_tail | length),
              first_qualifying_at_unix_ms:
                ($online_envelope_tail | first | .observed_at_unix_ms),
              last_qualifying_at_unix_ms:
                ($online_envelope_tail | last | .observed_at_unix_ms)
            }
          },
          checks: {
            load_contract: (
              $r.schema_version >= 10
              and $r.metadata.threshold_result == "passed"
              and $r.configured_max_concurrency == 272
              and $r.metadata.mode == "duel"
              and $r.metadata.command_profile == "every-tick"
              and $r.metadata.spawn_rate_per_second == "4"
              and $r.session_counts.peak_authenticated_concurrency == 272
              and $r.session_counts.peak_active_game_concurrency >= 136),
            clean_completion: (
              $r.session_counts.failed == 0
              and $r.session_counts.cancelled == 0
              and $r.session_counts.incomplete == 0
              and $r.session_counts.completed == $r.session_counts.total
              and $r.games.pairing_violations == 0
              and all($r.sessions[];
                .outcome == "completed" and .failure_phase == null)),
            single_observed_fault: (
              $m.ecs_exec_max_attempts == 1
              and $m.ecs_exec_session_started == true
              and $m.observer_ready_before_exec_at_unix_ms
                == $observer_ready[0].observation_completed_at_ms
              and $observer_ready[0].observation_started_at_ms
                <= $observer_ready[0].captured_at_ms
              and $observer_ready[0].captured_at_ms
                <= $observer_ready[0].observation_completed_at_ms
              and $observer_ready[0].partition == $partition
              and $observer_ready[0].requested_consumer
                == $m.selected_partition.lease_token
              and $observer_ready[0].pending_entry != null
              and ($observer_ready[0].pending_entry.id
                | type == "string" and length > 0)
              and $observer_ready[0].pending_entry.consumer
                == $m.selected_partition.lease_token
              and $m.ownership_observer_ready_before_exec_at_unix_ms
                == $ownership_observer_ready[0].observation_completed_at_ms
              and $ownership_observer_ready[0].authority_stable
              and $ownership_observer_ready[0].killed_member_live
              and $ownership_observer_ready[0].observation_started_at_ms
                <= $ownership_observer_ready[0].captured_at_ms
              and $ownership_observer_ready[0].captured_at_ms
                <= $ownership_observer_ready[0].observation_completed_at_ms
              and $ownership_observer_ready[0].membership_observed_at_ms
                >= $ownership_observer_ready[0].observation_started_at_ms
              and $ownership_observer_ready[0].membership_observed_at_ms
                <= $ownership_observer_ready[0].observation_completed_at_ms
              and $ownership_observer_ready[0].authority_observed_at_ms
                >= $ownership_observer_ready[0].observation_started_at_ms
              and $ownership_observer_ready[0].authority_observed_at_ms
                <= $ownership_observer_ready[0].observation_completed_at_ms
              and ($ownership_observer_ready[0].authority_event_tail_id
                | type == "string" and test("^[0-9]+-[0-9]+$"))
              and $ownership_observer_partition.owner_matches
              and $ownership_observer_partition.active_owner
                == $m.selected_member.boot_id
              and $ownership_observer_partition.lease_token
                == $m.selected_partition.lease_token
              and $m.observer_ready_before_exec_at_unix_ms
                <= $m.ecs_exec_invoked_at_unix_ms
              and $m.ownership_observer_ready_before_exec_at_unix_ms
                <= $m.ecs_exec_invoked_at_unix_ms
              and $m.ecs_exec_invoked_at_unix_ms <= $exact_stop
              and $exact_stop <= $m.task_stop_observed_at_unix_ms
              and $timing_origin == (($exact_stop / 1000 | floor) * 1000)),
            pre_crash_envelope: hard_crash_envelope_passes(
              $pre_crash_seconds;
              $envelope[0];
              $m.ecs_exec_invoked_at_unix_ms
            ),
            affected_reconnects: (
              ($affected | length) > 0
              and ($all_recoveries | length) == ($affected | length)
              # Traffic counters count connection attempts, including a
              # retryable reconnect whose first replacement socket does not
              # reach a fresh snapshot. Hard recoveries count completed
              # game-ready outcomes, so require every attempt to be balanced
              # without incorrectly requiring the populations to be equal.
              and $r.metrics.traffic.disconnects >= ($affected | length)
              and $r.metrics.traffic.reconnects
                == $r.metrics.traffic.disconnects),
            pending_backlog: (
              $m.pending_after_kill.partition == $partition
              and $m.pending_after_kill.killed_lease_token
                == $m.selected_partition.lease_token
              and $pending_after_kill[0].partition == $partition
              and $pending_after_kill[0].requested_consumer
                == $m.selected_partition.lease_token
              and $pending_after_kill[0].pending_entry != null
              and ($pending_after_kill[0].pending_entry.id
                | type == "string" and length > 0)
              and $pending_after_kill[0].pending_entry.consumer
                == $m.selected_partition.lease_token
              and $m.pending_after_kill.observation_started_at_unix_ms
                >= $exact_stop
              and $m.pending_after_kill.captured_at_unix_ms
                >= $m.pending_after_kill.observation_started_at_unix_ms
              and $m.pending_after_kill.captured_at_unix_ms
                <= $m.pending_after_kill.observation_completed_at_unix_ms
              and $m.pending_after_kill.observation_completed_at_unix_ms
                <= ($exact_stop + 2000)
              and $m.pending_after_kill.captured_at_unix_ms
                == $pending_after_kill[0].captured_at_ms
              and $m.pending_after_kill.observation_started_at_unix_ms
                == $pending_after_kill[0].observation_started_at_ms
              and $m.pending_after_kill.observation_completed_at_unix_ms
                == $pending_after_kill[0].observation_completed_at_ms
              and ($pending_ids | length) > 0
              and ($pending_ids | length)
                == ($m.pending_after_kill.entries | length)
              and $m.pending_after_kill.entries == $observed_pending
              and $m.selected_partition == $old),
            fenced_survivor: (
              $m.owner_observation_started_at_unix_ms
                == $owner_ready[0].observation_started_at_ms
              and $m.owner_sample_captured_at_unix_ms
                == $owner_ready[0].captured_at_ms
              and $m.owner_membership_observed_at_unix_ms
                == $owner_ready[0].membership_observed_at_ms
              and $m.owner_authority_observed_at_unix_ms
                == $owner_ready[0].authority_observed_at_ms
              and $m.owner_authority_event_tail_id
                == $owner_ready[0].authority_event_tail_id
              and $m.owner_ready_at_unix_ms
                == $owner_ready[0].observation_completed_at_ms
              and $owner_ready[0].observation_started_at_ms >= $exact_stop
              and $owner_ready[0].captured_at_ms
                >= $owner_ready[0].observation_started_at_ms
              and $owner_ready[0].captured_at_ms
                <= $owner_ready[0].observation_completed_at_ms
              and $owner_ready[0].membership_observed_at_ms
                >= $owner_ready[0].observation_started_at_ms
              and $owner_ready[0].membership_observed_at_ms
                <= $owner_ready[0].observation_completed_at_ms
              and $owner_ready[0].authority_observed_at_ms
                >= $owner_ready[0].observation_started_at_ms
              and $owner_ready[0].authority_observed_at_ms
                <= $owner_ready[0].observation_completed_at_ms
              and ($owner_ready[0].authority_event_tail_id
                | type == "string" and test("^[0-9]+-[0-9]+$"))
              and $owner_ready[0].observation_completed_at_ms
                <= ($exact_stop + 5000)
              and $m.assignment_version_after
                == $owner_ready[0].assignment.version
              and $owner_ready[0].assignment.version
                > $pre[0].assignment.version
              and $owner_ready[0].assignment.computed_at_ms >= $exact_stop
              and $owner_ready[0].authority_stable
              and $owner_ready[0].killed_member_live == false
              and $new.owner_matches
              and $new.desired_owner != $m.selected_member.boot_id
              and $new.active_owner == $new.desired_owner
              and ($new.lease_token | type == "string" and length > 0)
              and $new.lease_token != $m.selected_partition.lease_token
              and ($new.desired_owner as $owner
                | [$owner_ready[0].live_members[]
                    | select(
                        .boot_id == $owner
                        and .lifecycle == "ACTIVE")] as $current
                | ($current | length) == 1
                  and $current[0].ecs_task_id
                    != $m.selected_member.ecs_task_id
                  and ($current[0].ecs_task_id as $owner_task_id
                    | any($pre[0].live_members[];
                        .boot_id == $owner
                        and .ecs_task_id == $owner_task_id)))),
            client_recovery_budget: (
              $crash_to_ready_p99_upper_bound_ms != null
              and $crash_to_ready_p99_upper_bound_ms <= 10000),
            client_recovery_integrity: (
              all($affected[];
                .to_task_boot_id != .from_task_boot_id
                and .fresh_snapshot_received)),
            exact_command_accounting: (
              ([ $r.metrics.command_counts_by_unix_second[] ] | add // 0)
                == $r.metrics.traffic.commands_sent
              and ([
                  $r.metrics.command_outcome_counts_by_sent_unix_second[]
                ] | add // 0) == $r.metrics.traffic.commands_sent
              and $r.metrics.planned_handoffs.pending_commands_at_finish == 0),
            authoritative_output_budget: (
              $m.authoritative_output.observation_started_at_unix_ms
                == $authoritative_output[0].observation_started_at_ms
              and $m.authoritative_output.captured_at_unix_ms
                == $authoritative_output[0].captured_at_ms
              and $m.authoritative_output.observation_completed_at_unix_ms
                == $authoritative_output[0].observation_completed_at_ms
              and $m.authoritative_output.partition == $partition
              and $m.authoritative_output.partition
                == $authoritative_output[0].partition
              and $m.authoritative_output.after_stream_id
                == $owner_ready[0].authority_event_tail_id
              and $m.authoritative_output.after_stream_id
                == $authoritative_output[0].after_stream_id
              and $m.authoritative_output.first_scheduled_output
                == $authoritative_output[0].first_scheduled_output
              and $authoritative_output[0].observation_started_at_ms
                >= $exact_stop
              and $authoritative_output[0].captured_at_ms
                >= $authoritative_output[0].observation_started_at_ms
              and $authoritative_output[0].captured_at_ms
                <= $authoritative_output[0].observation_completed_at_ms
              and ($output_id.milliseconds > $output_anchor.milliseconds
                or ($output_id.milliseconds == $output_anchor.milliseconds
                  and $output_id.sequence > $output_anchor.sequence))
              and $authoritative_output[0].first_scheduled_output.stream_unix_ms
                == $output_id.milliseconds
              and $authoritative_output[0].first_scheduled_output.stream_unix_ms
                >= $exact_stop
              and $authoritative_output[0].first_scheduled_output.stream_unix_ms
                <= ($exact_stop + 5000)
              and $authoritative_output[0].first_scheduled_output.stream_unix_ms
                <= $authoritative_output[0].captured_at_ms
              and $authoritative_output[0].first_scheduled_output.game_id % 10
                == $partition
              and $authoritative_output[0].first_scheduled_output
                .command_id.game_id
                == $authoritative_output[0].first_scheduled_output.game_id
              and $authoritative_output[0].first_scheduled_output
                .command_id.sequence > 0
              and ($authoritative_output[0].first_scheduled_output
                .command_id.client_game_session_id
                | type == "string" and length > 0)
              and $authoritative_output[0].first_scheduled_output
                .deduplicated_replay == false),
            final_executor_drain: (
              all($final[0].runtime_partitions[];
                .consumer_group_exists
                and .owner_matches
                and .pending_count == 0
                and .pending_completion_count == 0
                and .quarantined_command_count == 0))
          }
        }
      | .passed = ([.checks[]] | all)
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
  require_command nc
  require_command timeout
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
  local staging_valkey_name=""
  local staging_valkey_host=""
  local staging_valkey_port=""
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

  # Cleanup always restores the supported canonical service state. Planned
  # certification must already be in that state; crash certification first
  # isolates itself from any delayed target-tracking action left by a prior
  # phase.
  original_desired="1"
  scaling_state="$canonical_scaling_state"

  load_pid=""
  capacity_pid=""
  admission_population_pid=""
  idle_population_pid=""
  lobby_population_pid=""
  matchmaking_population_pid=""
  traefik_monitor_pid=""
  traefik_monitor_dir=""
  ecs_runtime_monitor_pid=""
  ecs_runtime_monitor_dir=""
  hard_crash_control_observer_pid=""
  hard_crash_control_observer_stop_file=""
  hard_crash_ownership_observer_pid=""
  hard_crash_ownership_observer_stop_file=""
  hard_crash_ecs_exec_pid=""

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
    stop_ecs_runtime_monitor
    stop_hard_crash_control_observer
    local population_pid
    for population_pid in \
      "$load_pid" \
      "$capacity_pid" \
      "$admission_population_pid" \
      "$idle_population_pid" \
      "$lobby_population_pid" \
      "$matchmaking_population_pid" \
      "$hard_crash_ecs_exec_pid"; do
      if [[ -n "$population_pid" ]] && kill -0 "$population_pid" 2>/dev/null; then
        kill -TERM "$population_pid" 2>/dev/null || true
        wait "$population_pid" 2>/dev/null || true
      fi
    done
    # Suspend policy writes while restoring the exact count, then restore the
    # original fully enabled policy state. Every step retries and is verified.
    retry_command 5 set_scaling_suspended true || cleanup_ok=false
    wait_for_no_active_scaling_activity \
      "$report_dir/cleanup-scaling-activities.json" || cleanup_ok=false
    retry_command 5 aws ecs update-service \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --service "$SNAKETRON_ECS_SERVICE" \
      --desired-count "$original_desired" >/dev/null || cleanup_ok=false
    wait_for_running_count "$original_desired" || cleanup_ok=false
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
    # Verify the exact count while policy writes are still suspended. Enabling
    # target tracking is the final mutation; a still-hot historical metric may
    # legitimately request new capacity immediately afterward.
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
    jq -n \
      --argjson restored "$cleanup_ok" \
      --arg scaling_state "$restored_scaling_state" \
      --arg counts "$restored_counts" \
      '{
        restored: $restored,
        scaling_state: $scaling_state,
        counts_verified_while_suspended: $counts
      }' \
      >"$report_dir/cleanup.json"
    if [[ "$cleanup_ok" != true ]]; then
      echo "Staging cleanup could not verify restoration of desired count and autoscaling policy; inspect cleanup.json" >&2
      exit_code=1
    fi
    exit "$exit_code"
  }
  local observed_desired
  observed_desired="$(aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" \
    --query 'services[0].desiredCount' \
    --output text)"
  local observed_scaling_state
  observed_scaling_state="$(aws application-autoscaling describe-scalable-targets \
    --region "$SNAKETRON_AWS_REGION" \
    --service-namespace ecs \
    --scalable-dimension ecs:service:DesiredCount \
    --resource-ids "$scaling_resource" \
    --query 'ScalableTargets[0].[MinCapacity,MaxCapacity,SuspendedState.DynamicScalingInSuspended,SuspendedState.DynamicScalingOutSuspended,SuspendedState.ScheduledScalingSuspended]' \
    --output text)"

  if [[ "$crash_mode" == false ]]; then
    if [[ "$observed_desired" != "1" ]]; then
      echo "Staging service must begin at desiredCount=1; found $observed_desired" >&2
      exit 1
    fi
    if ! staging_entry_state_is_valid \
      planned "$observed_desired" "$observed_scaling_state"; then
      echo "Staging autoscaling must be min=1, max=10, and fully enabled; found: $observed_scaling_state" >&2
      exit 1
    fi
    trap restore_and_verify EXIT
  fi

  if [[ "$crash_mode" == true ]]; then
    # Install cleanup before the first mutation. Suspension prevents a late
    # target-tracking action from a prior planned phase from racing this
    # explicit one-task baseline.
    trap restore_and_verify EXIT
    retry_command 5 set_scaling_suspended true
    local pre_normalization_scaling_state
    pre_normalization_scaling_state="$(aws application-autoscaling describe-scalable-targets \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --scalable-dimension ecs:service:DesiredCount \
      --resource-ids "$scaling_resource" \
      --query 'ScalableTargets[0].[MinCapacity,MaxCapacity,SuspendedState.DynamicScalingInSuspended,SuspendedState.DynamicScalingOutSuspended,SuspendedState.ScheduledScalingSuspended]' \
      --output text)"
    if [[ "$pre_normalization_scaling_state" != "$suspended_scaling_state" ]]; then
      echo "Crash certification could not suspend target tracking; found: $pre_normalization_scaling_state" >&2
      exit 1
    fi
    wait_for_no_active_scaling_activity \
      "$report_dir/crash-entry-scaling-activities.json"
    retry_command 5 aws ecs update-service \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --service "$SNAKETRON_ECS_SERVICE" \
      --desired-count 1 >/dev/null
    # Require three exact observations after suspension so an already
    # in-flight target-tracking write cannot pass through between setup and
    # crash selection.
    wait_for_running_count 1 3

    local normalized_counts
    normalized_counts="$(aws ecs describe-services \
      --region "$SNAKETRON_AWS_REGION" \
      --cluster "$SNAKETRON_ECS_CLUSTER" \
      --services "$SNAKETRON_ECS_SERVICE" \
      --query 'services[0].[desiredCount,runningCount,pendingCount]' \
      --output text)"
    local normalized_desired
    local normalized_running
    local normalized_pending
    IFS=$'\t' read -r \
      normalized_desired normalized_running normalized_pending \
      <<<"$normalized_counts"
    local normalized_scaling_state
    normalized_scaling_state="$(aws application-autoscaling describe-scalable-targets \
      --region "$SNAKETRON_AWS_REGION" \
      --service-namespace ecs \
      --scalable-dimension ecs:service:DesiredCount \
      --resource-ids "$scaling_resource" \
      --query 'ScalableTargets[0].[MinCapacity,MaxCapacity,SuspendedState.DynamicScalingInSuspended,SuspendedState.DynamicScalingOutSuspended,SuspendedState.ScheduledScalingSuspended]' \
      --output text)"
    if [[ "$normalized_running" != "1" || "$normalized_pending" != "0" ]] \
      || ! staging_entry_state_is_valid \
        crash "$normalized_desired" "$normalized_scaling_state"; then
      echo "Crash certification could not establish an isolated one-task baseline; found ECS counts: $normalized_counts, autoscaling: $normalized_scaling_state" >&2
      exit 1
    fi
    jq -n \
      --arg observed_desired "$observed_desired" \
      --arg observed_scaling_state "$observed_scaling_state" \
      --arg normalized_desired "$normalized_desired" \
      --arg normalized_running "$normalized_running" \
      --arg normalized_pending "$normalized_pending" \
      --arg normalized_scaling_state "$normalized_scaling_state" \
      '{
        observed: {
          desired_count: ($observed_desired | tonumber),
          scaling_state: $observed_scaling_state
        },
        normalized: {
          desired_count: ($normalized_desired | tonumber),
          running_count: ($normalized_running | tonumber),
          pending_count: ($normalized_pending | tonumber),
          scaling_state: $normalized_scaling_state
        }
      }' >"$report_dir/crash-phase-entry.json"
  fi

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

  wait_for_executor_drain() {
    local label="$1"
    local expected_tasks="$2"
    local snapshot="$report_dir/control-plane-$label.json"
    local candidate="$snapshot.pending"
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
      if capture_control_status "$candidate" 2>/dev/null \
        && jq -e --argjson expected "$expected_tasks" '
          ([.live_members[] | select(.lifecycle == "ACTIVE")] | length) == $expected
          and .assignment != null
          and (.runtime_partitions | length) == 10
          and all(.runtime_partitions[];
            .consumer_group_exists
            and .owner_matches
            and .pending_count == 0
            and .pending_completion_count == 0
            and .quarantined_command_count == 0)
        ' "$candidate" >/dev/null; then
        mv "$candidate" "$snapshot"
        return 0
      fi
      sleep 1
    done
    echo "One or more executor partitions did not fully drain for $label" >&2
    [[ -f "$candidate" ]] && mv "$candidate" "$snapshot"
    return 1
  }

  wait_for_control_plane initial 1
  wait_for_ecs_health "$report_dir" initial 1
  if [[ "$crash_mode" == true ]]; then
    verify_crash_exec_configuration "$report_dir" initial
    # Snapshot desired-STOPPED service tasks immediately before the evidence
    # clock starts. A task already STOPPING here may acquire its final
    # executionStoppedAt/stoppedAt later and must not masquerade as an
    # in-window unrelated failure.
    capture_stopped_tasks_snapshot \
      "$report_dir/ecs-runtime-baseline-before-evidence.json"
  fi
  local evidence_started_at
  local evidence_started_epoch
  evidence_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  evidence_started_epoch="$(date -u +%s)"
  start_traefik_monitor "$report_dir"
  start_ecs_runtime_monitor "$report_dir"
  # Keep the one-task continuity/staircase load separate from the supported
  # 272-session capacity load. Target-tracking latency must never leave the
  # complete capacity envelope on the initial two-vCPU task.
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

  wait_for_admission_wave_seed() {
    local log_file="$1"
    local pid="$2"
    local required_waves="$3"
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
      require_population_running admission "$pid"
      local observed_waves
      observed_waves="$(grep -cF 'launched coordinated wave' "$log_file" || true)"
      if [[ "$observed_waves" =~ ^[0-9]+$ ]] \
        && (( observed_waves >= required_waves )); then
        return 0
      fi
      sleep 0.1
    done
    echo "Admission runner did not emit $required_waves launch waves within 60 seconds" >&2
    return 1
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
    # At four sessions per second, the fixed 128-session Run A needs about
    # 32 seconds to launch before the heartbeat-delayed public count can
    # expose its floor. Ninety seconds leaves bounded scheduling jitter without
    # weakening any load, latency, or admission assertion.
    local deadline=$((SECONDS + 90))
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
    echo "$label did not expose at least $required_floor regional WebSockets within 90 seconds" >&2
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
    # A prior planned-suite failure may have restored desired count and policy
    # state while its short-lived clients or durable executor work were still
    # draining. Prove this crash run starts from an independent empty one-task
    # baseline before adding capacity or selecting any affected partition.
    wait_for_zero_certification_load crash-initial-zero
    wait_for_executor_drain crash-initial-drained-1 1
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
    wait_for_control_plane hard-crash-replacement-10 10
    require_load_running
    wait "$load_pid"
    load_pid=""

    wait_for_executor_drain hard-crash-final-10 10

    local load_summary="$report_dir/$run_id/summary.json"
    local evidence_finished_at
    evidence_finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    stop_ecs_runtime_monitor
    collect_crash_ecs_runtime_evidence "$report_dir"
    stop_traefik_monitor
    assert_traefik_monitor "$report_dir"
    assert_hard_crash_report "$report_dir" "$load_summary"
    echo "Hard-crash staging evidence written to $report_dir"
    return 0
  fi

  local natural_scale_out_run_id="${run_id}-natural-scale-out"
  local continuity_run_id="${run_id}-continuity"
  local capacity_run_id="${run_id}-capacity"
  local idle_run_id="${run_id}-idle"
  local lobby_run_id="${run_id}-lobby"
  local matchmaking_run_id="${run_id}-matchmaking"
  local admission_run_id="${run_id}-admission"
  local natural_scale_out_command=(
    "$loadtest_runner"
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --mode duel \
    --stages 128@20m \
    --spawn-rate 4 \
    --max-total-sessions 4096 \
    --command-profile every-tick \
    --run-id "$natural_scale_out_run_id" \
    --report-dir "$report_dir"
  )
  local automatic_scale_out_baseline_started_ms
  local automatic_scale_out_started_ms
  "${natural_scale_out_command[@]}" &
  load_pid=$!
  # One hundred twenty-eight sessions retain active games on every partition
  # and drive the two-vCPU minimum task above its headroom-preserving
  # target-tracking threshold.
  # They must still trigger the configured CPU/memory policy naturally;
  # failure to trigger is a certification failure, not permission to force
  # the transition.
  wait_for_region_socket_floor automatic-scale-out-baseline 128
  automatic_scale_out_baseline_started_ms="$(unix_time_ms)"
  wait_for_automatic_scale_out \
    "$report_dir" "$evidence_started_epoch" "$load_pid"
  # The load ramp is not itself an ownership transition. Start the strict
  # movement window at the whole second containing the first successful
  # target-tracking action. Including that bucket is conservative: immediate
  # disruption cannot hide in fractional control-plane timestamp precision.
  automatic_scale_out_started_ms="$(jq -er \
    --argjson started "$evidence_started_epoch" '
      def epoch:
        sub("\\.[0-9]+\\+00:00$"; "Z")
        | sub("\\.[0-9]+Z$"; "Z")
        | sub("\\+00:00$"; "Z")
        | fromdateiso8601;
      [.ScalingActivities[] as $activity
        | ($activity.StartTime | epoch) as $at
        | select(
            $activity.StatusCode == "Successful"
            and $at >= $started
            and ($activity.Cause | test("alarm|target.tracking"; "i")))
        | (($at * 1000) | floor)]
      | min
    ' "$report_dir/automatic-scale-out-activities.json")"
  jq -n \
    --argjson started_at_unix_ms "$automatic_scale_out_baseline_started_ms" \
    --argjson finished_at_unix_ms "$automatic_scale_out_started_ms" '
      {
        started_at_unix_ms: $started_at_unix_ms,
        finished_at_unix_ms: $finished_at_unix_ms,
        duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
      }
    ' >"$report_dir/automatic-scale-out-baseline-window.json"

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
  # Refresh the trigger snapshot after convergence so the final phase summary
  # records the same fully ready count proven by the checks above.
  aws ecs describe-services \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --services "$SNAKETRON_ECS_SERVICE" \
    >"$report_dir/automatic-scale-out.json"
  local automatic_scale_out_finished_ms
  automatic_scale_out_finished_ms="$(unix_time_ms)"
  jq -n \
    --argjson started_at_unix_ms "$automatic_scale_out_started_ms" \
    --argjson finished_at_unix_ms "$automatic_scale_out_finished_ms" '
      {
        started_at_unix_ms: $started_at_unix_ms,
        finished_at_unix_ms: $finished_at_unix_ms,
        duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
      }
  ' >"$report_dir/automatic-scale-out-window.json"
  require_load_running
  # Ownership movement ends here. Keep the clients running so Gate A can also
  # verify steady post-ready command delivery; this cohort is not a
  # capacity-valid destination load for the forced ten-to-one handoff. Let the
  # runner finish and prove that all sockets and games are gone before reset.
  local natural_scale_out_exit_status=0
  wait "$load_pid" || natural_scale_out_exit_status=$?
  load_pid=""
  local natural_scale_out_summary="$report_dir/$natural_scale_out_run_id/summary.json"
  # Cleanup evidence is independent of acceptance. Collect it before any
  # fail-fast report assertion, including when the load runner itself failed
  # after writing its report.
  local natural_scale_out_zero_status
  set +e
  ( set -e; wait_for_zero_certification_load after-natural-scale-out )
  natural_scale_out_zero_status=$?
  set -e
  local gate_a_baseline_diagnostics="$report_dir/gate-a-baseline-diagnostics.json"
  local gate_a_movement_diagnostics="$report_dir/gate-a-movement-diagnostics.json"
  local gate_a_post_ready_window="$report_dir/automatic-scale-out-post-ready-window.json"
  local gate_a_post_ready_steady_diagnostics="$report_dir/gate-a-post-ready-steady-diagnostics.json"
  local gate_a_summary="$natural_scale_out_summary"
  if [[ -s "$natural_scale_out_summary" ]]; then
    command_outcome_window_diagnostics \
      "$gate_a_summary" \
      "$report_dir/automatic-scale-out-baseline-window.json" \
      1000 10 >"$gate_a_baseline_diagnostics"
    command_outcome_window_diagnostics \
      "$gate_a_summary" \
      "$report_dir/automatic-scale-out-window.json" \
      1000 10 >"$gate_a_movement_diagnostics"
    # Once every new task is healthy and visible through Traefik/control-plane
    # readiness, keep enforcing the same strict command budget through the
    # load stage's recorded finish. The shared window helper evaluates only
    # complete sent-time seconds.
    write_gate_a_post_ready_window \
      "$gate_a_summary" "$automatic_scale_out_finished_ms" \
      >"$gate_a_post_ready_window"
    command_outcome_window_diagnostics \
      "$gate_a_summary" \
      "$gate_a_post_ready_window" \
      1000 10 "$gate_a_post_ready_required_full_seconds" \
      >"$gate_a_post_ready_steady_diagnostics"
  else
    gate_a_summary="$report_dir/gate-a-missing-load-summary.json"
    jq -n '{missing: true}' >"$gate_a_summary"
    jq -n '{
      passed: false,
      error: "load_summary_missing",
      failed_seconds: []
    }' >"$gate_a_baseline_diagnostics"
    cp "$gate_a_baseline_diagnostics" "$gate_a_movement_diagnostics"
    cp "$gate_a_baseline_diagnostics" \
      "$gate_a_post_ready_steady_diagnostics"
  fi
  write_gate_a_acceptance_report \
    "$gate_a_summary" \
    "$gate_a_baseline_diagnostics" \
    "$gate_a_movement_diagnostics" \
    "$gate_a_post_ready_steady_diagnostics" \
    "$report_dir/zero-load-after-natural-scale-out.json" \
    "$natural_scale_out_exit_status" \
    "$report_dir/gate-a-acceptance.json"
  jq -e '.passed' "$report_dir/gate-a-acceptance.json" >/dev/null || {
    echo "Natural scale-out Gate A acceptance failed; see gate-a-acceptance.json" >&2
    if (( natural_scale_out_zero_status != 0 )); then
      echo "Gate A cleanup also failed to reach zero sockets and games" >&2
    fi
    exit 1
  }

  # Automatic scale-out may have stopped at any count from two through ten.
  # Return without load to one healthy task, then start the independent
  # capacity-valid planned-transition cohort.
  local reset_to_one_started_ms
  reset_to_one_started_ms="$(unix_time_ms)"
  retry_command 5 aws ecs update-service \
    --region "$SNAKETRON_AWS_REGION" \
    --cluster "$SNAKETRON_ECS_CLUSTER" \
    --service "$SNAKETRON_ECS_SERVICE" \
    --desired-count 1 >/dev/null
  wait_for_running_count 1
  wait_for_control_plane forced-initial-1 1
  wait_for_ecs_health "$report_dir" forced-initial-1 1
  local reset_to_one_finished_ms
  reset_to_one_finished_ms="$(unix_time_ms)"
  jq -n \
    --argjson started_at_unix_ms "$reset_to_one_started_ms" \
    --argjson finished_at_unix_ms "$reset_to_one_finished_ms" '
      {
        started_at_unix_ms: $started_at_unix_ms,
        finished_at_unix_ms: $finished_at_unix_ms,
        duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
      }
    ' >"$report_dir/reset-to-one-window.json"
  local continuity_command=(
    "$loadtest_runner"
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --mode duel \
    --stages 128@15m \
    --spawn-rate 4 \
    --max-total-sessions 4096 \
    --command-profile every-tick \
    --require-planned-handoff \
    --run-id "$continuity_run_id" \
    --report-dir "$report_dir"
  )
  "${continuity_command[@]}" &
  load_pid=$!
  wait_for_region_socket_floor planned-transition-baseline 128 "$load_pid"
  require_load_running
  wait_for_control_plane planned-transition-loaded-1 1
  wait_for_ecs_health "$report_dir" planned-transition-loaded-1 1
  local planned_transition_baseline_started_ms
  planned_transition_baseline_started_ms="$(unix_time_ms)"
  sleep 10
  require_load_running
  jq -n \
    --argjson started_at_unix_ms "$planned_transition_baseline_started_ms" \
    --argjson finished_at_unix_ms "$(unix_time_ms)" '
      {
        started_at_unix_ms: $started_at_unix_ms,
        finished_at_unix_ms: $finished_at_unix_ms,
        duration_ms: ($finished_at_unix_ms - $started_at_unix_ms)
      }
    ' >"$report_dir/planned-transition-baseline-window.json"
  local scale_out_started_ms
  scale_out_started_ms="$(unix_time_ms)"
  jq -n \
    --argjson started_at_unix_ms "$scale_out_started_ms" '
      {
        status: "in_progress",
        started_at_unix_ms: $started_at_unix_ms,
        finished_at_unix_ms: null,
        duration_ms: null
      }
    ' >"$report_dir/scale-out-window.json"
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
      status: "completed",
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
  # Generate four fresh idle admissions per second through the direct
  # ten-to-one action without accumulating another steady-state population on
  # the one-task destination. The open-loop probe holds a successful ready
  # session for one second; its stage target is only a 64-session in-flight
  # safety ceiling. The ten-second admission deadline therefore remains fully
  # exercised while successful clients turn over promptly.
  # The public count is heartbeat-delayed. It proves the fixed 128 game sockets
  # plus 23 context probes before the rolling admission wave begins.
  wait_for_region_socket_floor pre-admission 151
  local admission_runner_log="$report_dir/admission-runner.log"
  : >"$admission_runner_log"
  "$loadtest_runner" \
    --target "$SNAKETRON_STAGING_TARGET" \
    --confirm-production \
    --require-same-origin \
    --region "$SNAKETRON_REGION_CODE" \
    --population idle \
    --mode duel \
    --stages 64@2m \
    --spawn-rate 4 \
    --open-loop-admission \
    --max-total-sessions 512 \
    --untimed-play-duration 1s \
    --drain-timeout 1m \
    --run-id "$admission_run_id" \
    --report-dir "$report_dir" \
    > >(tee -a "$admission_runner_log") 2>&1 &
  admission_population_pid=$!
  # Wait on the runner's first three explicit launch records rather than a
  # fixed sleep: serial target preflight happens before the first wave and can
  # legitimately take longer than three seconds. The finished report below
  # remains authoritative for exact wave size, cadence, and readiness.
  wait_for_admission_wave_seed \
    "$admission_runner_log" "$admission_population_pid" 3
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
      . as $report
      | (($scale_in[0].started_at_unix_ms / 1000) | ceil)
        as $scale_in_first_second
      | (($scale_in[0].finished_at_unix_ms / 1000) | floor)
        as $scale_in_after_last_second
      | .schema_version >= 10
      and .metadata.threshold_result == "passed"
      and .configured_max_concurrency == 128
      and .metadata.mode == "duel"
      and .metadata.command_profile == "every-tick"
      and .metadata.spawn_rate_per_second == "4"
      and .session_counts.peak_authenticated_concurrency == 128
      and .session_counts.peak_active_game_concurrency >= 64
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
      and ([.metrics.command_outcome_counts_by_sent_unix_second[]] | add)
        == .metrics.traffic.commands_sent
      and (.metrics.usable_session_gap_ms.max_ms // 0) == 0
      and .metrics.planned_handoffs.attempts > 0
      and .metrics.planned_handoffs.failures == 0
      and .metrics.planned_handoffs.successes == .metrics.planned_handoffs.attempts
      and .metrics.planned_handoffs.outcome_barriers > 0
      and .metrics.planned_handoff_duration_ms.max_ms <= 20000
      and .metrics.planned_handoffs.pending_commands_at_finish == 0
      and any(.sessions[];
        any(.planned_game_handoff_at_unix_ms[];
          . >= $scale_in[0].started_at_unix_ms
          and . <= $scale_in[0].finished_at_unix_ms))
      and ($scale_in[0].duration_ms >= 1000 and $scale_in[0].duration_ms <= 45000)
      and (.metrics.scheduled_command_counts_by_partition_and_unix_second | length) == 10
      and ([.metrics.scheduled_command_counts_by_partition_and_unix_second[] | .[]] | add) > 0
      and all(range(0; 10);
        . as $partition
        | any(
          ($report.metrics.scheduled_command_counts_by_partition_and_unix_second
            [($partition | tostring)] // {})
          | to_entries[];
          (.key | tonumber) < (($scale_out[0].started_at_unix_ms / 1000) | ceil)
          and .value > 0))
      and all(range(0; 10);
        . as $partition
        | any(
          ($report.metrics.scheduled_command_counts_by_partition_and_unix_second
            [($partition | tostring)] // {})
          | to_entries[];
          ((.key | tonumber) >= ($scale_in_first_second - 10))
          and ((.key | tonumber) < $scale_in_first_second)
          and .value > 0))
      and $scale_in_after_last_second > $scale_in_first_second
      and all(range($scale_in_first_second; $scale_in_after_last_second);
        . as $second
        | all(range(0; 10);
            . as $partition
            | (($report.metrics.scheduled_command_counts_by_partition_and_unix_second
              [($partition | tostring)][($second | tostring)] // 0) > 0)))
    ' "$continuity_summary" >/dev/null || {
      echo "Continuity load did not satisfy the active-game transition invariants" >&2
      exit 1
    }

  # Cover both directions of the forced staircase. Natural scale-out was
  # already checked against its separate 128-session report. Receipt-time
  # bucket counts alone can hide a one-second
  # executor stall followed by a catch-up burst. One second is a deliberately
  # strict user-continuity budget: the predictive client remains smooth while
  # every input still receives a prompt authoritative result.
  local movement_window
  for movement_window in \
    "$report_dir/planned-transition-baseline-window.json" \
    "$report_dir/scale-out-window.json" \
    "$report_dir/scale-in-window.json"; do
    command_outcomes_meet_window_budget \
      "$continuity_summary" "$movement_window" 1000 || {
        echo "Command outcomes exceeded the continuity budget in $movement_window" >&2
        exit 1
      }
  done

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
    | (.sessions
        | group_by(.wave_index)
        | map({
            wave_index: .[0].wave_index,
            started_at_unix_ms: (map(.started_at_unix_ms) | min),
            sessions: .
          })
        | sort_by(.started_at_unix_ms)) as $admission_waves
    | ([$admission_waves[]
        | select(
            .started_at_unix_ms
              >= ($scale_in[0].started_at_unix_ms - 1100)
            and .started_at_unix_ms
              <= ($scale_in[0].finished_at_unix_ms + 1100))]
      ) as $coverage_waves
    | .schema_version >= 10
    and .metadata.threshold_result == "passed"
    and .metadata.population == "idle"
    and .metadata.open_loop_admission == "true"
    and .configured_max_concurrency == 64
    and .metadata.spawn_rate_per_second == "4"
    and .metadata.untimed_play_duration_ms == "1000"
    and .session_counts.peak_token_sent_concurrency <= 64
    and .session_counts.peak_authenticated_concurrency <= 64
    and .session_counts.peak_authenticated_concurrency > 0
    and .session_counts.failed == 0
    and .session_counts.cancelled == 0
    and .session_counts.incomplete == 0
    and .session_counts.completed == .session_counts.total
    and .games.expected == 0
    and .games.observed == 0
    and all(.sessions[]; .outcome == "completed" and .failure_phase == null)
    and (.ramp_stages | length) == 1
    and .metadata.all_stages_completed == "true"
    and .metrics.traffic.disconnects == 0
    and .metrics.traffic.reconnects == 0
    and (.metrics.usable_session_gap_ms.max_ms // 0) == 0
    and .metrics.planned_handoffs.failures == 0
    and .metrics.planned_handoffs.successes == .metrics.planned_handoffs.attempts
    and .metrics.planned_handoff_duration_ms.max_ms <= 20000
    and ($scale_in[0].duration_ms >= 1000 and $scale_in[0].duration_ms <= 45000)
    and ($coverage_waves | length) >= 3
    and $coverage_waves[0].started_at_unix_ms
      >= ($scale_in[0].started_at_unix_ms - 1100)
    and $coverage_waves[0].started_at_unix_ms
      <= $scale_in[0].started_at_unix_ms
    and $coverage_waves[-1].started_at_unix_ms
      >= $scale_in[0].finished_at_unix_ms
    and $coverage_waves[-1].started_at_unix_ms
      <= ($scale_in[0].finished_at_unix_ms + 1100)
    and all($coverage_waves[];
      (.sessions | length) == 4
      and all(.sessions[];
        .outcome == "completed"
        and .failure_phase == null
        and .initial_admission_ready_ms != null
        and .initial_admission_ready_ms <= 10000))
    and all(range(1; ($coverage_waves | length));
      . as $index
      | ($coverage_waves[$index].started_at_unix_ms
        - $coverage_waves[$index - 1].started_at_unix_ms) <= 1100)
    and all($scale_in_sessions[];
      .outcome == "completed"
      and .failure_phase == null
      and .initial_admission_ready_ms != null)
    and ([$scale_in_sessions[].initial_admission_ready_ms] | p99) <= 10000
    and (.metrics.initial_admission_ready_ms.p99_ms // 10001) <= 10000
  ' "$admission_summary" >/dev/null || {
    echo "Planned scale-in did not preserve four-per-second admission, ten-second readiness, and the 64-session safety ceiling" >&2
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
        | .schema_version >= 10
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
          configured_game_websockets: 128,
          companion_websockets: 23,
          configured_total_websockets: 151,
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
    and .transition.configured_game_websockets == 128
    and .transition.companion_websockets == 23
    and .transition.configured_total_websockets == 151
    and .transition.observed_game_websockets == 128
    and .transition.observed_total_websockets == 151
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
    --stages 272@10m \
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

  write_capacity_acceptance_report \
    "$capacity_summary" "$report_dir/capacity-acceptance.json"
  jq -e '.passed' "$report_dir/capacity-acceptance.json" >/dev/null || {
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
  wait_for_executor_drain automatic-final-drained-1 1
  stop_ecs_runtime_monitor
  collect_ecs_runtime_evidence "$report_dir"
  jq -n \
    --slurpfile automatic_out "$report_dir/automatic-scale-out.json" \
    --slurpfile forced "$report_dir/assignment-movement.json" \
    --slurpfile automatic_in "$report_dir/automatic-scale-in.json" '
      {
        automatic_scale_out: {
          desired: $automatic_out[0].services[0].desiredCount,
          running: $automatic_out[0].services[0].runningCount,
          evidence: "target-tracking activity",
          configured_game_sessions: 128,
          configured_duels: 64
        },
        deterministic_forced_staircase: ($forced[0] + {
          configured_game_sessions: 128,
          configured_duels: 64,
          fixed_context_sessions: 23,
          admission_starts_per_second: 4,
          admission_in_flight_ceiling: 64
        }),
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
  collect_cloudwatch_evidence_with_retry "$report_dir"
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
