#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
state="$package/var/hermes-job-id"
installed="${HERMES_HOME:-$HOME/.hermes}/scripts/snaketron-skin-factory.sh"

if [[ -f "$state" ]]; then
  job_id="$(tr -d '[:space:]' < "$state")"
  [[ "$job_id" =~ ^[0-9a-f]+$ ]] || { printf 'invalid recorded Hermes job id\n' >&2; exit 1; }
  hermes cron remove "$job_id"
  rm -f "$state"
else
  printf 'No recorded Skin Factory cron id; no cron was removed.\n'
fi

if [[ -f "$installed" ]] && cmp -s "$package/scripts/hermes-run-once.sh" "$installed"; then
  rm -f "$installed"
fi

printf 'Scheduler integration rolled back. Factory data, backups, env, and virtual environments were preserved.\n'
