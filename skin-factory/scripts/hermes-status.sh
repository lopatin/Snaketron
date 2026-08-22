#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
factory="$package/.venv/bin/factory"
environment="$package/.factory.env"

[[ -x "$factory" ]] || { printf 'factory is not installed\n' >&2; exit 1; }
[[ -f "$environment" ]] || { printf 'explicit service env is missing\n' >&2; exit 1; }
"$factory" status --config "$package/config/factory.yaml" --env-file "$environment" --json
hermes cron status
hermes cron list --all
