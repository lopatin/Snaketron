#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
environment="$package/.factory.env"
factory="$package/.venv/bin/factory"

"$factory" doctor --config "$package/config/factory.yaml" --env-file "$environment" --identity service --json
"$factory" status --config "$package/config/factory.yaml" --env-file "$environment" --json

if [[ "${1:-}" == "--run-once" ]]; then
  # This may make paid provider calls; it is never implicit in a smoke check.
  "$package/scripts/hermes-run-once.sh"
fi
