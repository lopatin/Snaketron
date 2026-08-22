#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
environment="$package/.factory.env"
factory="$package/.venv/bin/factory"

if [[ $# -gt 1 || ( $# -eq 1 && "${1:-}" != "--run-once" ) ]]; then
  printf 'usage: %s [--run-once]\n' "$0" >&2
  exit 2
fi

"$factory" doctor --config "$package/config/factory.yaml" --env-file "$environment" --identity service --json
"$factory" status --config "$package/config/factory.yaml" --env-file "$environment" --json

if [[ "${1:-}" == "--run-once" ]]; then
  # This may make paid provider calls; it is never implicit in a smoke check.
  paid_result="$(mktemp "$package/var/.paid-smoke-result.XXXXXX")"
  trap 'rm -f "$paid_result"' EXIT
  chmod 600 "$paid_result"
  # The installed Hermes wrapper requires an existing behavior pin on every
  # invocation.  This explicit operator-requested cycle is what creates that
  # first pin, so run the same service command directly after the online
  # doctor above rather than weakening the scheduled guard with a bypass flag.
  "$factory" run-once \
    --config "$package/config/factory.yaml" \
    --env-file "$environment" \
    --json > "$paid_result"
  sed -n '1,$p' "$paid_result"
  python3 -I - "$paid_result" <<'PY'
import json, pathlib, sys

value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
advanced = value.get("advanced") if isinstance(value, dict) else None
if not isinstance(advanced, list) or not advanced:
    raise SystemExit("paid smoke did not advance a retained Attempt")
if value.get("halt") is not None:
    raise SystemExit("paid smoke halted before reaching human review")
if any(isinstance(item, dict) and (item.get("failure") or item.get("state") == "blocked") for item in advanced):
    raise SystemExit("paid smoke recorded a failed or blocked transition")
if not any(isinstance(item, dict) and item.get("to") == "prototype_review" for item in advanced):
    raise SystemExit("paid smoke did not prove concept, image, visual judge, and retained prototype review")
PY
  "$factory" readiness-pin \
    --config "$package/config/factory.yaml" \
    --env-file "$environment" \
    --record-paid-smoke \
    --json
  rm -f "$paid_result"
  trap - EXIT
fi
