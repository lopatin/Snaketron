#!/usr/bin/env bash
set -euo pipefail

# A Hermes daemon can inherit its launch environment. Strip every human-only
# capability before locating or executing the factory, even though the
# installed service JSON is independently forbidden from containing them.
unset SKIN_FACTORY_REVIEW_TOKEN
unset SKIN_FACTORY_REVIEW_ACTOR
unset SNAKETRON_FACTORY_OPERATOR_TOKEN
unset GEMINI_API_KEY
unset LMSTUDIO_API_KEY
unset SNAKETRON_FACTORY_SERVICE_TOKEN
unset SKIN_FACTORY_OUTBOX_WEBHOOK_URL
unset SKIN_FACTORY_OUTBOX_WEBHOOK_TOKEN

# Hermes records the explicit repository workdir, but released no-agent
# schedulers execute scripts from HERMES_HOME/scripts. Prefer a caller's real
# workdir; otherwise use the installer-owned locator and prove it still points
# at the exact checkout containing this wrapper version.
package=""
if [[ -f "$PWD/skin-factory/pyproject.toml" ]]; then
  package="$PWD/skin-factory"
elif [[ -f "$PWD/pyproject.toml" && -d "$PWD/src/snaketron_factory" ]]; then
  package="$PWD"
else
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
  locator="$script_dir/snaketron-skin-factory.workdir"
  if ! configured_workdir="$(python3 -I - "$locator" <<'PY'
import pathlib, stat, sys

path = pathlib.Path(sys.argv[1])
if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
    raise SystemExit(1)
lines = path.read_text(encoding="utf-8").splitlines()
if len(lines) != 1 or not lines[0]:
    raise SystemExit(1)
workdir = pathlib.Path(lines[0])
if not workdir.is_absolute() or not workdir.is_dir():
    raise SystemExit(1)
print(workdir.resolve())
PY
  )"; then
    printf '%s\n' '{"ok":false,"error":"Hermes workdir locator is missing, public, or invalid"}'
    exit 1
  fi
  candidate="$configured_workdir/skin-factory"
  source_wrapper="$candidate/scripts/hermes-run-once.sh"
  if [[ ! -f "$candidate/pyproject.toml" || ! -f "$source_wrapper" ]] || \
     ! cmp -s "$source_wrapper" "${BASH_SOURCE[0]}"; then
    printf '%s\n' '{"ok":false,"error":"Hermes workdir does not contain the installed Skin Factory version"}'
    exit 1
  fi
  cd -P "$configured_workdir"
  package="$candidate"
fi

environment_manifest="$package/config/service-env.json"
if [[ -f "$environment_manifest" ]]; then
  credential_names="$(python3 - "$environment_manifest" <<'PY'
import json, pathlib, sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
entries = [*value.get("required", []), *value.get("optional", [])]
if not entries:
    raise SystemExit("service environment manifest is empty")
for entry in entries:
    name = entry.get("name")
    if not isinstance(name, str) or not name or not name.replace("_", "A").isalnum() or name[0].isdigit():
        raise SystemExit("service environment manifest contains an invalid name")
    print(name)
PY
)"
  while IFS= read -r credential_name; do
    unset "$credential_name"
  done <<< "$credential_names"
fi

factory="$package/.venv/bin/factory"
environment="$package/.factory.env"
if [[ ! -x "$factory" || ! -f "$environment" ]]; then
  printf '%s\n' '{"ok":false,"error":"Skin Factory environment is not installed"}'
  exit 1
fi

# A successful install is not a permanent authorization to spend. Check the
# behavior-bound paid-smoke proof immediately before every scheduled cycle.
# Keep its success document out of stdout so the wrapper still emits exactly
# the single run-once JSON document Hermes and smoke validation consume.
"$factory" readiness-pin \
  --config "$package/config/factory.yaml" \
  --env-file "$environment" \
  --check-paid-smoke \
  --json >/dev/null

exec "$factory" run-once \
  --config "$package/config/factory.yaml" \
  --env-file "$environment" \
  --json
