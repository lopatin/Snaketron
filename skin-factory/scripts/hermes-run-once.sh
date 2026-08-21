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

# Hermes supplies the explicit repository workdir. No interactive profile is
# sourced, and the JSON env file is parsed by the factory rather than executed.
if [[ -f "$PWD/skin-factory/pyproject.toml" ]]; then
  package="$PWD/skin-factory"
elif [[ -f "$PWD/pyproject.toml" && -d "$PWD/src/snaketron_factory" ]]; then
  package="$PWD"
else
  printf '%s\n' '{"ok":false,"error":"Skin Factory package is not under the configured Hermes workdir"}'
  exit 1
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

exec "$factory" run-once \
  --config "$package/config/factory.yaml" \
  --env-file "$environment" \
  --json
