#!/usr/bin/env bash
set -euo pipefail

# The installer may be launched from an operator shell. Do not let credentials
# inherited from that shell reach doctor, package hooks, Git, or Hermes. The
# scheduled identity receives only the separately validated service JSON.
unset SKIN_FACTORY_REVIEW_TOKEN
unset SKIN_FACTORY_REVIEW_ACTOR
unset SNAKETRON_FACTORY_OPERATOR_TOKEN

if [[ $# -lt 1 || $# -gt 2 ]]; then
  printf 'usage: %s /absolute/path/to/service-env.json [schedule]\n' "$0" >&2
  exit 2
fi

source_env="$1"
schedule="${2:-every 6h}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
repo_root="$(cd "$package/.." && pwd -P)"
environment_manifest="$package/config/service-env.json"

[[ -f "$environment_manifest" ]] || {
  printf 'service environment manifest is missing\n' >&2
  exit 1
}

# The checked-in manifest is the shell boundary: remove every declared model,
# webhook, service, and human capability before uv, package hooks, renderer
# builds, Git, or Hermes are invoked. The service JSON is never exported here.
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

uv_bin="$(command -v uv || true)"
hermes_bin="$(command -v hermes || true)"

[[ -n "$uv_bin" ]] || { printf 'uv is required\n' >&2; exit 1; }
[[ -n "$hermes_bin" ]] || { printf 'Hermes is required\n' >&2; exit 1; }
[[ "$source_env" = /* && -f "$source_env" ]] || {
  printf 'service env must be an existing absolute path\n' >&2
  exit 1
}

# The scheduled process must never possess review, resolution, or publication
# authority. Validate the JSON before copying it into the explicit workdir.
python3 - "$source_env" "$environment_manifest" <<'PY'
import json, pathlib, stat, sys
path = pathlib.Path(sys.argv[1])
value = json.loads(path.read_text(encoding="utf-8"))
if not isinstance(value, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
    raise SystemExit("service env must be a JSON object of string values")
manifest = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
required = {entry["name"] for entry in manifest["required"]}
optional = {entry["name"] for entry in manifest["optional"]}
forbidden = {
    entry["name"]
    for entry in [*manifest["required"], *manifest["optional"]]
    if entry.get("identity") == "human_operator_only"
}
allowed = (required | optional) - forbidden
missing = sorted(required - value.keys())
present = sorted(forbidden & value.keys())
unknown = sorted(value.keys() - allowed)
if missing:
    raise SystemExit("service env is missing: " + ", ".join(missing))
if present:
    raise SystemExit("Hermes service env must not contain operator credentials: " + ", ".join(present))
if unknown:
    raise SystemExit("service env contains undeclared names: " + ", ".join(unknown))
if stat.S_IMODE(path.stat().st_mode) & 0o077:
    raise SystemExit("service env must be private (chmod 600)")
if any(not value[name] for name in required):
    raise SystemExit("required service credentials cannot be empty")
PY

install -d -m 700 "$package/var"
"$uv_bin" sync --project "$package" --frozen --no-dev --extra production
"$uv_bin" run --project "$package" --frozen --no-dev playwright install chromium
"$package/scripts/build-renderer-bundle.sh"

lama_dir="$package/var/lama-venv"
UV_PROJECT_ENVIRONMENT="$lama_dir" "$uv_bin" sync \
  --project "$package/lama" \
  --frozen \
  --no-dev \
  --no-install-project \
  --python 3.11 \
  --no-python-downloads

lama_manifest="$package/lama/manifest.json"
lama_model_dir="$package/var/lama"
[[ ! -L "$lama_model_dir" ]] || { printf 'LaMa model directory cannot be a symlink\n' >&2; exit 1; }
install -d -m 700 "$lama_model_dir"
lama_filename="$(python3 -I - "$lama_manifest" <<'PY'
import json, pathlib, sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["model"]
filename = value["filename"]
if pathlib.Path(filename).name != filename:
    raise SystemExit("LaMa model filename must be a basename")
print(filename)
PY
)"
lama_url="$(python3 -I - "$lama_manifest" <<'PY'
import json, pathlib, sys
url = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["model"]["url"]
if not isinstance(url, str) or not url.startswith("https://"):
    raise SystemExit("LaMa model URL must use HTTPS")
print(url)
PY
)"
lama_sha="$(python3 -I - "$lama_manifest" <<'PY'
import json, pathlib, sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["model"]["sha256"])
PY
)"
lama_size="$(python3 -I - "$lama_manifest" <<'PY'
import json, pathlib, sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["model"]["size_bytes"])
PY
)"
lama_model="$lama_model_dir/$lama_filename"

verify_lama_model() {
  python3 -I - "$1" "$lama_sha" "$lama_size" <<'PY'
import hashlib, pathlib, sys
path = pathlib.Path(sys.argv[1])
expected_sha, expected_size = sys.argv[2], int(sys.argv[3])
if path.is_symlink() or not path.is_file() or path.stat().st_size != expected_size:
    raise SystemExit(1)
digest = hashlib.sha256()
with path.open("rb") as handle:
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
raise SystemExit(0 if digest.hexdigest() == expected_sha else 1)
PY
}

if ! verify_lama_model "$lama_model"; then
  curl_bin="$(command -v curl || true)"
  [[ -n "$curl_bin" ]] || { printf 'curl is required to preload Big-LaMa\n' >&2; exit 1; }
  lama_temporary="$(mktemp "$lama_model_dir/.big-lama.XXXXXX")"
  trap 'rm -f "$lama_temporary"' EXIT
  chmod 600 "$lama_temporary"
  "$curl_bin" --disable --fail --location --proto '=https' --tlsv1.2 \
    --retry 3 --output "$lama_temporary" "$lama_url"
  verify_lama_model "$lama_temporary" || {
    printf 'downloaded Big-LaMa model differs from pinned size or sha256\n' >&2
    exit 1
  }
  chmod 400 "$lama_temporary"
  mv -f "$lama_temporary" "$lama_model"
  trap - EXIT
fi
chmod 400 "$lama_model"

# Promotion loads immutable behavior refs. Fetch tags explicitly even when the
# checkout was originally cloned with a restricted tag policy.
promotion_remote="$("$package/.venv/bin/python" - "$package/config/factory.yaml" <<'PY'
import pathlib, sys, yaml
value = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
print(value["optimizer"]["promotion_remote"])
PY
)"
git -C "$repo_root" fetch --tags "$promotion_remote"

environment="$package/.factory.env"
if [[ "$source_env" != "$environment" ]]; then
  install -m 600 "$source_env" "$environment"
else
  chmod 600 "$environment"
fi

"$package/.venv/bin/factory" doctor \
  --config "$package/config/factory.yaml" \
  --env-file "$environment" \
  --identity service \
  --offline \
  --json

hermes_scripts="${HERMES_HOME:-$HOME/.hermes}/scripts"
installed="$hermes_scripts/snaketron-skin-factory.sh"
mkdir -p "$hermes_scripts"
if [[ -e "$installed" && ! "$installed" -ef "$package/scripts/hermes-run-once.sh" ]] && \
   ! cmp -s "$package/scripts/hermes-run-once.sh" "$installed"; then
  printf 'refusing to overwrite a different Hermes script: %s\n' "$installed" >&2
  exit 1
fi
install -m 700 "$package/scripts/hermes-run-once.sh" "$installed"

if "$hermes_bin" cron list --all | grep -Fq 'snaketron-skin-factory'; then
  printf 'a snaketron-skin-factory cron already exists; refusing to create a duplicate\n' >&2
  exit 1
fi

creation="$($hermes_bin cron create "$schedule" \
  --name snaketron-skin-factory \
  --script "$installed" \
  --no-agent \
  --workdir "$repo_root")"
printf '%s\n' "$creation"
job_id="$(printf '%s\n' "$creation" | sed -n 's/.*Created job: \([0-9a-f][0-9a-f]*\).*/\1/p' | head -n 1)"
if [[ -z "$job_id" ]]; then
  printf 'Hermes created a job but its id could not be recorded; inspect cron list\n' >&2
  exit 1
fi
printf '%s\n' "$job_id" > "$package/var/hermes-job-id"
chmod 600 "$package/var/hermes-job-id"

printf 'Installed one no-agent Hermes job %s. Run scripts/hermes-smoke.sh before calibration.\n' "$job_id"
