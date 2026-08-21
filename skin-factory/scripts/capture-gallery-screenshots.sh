#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
repo="$(cd "$package/.." && pwd -P)"
output="${1:-$repo/specs/images/skin-factory}"
review_token="documentation-review-token-90"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/snaketron-gallery-capture.XXXXXX")"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf -- "$scratch"
}
trap cleanup EXIT

mkdir -p "$output"
"$package/.venv/bin/python" "$package/scripts/seed-gallery-screenshots.py" \
  --state "$scratch" \
  --review-token "$review_token" >/dev/null

port="$(python3 - <<'PY'
import socket
with socket.socket() as server:
    server.bind(("127.0.0.1", 0))
    print(server.getsockname()[1])
PY
)"

"$package/.venv/bin/factory" serve \
  --config "$scratch/config/factory.yaml" \
  --env-file "$scratch/operator.json" \
  --host 127.0.0.1 \
  --port "$port" >"$scratch/server.log" 2>&1 &
server_pid="$!"

ready=0
for _ in $(seq 1 100); do
  if curl --fail --silent "http://127.0.0.1:$port/healthz" >/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if [[ "$ready" -ne 1 ]]; then
  cat "$scratch/server.log" >&2
  exit 1
fi

node "$package/scripts/capture-gallery-screenshots.mjs" \
  "http://127.0.0.1:$port" \
  "$review_token" \
  "$scratch/fixture.json" \
  "$output"
"$package/.venv/bin/python" "$package/scripts/optimize-gallery-screenshots.py" "$output"
