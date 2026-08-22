#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
package="$(cd "$script_dir/.." && pwd -P)"
repo_root="$(cd "$package/.." && pwd -P)"
wasm_pack_bin="$(command -v wasm-pack || true)"
npm_bin="$(command -v npm || true)"

[[ -n "$wasm_pack_bin" ]] || { printf 'wasm-pack is required to build the renderer bundle\n' >&2; exit 1; }
[[ -n "$npm_bin" ]] || { printf 'npm is required to build the renderer bundle\n' >&2; exit 1; }
[[ -f "$repo_root/client/Cargo.toml" ]] || { printf 'Snaketron client crate is missing\n' >&2; exit 1; }
[[ -f "$repo_root/client/web/package-lock.json" ]] || { printf 'web package lock is missing\n' >&2; exit 1; }

(
  cd "$repo_root/client"
  "$wasm_pack_bin" build --target web --out-dir pkg --release -- --locked
)
(
  cd "$repo_root/client/web"
  "$npm_bin" ci
  SNAKETRON_FACTORY_RENDERER_BUILD=true "$npm_bin" run build:prod
)

[[ -f "$repo_root/client/web/dist/index.html" ]] || { printf 'renderer index was not built\n' >&2; exit 1; }
find "$repo_root/client/web/dist" -type f -name '*.js' -print -quit | grep -q . || {
  printf 'renderer JavaScript was not built\n' >&2
  exit 1
}
find "$repo_root/client/web/dist" -type f -name '*.wasm' -print -quit | grep -q . || {
  printf 'renderer WASM was not built\n' >&2
  exit 1
}

printf 'Built cached renderer bundle under %s\n' "$repo_root/client/web/dist"
