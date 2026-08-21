# Snaketron Skin Factory

The Skin Factory is a retained, provider-neutral pipeline that turns a human-
approved visual prototype into a validated first-class SkinDoc v2 skin. Hermes
schedules one deterministic `factory run-once` command; it is not the reasoning
engine. Gemini supplies the configured smart-text, visual-judge, and image
roles, while any conforming local or API task worker can consume the canonical
`skills/author-skin` package.

The image-generator role may instead use an explicitly configured
`openai_compatible` provider. Its adapter sends strict JSON-schema and
multimodal requests to Chat Completions, new images to Images generations, and
reference-guided images to Images edits. It retains only inline base64 image
bytes and never probes another endpoint, downloads a returned URL, changes the
configured model, or falls back to Gemini. Set the role's exact `model`, API
`base_url` (including `/v1`), and `api_key_env`; keep that environment variable
in the service identity's private JSON. Standard OpenAI image sizes are used:
the requested `1K`, `2K`, or `4K` tier and aspect ratio map to explicit pixel
dimensions (for example, `2K` 16:9 is `2048x1152`). A compatible service must
implement the endpoint and dimensions needed by the configured role.

Operational state lives under the configured `var/` directory in SQLite and a
content-addressed object store. Direction, gates, model-role configuration, and
the authoring skill remain canonical in Git. Every attempt records their exact
hashes.

Production builds open a server review request for the exact rendered
revision/content hash before entering final review. Final rejection cancels
only that same request, so it cannot race away a newer revision. Optimizer and
technique builds are registered in the server's `evaluation` namespace: they
use the real revision and browser-render paths but are refused by publication,
catalogue, purchase, and equipment boundaries even for an administrator.

## Development install

```sh
cd skin-factory
uv sync --extra production
uv run playwright install chromium
./scripts/build-renderer-bundle.sh
uv run factory doctor --config config/factory.yaml \
  --env-file "$HOME/.config/snaketron-skin-factory/service.json" \
  --identity service
uv run pytest
```

Browser evidence is valid only when the configured web endpoint serves the
exact cached release bundle built above. For a local operator session, run
`node skin-factory/scripts/serve-renderer-bundle.mjs client/web/dist` from the
`snaketron/` directory; the capture records and verifies the served HTML,
JavaScript, CSS, and WASM digests.

The default test command measures statement and branch coverage and enforces
the committed 70% whole-package baseline. Raise that floor as the CLI and
deployment adapters gain additional failure-injection coverage; do not lower
it to accommodate a regression.

LaMa is deliberately isolated from the main environment because its Pillow and
NumPy constraints conflict with the strict forge. Production uses the complete
transitive closure in `lama/uv.lock`, not an open-ended pip requirements file.
The Hermes installer preloads the versioned Big-LaMa TorchScript file, verifies
its pinned 205,803,670-byte size and SHA-256 before an atomic owner-only install,
and then runs a real offline smoke inference. Forge subprocesses receive only a
minimal environment containing that exact `LAMA_MODEL`; a checked-in
`sitecustomize.py` denies socket connections, so upstream download-on-first-use
is unreachable. The dependency lock, offline loader, and model identity share
one digest in every Attempt's behavior snapshot.

Production installation, service environment, Hermes cron setup, calibration,
backup, recovery, and rollback are documented in `docs/hermes-operations.md`.
For a safe real local checkout with isolated ports, automatic least-privilege
account provisioning, and an explicit paid `run` boundary, use
[`docs/local-calibration.md`](docs/local-calibration.md).
