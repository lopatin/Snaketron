# Install and operate the Skin Factory with Hermes

The production scheduler is a single Hermes `--no-agent` job. Hermes does not
select models, sequence stages, approve artifacts, resolve unknown operations,
or publish. It invokes the same durable `factory run-once` command an operator
can run from any agent or terminal.

## Identities and secret files

Use two different JSON files with mode `0600`:

- The **service** file contains `GEMINI_API_KEY` for the default roles (or the
  exact environment variable named by a configured OpenAI-compatible role) and
  `SNAKETRON_FACTORY_SERVICE_TOKEN`, plus `LMSTUDIO_API_KEY` only when the
  configured task-worker endpoint requires one. It must not contain the review
  token, review actor, or Snaketron operator token.
- The **operator** file contains `SKIN_FACTORY_REVIEW_TOKEN`,
  `SKIN_FACTORY_REVIEW_ACTOR`, and `SNAKETRON_FACTORY_OPERATOR_TOKEN`. It is
  used only for the gallery and explicit human commands.

These are JSON data, not shell scripts. The CLI parses them without `source`,
`eval`, or an interactive profile. Never paste a token on a command line.

Create private copies and edit the values in place:

```sh
mkdir -p "$HOME/.config/snaketron-skin-factory"
install -m 600 scripts/factory.service-env.example.json \
  "$HOME/.config/snaketron-skin-factory/service.json"
install -m 600 scripts/factory.operator-env.example.json \
  "$HOME/.config/snaketron-skin-factory/operator.json"
vi "$HOME/.config/snaketron-skin-factory/service.json"
vi "$HOME/.config/snaketron-skin-factory/operator.json"
```

The Snaketron service token must be scoped to create/update the factory's
private skins and open ordinary machine-routed publication review only. Final
soft-triage overrides, exact-request cancellation, and publication use the
separate admin/operator token. That token must never be issued to the cron
identity or task worker.

## Promotion bot Git identity

The deployment checkout also needs an explicitly configured bot identity,
secret signing key, and push credential for the configured promotion remote.
Before production promotion, configure `user.name`, `user.email`, and
`user.signingkey` in that checkout. Shadow calibration starts from the exact
committed `HEAD`; successful optimizer promotions replace that baseline with a
signed immutable tag. Do not put a private signing key or Git token in either
factory JSON file.

The installer fetches tags from the configured remote. Doctor verifies the
active ref resolves to a commit and whether the secret signing key is available;
the online check uses `git ls-remote` plus `git push --dry-run` to prove remote
authorization without changing any remote ref. Actual promotion creates a
signed unique tag/branch and verifies it from a clean clone. Missing promotion
credentials are warnings during early shadow calibration and become required
when promotion is eligible or routing moves to production.

## Install

From `skin-factory/`, install with an absolute service-env path:

```sh
./scripts/install-hermes.sh \
  "$HOME/.config/snaketron-skin-factory/service.json" \
  "every 6h"
```

The installer treats `config/service-env.json` as the checked-in capability
manifest. Its required/optional names must match `factory.yaml`; undeclared
keys in the private service JSON are rejected.

The installer:

1. validates that the service JSON contains no operator credentials;
2. runs `uv sync --frozen --no-dev --extra production` against the checked-in
   lock, excluding the default development group;
3. installs Playwright Chromium;
4. builds a cached release-mode web/WASM renderer from the locked Cargo and
   npm dependency graphs;
5. synchronizes `var/lama-venv` from the frozen transitive `lama/uv.lock`;
6. downloads the exact v0.1.0 Big-LaMa TorchScript weight over HTTPS only when
   the installed copy is absent or invalid, verifies its pinned size and
   SHA-256, and atomically installs it owner-read-only under `var/lama/`;
7. fetches immutable authoring tags from the promotion remote;
8. copies the service JSON to the ignored `.factory.env` with mode `0600`;
9. runs `factory doctor --identity service --offline --json`;
10. copies the fixed wrapper under `~/.hermes/scripts/`; and
11. creates exactly one named cron with `--script`, `--no-agent`, and an
   explicit repository `--workdir`.

It refuses to overwrite an unrelated script or create a duplicate named cron.
The scheduled wrapper does not load `~/.zshrc` or any other interactive shell
state. Both the installer and wrapper scrub every model, webhook, service, and
human capability named by the manifest before starting child processes. The
factory then loads the owner-private JSON authoritatively: inherited values
cannot override it, absent optional credentials stay absent, and direct
service commands fail closed if they inherit human authority. Promotion
validators and Git subprocesses receive the same scrubbed environment while
retaining Git, SSH-agent, and GPG configuration. Thus the explicit service JSON
is the only secret input to the scheduled factory command, and its credentials
do not leak into package hooks, renderer builds, or promotion commands.

## Online smoke test

Start the configured Snaketron server and task-worker endpoint. Serve the exact
cached renderer build with SPA fallback (from the `snaketron/` directory):

```sh
node skin-factory/scripts/serve-renderer-bundle.mjs client/web/dist 127.0.0.1 3000
```

Do not use `npm start` for factory evidence: that command compiles a separate
development bundle. Every attempt pins a manifest of the cached HTML,
JavaScript, CSS, and WASM bytes; the Playwright capture hashes the bytes it
actually receives, and Python independently rejects a missing or mismatched
attestation before accepting pixels.

Then run:

```sh
./scripts/hermes-smoke.sh
```

This launches Chromium, checks the pinned Gemini and worker model identifiers,
checks the Snaketron health endpoint, verifies the frozen LaMa environment and
model hash, loads the model and performs one bounded 32x32 inference with
network access disabled, verifies all retained object hashes, and performs
SQLite integrity/migration checks. It does not call a paid image provider.

After reviewing those results, one explicit paid/resumability smoke cycle is:

```sh
./scripts/hermes-smoke.sh --run-once
```

## Status and review gallery

Scheduler plus durable factory status:

```sh
./scripts/hermes-status.sh
```

Launch the human surface in a separate process carrying the operator identity:

```sh
uv run factory serve \
  --config config/factory.yaml \
  --env-file "$HOME/.config/snaketron-skin-factory/operator.json"
```

The gallery binds to `127.0.0.1` by default. It accepts an Authorization
`Bearer` review token for API callers and provides a same-site, HttpOnly local
browser session after sign-in. Every action records the authenticated actor.
Prototype approval binds exact retained bytes. A production build opens a
journaled server review request for its exact rendered revision/content hash
before it enters final review. Publication binds and immediately publishes
that exact private revision/content hash. Final rejection first journals an
exact cancellation; the server condition prevents a stale rejection from
clearing a newer pending revision. If that cancellation is unknown or
conflicts, the local attempt remains in final review for reconciliation.

Optimizer and technique trials use a durable server `evaluation` namespace.
They can be fetched by the factory service for real-browser evidence, but the
server refuses publication requests, publication decisions, catalogue entry,
purchase, and equipment for that namespace.

In shadow mode, machine evaluation scores and reasons stay hidden until an
independent `prototype_label` or `build_quality_label` is submitted. A label
never starts a build or publishes; the separate approval forms remain required.

## Calibrate before production routing

Keep `mode: shadow` while collecting independent labels.

1. Let `run-once` fill the bounded prototype inbox.
2. Label prototypes blind, then separately approve exact usable directions.
3. Label completed builds blind, then separately publish or reject them.
4. Browse Machine rejected regularly; re-evaluate retained bytes or create a
   linked retry from prototype, assets, or build with literal feedback.
5. Check `factory status --json` for prototype/build calibration and WIP.
6. Use `factory optimize --if-ready --target authoring-playbook --json` only
   after the configured evidence threshold. Promotion remains held-out and
   bounded to the playbook.

The promotion holdout is sealed by `optimizer.holdout_epoch`. Each concept is
queried at most once in an epoch and is excluded from later optimizer datasets
in that epoch. Rotate the epoch only as an intentional, reviewed config change
when retiring the old holdout; the new value deterministically repartitions the
eligible corpus and leaves the prior query ledger intact.

Do not switch to production routing until the PRD's labeled sample and false-
approve/false-reject bounds are satisfied.

## Backup and recovery

Create a new, non-overwriting backup directory containing a consistent SQLite
snapshot, immutable object tree, and checksummed manifest:

```sh
uv run factory backup --config config/factory.yaml --json
```

Backups default under `var/backups/<UTC timestamp>`. Copy that directory to the
deployment's protected backup storage. Before a restore, pause/remove the
Hermes job, preserve the current `var/`, verify `manifest.json`, and restore
both `factory.sqlite3` and `objects/` together. Run offline and online doctor
checks before recreating the schedule. Never delete an operation with an
unknown outcome; resolve it with evidence through the gallery or
`factory resolve-operation`.

For a recovered result, the resolution file binds enough provenance to replay
the exact semantic result rather than merely pointing at bytes:

```json
{
  "resolution": "executed_result_recovered",
  "evidence_ref": "provider:audit:request-123",
  "result_hash": "sha256:<retained-object-hash>",
  "resolved_model": "exact-provider-model-version",
  "provider_request_id": "request-123",
  "media_type": "image/png"
}
```

`media_type` is required for recovered image-generation operations and omitted
for structured text/API results. The provider request ID is retained whenever
the provider exposes one. A recovered image without exact media/model metadata
stays unresolved rather than being replayed under guessed provenance.

## Scheduler rollback

```sh
./scripts/hermes-rollback.sh
```

Rollback removes only the recorded cron and the wrapper when it still matches
this checkout. It intentionally preserves the service env, virtual
environments, object store, database, and backups. A missing or malformed job
id is not guessed from unrelated crons.
