# One-command local shadow calibration

The local launcher prepares a real, retained Skin Factory from this checkout.
It builds the current Snaketron server, provisions dedicated least-privilege
local identities, starts the pinned renderer and review gallery, and runs the
online non-generation doctor. It never runs a paid provider cycle unless the
operator explicitly selects `run` or `--generate`.

## Prerequisites

- Docker Desktop with `docker compose`;
- `uv`, Hermes, `wasm-pack`, Node.js/npm, Git, and `curl` on `PATH`;
- LM Studio 0.4 or newer serving at `http://127.0.0.1:1234/v1`, with the
  task-worker model installed; put its `lms` CLI on `PATH` for automatic loading;
- a Gemini key already exported in the invoking shell as `GEMINI_API_KEY`, or
  already retained in the owner-private service JSON; and
- network access for locked dependency/browser installation, the pinned
  205,803,670-byte Big-LaMa download when absent, Git tag fetch, and provider
  identity probes.

The launcher never sources `~/.zshrc`. If that file exports the key, load it in
the invoking shell first. The value is atomically installed in JSON with mode
`0600` and is never printed. An existing non-placeholder key or service token
is authoritative and is never overwritten.

## Start without generation spend

From the `snaketron/` directory:

```sh
./skin-factory/scripts/local-calibration.py start
```

On an empty local install, that one command:

1. creates an ignored, owner-private checkout identity and stable local JWT
   secret under `skin-factory/var/local-runtime/`;
2. builds and starts checkout-hash-named LocalStack, Valkey, and the current
   server image without stopping or reusing another checkout's containers;
3. creates dedicated local admin and factory accounts, restarts only this
   server with the admin's numeric ID, proves the factory account is not an
   admin, and provisions a revocable factory credential;
4. verifies the authenticated capability envelope permits private/evaluation
   skins and private forge textures but forbids publishing/administering;
5. installs frozen production dependencies, Chromium, the renderer bundle,
   and pinned LaMa assets without creating a Hermes cron;
6. pins the exact model ID selected from LM Studio's installed/advertised state,
   verifies its loaded-instance state, and uses `lms load` only when that exact
   installed model is not loaded; it never issues an unload command; and
7. starts the exact renderer and authenticated gallery, then runs live
   service, model, browser, SQLite/CAS, and LaMa checks.

If LM Studio advertises several plausible models, select the exact public
identifier explicitly:

```sh
./skin-factory/scripts/local-calibration.py start \
  --worker-model publisher/exact-model-id
```

`--worker-model` selects an exact identifier; it does not permit fallback to a
different model. If the selected local model is installed but unloaded, the
launcher idempotently runs:

```sh
lms load publisher/exact-model-id \
  --identifier publisher/exact-model-id \
  --yes
```

If `lms` is unavailable or loaded-instance state cannot be verified safely, the
launcher exits before installation and prints that exact recovery command. It
never issues an unload command or substitutes a different model identifier.
Automatic loading is bounded to five minutes and the exact loaded instance is
verified again afterward, including when another launcher wins the load race.
The generated local configuration also pins each task-worker request to a true
900-second end-to-end deadline, independent of HTTP client's per-phase timers.
This covers the large canonical skill, retained prototype, and strict SkinDoc
schema prefill plus structured generation on the supported 27B model. Factory
admission reserves that entire deadline and a settlement second before starting
the request, all within the 1,800-second tick wall.

Successful startup prints only the local gallery URL and an `open` command.
Open it without putting its review secret in a URL or the clipboard:

```sh
./skin-factory/scripts/local-calibration.py open
```

On the sign-in page, use the `SKIN_FACTORY_REVIEW_ACTOR` and
`SKIN_FACTORY_REVIEW_TOKEN` values retained in the owner-private
`~/.config/snaketron-skin-factory/operator.json`. The launcher deliberately
does not print the token, embed it in a URL, or copy it to the system
clipboard. The service credential is rejected by this human-only surface.

The default private credential files are:

- `~/.config/snaketron-skin-factory/local-accounts.json`
- `~/.config/snaketron-skin-factory/operator.json`
- `~/.config/snaketron-skin-factory/service.json`

All paths can be replaced with `--accounts-env`, `--operator-env`, and
`--service-env`. Use separate files if an existing token belongs to another
deployment; the launcher deliberately refuses to replace a present token even
when the isolated server rejects it. A partially completed identity bootstrap
fails with exact retry-safe account, restart, admin-confirmation, and
provisioning commands. `--admin-user-id` exists for that recovery path and
persists the ID in the checkout's private state.

## Run one real paid cycle

After `start` succeeds, this explicit action performs one real provider-backed
factory tick:

```sh
./skin-factory/scripts/local-calibration.py run
```

The first prototype tick sends Gemini the exact repository-owned blank snake
guide plus the locked
`skills/author-skin/references/design-guidelines.md`. The guide is a flat,
right-facing, continuous, round-capped 16-cell *review pose* derived from the
real renderer at its maximum live cell size; it is not the live spawn length.
Gemini is instructed to paint only that body. The selected prototype's manifest
binds the guide, geometry contract, and shared-guideline hashes, and the local
task worker receives those same retained inputs during the later build.

After the tick reaches prototype review, open the gallery, record the blind
prototype label, and separately approve the exact candidate you want built:

```sh
./skin-factory/scripts/local-calibration.py open
./skin-factory/scripts/local-calibration.py run
```

The second `run` resumes the retained approved Attempt through the same worker,
forge, private-registration, exact-browser, and final-review path used by the
Hermes job. At final review, record the blind build-quality label before the
separate publish or reject action. Repeat `run` only when `status` shows a
retained Attempt that still has machine work to advance; human review states do
not spend or advance themselves.

`--generate` is an alias. The first paid cycle for an exact behavior/config
must reach a retained `prototype_review` transition before the launcher writes
the paid-smoke readiness marker. A stale marker is not refreshed from a later
build stage. Once the current marker validates, later `run` actions safely
resume retained attempts without requiring another prototype transition.

The launcher invokes the same deterministic `factory run-once` operation used
by Hermes, but leaves cron disabled during local calibration. Enabling the
production scheduler remains the separate, explicit procedure in
[`hermes-operations.md`](hermes-operations.md); it requires a current paid
marker and repeats all live least-privilege checks before creating a spender.

## Isolation and lifecycle

The local runtime uses these loopback ports:

| Service | Port |
| --- | ---: |
| Snaketron HTTP | 18080 |
| Snaketron gRPC | 15051 |
| LocalStack | 14566 |
| Valkey | 16379 |
| exact renderer | 13000 |
| review gallery | 18765 |

It also uses a checkout-specific Compose project, container names, LocalStack
directory, DynamoDB table prefix, replay bucket, texture bucket, and replay
directory. The generated `skin-factory/var/local-runtime.yaml` points mutable
factory truth at `var/local-runtime/factory-data/`; it does not reuse the
canonical ignored `var/factory.sqlite3` or CAS. Only the installed, hash-pinned
LaMa environment/model is shared. Both S3 buckets are private, encrypted, and
configured to abort incomplete multipart uploads. LocalStack persistence is
enabled for this isolated runtime so account and credential records survive
container recreation.

Inspect or stop only this checkout's runtime with:

```sh
./skin-factory/scripts/local-calibration.py status
./skin-factory/scripts/local-calibration.py stop
```

`stop` verifies ownership, sends termination only to the recorded renderer and
gallery processes, and uses Compose `stop` rather than `down`. It preserves
the database, CAS, account secrets, buckets, and every unrelated container or
listener.
