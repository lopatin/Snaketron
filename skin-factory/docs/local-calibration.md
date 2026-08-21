# One-command local shadow calibration

The local launcher prepares a real, retained Skin Factory from this checkout.
It builds the current Snaketron server, provisions dedicated least-privilege
local identities, starts the pinned renderer and review gallery, and runs the
online non-generation doctor. It never runs a paid provider cycle unless the
operator explicitly selects `run` or `--generate`.

## Prerequisites

- Docker Desktop with `docker compose`;
- `uv`, Hermes, `wasm-pack`, Node.js/npm, Git, and `curl` on `PATH`;
- LM Studio serving a loaded task-worker model at `http://127.0.0.1:1234/v1`;
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
6. pins the exact model ID advertised by LM Studio into the ignored runtime
   config; and
7. starts the exact renderer and authenticated gallery, then runs live
   service, model, browser, SQLite/CAS, and LaMa checks.

If LM Studio advertises several plausible models, select the exact public
identifier explicitly:

```sh
./skin-factory/scripts/local-calibration.py start \
  --worker-model publisher/exact-model-id
```

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
