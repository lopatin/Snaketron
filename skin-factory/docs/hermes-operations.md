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

Create a dedicated, durable Snaketron account through `/api/auth/register`.
Do not add its numeric user id to `SNAKETRON_ADMIN_USER_IDS`; do not reuse an
administrator, stress-test, or guest account. Its ordinary `/api/auth/login`
JWT expires after 24 hours and is **not** a factory service credential. Never
put that login JWT in the service JSON.

An administrator provisions the service account's dedicated credential through
`POST /api/admin/factory-credentials`. The server returns the opaque 256-bit
token once and persists only its SHA-256 digest. The token has no time-based
expiry: it remains valid across unattended Hermes ticks until an administrator
rotates or revokes it. Every request still reloads the credential and account,
so revocation or accidental guest/stress/admin drift takes effect immediately.

Put a current, short-lived administrator login JWT in the owner-private
operator JSON as `SNAKETRON_FACTORY_OPERATOR_TOKEN`, then use the helper. It
reads both secrets from private files, writes the one-time response atomically
to the service JSON with mode `0600`, and never prints raw token material:

```sh
python3 scripts/manage-service-credential.py provision \
  --base-url https://snaketron.example \
  --operator-env "$HOME/.config/snaketron-skin-factory/operator.json" \
  --service-env "$HOME/.config/snaketron-skin-factory/service.json" \
  --user-id 12345
```

The helper refuses remote plain HTTP, URL userinfo/path/query/fragment, and
all redirects so the short-lived administrator bearer can never be forwarded
to another origin. Plain HTTP is accepted only for an explicit loopback host.

The deployed server must have texture storage enabled. The resulting
`snk_factory_v1.<credential-id>.<secret>` bearer is intentionally accepted only
for the exact factory routes: capability inspection, private/evaluation skin
creation and revision, private document reads, forge upload, and opening an
exact publication-review request. It cannot equip or purchase cosmetics, spend
through server-side generation, cancel a human request, publish, or call any
administrator route.

The authenticated, side-effect-free `GET /api/factory/capabilities` probe is
the authority check and accepts only this dedicated credential type, never an
ordinary login JWT. Its envelope includes the credential id, `revocable: true`,
and `expiresAt: null`. Online doctor requires that the account can create
private production skins, permanently non-publishable evaluation skins, and
private forge textures, while `publishSkins`, `administerSkins`, and `isAdmin`
are all false. Final soft-triage overrides, exact-request cancellation, and
publication use the separate admin/operator token. That token must never be
issued to the cron identity or task worker.

### Rotate or revoke the service credential

Rotate on the normal secret-rotation schedule and immediately after suspected
exposure. Rotation atomically activates a new stored digest and revokes the old
credential on the server, then the helper atomically replaces the private local
token:

```sh
python3 scripts/manage-service-credential.py rotate \
  --base-url https://snaketron.example \
  --operator-env "$HOME/.config/snaketron-skin-factory/operator.json" \
  --service-env "$HOME/.config/snaketron-skin-factory/service.json"
```

If the client is interrupted after the server commits but before the local
file is replaced, the old token correctly remains revoked and the one-time new
secret is lost. Recover by running `provision` again; never re-enable the old
digest. Run `./scripts/hermes-smoke.sh` after rotation so the live capability
probe confirms the new identity before the next paid tick.

Emergency revocation removes the matching local token after the server confirms
revocation. Hermes then fails closed until a new credential is provisioned:

```sh
python3 scripts/manage-service-credential.py revoke \
  --base-url https://snaketron.example \
  --operator-env "$HOME/.config/snaketron-skin-factory/operator.json" \
  --service-env "$HOME/.config/snaketron-skin-factory/service.json"
```

When the local token is already unavailable, pass its non-secret id with
`--credential-id <32-lowercase-hex>`. Rotating or revoking is administrator
authority and must never be available to the scheduled service credential.

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

## Prepare the install

From `skin-factory/`, install with an absolute service-env path:

```sh
./scripts/install-hermes.sh \
  "$HOME/.config/snaketron-skin-factory/service.json" \
  "every 6h"
```

This command prepares the checkout but deliberately creates **no cron job**.
A new installation cannot become a live provider spender until the online
checks, explicit paid smoke, and behavior-pin check below have all succeeded.

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
10. copies the fixed wrapper plus an owner-private exact-checkout locator under
    `~/.hermes/scripts/`; and
11. stops in a prepared, unscheduled state.

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

Hermes releases execute a no-agent script from their scripts directory even
when the cron stores a workdir. The wrapper therefore validates the private
locator, proves that the located checkout contains the exact installed wrapper
bytes, and only then enters that checkout. A modified, public, symlinked, stale,
or cross-checkout locator fails closed.

## Start the task worker

The default worker is the exact `qwen/qwen3.8-27b` identifier served from LM Studio's
OpenAI-compatible API at `http://localhost:1234/v1`. Load that exact model,
enable the local server, and confirm that both `/v1/models` and
`/v1/chat/completions` are available. The completion endpoint must support
strict `json_schema` response format, reject tool execution, accept prototype
images as data URLs for real authoring requests, and return the exact model id
in its response. LM Studio's Qwen thinking parser may return the schema-valid
JSON in `reasoning_content` with an empty `content`; the adapter accepts only
that narrow representation and still validates the exact closed
`WorkerResult` schema. If the endpoint requires authentication, add
`LMSTUDIO_API_KEY` to the owner-private service JSON.

The online doctor does more than query `/models`: it sends one bounded
side-effect-free WorkerRequest fixture with a generated image-only identifier
but no artifact references, network, tools, or asset work. It requires the
identifier in the returned document name, a schema-valid WorkerResult, a
procedural layer-only handoff, deterministic SkinDoc gates, and an exact
resolved-model match. Cron enablement fails if this real worker conformance
request fails.

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

Then run the online, non-generation checks:

```sh
./scripts/hermes-smoke.sh
```

This launches Chromium, checks the pinned Gemini and worker model identifiers,
checks the Snaketron health endpoint and authenticated least-privilege
capability envelope, verifies the frozen LaMa environment and model hash,
loads the model and performs one bounded 32x32 inference with network access
disabled, verifies all retained object hashes, and performs SQLite
integrity/migration checks. It does not create a Snaketron canary or call a
paid image provider.

After reviewing those results, opt in to one explicit paid/resumability smoke
cycle. This is the only command in the install flow that authorizes generation
spend:

```sh
./scripts/hermes-smoke.sh --run-once
```

On success it writes `var/hermes-paid-smoke.json` with mode `0600`. The marker
binds the exact current config, model roles, canonical skill commit and package
hash, direction/gates/capabilities, renderer execution bundle, frozen LaMa
behavior, and the numeric factory service-account user id observed by the live
capability probe immediately after that run. The rotatable credential id is
deliberately not pinned: rotating `snk_factory_v1` for the same account keeps
the proof valid, while substituting another otherwise-valid least-privilege
account fails before provider spend. Config, model, renderer, or LaMa drift
makes the marker stale and requires another explicit paid smoke. The only
skill-only change accepted without a new smoke is an exact automatic
author-playbook promotion backed by the optimizer's successful retained
operation, promoted run, and immutable Git ref; an arbitrary checkout or
database pointer change is refused.

Finally, make sure the Hermes gateway is installed/running, then explicitly
enable the schedule:

```sh
hermes gateway install
hermes cron status
./scripts/install-hermes.sh \
  "$HOME/.config/snaketron-skin-factory/service.json" \
  "every 6h" \
  --enable-cron
./scripts/hermes-status.sh
```

The enable invocation reruns the deterministic install and offline doctor,
then requires a successful online doctor (including real-worker conformance)
and a current paid-smoke marker before creating exactly one named cron with
`--script`, `--no-agent`, and the explicit repository `--workdir`. If the
gateway status says it is not running, the job is stored but cannot fire;
start the gateway before calibration.

The installer also writes `HERMES_CRON_SCRIPT_TIMEOUT` to the owner-private
Hermes `.env` at no less than the factory's 1,800-second tick budget plus a
120-second shutdown margin, then restarts and checks the gateway immediately
before cron creation. This overrides Hermes v0.14's 120-second no-agent script
default; enabling fails closed if the real gateway cannot reload it. The
non-secret timeout may remain after rollback because lowering it could kill an
unrelated long-running Hermes script.

The installed wrapper repeats the behavior-pin check and authenticated
least-privilege Snaketron probe immediately before **every** `run-once`. It
emits no readiness success document on stdout, so Hermes still receives one
run result. A stale pin, removed texture store, guest conversion, or accidental
administrator grant exits before the factory command can make a provider call.
The same guard compares the current capability envelope's service-account user
id to the paid-smoke marker on cron enablement and every scheduled tick.

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
that exact private revision/content hash. Final rejection, retry, and
re-evaluation first journal an exact cancellation; the server condition
prevents a stale action from clearing a newer pending revision. The linked
child is created only after known success or authenticated recovery proves the
old revision is neither pending nor published. If cancellation is unknown or
conflicts, the local Attempt remains in final review for reconciliation.

Optimizer and technique trials use a durable server `evaluation` namespace.
They can be fetched by the factory service for real-browser evidence, but the
server refuses publication requests, publication decisions, catalogue entry,
purchase, and equipment for that namespace.

In shadow mode, machine evaluation scores and reasons stay hidden until an
independent `prototype_label` or `build_quality_label` is submitted. A label
never starts a build or publishes; the separate approval forms remain required.
The label endpoint accepts only the exact prototype/contact-sheet artifact in
its eligible review state, after a hidden visual-judge evaluation has already
been retained. It records one transactional authority link to that evaluation:
an exact retry is idempotent, while a pre-judge, already revealed, wrong-state,
or conflicting duplicate label is rejected. In shadow mode the gallery and API
also withhold prototype approval or final publication until that exact artifact
has this blind label. Production mode keeps labeling and approval/publication
as independent actions without making a label a publication decision.

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

`factory status --json` also reports progress toward
`program.target_published_skins` (100 by default). Once that many distinct
production concepts have a human-approved published Attempt, `run-once` stops
creating new concepts with `published_target_reached` while review, gallery,
backup, and retained-artifact recovery remain available.

The same status surface exposes a generation halt when recent production
Attempts repeat one blocking deterministic gate, or when a high-confidence
authoring root-cause signature recurs after an `author-skin` promotion. These
halts deliberately leave the gallery, labeling, recovery, and already-active
work usable; resume generation only after fixing the platform regression or
reviewing the ineffective promotion.

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
uv run factory verify-backup --source var/backups/<UTC timestamp> --json
```

Backups default under `var/backups/<UTC timestamp>`. Copy that directory to the
deployment's protected backup storage. Manifest v2 binds the database hash,
every canonical CAS path/hash/size, a deterministic object-tree digest, and
every object reference reachable from the database. `verify-backup` checks all
of those facts plus SQLite integrity and refuses an omitted, substituted, or
unreferencedly truncated object tree. Before a restore, pause/remove the Hermes
job, preserve the current `var/`, run `verify-backup`, and restore both
`factory.sqlite3` and `objects/` together. Run offline and online doctor checks
before recreating the schedule. Never delete an operation with an unknown
outcome; resolve it with evidence through the gallery or `factory
resolve-operation`.

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
stays unresolved rather than being replayed under guessed provenance. Both the
CLI and authenticated gallery verify the CAS hash and bytes, exact configured
model identity (`service.resolved_model_pattern` pins Snaketron responses),
expected operation role, complete PNG/JPEG/WebP decode, or the side-effect's
strict structured schema before changing operation state. Every recovery also
requires the immutable journaled request CAS/ref/hash, including recovered
structured and image content. State-changing responses additionally bind their
semantics to that request: registration must name the exact
document/skin/revision, publication responses must name the exact review
authority, and Git recovery is accepted only when its deterministic ref and SHA
already equal the committed `active_behavior` pointer. Replayed
registration, publication-request, cancellation, and publish outcomes receive
an authenticated server readback before local state advances. If an
older/direct database recovery bypassed that boundary, the factory changes it
from `succeeded` to `failed_terminal` on first invalid replay, retains the exact
bytes and audit reference, and blocks the Attempt with an operator-action
message.

## Scheduler rollback

```sh
./scripts/hermes-rollback.sh
```

Rollback removes only the recorded cron, matching wrapper, and matching private
workdir locator. It intentionally preserves the service env, virtual
environments, object store, database, and backups. A missing or malformed job
id is not guessed from unrelated crons.
