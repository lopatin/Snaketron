# Draft automation operations

The executable driver is `skin-factory` and the recurring entry point remains
the existing Hermes-safe `factory run-once` command. The prompt inbox and
Factory database are durable; re-running a command resumes exact journaled
operations.

From `skin-factory/`:

```sh
uv run factory enqueue-draft "Skin name" \
  --brief "A complete visual concept with game-scale shape and material detail." \
  --motion "A true cyclic motion intent, or explicitly say that the skin is static."
uv run factory draft-status
uv run factory run-once \
  --config config/factory.yaml \
  --env-file "$HOME/.config/snaketron-skin-factory/service.json"
```

The checked-in production config has draft automation enabled. An installed
Hermes job consumes the inbox automatically, so the third command is only the
production-equivalent manual tick for testing or recovery; it is not a daily
operator step.

Before a scheduler can use Fal, import the key once into the owner-private
service environment without printing it:

```sh
uv run python scripts/manage-service-credential.py import-fal \
  --service-env ~/.config/snaketron-skin-factory/service.json
```

The import reads an already-exported `FAL_API_KEY`/`FAL_KEY`, or a fresh zsh
login context, and writes only canonical `FAL_API_KEY` to a mode-0600 JSON
file. Scheduled runs read that file; they never source `.zshrc`.

The driver may advance only this sequence:

1. immutable inbox item and retained concept artifact;
2. retained prototype candidates and deterministic safe selection;
3. hash-free media preplan from this skill;
4. journaled endpoint generation, paid Fal submit, retained queue ticket,
   repeatable ticket polling, deterministic frame extraction, and exact forge;
5. final `author-skin` pass using the exact retained modifier catalog;
6. exact private revision upload and an exact Admin review request.

It must never call publish. Missing credentials, cost or wall admission,
provider capability, ffmpeg, matte verification, loop evidence, renderer
limits, or exact server authority are typed fail-closed states. A paid Fal
submit persists its queue request id before polling; a crash resumes the poll
and never submits a replacement request blindly.

The planning worker cannot choose a media license. The driver derives the
authoritative provider-generated/current-concept license and provenance only
from the operations and bytes it actually retained. Cross-concept or
within-batch modifier reuse is not yet an advertised capability: each queue
item is an independently authorized concept lineage, so a requested reuse must
remain a retained `platform_gap` until the inbox has an explicit batch-lineage
authority and content-addressed reuse contract. Do not claim or emulate reuse
by copying a hash from another Attempt.
