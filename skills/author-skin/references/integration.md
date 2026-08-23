# Agent and factory integration

`skills/author-skin/` is canonical. The factory loads this directory explicitly,
validates it with `scripts/validate_package.py`, hashes the complete package,
and includes the pinned bundle in `WorkerRequest`. Production correctness must
not depend on ambient skill discovery.

Thin repository wrappers exist for interactive tools:

- Claude-compatible: `.claude/skills/author-skin/SKILL.md`
- Codex/Agent Skills-compatible: `.agents/skills/author-skin/SKILL.md`
- Hermes operator environments: `hermes/skills/author-skin/SKILL.md`

Each wrapper instructs the agent to load this canonical file and has no
authoring rules of its own. If a host cannot resolve cross-directory references,
install or bundle the canonical directory itself at that host's normal skill
root. Do not copy the content and let it drift.

The skill has no Claude, Codex, Hermes, Gemini SDK, or shell initialization
dependency. A factory task worker returns structured requests; the driver owns
all external actions. The legacy/local-review route accepts only
`approved_prototype`. The queue-only private-draft Factory host may additionally
accept `draft_submission` with maximum action `request_admin_review`, and
extraction,
modifier reuse, or video only when the user has authorized the workflow and
the host advertises the exact private-upload, request-review, journaling,
retention, and media capabilities required by the schemas. Unsupported
operations return `platform_gap`; the skill must not improvise shell/provider
calls.

The bundled queue-only automation currently advertises video materialization
but not cross-concept or within-batch modifier reuse. Its queue authority is
one current concept at a time. `author-skin` must return `platform_gap` for
reuse unless a future host supplies an explicit batch-lineage scope plus an
exact content-addressed catalog and provenance policy.

The bundled queue-only draft host preserves `raster_overhang_px` from 0 through
4 only for an exact materialized modifier catalog record. Its retained RGBA
sheet, apron-aware forge ladder, descriptor, upload, and readback all bind the
same value. Legacy/direct image generation remains a zero-overhang path;
nonzero direct generation returns `platform_gap` before asset-provider spend.
Every other host must advertise and prove an equally complete path; never
silently downshape overhang to zero.

For `draft_submission`, the host retains the selected candidate bytes,
selection rationale, and selection-record hash. It may upload/register only a
private draft and request Snaketron admin review. It must not route the draft
through the factory's human-approval fields, call it approved, or publish it.
Admin approval later binds the exact private revision/contentRef outside this
skill.

An authorized direct interactive host follows this sequence:

1. Retain and hash the exact candidate, manifest, selection/approval record,
   schemas, capability manifest, and user authority.
2. Run the pure authoring pass. Journal each advertised generation,
   extraction, deterministic video-frame, reuse, forge, and binding operation;
   retain every input/output/report and stop with `platform_gap` before any
   unsupported operation.
3. Validate and bind exact immutable descriptors/content refs, then register
   only a private revision through the advertised Snaketron operation.
4. Capture exact real-renderer evidence and request admin review for that exact
   revision/contentRef. Do not call publication from this skill.

Repository maintainers can validate the package and all four route fixtures
with:

```bash
python3 skills/author-skin/scripts/validate_package.py --cargo
python3 -m unittest discover -s skills/author-skin/tests -v
```

Use `--candidate-playbook PATH` on the first command before accepting a GEPA
candidate; it rejects changes outside the one marked playbook body.
