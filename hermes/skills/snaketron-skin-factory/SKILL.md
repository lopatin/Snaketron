---
name: snaketron-skin-factory
category: snaketron
description: Run and inspect the production Snaketron skin factory through its deterministic, provider-neutral CLI.
tags: [snaketron, skins, hermes, factory]
platforms: [macos, linux]
---

# Snaketron Skin Factory

This is the thin Hermes operator wrapper for
`specs/skin-factory-prd.md`. The `factory` CLI owns the state machine,
provider calls, persistence, gates, budgets, leases, and retries. Do not
reimplement those responsibilities in the agent.

The scheduled Hermes cron runs the same CLI through a non-agent wrapper. This
skill handles human status and recovery requests; it is not required to reason
through routine production ticks.

The factory is model-role based. Its versioned config selects the task worker,
Gemini 3.7 Flash smart/judge roles, Gemini 3 Pro Image generation, and the image
editor. Never substitute a model silently.

## Scheduled run

Run exactly one resumable cycle:

```bash
factory run-once --json
```

Read the structured result and report only:

- attempts created, resumed, completed, or rejected;
- items newly waiting for prototype review or final review;
- spend against caps;
- pause/halt reason and the action required;
- whether an optimization candidate was promoted.

The command runs its own doctor checks, including optimizer readiness, and is
idempotent at each factory stage. If Hermes is interrupted, invoke it again;
the operation journal reconciles external calls and reports an unknown outcome
instead of blindly repeating spend. Do not repair state files by hand.

## Preflight failures

Stop and report the CLI's exact error when it identifies:

- another live lease;
- a missing or mismatched pinned provider/model;
- missing service credentials;
- a budget or WIP cap;
- unavailable database/object storage/browser renderer;
- corrupt state or a failed integrity check.

Do not source an interactive shell profile to discover credentials, print
secrets, switch providers, clear a halt, or bypass a deterministic gate.

## Human requests

Use the CLI rather than editing the database or repository directly:

```bash
factory status --json
factory re-evaluate artifact_01 --json
factory retry attempt_01 --from build --feedback-file feedback.txt --json
factory resolve-operation operation_01 --resolution-file resolution.json --json
factory pause --reason-file pause-reason.txt --json
factory resume --json
```

These operator commands require an authenticated human credential. Prototype
and final approvals are issued through authenticated review actions that bind
an exact artifact or revision hash. The cron service identity has no permission
to approve a prototype, resolve an unknown provider operation, publish,
override blocking gates, or resume a human halt. Operation resolution requires
evidence and records one of the PRD's immutable outcomes. Gates marked
`blocking: true` in the shared manifest are never overrideable.

## Optimization

`run-once` automatically enqueues or advances eligible optimization work.
Optimization is also available on demand to an operator; it is not an
interactive-agent dependency or a second required schedule:

```bash
factory optimize --if-ready --target authoring-playbook --json
```

The command freezes its dataset, calls Gemini 3.7 Flash as GEPA reflection
teacher, runs the configured task worker as student, evaluates candidates, and
atomically promotes only an allowed playbook diff that passes the PRD's gates.
Hermes reports the result; it does not edit the skill, rubric, direction, or
tests itself.

## Hard rules

1. Every concept and artifact is retained, including machine rejects.
2. Re-evaluation appends an evaluation; retry creates a child attempt.
3. Operational state belongs in the platform database/object store, never in
   ad-hoc JSONL or per-tick git commits.
4. Behavioral artifacts are pinned for an attempt and change only between
   attempts.
5. A machine may triage but may not start a build. Human `prototype_approval`
   binds the exact build-reference artifact; a training label never authorizes
   execution.
6. Human `publish_approval` binds the reviewed revision/content hash and
   publishes it immediately; shadow labels never publish.
7. The repository is canonical for direction, skills, rubrics, fixtures, and
   config. Notion and agent transcripts are optional import sources only.
8. Workers never receive external side-effect credentials. Only the factory
   driver may execute their structured requests through the operation journal.
