---
name: author-skin
description: Design, validate, render, and self-review a new Snaketron snake skin end to end — as a data document when the schema can express it, or as Rust when it genuinely cannot. Use whenever asked to create, restyle, or theme a snake skin, a team base, or a goal celebration.
---

# Authoring a Snaketron skin

Read `specs/skins-prd.md` §5 and §7 once before starting. The short version:
a skin owns the pixels of one snake's body plus optional dressing for the team
base and the goal celebration. It does **not** own geometry, occlusion,
layering, or the decision about who is a friend — those are the renderer's, and
the boundary is what stops a skin from being able to misreport the game.

## 0. Pick the path

**Write a document** (`skin-schema/skins/<name>.skin.json`) unless you need
something the schema has no vocabulary for. The schema covers: per-role
palettes, head core size and colour, the head gradient, outline width, base
dressing, celebration dressing, and bounded animation over lightness and
gradient opacity.

**Escalate to Rust** (`client/src/skin/<name>.rs`) only for painting the schema
cannot describe — a real canvas gradient, a custom pass, geometry-dependent
detail. `client/src/skin/ember.rs` is the worked example: it uses the shared
body painter for everything and hand-paints only the one thing that needed it.

Escalating "because it will be easier" is how the document layer rots. If you
reach for Rust, say in the PR which specific effect forced it.

## 1. Write it

Start from `templates/skin.template.json` (or `templates/custom_skin.rs.tmpl`).
Every constraint the validator enforces is commented in the template.

The rules that are not negotiable, and why:

- **Friendly reads cool, hostile reads warm.** Team games are played through
  colour. A teammate who looks like an enemy is a competitive bug.
- **The Boost band stays `#fff200` at +6px** in documents. Opponents read that
  band to know you are boosting.
- **Reported colours are flat 6-digit hex**, even for a gradient skin — the
  results-table pill and the contrast maths need one representative colour.
- **The head core stays dark**, or the roster's white ready-check vanishes.
- **Animation varies paint arguments, never structure.** Same ops, different
  values. This is what keeps an animated skin as cheap as a still one.

## 2. Validate

```bash
cargo run -p skin-schema --bin validate-skin -- skin-schema/skins/<name>.skin.json
```

A dedicated binary, not `cargo test -- <name>`: everything after `--` in
`cargo test` is a test-*name* filter, so a novel filename matches zero tests and
exits green having validated nothing.

Errors name the field and say what the rule protects. Fix, re-run.

## 3. Register it

- Document skin: add the `include_str!` to `document_skins()` in
  `client/src/skin/registry.rs`.
- Rust skin: add the field and the `entries()` line in the same file.
- Both: add the id to `CATALOG` in `server/src/skin_catalog.rs`. The two lists
  are compared by a test, so forgetting one fails the build rather than
  silently giving players a skin that turns back into classic at join.

## 4. Look at it

```bash
cd client && wasm-pack build --target web --out-dir pkg   # ALWAYS rebuild first
cd web && npm start
```

Open `/qa/skins`, pick the skin, and **actually look**. The tiles are painted by
the real renderer against the same fixture corpus the golden traces use, so
what you see is what ships.

The **Live** section is the only place motion is visible — it runs off the real
animation-frame clock, exactly as the arena does, with a play/pause control.
Everything below it paints one fixed sample, which is what makes a screenshot of
it reproducible. If you are judging an animation, judge it there.

Note that an embedded or backgrounded browser pane freezes `requestAnimationFrame`
entirely (`document.visibilityState === 'hidden'`), so live tiles will sit
perfectly still through no fault of the skin. Look in a real, focused window.

Then capture the sheet and read the images back:

```bash
cd client/web
node tests/capture-skin-sheet.mjs http://localhost:3100 <skin-ref> ../../docs/screenshots/skins/<name>
```

Review against the brief. Things worth checking that tests cannot: does the
head read as a head at small cell sizes? Do the two within-team shades actually
look like the same team? Does the animation read as alive or as flicker — and
does it still read at all on a *short* snake, where a long wave has no room to
show itself?

**The stale-WASM trap:** a fresh worktree with a symlinked `node_modules` will
happily serve the *main* repo's old WASM, so your skin appears not to exist or
renders as classic. If the catalogue looks wrong, rebuild before debugging
anything else.

## 5. Prove it

```bash
cargo test -p client skin_conformance   # always unfiltered — see below
cargo test -p client                    # includes the classic golden traces
```

Never filter conformance to your skin's name: a typo would match zero tests and
exit green. The suite discovers every registered skin itself.

The classic golden traces must be **untouched**. If they moved, you changed
shared code, not just your skin — go find out what.

## 6. Ship it

PR with the contact sheet committed under `docs/screenshots/skins/<name>/`, the
conformance and golden runs green, and one line on why the skin needed Rust if
it did.

## Files

- `templates/skin.template.json` — every field, commented
- `templates/custom_skin.rs.tmpl` — Rust escalation skeleton
- `checklists/parity-and-review.md` — what to check before opening the PR
- `skin-schema/skins/classic.skin.json` — the current look as a document, and
  the interpreter's own regression fixture
- `client/src/skin/fixtures.rs` — the shared pose corpus
