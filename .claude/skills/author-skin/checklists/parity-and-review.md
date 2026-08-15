# Before opening the PR

## The classic skin did not move

`cargo test -p client` must pass with the golden traces untouched. If
`skin::goldens` fails, you changed shared code, not just your skin. Read the
reported first divergence before blessing anything — the trace is the
definition of "the default look still looks the same", and re-recording it is
how that guarantee gets thrown away by accident.

These are behaviours, not bugs. If a diff touches one, you broke something:

- single-cell snakes get outline layers but **no** occlusion mask and **no**
  white head overlay
- the occlusion mask paints *before* the outline layers
- boost widens the mask from 1px to 3px per side
- dead snakes paint *before* the goal walls; carried-food labels paint *after*
- the corpse always uses its own 1px extents, never the skin's overhang
- the head gradient counts a corner once, in the run that reached it

## Your skin is honest

- `cargo test -p client skin_conformance` — unfiltered. Covers boost
  visibility, overhang honesty, flat-hex colours, hue windows, animation
  op-count invariance, and reduced-motion stillness.
- `cargo run -p skin-schema --bin validate-skin -- <file>` for documents.
- `cargo clippy --all-targets` and `cargo fmt --all` clean.

## You looked at it

- Fresh `wasm-pack build` in *this* worktree before opening `/qa/skins`.
  A symlinked `node_modules` will serve the main repo's stale WASM and show you
  someone else's build.
- Every tile on the contact sheet painted — no blank canvases, no error
  captions.
- Small cell sizes still read as a snake with a head.
- The two within-team shades look like the same team.
- Animated? Watched it in the **Live** section of `/qa/skins`, in a real
  focused window — a hidden or embedded pane freezes rAF and every skin looks
  static there. It reads as alive rather than as flicker, and the
  reduced-motion tile is genuinely still.
- A wave? Checked it on a short snake too. A crest spacing wider than the body
  shows nothing, and most snakes are short for most of a match.

## It is actually selectable

- Registered in `client/src/skin/registry.rs`.
- Id added to `CATALOG` in `server/src/skin_catalog.rs`. The catalogue-match
  test fails if you forget, which is the point.
- Picked it on `/qa/skins` and confirmed it persists.

## The PR says enough

- Contact sheet committed under `docs/screenshots/skins/<name>/`.
- **The screenshots are embedded in the PR body, not just committed.** A skin PR
  is a visual change, and a reviewer should not have to check out the branch to
  see it. At minimum: the `all-skins` comparison sheet, and per skin the roles
  strip and — if it animates — the animation film strip. The rest goes in a
  `<details>` block so the body stays readable.
- **Image URLs are absolute and pinned to the commit.** Relative paths do not
  resolve in a PR body; they render as broken links. Use
  `https://raw.githubusercontent.com/<owner>/<repo>/<sha>/docs/screenshots/...`
  for images and `.../tree/<sha>/...` for folder links. Pin the **SHA**, not the
  branch: branch URLs break the moment the branch is deleted after merge, and a
  branch name containing a `/` is ambiguous in a raw URL. Then actually check
  them — `curl -o /dev/null -w '%{http_code}'` over every URL in the body.
- A line telling a reviewer how to see it move: `/qa/skins` after a fresh
  `wasm-pack build`, because the film strip is fixed samples and never animates.
- If it is a Rust skin: the one specific effect that the document schema could
  not express.
- If you changed anything shared: what, and why the goldens still pass.
