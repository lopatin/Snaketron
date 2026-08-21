# Skin Factory review UI captures

These screenshots use deterministic disposable SQLite/object-store fixtures and
the real authenticated FastAPI gallery. They do not contain production data or
credentials. Their snake previews use the same thin, round-capped, continuous
body geometry as the live renderer rather than the earlier linked-circle
schematic.

- `gallery-all.png` shows retained production, rejected, published, and
  experimental attempts together with visual previews and lineage state.
- `gallery-machine-rejected.png` shows rejected prototype and completed-build
  artifacts plus the bulk retry-from-stage workflow.
- `prototype-review-detail.png` shows the exact behavior-pinned blank geometry
  guide beside the candidate, contract/guide hashes, native and presentation
  scales, retained candidate provenance, and blind-evaluation protection.
- `soft-triage-override.png` shows a retained machine-rejected prototype and
  the exact-artifact human override, feedback-only annotation, re-evaluation,
  and linked-retry actions.
- `local-real-gemini-prototypes.jpg` is a montage of the three exact prototype
  images retained by the first real local calibration run on 2026-08-21. The
  run used `gemini-3.7-flash` for concept/triage and
  `gemini-3-pro-image` for pixels, then stopped at blind prototype review.
  Unlike the gallery fixtures, provider output is intentionally not
  byte-reproducible.

Regenerate from `skin-factory/` with:

```sh
./scripts/capture-gallery-screenshots.sh
```

The command seeds a temporary database, launches the gallery on an ephemeral
localhost port, captures through the repository's Playwright Chromium, and
losslessly recompresses the PNGs with Pillow.

The final PR #90 captures reproduced byte-for-byte on two consecutive runs:

```text
cc65be2da7d38590f557e1ae4da74641757c7337a75df6717682a2e142981497  gallery-all.png
46d0cc17bbb955e2a02be4aa0583fdad1d29b0381dabff0148067b1e883dbcf4  gallery-machine-rejected.png
c9c9a39e88ab0f1e86674611da4ec35e02ca200eb3ff69a4e4a02bd241473aae  prototype-review-detail.png
df1fbbb3e3b3e5fc9f78041629bcbbb1efc41a304d17b6efb15a2a916d94641f  soft-triage-override.png
```

The retained real-run montage is:

```text
5621b0153e57e2dfcd0de7a65fd827d46b48384770a609201e55385c02c70fde  local-real-gemini-prototypes.jpg
```
