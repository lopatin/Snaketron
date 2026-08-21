# Skin Factory review UI captures

These screenshots use deterministic disposable SQLite/object-store fixtures and
the real authenticated FastAPI gallery. They do not contain production data or
credentials.

- `gallery-all.png` shows retained production, rejected, published, and
  experimental attempts together with visual previews and lineage state.
- `gallery-machine-rejected.png` shows rejected prototype and completed-build
  artifacts plus the bulk retry-from-stage workflow.
- `prototype-review-detail.png` shows an exact retained prototype, its content
  hash/provenance, and blind-evaluation protection before a human label.
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
d190532f193712f69b42893a746781f0383a3b36564671ce8016c2da1c996a72  gallery-all.png
7e275adf5a757ea074dd443767c0cce6f4fb85e4ee3460d2659f6316a99e58a3  gallery-machine-rejected.png
2c54a1beebc23d837a09da5e03ad6578fbfebc80706cc55c16815d7934322d79  prototype-review-detail.png
c6f154fd49a822bdc6122043bf7c9561c234890681977a86d212359d07edf593  soft-triage-override.png
```

The retained real-run montage is:

```text
5621b0153e57e2dfcd0de7a65fd827d46b48384770a609201e55385c02c70fde  local-real-gemini-prototypes.jpg
```
