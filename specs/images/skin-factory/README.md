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
  the exact-artifact human override that can route it back to prototype review.

Regenerate from `skin-factory/` with:

```sh
./scripts/capture-gallery-screenshots.sh
```

The command seeds a temporary database, launches the gallery on an ephemeral
localhost port, captures through the repository's Playwright Chromium, and
losslessly recompresses the PNGs with Pillow.

The final PR #90 captures reproduced byte-for-byte on two consecutive runs:

```text
1c85fbf47af220709f750487ff2e161c2e82b5a8f0232bfb19607d4a73cb87c6  gallery-all.png
fccd940022ba246aac3087cb2d86cfd052e59ec5505bf3f1b34e83326327a599  gallery-machine-rejected.png
2c54a1beebc23d837a09da5e03ad6578fbfebc80706cc55c16815d7934322d79  prototype-review-detail.png
b4bf591ecfbb12618e800761fc91098b7c0fc30dd762c22f220c41414bd8ef70  soft-triage-override.png
```
