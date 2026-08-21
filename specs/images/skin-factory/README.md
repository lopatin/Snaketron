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

Regenerate from `skin-factory/` with:

```sh
./scripts/capture-gallery-screenshots.sh
```

The command seeds a temporary database, launches the gallery on an ephemeral
localhost port, captures through the repository's Playwright Chromium, and
losslessly recompresses the PNGs with Pillow.
