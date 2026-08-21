# Validation and evidence

Validate in this order and retain every report, including failures.

1. **Input binding:** approval hash equals prototype hash; direction, skill,
   capability, schema, renderer, and gate SHAs are pinned.
2. **Plan:** validate `implementation-plan.json`; every fidelity feature has an
   owner, image layer has a fallback, asset X/Y and wrap use agree, and the path
   matches its requests.
3. **Document:** parse and validate SkinDoc v2 with the repository binary. Do
   not use a test-name filter that can match zero tests.
4. **References:** resolve owned/shareable immutable texture descriptors, kinds,
   exact variants and hashes before compilation.
5. **Assets:** gate exact served variants for dimensions, bytes, grid, required
   seams, multi-scale alignment, detail/chroma, mark scale, temporal continuity,
   and loop behavior.
6. **Conformance/cost:** run the pinned operation budget and conformance corpus
   without narrowing it to the new skin's name. Animation may vary arguments,
   never operation sequence.
7. **Real renderer:** use the cached WASM/browser bundle pinned to renderer SHA.
   Wait for atlas readiness, assert image pixels painted, then capture contact
   sheets for friendly/enemy and short/median/long/one-cell/corner/boost poses.
   For motion capture row zero, representative frames, final row, loop to zero,
   normal play, reduced motion, and slow motion.
8. **Fidelity package:** pair prototype and render at comparable scale with the
   plan's ordered fidelity features, all gate reports, and repair lineage.

The canonical document command is:

```bash
cargo run -p skin-schema --bin validate-skin -- path/to/skin.skin.json
```

The factory driver supplies the authoritative commands for asset forge,
registration, conformance, and browser capture in its capability manifest.
Never substitute an old hard-coded `/32.png`, 20-row assumption, stale WASM,
or a native fallback screenshot.

A successful worker result means the requested artifacts and evidence conform
to their schemas. It does not mean the skin is approved or published.
