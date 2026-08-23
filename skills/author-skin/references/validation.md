# Validation and evidence

Validate in this order and retain every report, including failures.

1. **Input binding:** the explicit `input_authority` mode and artifact hash
   equal the projected prototype `image_sha256`; `source_image_sha256` remains audit-only, and
   `geometry_projection` is exactly `prototype-body-mask-v1`. The projected
   image—not the raw provider source—is the only authoring input. Direction,
   skill, capability, schema, renderer, and gate SHAs are pinned.
2. **Plan:** validate `implementation-plan.json`; its exact bounded
   `design_guidelines` object names one artistic direction, primary structure,
   four-cell/early-growth/long-turn behavior, supported head-zone polarity, and
   an asset strategy that agrees with executable seams and cadence. Every
   fidelity feature has an owner, every image layer has a fallback, asset X/Y
   and wrap use agree, and the path matches its requests.
3. **Document:** parse and validate SkinDoc v2 with the repository binary. Do
   not use a test-name filter that can match zero tests. For every band, use
   the pinned baked frames to prove
   `max_frame(abs(t_center)) + max_frame(abs(half_width)) <= 0.5`; independently
   valid lane fields do not establish this combined bound.
4. **References:** resolve owned/shareable immutable texture descriptors, kinds,
   exact variants and hashes before compilation.
5. **Assets:** gate exact served variants for dimensions, bytes, grid, required
   seams, multi-scale alignment, detail/chroma, mark scale, temporal continuity,
   and loop behavior. For every modifier, prove a one-to-one asset/texture/image
   layer/fallback mapping plus exact identity, manifest/content hash, license,
   provenance, and authorized-lineage scope. Extraction must fail closed on a
   contaminated background, matte fringe, or ambiguous alpha; retain the
   transparent RGBA object or exact mask/matte and its report.
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
9. **Authority ceiling:** an `approved_prototype` build may proceed only as far
   as the factory's private revision/review flow. A `draft_submission` may only
   be privately uploaded/registered and sent for admin review by an advertised
   interactive host. Neither mode publishes; later admin approval binds the
   exact revision/contentRef.

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
