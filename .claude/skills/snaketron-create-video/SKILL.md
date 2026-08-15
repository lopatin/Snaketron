---
name: snaketron-create-video
description: Produce, revise, and quality-check frame-exact SnakeTron trailers and promotional videos from ScenarioCanvas scenarios or replay clips. Use for capture planning, clip-library reuse, source-master EDL authoring, true high-VFPS slow motion, beat-synced editing, branded title cards, ffmpeg preview/final rendering, or trailer QC.
---

# Create a SnakeTron video

Follow this workflow. Keep source timing in structured JSON; never hand-author a filtergraph.

1. Turn the brief into a 30–45 second shot list with one idea per shot and an explicit payoff.
   Start from `assets/launch-trailer/brief.md` and `launch-trailer.edl.json` for the launch sequence.
2. Reuse `tools/video/clips/<slug>/master.mkv` plus `meta.json` when its scenario, seed, and `capture_vfps` fit. Otherwise run `node scripts/capture.mjs ...`. Capture slow motion at the VFPS required by the intended rate.
   For rank-up and leaderboard hero shots, serve and capture the deterministic fixtures described in [references/fixture-cards.md](references/fixture-cards.md).

   ```bash
   node scripts/capture_card.mjs --card rank-up --out tools/video/clips/rank-up-card --capture-vfps 60
   node scripts/capture_card.mjs --card leaderboard --out tools/video/clips/leaderboard-card --capture-vfps 60
   ```
3. Author an EDL using [references/edl.md](references/edl.md). Place effects, text, and SFX against `meta:` cues in source-master seconds. Use [references/brand.md](references/brand.md) for visual and audio direction.
4. Extract a music grid, compile, and preview:

   ```bash
   python3 scripts/beats.py music.wav -o music.beats.json
   python3 scripts/compile_edl.py trailer.edl.json --clips-dir tools/video/clips -o trailer.compiled.json
   python3 scripts/render.py trailer.compiled.json --profile preview -o trailer-preview.mp4
   python3 scripts/review.py trailer-preview.mp4 --compiled trailer.compiled.json --strip review.jpg
   ```

5. Inspect the frame strip and preview. Revise the EDL, not generated filter syntax. Preserve event timing and narrative clarity before adding more effects.
6. Render the approved master with `--profile final`; run `review.py --strict --fps 60`; complete its manual checklist.

Enforce these guardrails:

- Keep every input and intermediate CFR before concat or xfade.
- Treat `in`, `out`, `at`, `until`, `dur`, and `meta:` values inside clips as source-master seconds. Let `compile_edl.py` resolve output-local/global time.
- Recapture when `min(rate) × capture_vfps < output_fps`. Never interpolate rerunnable gameplay slow motion.
- Keep masters RGB. Convert once in the final assembly to yuv420p with tagged BT.709 and faststart.
- Use the bundled fonts for ffmpeg title text. Treat canvas-font parity as a capture-image responsibility.
- Prefer user-provided licensed music, then documented CC0/original assets. Record every asset in `assets/LICENSES.json`.
- Mix at 48 kHz, duck music beneath SFX, and apply `loudnorm=I=-14:TP=-1.5` last.
- Use only named effects and transitions. Read [references/ffmpeg-recipes.md](references/ffmpeg-recipes.md) when extending the vocabulary.
- Keep Remotion optional; verify its current commercial license before using it for a team of four or more.

Use `--dry-run` on `render.py` to inspect commands without rendering. Reuse `.video-cache/segments`; invalidate only segments whose source, timing, effects, font, or output contract changed.
