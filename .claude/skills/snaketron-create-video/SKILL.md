---
name: snaketron-create-video
description: Produce, revise, and quality-check frame-exact SnakeTron trailers and promotional videos from ScenarioCanvas scenarios or replay clips. Use for capture planning, clip-library reuse, source-master EDL authoring, true high-VFPS slow motion, beat-synced editing, branded title cards, ffmpeg preview/final rendering, or trailer QC.
---

# Create a SnakeTron video

Follow this workflow. Keep source timing in structured JSON; never hand-author a filtergraph.

1. Turn the brief into a 30–45 second shot list with one idea per shot and an explicit payoff.
   Start from `assets/launch-trailer/brief.md` and `launch-trailer.edl.json` for the launch sequence.
   Read [references/narrative.md](references/narrative.md) first — it covers captions, hook, energy, and motion vocabulary, and every rule in it comes from a defect that shipped.
   **Then stage each gameplay shot so the payoff is actually on camera** — see "Shot composition" below. This is the step that is easiest to skip and most expensive to get wrong: a shot whose advertised moment happens off-frame passes every automated check in this skill.
2. Reuse `tools/video/clips/<slug>/master.mkv` plus `meta.json` when its scenario, seed, and `capture_vfps` fit. Otherwise run `node tools/video/capture.mjs --scenario <path> --out tools/video/clips/<slug> --virtual-time`. Capture slow motion at the VFPS required by the intended rate.
   **Pass `--virtual-time` unless you have pinned `chrome-headless-shell`.** The BeginFrame path needs `HeadlessExperimental`, which Playwright's bundled Chromium does not expose; without the flag the renderer dies and the failure surfaces as the misleading `Capture contract was not installed within 30s: … browser has been closed`.
   For non-gameplay frames (logo slates, rank up, rankings) capture the **real app components** from the dev-only `/qa/trailer-card` route, so the video shows the product's own animation rather than a drawn replica:

   ```bash
   # dev server must be running (npm start on :3000)
   node scripts/capture_card.mjs --url "http://127.0.0.1:3000/qa/trailer-card?card=rank-up&ms=6000" \
     --out tools/video/clips/rank-up --capture-vfps 60 --duration-ms 6000 --virtual-time
   ```

   Cards: `logo-intro`, `logo-outro`, `rank-up`, `rankings`. Add new ones in
   `client/web/components/TrailerCardQA.tsx` — they must be a pure function of
   `elapsedMs` and expose the capture contract. The standalone HTML fixtures in
   `assets/cards/` remain only as a no-dev-server fallback ([references/fixture-cards.md](references/fixture-cards.md)).
3. Author an EDL using [references/edl.md](references/edl.md). Place effects, text, and SFX against `meta:` cues in source-master seconds. Use [references/brand.md](references/brand.md) for visual and audio direction.
4. Extract a music grid, fit the cuts to it, compile, and preview:

   ```bash
   python3 scripts/beats.py music.wav -o music.beats.json
   python3 scripts/fit_beats.py trailer.edl.json --clips-dir tools/video/clips
   python3 scripts/compile_edl.py trailer.edl.json --clips-dir tools/video/clips -o trailer.compiled.json
   python3 scripts/render.py trailer.compiled.json --profile preview -o trailer-preview.mp4
   python3 scripts/review.py trailer-preview.mp4 --compiled trailer.compiled.json --strip review.jpg
   ```

   `fit_beats.py` exists because changing any shot's length moves every later
   cut; never hand-solve the beat grid.

5. Inspect the frame strip and preview. Revise the EDL, not generated filter syntax. Preserve event timing and narrative clarity before adding more effects.
6. Render the approved master with `--profile final`; run `review.py --strict --fps 60`; complete its manual checklist.

## Shot composition

A gameplay capture is a staged shot, not a recording of whatever the camera happened to hold.

- **The payoff must be on camera.** Every participant in the advertised event — for an elimination, *both* the killer and the victim — is inside the camera rect through the whole beat. `Follow` tracks one snake's head, so an event that happens away from that head can be entirely off-frame. Stage the kill within ~2 cells of the star's head: have the star cross the victim's path just ahead of it so the victim strikes the body right behind the head. The native guard `cargo test -p client trailer_payoffs` asserts this for every checked-in trailer scenario and prints the offending cell and camera rect when it fails.
- **Set the field of view explicitly.** `presentation.camera.Follow.width_cells` is the horizontal window in grid cells (default 26). Height is derived as `width / aspect`, so at 16:9 a 26-cell window is only 14.6 cells tall — stage vertical action tighter (18–20 cells) or it falls out of frame top and bottom.
- **Give the frame scale references.** A scenario with `rng_seed: null` and an empty `pose.food` has *no food at all* — pose food explicitly. Keep a wall, team zone, or opponent in view. Target ≥ 15% non-background ink; build 1 shipped 6.5%.
- **Check the HUD survives capture.** Addons that render as siblings below the arena viewport (the boost meter) are clipped by capture mode's full-bleed layout. If a shot advertises Boost, confirm the meter is actually in frame.
- **Keep the subject large.** At the focus frame the subject's bounding box should span ≥ ⅓ of frame width. `punch_in` is a centre crop and cannot reframe a bad plate — fix framing at capture time.
- **Populate the arena, and make the caption true of it.** Count the snakes actually inside the camera rect, not the ones in the scenario: a shot captioned "FREE FOR ALL!" shipped holding two, and a Boost/combo shot shipped holding one. Pose extras into lanes the star never occupies and food across the whole window, then diff the new `cue_timeline` against the old — the star's pickups, kills, and banks must land on the same ticks or every `meta:` anchor has moved. Give an extra whose path ends in a wall a turn command, or start it so its exit is off camera; a snake suiciding into a wall in frame reads as broken AI.

## Narrative and energy

Full detail in [references/narrative.md](references/narrative.md); the rules that most often get skipped:

- **One caption per beat, anchored to the payoff it names.** Do not add a setup line before it — pre-captions over-share and double the reading load. A scene with two beats puts them on opposite `side`s of the band (`"side": "left"|"right"`), staggered in and leaving together on the cut; never stacked in one corner, never merged into one phrase.
- **Hook inside 5 seconds** — keep the intro slate short and land the biggest moment by ~4s.
- **Never cut a static screenshot into a moving video.** Capture the real component animating (`/qa/trailer-card`), and give every non-gameplay card the drifting dot field so it stays alive.
- **Prove the card animated before you cut it.** Mounting the real component is not enough: the component must take its time from the harness clock (`RatingReveal`'s `clockMs`), and the capture context must not request `reducedMotion: "reduce"`, which makes well-behaved components render their settled state. Tile the card's own master and check the values change. `splice_duplicate_frames` will not catch this — the flow field underneath is still drifting.
- **Cut a shot to its payoff.** A card lasts as long as its animation plus one beat; a gameplay shot ends ~1–1.5s after the payoff. Use `push_in` (an eased dolly that holds) for whatever tail remains — never `punch_in`, which over a long `dur` is a constant scale, i.e. a still frame.
- **Check the renderer's fixed-pixel weights before blaming the shot.** Capture draws a ~60px cell where a 1× display draws 15, so anything sized in absolute pixels (contours, glows, in-body labels) arrives at a quarter weight. Crop a frame 1:1 and measure against the body.
- **One transform per entrance, and animate the call to action inside the card.** A translate plus a scale reads as a diagonal float; burnt-in `texts` cannot move at all.
- **Default to 1.0× speed.** Cell-stepped snake motion reads as dropped frames in slow motion; only slow footage captured at matching high VFPS.
- **One motion vocabulary** — two or three transitions, non-linear easing throughout, effects as brief accents. No vignette or grain on a paper ground.
- **Captions sit in the band at ~0.655–0.75 frame height**, inset 5.5% from the `side` they are anchored to, clear of the boost meter (bottom 12%), the centre score readout, and the top combo callout.

Enforce these guardrails:

- Keep every input and intermediate CFR before concat or xfade.
- Treat `in`, `out`, `at`, `until`, `dur`, and `meta:` values inside clips as source-master seconds. Let `compile_edl.py` resolve output-local/global time.
- Recapture when `min(rate) × capture_vfps < output_fps`. Never interpolate rerunnable gameplay slow motion.
- Keep masters RGB. Convert once in the final assembly to yuv420p with tagged BT.709 and faststart.
- Use the bundled fonts for ffmpeg title text. Treat canvas-font parity as a capture-image responsibility.
- Prefer user-provided licensed music, then documented CC0/original assets. Record every asset in `assets/LICENSES.json`.
- Mix at 48 kHz, duck music beneath SFX, and apply `loudnorm=I=-14:TP=-1.5` last.
- Use only named effects and transitions. Read [references/ffmpeg-recipes.md](references/ffmpeg-recipes.md) when extending the vocabulary.
- Length comes from more content, never from holding a finished animation on screen — the duplicate-frame check will fail it, and it kills the energy anyway.
- Keep Remotion optional; verify its current commercial license before using it for a team of four or more.

Use `--dry-run` on `render.py` to inspect commands without rendering. Reuse `.video-cache/segments`; invalidate only segments whose source, timing, effects, font, or output contract changed.
