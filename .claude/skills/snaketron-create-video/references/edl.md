# SnakeTron EDL contract

Author JSON, not JSONC. Compile it before rendering.

```json
{
  "output": { "w": 1920, "h": 1080, "fps": 60 },
  "music": {
    "src": "music.wav",
    "manifest": "music.beats.json",
    "beat_snap": true,
    "beat_snap_max_s": 0.05,
    "duck_under": "sfx"
  },
  "timeline": [
    { "title": { "text": "SNAKETRON", "subtitle": "ENTER THE GRID", "style": "logo", "duration": 2.0 } },
    { "transition": "fadeblack", "duration": 0.3 },
    {
      "clip": "demolition-cutoff",
      "in": 1.0,
      "out": 7.5,
      "speed": [
        { "until": "meta:kill-0.4", "rate": 1.0 },
        { "until": "meta:kill+0.6", "rate": 0.25 },
        { "rate": 1.0 }
      ],
      "effects": [
        { "t": "shake", "at": "meta:kill", "dur": 0.35, "amp": 12, "decay": 3 },
        { "t": "punch_in", "at": "meta:kill", "dur": 0.35, "zoom": 1.25 },
        { "t": "rgb_split", "at": "meta:kill", "dur": 0.25 }
      ],
      "text": { "value": "DEMOLITIONS!", "at": "meta:kill+0.2", "dur": 1.0, "style": "impact" },
      "sfx": [{ "src": "assets/sfx/impact.wav", "at": "meta:kill" }]
    }
  ]
}
```

## Resolution rules

- Resolve `clip` against `--clips-dir/<slug>/master.mkv` and `meta.json`. Override with `src` or `meta` only when necessary.
- Express numeric clip times and `meta:<cue>[+|-offset]` in source-master seconds.
- Define speed intervals in increasing source time. Let the final interval omit `until`.
- Interpret `rate` as output speed relative to gameplay. A rate of `0.25` makes one source second occupy four output seconds.
- Let transitions overlap adjacent output segments. Place a transition object only between two visual entries.
- Snap the start of each incoming segment to a manifest beat when it is within `beat_snap_max_s`. The compiler records the exact tail adjustment and global transition offset.
- Supply `capture_vfps`, `encoded_fps`, `duration`, and named cues in clip metadata. Put cues in `anchors`, `markers`, `events`, `timeline`, `cues`, or `cue_track`.

## Vocabulary

Use effects `shake`, `punch_in`, `push_in`, `drift`, `rgb_split`, `glow`, `grain`, `vignette`, `lut`, and `letterbox`.

Use transitions `fadeblack`, `fadewhite`, `dissolve`, `pixelize`, `slices`, `wipe_left`, `wipe_right`, `wipe_up`, `wipe_down`, `smooth_left`, `smooth_right`, `smooth_up`, `smooth_down`, `circle_open`, `circle_close`, and `zoom_in`.

Prefer a single strong impact stack—shake, punch, split, and SFX—at the payoff. Use grain, vignette, glow, LUT, and letterbox as restrained shot treatments.

### Accents vs. moves

`punch_in` is an **accent**: the zoom snaps on at `at` and eases back out over
`dur`. Give it a short `dur` (0.3–0.5 s) on the frame of an impact.

`push_in` and `drift` are **moves**: a continuous eased ramp that holds its
final framing, spanning the whole shot.

Do not reach for `punch_in` when you want a slow dolly — over a multi-second
`dur` it is a *constant* scale for the whole window with a hard step at each
end, which is visually identical to a still image. This is exactly how build 3
shipped a 5-second frozen title card that passed every automated check:

```json
{ "t": "push_in", "at": 0.0, "dur": 3.4, "zoom": 1.08 }
```

Any shot that is not gameplay needs one of these. See the "never cut a static
screenshot into a moving video" rule in [narrative.md](narrative.md) — a card
whose own animation finishes early is a static screenshot for the remainder.
