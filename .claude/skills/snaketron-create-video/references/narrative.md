# Narrative, energy, and motion

Brand fidelity (`brand.md`) stops a video looking like the wrong product. This
file is about whether it holds a viewer. Both are required; a trailer can be
perfectly on-brand and still be boring.

Every rule below came from a real defect in a shipped cut. They apply to any
SnakeTron video, not just the launch trailer.

## 1. Every scene carries a caption

A shot with no words is a shot the viewer has to decode. By the time they have,
it has cut away.

- **No gameplay scene ships without a caption.** If you cannot write one, the
  shot has no point and should be cut.
- Use two registers, and alternate them:
  - **`quiet`** — lower-case sentence, sets up the shot ("Cut them off.",
    "Carry the points home."). Lands early, ~0.2s in.
  - **`impact`** — upper-case shout, names the payoff ("DEMOLITIONS!"). Lands
    *on* the payoff anchor, not before it.
- The setup→payoff pair is what creates narrative: the quiet line poses a
  question, the impact line answers it as the action resolves.

## 2. Captions are sequenced, never stacked by accident

Two ideas need two captions with their own timing. `texts` takes an array:

```jsonc
"texts": [
  {"value": "BOOST!",   "at": 0.3,                "dur": 1.5, "style": "impact", "line": 0},
  {"value": "COMBOS!",  "at": "meta:combo-0.2",   "dur": 1.6, "style": "impact", "line": 1}
]
```

`line` stacks them so the second never lands on the first. Anchor each to the
moment it describes — `COMBOS!` fires on the clutch pickup, not on a guess.

## 3. Captions live in a band, clear of the game's own UI

The arena already draws things in most of the frame:

| Region | Occupied by |
|---|---|
| Bottom ~12% | the boost meter |
| Centre | score celebrations (`+46`), the action itself |
| Top-centre | the combo callout (`+2 COMBO!`) |

Trailer captions therefore sit **left-aligned in a band around 0.655–0.75 of
frame height** (`CAPTION_BASE_Y_FRAC` in `render.py`), with `line` stacking
upward from there. Do not centre them and do not push them below 0.8 — build 2
did both and the callouts collided with the meter and the score readout.

## 4. Hook inside five seconds

The first five seconds decide whether the rest is watched. Put the single most
arresting thing there — for SnakeTron that is a demolition: the cut-off, the
shake, the fireball, the protagonist emerging from it. Keep the intro slate
short (≈2s) so the hook lands by ~4s.

## 5. Energy must not sag

Energy drops when a scene stops moving. The usual culprit is a UI screen
dropped in among gameplay.

- **Never cut a static screenshot into a moving video.** If a screen matters
  (rank up, rankings), show its *animation* — mount the real component in
  `/qa/trailer-card` and capture it playing. `RatingReveal` runs its genuine
  promotion sequence; a still of the same thing reads as a slide deck.
- **Give every non-gameplay card the drifting dot field** (`arenaFlowField.ts`,
  the same field as the home screen). It keeps the frame alive, unifies the
  non-gameplay frames into one language, and prevents frozen-frame QC failures.
- **Name the screen with a caption in the same style as the gameplay captions**,
  rather than relying on a heading inside the captured UI. The caption is what
  carries continuity from the previous shot.

## 6. Slow motion needs frame rate, and rarely earns its place

Cell-stepped snake motion does not slow down gracefully: the engine moves a
snake a whole cell at a time, so at 0.25× you see four discrete steps a second
and the shot reads as dropped frames.

- **Default to 1.0× everywhere.** Real-time gameplay is more exciting than
  janky slow motion.
- Only slow a shot down if it was captured at a matching high VFPS *and* the
  motion in it is continuous (an effect, a UI animation) rather than
  cell-stepped. `compile_edl.py` enforces the VFPS half of that; the judgement
  is yours.

## 7. Motion has one vocabulary

A cut looks designed when every move belongs to the same family.

- **Transitions:** pick two or three and reuse them. `smooth_left` between
  shots reads as forward momentum; a `circle_open` marks a change of chapter
  (arena → progression). Avoid `fadeblack` on this product — flashing to black
  punches a hole in a light-grounded video; use `fadewhite` if you need one.
- **Easing:** nothing moves linearly. Cards enter on `easeOutCubic`, stamps and
  badges land on `easeOutBack` (a slight overshoot), exits use `easeInCubic`.
  These match the app's own curves (`cubic-bezier(0.2,0.8,0.2,1)` entrances,
  `cubic-bezier(0.2,1.4,0.3,1)` stamps).
- **Effects are accents, not decoration.** One impact gets a shake plus a 2px
  RGB split for ~0.12s. A 6px split for a quarter second reads as a rendering
  fault. Never apply a vignette or grain — they darken a paper-ground frame and
  fail the polarity gate.

## 8. Cut on the beat, and let the tool do the arithmetic

Beat-snapped cuts are a hard constraint, and every length change shifts every
downstream cut. Do not solve it by hand:

```bash
python3 scripts/fit_beats.py assets/launch-trailer/launch-trailer.edl.json \
    --clips-dir tools/video/clips
```

It nudges each shot's `out` to the nearest beat and reports what it changed.
Run it after any retiming, before rendering.

## 9. Length comes from content, not from holding frames

If a cut is under its target length, extend the *scenarios* (more run ticks,
another beat of action) or add a shot. Do not hold a finished animation on
screen — the duplicate-frame QC check exists precisely to catch that, and a
held frame is where energy goes to die.
