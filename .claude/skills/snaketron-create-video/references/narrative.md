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
- **One caption per scene.** Name the payoff and stop:
  **`impact`** — upper-case, short, landing *on* the payoff anchor
  ("DEMOLITIONS!", "TEAM MATCHES!", "FREE FOR ALL!").
- **Do not write a setup line for it.** An early "Cut them off." before a
  demolition, or "Four snakes, one arena." before an FFA shot, tells the
  viewer what they are about to see and then shows them — it over-shares, and
  it doubles the reading load in a shot that is already brief. The footage is
  the setup; the caption is the punchline.
- The closing call to action is the one exception, and it does not belong in
  `texts` at all: burnt-in ffmpeg captions cannot move, and the last thing on
  screen should be the liveliest. Animate it inside the end-slate card
  (`trailer-card__cta`) so it can arrive on its own beat.

## 2. Two beats sit side by side, never stacked

A scene with two things to name gets two captions on opposite sides of the
band — not two lines of copy in the same corner, and not one merged phrase.

```jsonc
"texts": [
  {"value": "BOOST!",   "at": 0.3,              "dur": 3.1, "style": "impact", "side": "left"},
  {"value": "COMBOS!",  "at": "meta:combo-0.1", "dur": 2.5, "style": "impact", "side": "right"}
]
```

- **`side` is `left` (default) or `right`**, an inset from that edge. Two
  captions on the same `line` must take opposite sides.
- **Stagger the entrances, share the exit.** The second lands shortly after
  the first, and both run to the cut — so they leave with the shot rather than
  blinking out while it is still playing. Set each `dur` so `at + dur` equals
  the clip's `out`.
- **Captions arrive, they do not switch on.** `render.py` fades each one up
  over ~110 ms and pops it: in oversized, a dip just under, settling at rest
  inside 300 ms. It is automatic; the point is that a hard on/off is the one
  linear move in an otherwise fully eased film and reads as a subtitle track.
- **`line` still exists** for the rare third caption, but a stacked pair reads
  as a block of copy dumped in one corner; that is what the side split fixes.
- Anchor each to the moment it describes — `meta:combo`, not a guessed
  timestamp.

Merging two beats into one phrase ("BOOST COMBOS!") is the other failure mode:
it names neither moment and lands on neither.

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
- **Verify the animation actually ran; do not assume it did.** Mounting the
  real component is not enough — see "Cards must animate under capture" below.
  Pull a frame strip of the card's own master before it goes anywhere near an
  EDL, and confirm the values change from frame to frame.
- **A card is only as long as its animation.** The rank-up card ran 5s for a
  1.4s odometer, so 3 of its 5 seconds were a finished scoreboard. Cut the
  clip to the animation plus one beat to read the result, and put a `push_in`
  under whatever tail remains.
- **Trim gameplay tails to the payoff.** The demolition shot held 3.1s of
  empty arena after the fireball. One to one-and-a-half seconds after the
  payoff is enough to register it; past that the shot is over and the video
  does not know it.
- **The end slate is the one card allowed to hold.** Nothing on it animates
  out: a trailer's last frame is what a paused or looping player sits on, so it
  has to be the finished lockup rather than whatever a fade left behind. Every
  other held frame in this file is a defect; this one is the deliverable.
- **Give every non-gameplay card the drifting dot field** (`arenaFlowField.ts`,
  the same field as the home screen). It keeps the frame alive, unifies the
  non-gameplay frames into one language, and prevents frozen-frame QC failures.
- **Name the screen with a caption in the same style as the gameplay captions**,
  rather than relying on a heading inside the captured UI. The caption is what
  carries continuity from the previous shot.

## 6. Cards must animate *under capture*, not just in a browser

The capture harness advances the page by calling `stepMs` on a clock it owns,
and screenshots each frame. A component that animates itself off
`requestAnimationFrame` or a CSS timeline is running on a completely different
clock from the one the frames are numbered by. It will look perfect when you
open the route in a browser and be a still image in the master.

Two things have to be true, and both have failed in a shipped cut:

1. **The component takes its time from the harness.** Give it an explicit prop
   (`RatingReveal`'s `clockMs`) and pass the card's `elapsedMs`. Without it the
   odometer finished during page load — before frame 0 — and all 300 frames
   captured the settled state.
2. **The capture context does not ask for reduced motion.** Playwright's
   `reducedMotion: "reduce"` makes every well-behaved component render its
   *settled* state, which is the exact opposite of what a card exists to show.
   Determinism comes from owning the clock and from `animations: "disabled"` at
   screenshot time, not from asking the product to stop animating. The context
   also renders `colorScheme: "light"`, because that is the product.

The check that catches both, before an EDL is written:

```bash
ffmpeg -i tools/video/clips/<card>/master.mkv \
  -vf "select='not(mod(n\,17))',scale=700:-1,tile=2x6" -frames:v 1 /tmp/card.png
```

If the tiles are identical, the card did not animate — no amount of EDL work
will fix it, and `splice_duplicate_frames` will not catch it because the flow
field underneath is still drifting.

## 7. Capture at 4× exposes every fixed-pixel weight in the renderer

Capture lays the page out at 480×270 CSS and reaches 1080p with
`deviceScaleFactor`, so the arena canvas draws a ~60 px cell where a 1×
display draws 15. Anything sized in *pixels* rather than as a share of the
cell therefore arrives at a quarter of its intended weight, and the shot reads
as a different game's art:

| Was | Symptom in the capture |
|---|---|
| Snake contour, flat 2 px | a hairline scratch around a 60 px body |
| Boost band, flat 6 px | the glow all but gone at the moment it matters |
| Carried-food numeral, capped at 14 px | a UI label pasted onto the snake |

The fix belongs in the renderer, not the capture: `body_detail_scale` in
`client/src/render.rs` quotes every body weight at the 15 px cell the arena
caps at and scales it above that. 1× rendering is unchanged and high-DPI
*displays* get the same repair for free — this was never only a trailer bug.

When a shot looks subtly wrong at 1080p and you cannot name why, crop a frame
1:1 and measure it against the body:

```bash
ffmpeg -i tools/video/clips/<slug>/master.mkv \
  -vf "select='eq(n\,30)',crop=960:540:480:270" -frames:v 1 /tmp/crop.png
```

## 8. The caption has to be true of the frame

A caption promises the viewer something; the frame has to deliver it in the
same second.

- **Count what is actually on camera.** A shot captioned "FREE FOR ALL!" held
  two snakes; one captioned for Boost and combos held exactly one snake in an
  otherwise empty arena. `Follow` frames one head, and everything staged more
  than half a camera width away is simply not in the video.
- **Populate the arena.** Pose opponents into lanes the camera holds and food
  across the whole window — an empty paper ground reads as an unfinished game,
  not a minimalist one. Target the ≥15% non-background ink from the shot
  composition rules.
- **Route extras where they cannot change the star's trace.** Put them on rows
  and columns the star never occupies, then diff the new `cue_timeline`
  against the old one: the star's pickups, kills, and banks must land on the
  same ticks, or every `meta:` anchor in the EDL has silently moved.
- **Do not let an extra die on camera for no reason.** A snake walking into a
  wall at the edge of frame reads as broken AI. Give it a turn command, or
  start it where its exit falls outside the camera rect.

## 9. Slow motion needs frame rate, and rarely earns its place

Cell-stepped snake motion does not slow down gracefully: the engine moves a
snake a whole cell at a time, so at 0.25× you see four discrete steps a second
and the shot reads as dropped frames.

- **Default to 1.0× everywhere.** Real-time gameplay is more exciting than
  janky slow motion.
- Only slow a shot down if it was captured at a matching high VFPS *and* the
  motion in it is continuous (an effect, a UI animation) rather than
  cell-stepped. `compile_edl.py` enforces the VFPS half of that; the judgement
  is yours.

## 10. Motion has one vocabulary

A cut looks designed when every move belongs to the same family.

- **Transitions:** pick two or three and reuse them. `smooth_left` between
  shots reads as forward momentum; a `circle_open` marks a change of chapter
  (arena → progression). Avoid `fadeblack` on this product — flashing to black
  punches a hole in a light-grounded video; use `fadewhite` if you need one.
- **Easing:** nothing moves linearly. Cards enter on `easeOutCubic`, stamps and
  badges land on `easeOutBack` (a slight overshoot), exits use `easeInCubic`.
  These match the app's own curves (`cubic-bezier(0.2,0.8,0.2,1)` entrances,
  `cubic-bezier(0.2,1.4,0.3,1)` stamps).
- **One transform per entrance.** A translate and a scale ramping together at
  different rates is read as depth — the wordmark appeared to float diagonally
  through space, which is the wrong register for a game drawn on a grid. The
  logo is *dropped* in from above and *lifted* back out on a single vertical
  axis, fast, with an `easeOutBack` overshoot doing the work the scale was
  doing. Where entrance and exit bracket the film, mirror them.
- **An odometer eases in and out, not just out.** A front-loaded count crosses
  its division boundary in the first fifth and leaves 80% of the sweep with
  nothing to happen; `easeInOutCubic` puts the crossing mid-count, which is
  where the promotion beat belongs.
- **Effects are accents, not decoration.** One impact gets a shake plus a 2px
  RGB split for ~0.12s. A 6px split for a quarter second reads as a rendering
  fault. Never apply a vignette or grain — they darken a paper-ground frame and
  fail the polarity gate.

## 11. Cut on the beat, and let the tool do the arithmetic

Beat-snapped cuts are a hard constraint, and every length change shifts every
downstream cut. Do not solve it by hand:

```bash
python3 scripts/fit_beats.py assets/launch-trailer/launch-trailer.edl.json \
    --clips-dir tools/video/clips
```

It nudges each shot's `out` to the nearest beat and reports what it changed.
Run it after any retiming, before rendering.

## 12. Length comes from content, not from holding frames

If a cut is under its target length, extend the *scenarios* (more run ticks,
another beat of action) or add a shot. Do not hold a finished animation on
screen — the duplicate-frame QC check exists precisely to catch that, and a
held frame is where energy goes to die.
