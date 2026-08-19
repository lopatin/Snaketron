---
name: author-skin
description: Design, validate, render, and self-review a new Snaketron snake skin end to end — as a data document when the schema can express it, or as Rust when it genuinely cannot. Use whenever asked to create, restyle, or theme a snake skin, a team base, or a goal celebration.
---

# Authoring a Snaketron skin

Read `specs/skins-prd.md` §5 and §7 once before starting. The short version:
a skin owns the pixels of one snake's body plus optional dressing for the team
base and the goal celebration. It does **not** own geometry, occlusion,
layering, or the decision about who is a friend — those are the renderer's, and
the boundary is what stops a skin from being able to misreport the game.

## 0. Pick the path

**Write a document** (`skin-schema/skins/<name>.skin.json`) unless you need
something the schema has no vocabulary for. The schema covers: per-role
palettes, head core size and colour, the head gradient, outline width, base
dressing, celebration dressing, and bounded animation over lightness and
gradient opacity.

**Escalate to Rust** (`client/src/skin/<name>.rs`) only for painting the schema
cannot describe — a real canvas gradient, a custom pass, geometry-dependent
detail. `client/src/skin/ember.rs` is the worked example: it uses the shared
body painter for everything and hand-paints only the one thing that needed it.

Escalating "because it will be easier" is how the document layer rots. If you
reach for Rust, say in the PR which specific effect forced it.

### The three Rust escalations that already exist

Read the one closest to what you want before writing a fourth.

- **`ember.rs`** — one hand-painted element on an otherwise shared stack.
- **`checker.rs`** — a *pattern made of shapes*: two `Source::Tiled` lanes,
  which is what a checkerboard, a stripe or a dashed band actually is. Cheap,
  sharp at any size, and fully checkable natively.
- **`sprite.rs`** — *pixels that move*: a square **sprite sheet** whose rows are
  frames of animation and whose columns are distance along the body. Also the
  home of the head/tail-pinned, optionally-repeating, optionally-faded sprite
  layer. Reach for a coat instead unless the skin genuinely moves — a sheet is
  twenty times taller for the same length of pattern.
- **`animal.rs`** — a *pattern made of pixels*: an **opaque** tiling PNG texture
  that replaces the body fill outright, for coats no rectangle can describe. The
  repeat may be any number of cells long — six or seven here — which is what
  stops a coat reading as a grid. Its art is built by
  `client/design/tools/build_coat_textures.py` from third-party game textures;
  the PNGs are build output and are never hand-edited.

Prefer shapes to pixels when both would work. A texture costs a fetch, cannot be
tinted per role, and is invisible to every native test — the only oracle for it
is the browser suite in `client/web/tests/skins`.

### Making a texture tile: the `[T, X, T]` trick

This is for a texture **cropped out of a larger image** — a photograph, a hide,
anything whose two cut ends were never meant to meet. If the image was already
generated as a tileable square, its join exists but is small, and the
roll-and-repair below is the tool for it; inventing a fresh join with this one
throws away texture that was already almost right.

`client/design/tools/build_coat_textures.py` implements it; copy that file's
`inpaint_wrap` rather than reinventing it.

Do **not** cross-fade the two ends together. That was the first attempt here and
it shipped a bug: the overlap is a ghost of two different pieces of texture
averaged together, and tiling it repeats that ghost every few cells as a visible
blotch. It reads as a smudge, not a seam, which is exactly why it survived
review.

Instead, let an inpainting model paint the join:

```
[T, X, T]   put a gap between two copies of the tile; mask only the gap
[T, X]      keep one copy plus the filled gap — that pair is the new tile
```

Tiling `[T, X]` gives `… T X T X …`, and both junctions in that sequence —
`T→X` and `X→T` — are ones the model generated the gap against, so the wrap is
seamless by construction. Keep `T` byte-identical to what the model saw; that is
what makes the guarantee hold.

```bash
pip install simple-lama-inpainting opencv-python-headless   # --no-deps if pip fights Pillow
python3 client/design/tools/build_coat_textures.py --report
```

Four things that are easy to get wrong, all of them measured:

- **`X` alone does not tile.** It is tempting — it is the generated part — but
  its two ends were shaped for *different* neighbours, and butting them together
  is the original problem again. Measured worse than the raw crop (10.2 vs 8.8).
- **A wide gap comes out mush.** LaMa is a removal model; asked to invent a gap
  as wide as its context it regresses to a blurry, desaturated mean (detail 0.59,
  chroma 0.23). Keep the gap around 15%.
- **Iterating the trick to lengthen compounds that mush**, because each pass
  paints over the last one's output. Buy length by cropping a longer band of
  real texture instead.
- **Every filter *after* the inpainting must wrap too.** The final downsample
  and any blur read a neighbourhood, and Pillow invents it by clamping at the
  edges — which silently undoes the seam. Run them on a three-up tiling and keep
  the middle (`wrapped()` in that script).
- **A three-up tiling is not enough on its own.** If the filter resamples by a
  factor that does not divide the tile's period, each copy is filtered at a
  different sub-pixel phase and the middle one is *not* what an infinite tiling
  would produce. That is invisible until it feeds something with a hard
  threshold — a posterise step flipped whole band labels on 26% of the tiger
  from a sub-level of drift. Write the filter to wrap by construction
  (`np.pad(..., mode="wrap")`), rather than trusting the wrapper.

`--report` prints the numbers to judge by.

**`seam`** is a **percentile**, not a ratio: where the wrap join ranks among the
strip's own column-to-column steps. 0.5 means the join is a perfectly ordinary
column boundary; above ~0.9 means it stands out from its own texture and will be
visible when it repeats. It is deliberately not "the join over the mean step" —
that number is not diagnostic on the strips it has to sign off, because a
posterised print is mostly flat and one legitimate stripe edge on the join reads
as alarming, while a high-contrast hide has a mean large enough to bury a real
misalignment. Both directions were measured on shipped files before the metric
was changed.

**`detail`** and **`chroma`** compare the strip's second half against its first
and are what catch the mush. Shipped values are seam 0.0-0.85 and detail
0.85-1.22.

### Sprite sheets: art that plays

A **sprite sheet** is a square image read as rows. **`y` is time and `x` is the
body**: one row is a whole snake's worth of skin laid out from the head, and
playing the rows in order animates the skin in place. Twenty rows by default —
and a square sheet at twenty rows is also twenty cells long, so the sheet
carries its own scale and no number is written down twice.

**One cell of body is 16x16 texels, and that is a hard ceiling.** A row is one
cell tall, so a twenty-row sheet is **320x320** and that is the whole file — 65
to 90 KB, in line with the painted coats. It is a decision about the art rather
than a compression setting: the arena draws cells at 15px and below, so 16
texels is about one texel per pixel and the skins read as pixel art rather than
as photographs shrunk down.

Animation is structurally free. The row is an argument to the same single
`drawImage` a still texture emits (`AtlasRegion::source_rect` walks the source
rectangle down the sheet), so an animated skin costs exactly what a still one
does and op-count invariance needs no special case. Bake **one frame per row**,
so `frame_index` walking the frames *is* the sheet walking its rows; two clocks
at different rates dwell unevenly on some rows and skip others, which reads as
a stutter nobody will attribute to the frame count.

`client/src/skin/sprite.rs` is the worked example. A sheet is worn three ways,
all the same layer with different numbers:

| | `Span.from` | `Fit` | `Fade` |
|---|---|---|---|
| living coat | `Anchor::Whole` | `Tile` | none |
| sprite from the head | `Anchor::Head` | `Clip` | trailing |
| sprite from the tail | `Anchor::Tail` | `Clip` | leading |

**Three knobs move a sheet**, and only two of them are paint-time:

| property | where it lives | what it does |
|---|---|---|
| `anim_speed` | recipe -> `period_ms` | multiplier on how fast rows advance; 0.5 is half speed |
| `drift_cells` | `Source::Image` | cells the pattern slides along the body per cycle, away from the anchor |
| `rotation_degrees` | **the PNG** | how far the art was turned when the sheet was built |

**Drift moves the sampling, never the repeats.** Sliding the repeats along the
body is the obvious implementation and it fails conformance: the number of them
overlapping the body changes as the phase crosses a boundary, and op-count
invariance is checked across the clock. Offsetting *which part of the texture*
each repeat samples keeps the destination grid fixed; the wrap costs one extra
blit per repeat, **always**, so the count stays a property of the skin.

That "always" is load-bearing. Branching on whether the phase is currently zero
costs a blit at exactly the frames where it lands on zero — which includes frame
one, so the very first sample disagrees with every other and conformance fails
on a skin that looks perfect. Branch on whether drift is *declared*.

**Rotation is baked into the pixels and cannot be a transform.** A row is drawn
as a one-cell-tall strip along the body, so rotating that quad turns the snake's
silhouette rather than the pattern inside it. `sprite_sheet.py --rotate` turns a
three-by-three tiling and crops the middle, so no corner comes up empty; the
recipe records the angle purely as provenance, and a test asserts paint time
never reads it — applying it twice would be silent and wrong. Turning breaks
tileability, which is what the repair pass is for.

**A sprite that ends needs a fade.** A bitmap's own edge is a hard vertical line
across the body and reads as the art being *cut off* rather than as the art
ending. `Fade` ramps alpha over a declared number of cells at either end,
approximated by a fixed number of constant-alpha slices — deliberately not a
canvas gradient, because `globalAlpha` is a number the recorder captures and a
gradient-masked blit would need an offscreen canvas and be invisible to every
native test. The slice grid is laid out over the **whole allocation**, so a
corner or a tile join does not restart the ramp.

Transparency works and is what makes sheets *stack*: these are ordinary blits,
so a sheet's alpha composites over whatever is beneath it, and a second sheet
layer is just another entry in the stack.

### Making a sourced image tile: the roll-and-repair

For an image that is **already meant to be seamless** — anything generated as a
tileable texture — do not reach for `[T, X, T]`. That trick invents a join
where there was none. Use `client/design/tools/sprite_sheet.py`, which repairs
the join that is already there:

```bash
python3 client/design/tools/sprite_sheet.py client/design/sprites/*.png --rows 20
python3 client/design/tools/sprite_sheet.py IMAGE --dry-run      # measure only
```

The algorithm, and why each step is that way:

0. **Resize to `rows * 16` first**, wrap-aware, before anything measures or
   repairs. The ordering is load-bearing, and was measured both ways:

   | | zebra | tiger | racing | flag | time |
   |---|---|---|---|---|---|
   | resize -> repair (s8) | 1.33 | 0.83 | 1.01 | 0.68 | ~1s |
   | repair -> resize (s8) | 1.30 | 0.81 | 0.86 | 0.69 | 16-64s |

   Quality is a wash — repairing at source resolution is marginally better on
   two sheets and worse on one, all far inside the limit. It is 14-70x slower,
   and it **never verifies what ships**: the downsample happens after every
   check has passed, so the join in the finished 320px file is created by the
   resize and measured by nothing. That is the same shape of mistake as a metric
   that certifies its own repair. Resizing first makes the pixels under test the
   pixels that ship, and it puts LaMa back inside the few-hundred-pixel regime
   it was trained for — the same reason `build_coat_textures` works at 128px.

   Wrap-aware for the usual reason: a plain resize invents the edge
   neighbourhood by clamping, which would fabricate a join that is not in the
   source or hide one that is.

1. **Roll by half the width and half the height.** This moves both wrap joins to
   the centre lines, where there is ordinary texture either side of them to
   measure against — a join at the image border has no neighbourhood at all. It
   also moves genuinely-interior pixels to the borders, where they wrap *by
   construction* because they were adjacent before the roll. So after the roll
   the borders are already correct and must not be touched.
2. **Measure each centre line** as a percentile: where that boundary ranks among
   all the image's own boundaries on that axis. Same metric as `seam` above and
   for the same reason — a raw ratio is not diagnostic across textures.
3. **Above 0.90 *and* above 1.0x, inpaint a slice** down that centre line with
   LaMa, restoring everything outside the slice byte-for-byte. LaMa returns a
   whole re-rendered image, and keeping its version of the untouched texture
   softens the entire sheet to fix a one-pixel line.

   Both gates are needed. The percentile is the metric that works across
   textures of different busyness, but on a very smooth image the interior steps
   are tiny and tightly bunched, so an ordinary boundary can rank 0.91 just by
   being a hair above its neighbours. Measured on a probe with a known seam in
   `x` only: the innocent `y` axis ranked 0.906 at 0.98x while the real seam
   ranked 1.0 at **22.4x**. A false positive costs a slice of real texture.
4. **Re-measure, including the structural checks below. Any failure → warn,
   save state, move on.** A seam that survives inpainting will not be fixed by
   inpainting it again, and a batch of a dozen sourced images should report on
   all of them rather than stop at the first bad one. The saved state is the
   *rolled* image, because handing back the unrolled one puts the seam at the
   border where it is exactly as hard to see as it was before.
5. **Roll back** and write the sheet, quantised to a 128-colour palette. The
   palette is measured wrap-safe: quantising and dithering moved every seam
   ratio by at most 0.02, because the palette is global and error diffusion does
   not accumulate enough across one boundary to matter.

#### Repeat length is the sheet's width

A row is `width / 16` cells long, and **that is the repeat**. A square 320px
sheet is 20 cells — which on the 33-cell `tile_wrapping_length` fixture redraws
13 cells, 40% of the body, and reads unmistakably as repetition. It is not a
seam and no seam work touches it.

Do not go looking in the renderer for this. Confirmed twice: the blit dump shows
exactly two `drawImage` calls of 20 cells each, and laying the sprite row out by
hand at 20 cells/repeat reproduces the render including across the boundary. The
tiling is right; there is simply not enough of it.

The sheet is therefore **not required to be square**, and the tool sizes its
width from the source's aspect: a 2:1 source becomes a 640x320 sheet with a
**40-cell repeat** at the same 16px cell, longer than most snakes.

That trades against mark scale, and the two are coupled through the fixed cell
size — more cells per repeat needs more source pixels, which makes each mark
smaller for a given source. You cannot buy both from one square image. Ask for
a **wide** texture with **six to eight marks across its height**, and both come
out right.

#### Mark scale: the number nobody thinks to set

A row is `rows` cells long **whatever is in it**, so the source's own mark
spacing decides marks-per-cell. A zebra hide with twenty stripes across it, laid
over twenty cells, puts a stripe on every cell — and one mark per cell reads as
a barcode, not as an animal. A mark needs **two or three cells** to be seen as a
mark at all.

This is not a tiling bug and will not be found by looking for one. Confirmed by
dumping the blits: a forty-cell snake got exactly two `drawImage` calls of
twenty cells each, each sampling the full row, and the rendered strip matched
row 0 laid out at twenty cells per repeat. The engine was right; the art was too
fine.

**Fix it with the row length, never by cropping.** Cropping the source to
enlarge the marks was tried and is wrong twice over: it throws away half the
texture — an 18-stripe zebra became a 9-stripe one — and having *less* in a
repeat makes the repetition **more** visible, not less. It also costs
tileability, since a crop out of a seamless texture no longer wraps.

`cells_for` measures the source's marks and sizes the row so each spans about
`CELLS_PER_MARK` (2.2) cells. Nothing is discarded and both problems fall out at
once: the 18-stripe zebra becomes a **672x320 sheet, 42 cells per repeat**, with
stripes a little over two cells wide and a repeat longer than the fixtures.

Measured, per sheet: zebra 42 cells, tiger 56, racing 80, flag 28.

#### The check a pixel-scale metric cannot make

**A one-pixel step metric certifies its own repair.** It measures exactly the
boundary the inpainter is about to smooth, so LaMa blends a 28px band, the
adjacent-pixel difference collapses, and the sheet passes while the marks on
either side still plainly disagree. This is not a threshold that needed tuning
— the metric is blind to the defect by construction, because the defect does not
live at one pixel. It shipped a zebra whose halves never corresponded.

Two checks catch it, and both run **before** the repair as well as after:

- **`seam_scales`** — the same seam measurement on progressively downsampled
  copies. A blend cannot survive it: averaging sixteen pixels into one leaves a
  stripe's lateral displacement exactly where it was.
- **`alignment_anomaly`** — the circular shift that best aligns the two sides of
  the join, measured against the shifts everywhere else in the image. Judged
  relative to the texture's own drift, not against zero, because diagonal
  stripes are laterally offset from row to row by definition.

The shapes are diagnostic, and they point opposite ways:

| | s1 | s2 | s4 | s8 | s16 | alignment |
|---|---|---|---|---|---|---|
| local join (repairable) | 23.34 | 11.80 | 6.00 | 3.18 | 1.93 | — |
| structural mismatch | 2.11 | 3.01 | 3.86 | **4.05** | 2.97 | **lag −454px** |
| the same, after LaMa | **0.73** | 0.96 | 1.50 | **2.31** | **2.65** | lag −10px |
| genuinely seamless | 0.89 | 0.89 | 0.89 | 0.88 | 0.83 | lag 0 |

A real join **decreases** with scale. A structural mismatch **persists or
grows**, and repairing it only moves the fine column — 0.73 at scale 1 against
2.65 at scale 16, which is a sheet that looks perfect to the old gate and wrong
to a human.

A structural failure is **not** a rejection. It means the fill has to be *wide*
— wide enough for the model to redraw the region rather than blend across it —
so it opens the width search all the way to the cap instead of vetoing it. An
earlier version of this gate refused outright, on the reasoning that a thin fill
only hides such a defect; that is true of a thin fill and false of the search,
and it rejected a perfectly recoverable zebra without trying a single width.
Measured on that sheet: 16px left it broken, 32px cleared it, 86px cleared it
comfortably.

The check earns its place as the **acceptance** rule instead, applied to the
repair rather than to the source. Same rule, asked after the fact: "is this
tileable now?" is the same question as "was it ever?", and having one threshold
rather than two is what stops them disagreeing.

#### Choosing the slice width

Do not guess it, and do not trust the seam score alone. A wider slice **always**
scores better on the seam — because a wide enough fill replaces the whole
neighbourhood with something smooth — so optimising that number alone converges
on mush. Two measurements are needed:

- `centre_seam` — did the join go?
- `band_detail` — busyness inside the repaired band over busyness outside it.
  Near 1.0 means the fill has the same texture as its surroundings; well below
  means LaMa returned a blurry local mean, which tiles into a recurring soft
  stripe. This is the failure the coat pipeline hit and it is invisible in any
  seam metric.

The objective is the **narrowest width that clears the seam without mushing**,
and it is found by **bisection**, not by a fixed ladder. Both quantities are
monotone in width and they move in opposite directions:

| width | 16 | 32 | 64 | 96 | 128 | 192 | 256 | 320 |
|---|---|---|---|---|---|---|---|---|
| seam (s8) | 1.63 | 1.38 | 1.28 | 1.12 | 1.06 | 1.29 | 1.10 | 0.69 |
| detail | 0.87 | 0.84 | 0.81 | 0.79 | 0.80 | 0.77 | 0.74 | 0.73 |

So "cleared" is a monotone predicate — narrow fails, wide succeeds — which is
exactly what bisection needs, and the narrowest clearing width simultaneously
keeps the most texture. There is no trade-off left to weigh once the boundary is
found.

The loop is closed: each pass decides the next width. Two early exits, both
cheap. The **widest** allowed slice is tried first — if that cannot clear, no
narrower one will, and the sheet is rejected after a single pass. Then the
narrowest, which ends it immediately for an ordinary join. Typical cost is 2
passes for an easy sheet and 6 for a hard one.

**Aim tighter than you accept.** Bisecting straight to the acceptance limit
lands on the width that merely scrapes past it — 22px at 1.42 against a limit of
1.50 — when 86px reached 1.09 for three hundredths of detail. Sitting on the
threshold is exactly how the first bad zebra shipped, so `TARGET_RATIO` (1.2) is
what the search bisects on and `STRUCTURAL_RATIO` (1.5) is only the fallback
when nothing reaches the target.

```bash
python3 client/design/tools/sprite_sheet.py IMAGE --report-dir /tmp/check
```

The first guess is the texture's **correlation length** on that axis — the lag
at which the autocorrelation falls below `1/e`, computed from the power spectrum
(the image is already periodic, which is what an FFT assumes). It is a property
of the texture rather than a constant: a slice narrower than one correlation
length is being asked to join two halves of a feature whose ends it cannot see,
and a much wider one is discarding texture the model then has to invent back.
Everything is capped at `MAX_SLICE_FRACTION`; past that LaMa is not repairing a
join, it is repainting the sheet.

A measured example, on a zebra hide with a real `y` seam:

| slice | seam ratio | detail |
|---|---|---|
| **28px** | **0.72x** | **0.80** |
| 42px | 0.67x | 0.71 |
| 71px | 0.61x | 0.74 |
| 106px | 0.55x | 0.67 |

The seam improves monotonically with width and the texture degrades with it.
28px is chosen: it already cleared, and every wider option pays real texture for
a seam score nobody can see.

`--report-dir` writes a before/after wrap check — the sheet tiled two-by-two, so
both joins meet in the centre, plus each join at high magnification. It is
deliberately **unannotated**: a line drawn on a join hides the thing it points
at, and a join is only ever judged by whether the marks carry across it. The
numbers go in the caption.

Three more things that are easy to get wrong here:

- **Which axes must wrap depends on how the sheet is worn.** `y` is never
  optional — rows are animation frames, so row `n-1` is followed by row `0`
  whatever the sheet is. `x` is required only when the sheet **repeats** along
  the body; a head-pinned sprite drawn once has no wrap in x to be wrong. Pass
  `--axes y` for those. The flag is exactly this case: its canton meets its
  stripes at the horizontal wrap, a real structural mismatch that is never seen,
  and demanding x would reject good art.
- **The height must divide by the row count.** Otherwise every frame samples
  across a row boundary, which reads as permanently slightly-blurred art and
  looks like a bad source. The tool resizes to `rows * 16` to guarantee it, and
  `the_declared_sheet_matches_the_png` checks it.
- **The resize has to wrap**, for the same reason every filter after an
  inpainting does — see the three-up rule above. `wrapped_resize` tiles
  three-by-three and keeps the middle.

Rows are the one place the transparent-padding rule is inverted: rows must touch,
and the bleed between them is *wanted*. A row is downsampled hard — sixty source
pixels into a fifteen-pixel cell — so neighbouring rows mix in regardless, and
neighbouring rows are adjacent moments of the same animation. What would be a
stranger's pixels in an atlas is a frame of motion blur here.

### The artifact that is not a seam

If a texture tiles cleanly by every measure and a reviewer still says they can
see where the tile ends, check the **repeat length** before touching the join.
A 6-cell repeat on a 33-cell snake shows the same distinctive cluster of marks
five times, and that reads as a defect even when the joins are perfect. The fix
is a longer repeat, not a better seam — and the way to get one is more real
source, either by cropping a longer band or by stitching several different
windows into one tile with a generated join between each (`stitch()`).

### Where the friend/foe reading lives

Every skin must let a viewer tell a teammate from an opponent, and
`skin_conformance_team_colours_stay_on_their_own_side` enforces it — but a skin
chooses **which colour carries it**, by returning a `SideCue` from the trait.

A painted skin says it with the body, and that is the default. A skin whose body
is an animal's coat cannot: a tiger is orange whoever wears it, and tinting it
cool for a teammate gives you a striped blue snake instead of a tiger. Those
skins return `SideCue::Contour`, keep the coat truthful, and pay for it by
widening the contour — the conformance suite then checks the *rim's* hue windows
and that the two sides are visibly far apart in it.

If you reach for this, the rim is now doing a job the body used to do. Make it
thicker than a painted skin's, and look at the result at cell size 5 before
believing it works.

## 1. Write it

Start from `templates/skin.template.json` (or `templates/custom_skin.rs.tmpl`).
Every constraint the validator enforces is commented in the template.

The rules that are not negotiable, and why:

- **Friendly reads cool, hostile reads warm.** Team games are played through
  colour. A teammate who looks like an enemy is a competitive bug.
- **The Boost band stays `#fff200` at +6px** in documents. Opponents read that
  band to know you are boosting.
- **Every contour width is quoted at 1×.** The arena canvas is not
  devicePixelRatio-scaled, so on a high-DPI display it draws a cell several
  times larger than the 15 px the layout caps at; `SnakePose.detail_scale`
  multiplies your `extra` values to match. Author in 1× pixels and you are
  correct at every zoom — do not pre-compensate, and do not read `cell_size`
  to decide a width yourself. The roster glyph legitimately draws a ~28 px cell
  at 1×, so cell size alone cannot tell a large glyph from a zoomed one.
- **Reported colours are flat 6-digit hex**, even for a gradient skin — the
  results-table pill and the contrast maths need one representative colour.
- **The head core stays dark**, or the roster's white ready-check vanishes.
- **Animation varies paint arguments, never structure.** Same ops, different
  values. This is what keeps an animated skin as cheap as a still one.

## 2. Validate

```bash
cargo run -p skin-schema --bin validate-skin -- skin-schema/skins/<name>.skin.json
```

A dedicated binary, not `cargo test -- <name>`: everything after `--` in
`cargo test` is a test-*name* filter, so a novel filename matches zero tests and
exits green having validated nothing.

Errors name the field and say what the rule protects. Fix, re-run.

## 3. Register it

- Document skin: add the `include_str!` to `document_skins()` in
  `client/src/skin/registry.rs`.
- Rust skin: add the field and the `entries()` line in the same file.
- Both: add the id to `CATALOG` in `server/src/skin_catalog.rs`. The two lists
  are compared by a test, so forgetting one fails the build rather than
  silently giving players a skin that turns back into classic at join.
- Also add it to `SHIPPED_SKINS` in `client/web/tests/skins/baseline-specs.mjs`
  and record a baseline (step 5). Nothing fails if you skip this — the skin just
  ships with no appearance oracle at all, which for a textured skin means no
  oracle whatsoever.

## 4. Look at it

```bash
cd client && wasm-pack build --target web --out-dir pkg   # ALWAYS rebuild first
cd web && npm start
```

Open `/qa/skins`, pick the skin, and **actually look**. The tiles are painted by
the real renderer against the same fixture corpus the golden traces use, so
what you see is what ships.

The **Live** section is the only place motion is visible — it runs off the real
animation-frame clock, exactly as the arena does, with a play/pause control.
Everything below it paints one fixed sample, which is what makes a screenshot of
it reproducible. If you are judging an animation, judge it there.

Note that an embedded or backgrounded browser pane freezes `requestAnimationFrame`
entirely (`document.visibilityState === 'hidden'`), so live tiles will sit
perfectly still through no fault of the skin. Look in a real, focused window.

Then capture the sheet and read the images back:

```bash
cd client/web
node tests/capture-skin-sheet.mjs http://localhost:3100 <skin-ref> ../../docs/screenshots/skins/<name>
```

Review against the brief. Things worth checking that tests cannot: does the
head read as a head at small cell sizes? Do the two within-team shades actually
look like the same team? Does the animation read as alive or as flicker — and
does it still read at all on a *short* snake, where a long wave has no room to
show itself?

**The stale-WASM trap:** a fresh worktree with a symlinked `node_modules` will
happily serve the *main* repo's old WASM, so your skin appears not to exist or
renders as classic. If the catalogue looks wrong, rebuild before debugging
anything else.

## 5. Prove it

```bash
cargo test -p client skin_conformance   # always unfiltered — see below
cargo test -p client                    # includes the classic golden traces
```

Never filter conformance to your skin's name: a typo would match zero tests and
exit green. The suite discovers every registered skin itself.

The classic golden traces must be **untouched**. If they moved, you changed
shared code, not just your skin — go find out what.

Then the browser oracle, which is the only thing that has seen your skin's
actual pixels:

```bash
cd client/web && SKIN_BASELINE_BLESS=1 npx playwright test --config playwright.skins.config.js
```

Blessing re-records **every** skin, so `git status` on
`client/web/tests/skins/baselines/` afterwards is a free regression check: only
your new sheet should appear. Any existing baseline showing as modified means
you changed shared painting code, and the diff is the review. Re-run without
`SKIN_BASELINE_BLESS` to confirm the committed sheets pass.

## 6. Ship it

PR with the contact sheet committed under `docs/screenshots/skins/<name>/`, the
conformance and golden runs green, and one line on why the skin needed Rust if
it did.

**Embed the screenshots in the PR body.** A skin PR is a visual change and it
should be reviewable without checking out the branch. Relative paths render as
broken links in a PR body, so use absolute URLs pinned to the commit SHA — not
the branch, which disappears on merge:

```
https://raw.githubusercontent.com/<owner>/<repo>/<sha>/docs/screenshots/skins/<name>/<file>.png
```

Then verify them, because a broken image looks exactly like a missing one until
someone opens the page:

```bash
gh pr view <n> --json body -q .body | grep -oE 'https://[^")> ]+' | sort -u |
  while read -r u; do printf '%s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' "$u")" "$u"; done
```

Say how to see it move, too: the film strip in the sheet is fixed samples and
never animates, so a reviewer needs `/qa/skins` and a fresh `wasm-pack build`.

## Files

- `templates/skin.template.json` — every field, commented
- `templates/custom_skin.rs.tmpl` — Rust escalation skeleton
- `checklists/parity-and-review.md` — what to check before opening the PR
- `skin-schema/skins/classic.skin.json` — the current look as a document, and
  the interpreter's own regression fixture
- `client/src/skin/fixtures.rs` — the shared pose corpus
