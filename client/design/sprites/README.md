# Sourced sprite sheets

Drop sourced square images here, then run:

```bash
python3 client/design/tools/sprite_sheet.py client/design/sprites/*.png --rows 20
```

The script measures both wrap joins, repairs them if it can, and writes the
finished sheets to `client/web/public/images/skins/<name>.v1.png`. A sheet it
cannot repair is reported and saved to `client/design/tools/.sprite-state/`
rather than failing the batch — check that directory after any run that warns.

The output name is the input's basename, so the file names here decide the URLs
`client/src/skin/sprite.rs` declares. The three it currently expects:

| file to drop | becomes | worn as |
|---|---|---|
| `zebra-live.png` | `zebra-live.v1.png` | repeating coat |
| `tiger-live.png` | `tiger-live.v1.png` | repeating coat |
| `stars-and-stripes.png` | `stars-and-stripes.v1.png` | 20 cells from the head, fading out over the last 6 |
| `race-livery.png` | `race-livery.v1.png` | repeating coat |

Sources **are** committed here, which is the opposite of the rule the coat
textures follow — and deliberately so. Those are fetched from URLs recorded in
`build_coat_textures.py`, so the script alone can rebuild them. These are
one-off generations with no other copy: drop them and the sheets can never be
rebuilt at a different cell size, row count, or rotation. Record where each one
came from in `client/web/THIRD_PARTY_ASSETS.md` before shipping it.

## What makes a good source

- **Square**, and seamless-ish in both axes. Perfect is not required; that is
  what the repair pass is for. Badly wrong is **rejected rather than repaired**,
  and that is deliberate: if the two halves genuinely disagree, inpainting does
  not fix the sheet, it hides the disagreement well enough to pass a pixel-scale
  check. If a source is rejected as `not-tileable`, re-generate it — no amount
  of tool work will save it. See `.claude/skills/author-skin/SKILL.md`.
- **Wrapping in `y` matters as much as in `x`.** Rows are frames: the last row
  is followed by the first, so a break there is a visible jolt once per cycle.
- **At least 1280x1280**, so twenty rows are 64px each. Rows land on whole
  pixels or every frame samples across a boundary.
- **Flat, even lighting.** A vignette or a directional highlight becomes a dark
  patch that travels down the snake and draws the eye to the repeat.
