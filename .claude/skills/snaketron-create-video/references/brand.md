# SnakeTron trailer brand

**SnakeTron does not have a separate trailer identity. It has a game, and the trailer is made of it.**

This repo's standing rule is *"pose real state, render with the real renderer, never draw look-alikes"* (`client/src/tutorial.rs:1-12`, `client/web/components/RosterSnakeCanvas.tsx:14-22`, `client/web/utils/crashExplosion.ts:56`). That rule covers marketing graphics too. Every non-gameplay frame is built from the app's own visual system so a viewer cannot tell which frames came from the product and which were made for the trailer.

## The ground is paper

SnakeTron is a **light** product. The arena clears to `#ffffff` (`client/src/render.rs:1583`); `body`, `.game-over-card`, and `.rating-reveal` are all `#fff` (`client/web/index.css`). Cards, slates, and lower thirds sit on the same paper.

**Graphite is ink, never a backdrop.** Putting the graphics on a dark ground is the single fastest way to make the trailer look like a different game — it was the defect in build 1, which sampled every palette token correctly and still read as a generic esports package because it inverted the value structure.

## Palette — the app's token block, used the way the app uses it

Source of truth: `client/web/index.css` `:root`, plus the renderer's constants.

| Role | Token | Value | Used for |
|---|---|---|---|
| Ground | `--game-paper` | `#ffffff` | every card, slate, and lower third |
| Ink | — | `#14181f` | headlines, body copy, 1.5 px rules (`index.css` game-over header) |
| Graphite | `--game-graphite` | `#3f3f41` | hairline borders, secondary ink, never a fill |
| Energy | `--game-blue` | `#3b82f6` | team/accent rules, active states, the NOS canister body |
| Impact | `--game-red` | `#ef4444` | eliminations, danger, opposing team |
| Stakes | `--game-boost` | `#f8c84a` | rank/score emphasis |
| Signature | `NOS_ORANGE` | `#ff641e` | the boost-canister accent (`render.rs:80`) — **must appear in the graphics**, since it is in nearly every gameplay shot |
| Rule / muted | `--game-rule` / `--game-muted` | `#d1d5db` / `#667085` | dividers, labels |

Percentages to aim for on a card: ~80% paper, ink for type, one accent doing the work. If a frame is mostly a color, that color is white.

## Typography — only what the product ships

- **Headlines:** `900 <size>px "Arial Black", Arial, sans-serif` — the exact stack the canvas renderer uses (`render.rs:286, 1465, 1519`).
- **Heaviest stamps:** `Impact, "Arial Black", sans-serif` (`render.rs:1846`).
- **Utility copy:** the app's system stack (`-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, …`).
- **No fonts the product does not use.** The M4 font-allowlist check fails the build on any other family — no condensed-broadcast faces, no webfonts (the app ships zero `@font-face`).

**Italic is a real SnakeTron signal — use it where the app uses it.** `client/web/CLAUDE.md:7-9` is the checked-in design contract: *"Classy, modern, minimalist design. Black on white with a touch of color. For example, the logo is a stylized SNAKETRON in bold, italics."* The app backs this with 18 `font-style: italic` rules, all on **display numerals and score emphasis** at weight 950 (e.g. `.game-scoreboard-team-score`, `index.css:249`).

So the rule is by role, not blanket:

| Role | Treatment |
|---|---|
| Logotype / wordmark | **bold italic**, uppercase — per the design contract |
| Scores, ratings, deltas, rank numerals | **bold italic**, weight 900–950, in `--game-blue` or ink |
| Card headlines, labels, body | upright, heavy, uppercase where the app does |
| In-arena canvas text (NOS wordmark, food/carry labels, zone names) | **upright** `Arial Black` / `Impact` — never italic (`render.rs`) |

Getting this backwards in either direction is a brand error: an upright logotype is as wrong as an italic in-arena label.

## Caption typography

Trailer captions are SnakeTron headings, set the way the app sets headings:

| | |
|---|---|
| Face | `900 "Arial Black", Arial, sans-serif` — the canvas renderer's stack |
| Case | upper for `impact`, sentence case for `quiet` |
| Colour | ink `#14181f` with a 3–4px paper halo (the app's own on-field label treatment, `render.rs:2071-2072`) |
| Size | ~7.2% of frame height |
| Position | left-aligned at x ≈ 5.5%, in the band at y ≈ 0.655–0.75 |
| Never | gold on white (illegible), drop shadows, centre placement, condensed or italic faces |

Italic belongs to the logotype and to score numerals, not to captions — see the
typography table above. Placement rationale and the sequencing rules are in
[narrative.md](narrative.md).

## Construction — precise, not heavy

The app builds panels with hairlines and soft elevation. Match it:

- Borders **1–1.5 px** (`border: 1px solid rgb(63 63 65 / 52%)`, `border-bottom: 1.5px solid #14181f`), radii **6–7 px**.
- Elevation: `0 18px 48px rgb(31 41 55 / 22%)`. **No 8–18 px hard offset shadows, no `skewX`.**
- Motion uses the app's curves: entrances `180ms cubic-bezier(0.2, 0.8, 0.2, 1)`, stamps `360ms cubic-bezier(0.2, 1.4, 0.3, 1)` (`index.css` `game-over-card-in`, `rating-medallion-stamp`).
- Available motif: the arena's 1 px grid dots. It is the game's one signature texture and reads as SnakeTron instantly.

## Scale: render at the game's size, reach 1080p with DPR

The game never draws a cell larger than **15 CSS px** — `GameArena` starts there and only shrinks to fit (`client/web/components/GameArena.tsx:679`). That single number sets SnakeTron's visual scale on every monitor.

Capture must respect it. The canvas scales with the camera while the DOM addons beside it lay out in CSS pixels, so a camera tight enough to exceed 15px/cell blows the food, grid dots and snakes far out of proportion with the boost meter and callouts — the shot stops looking like the product even though every colour is right. Build 1's hero shot ran ~106px/cell, seven times the game's maximum.

- `capture.mjs` lays the page out at **480×270 CSS with `deviceScaleFactor: 4`**, so 1920×1080 device pixels carry the arena at the game's real scale (~32 cells across).
- `client/src/scenario.rs` enforces the ceiling (`MAX_CELL_SIZE_CSS_PX`): a camera that would exceed it is **widened about its centre** instead of zoomed. `width_cells` is therefore a minimum zoom-out, not a licence to magnify.
- Effects sized in cells stay proportional automatically — the crash blast is 7.5 cells across with a lower bound but deliberately **no upper bound** (`crashExplosion.ts`), so it grows with the snakes it destroys.

## Prefer the real component to a replica

Where the app already renders the thing being advertised, the fixture card **mounts the real component** with fixture props behind the capture contract — `RatingReveal` is pure-props and rAF-driven, so it virtualizes correctly under BeginFrame. Bespoke HTML is allowed only for the logo slate, and it imports the app's stylesheet rather than restating colors.

The cards in `assets/cards/` are the fallback path for capture without a dev server; they mirror the real components' CSS and must be updated when those components change.

## Motion and sound

- Transitions: short slice/wipe/`pixelize` on cuts; `fadeblack` reserved for chapter breaks. On a paper ground prefer `fadewhite` for chapter breaks.
- Cut on musical beats. Land eliminations with one dry impact plus a brief RGB split and a decaying shake.
- Boosts get a rising whoosh; banks get a bright confirmation chime. Anything
  *pitched* must be in the key of the bed and agree with the chord it lands on
  — `sfx.bank()` takes the chord, `song.chord_at()` says what it is. A clashing
  note is picked out by the ear at any level and reads as "too loud" when the
  problem is not loudness.
- Audio: `-14 LUFS` integrated, `-1.5 dBTP`. Duck music beneath SFX rather than
  raising SFX gain — but a *few* dB, releasing inside a beat. The shipped
  default was ~20 dB, which deletes the bed under every hit instead of making
  room for it.
- The bed itself is generated: `scripts/audio/`, deterministic and CC0. Its
  language is what the game's own is — snapped to a grid, dry, nothing that
  drifts or blooms — carried by a house/French-touch kit rather than the
  drone the first build shipped.
- **Grade:** `assets/lut/snaketron-identity.cube` is identity by default. Do not use a LUT that darkens or warms footage away from the arena's neutral white — the value-polarity gate measures exactly this.

## If you want a bespoke identity anyway

A distinct marketing identity that departs from the in-app system is allowed **only as a signed-off exception**, recorded here with the reason and the approver. Absent that record, "placeholder" means *the game's own system* — never an invented one. See the PRD §5.7 art direction (rule 6) and the M4 exit gates.

Use `assets/identity/snaketron-wordmark.svg` for the logo slate and the bundled original SFX. Check `assets/LICENSES.json` before distribution.
