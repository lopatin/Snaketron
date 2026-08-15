# Capture fixture cards

Capture both bundled fixtures directly; no app server or network access is required:

```bash
node scripts/capture_card.mjs \
  --card rank-up \
  --out tools/video/clips/rank-up-card \
  --capture-vfps 60 \
  --param headline="Rank up!" \
  --param before="Gold II · 1480" \
  --param after="Platinum I · 1512" \
  --param delta="+32 Rating"

node scripts/capture_card.mjs \
  --card leaderboard \
  --out tools/video/clips/leaderboard-card \
  --capture-vfps 60 \
  --param player="SnakeByte" \
  --param score="2,132"
```

Install `client/web` dependencies and its Playwright Chromium browser first. Pass `--headless-shell PATH` for BeginFrame capture or `--virtual-time` to force the contract-stepped screenshot path.

The command atomically produces `master.mkv` and `meta.json`. It encodes every PNG frame with lossless `libx264rgb -qp 0`, strips variable metadata, and records `capture_vfps`, `encoded_fps`, `duration`, dimensions, frame count, source/master hashes, named `anchors`, cue timeline, and deterministic capture scope. The metadata is directly consumable by `compile_edl.py`.

The corresponding raw fixture URLs and accepted query parameters are:

- `rank-up.html?headline=Rank%20up!&before=Gold%20II%20%C2%B7%201480&after=Platinum%20I%20%C2%B7%201512&delta=%2B32%20Rating`
- `leaderboard.html?player=SnakeByte&score=2%2C132`

Both fixtures expose `window.__SNAKETRON_CAPTURE__` and the compatibility alias `window.__snaketronCapture`:

```ts
{
  ready(): Promise<void>;
  durationMs(): number;
  stepMs(ms: number): Promise<void>;
  renderedTick(): number;
  cueTrack(): { anchors: Record<string, number>; duration: number; capture_vfps: number; encoded_fps: number };
}
```

The capture command calls `ready()` before the first frame and advances only with `stepMs(1000 / capture_vfps)`. Keep future fixtures free of wall-clock CSS animation and network dependencies.
