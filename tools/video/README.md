# SnakeTron deterministic capture

The capture rig drives the real `ScenarioCanvas` through its virtual clock and
streams each composed canvas + React-addon frame directly to ffmpeg. Capture
mode loads the checked-in OFL Inter/Barlow faces before the first frame and
launches Chromium with LCD text, subpixel positioning, and platform hinting
disabled; live-game typography is unchanged. A run
produces a lossless RGB `master.mkv` and `meta.json` with cue timestamps in
master seconds, normalized `kill`/`bank`/`combo`/`boost` anchors, source-master
`duration`, and the star snake's per-frame head track.

The default capture clock is source/gameplay time: a 9 s highlight produces a
9 s master, which is the timebase expected by the trailer EDL. Pass
`--viewer-timing` to reproduce the product's authored speed segments instead.
For the canonical PotG ramp this produces 12.5 s of viewer time and maps the
6 s source focus to 8 s in `meta.json`. `capture-potg-review.mjs` selects this
mode automatically so human calibration reviews match what players see.

## Fresh worktree

Prerequisites: Rust/wasm-pack, Node.js, ffmpeg/ffprobe, and a Playwright-supported
Chromium. From the repository root:

```sh
wasm-pack build client --target web --out-dir pkg
npm ci --prefix client/web
cd client/web
npx playwright install chromium chromium-headless-shell
npx webpack serve --config webpack.config.js --host 127.0.0.1 --port 3000
```

Leave that server running. In another shell, from the repository root:

```sh
# Fast contract/encoder smoke (five 1080p frames).
node tools/video/capture.mjs \
  --scenario demolition-cutoff \
  --capture-vfps 10 \
  --duration-ms 500 \
  --virtual-time \
  --out tools/video/clips/smoke

# Full fixture capture. Omit --duration-ms to use the scenario's duration.
node tools/video/capture.mjs \
  --scenario demolition-cutoff \
  --capture-vfps 60 \
  --out tools/video/clips/demolition-cutoff

# A checked ScenarioScript or server-produced HighlightClip JSON works too.
node tools/video/capture.mjs \
  --scenario ./path/to/scenario-or-highlight.json \
  --capture-vfps 60 \
  --viewer-timing \
  --out tools/video/clips/file-capture
```

A calibration review manifest can be rendered as one batch. The output is
intentionally explicit so lossless masters and capture caches stay outside the
checked-in compact review pack:

```sh
node tools/video/capture-potg-review.mjs \
  --manifest docs/qa/play-of-the-game-calibration/top-20-review.json \
  --capture-vfps 60 \
  --out /tmp/snaketron-potg-review
```

The file form is injected before the QA application boots. The raw JSON stays a
string across the browser boundary, preserving a HighlightClip's 64-bit end
hash exactly. A missing `.json` path or an unrecognized JSON shape fails loudly.

Do not point an entire `client/web/node_modules` directory at another worktree.
Webpack resolves `wasm-snaketron` directly from this worktree's `client/pkg`,
and the commands above rebuild that package first.

## Determinism and comparisons

On Linux, pass Playwright's pinned `chrome-headless-shell` via
`SNAKETRON_HEADLESS_SHELL` (or `--headless-shell`) to require BeginFrame. If no
explicit binary is supplied, the CLI probes BeginFrame and records
`capture_path: virtualtime` when it uses the macOS-compatible screenshot
fallback. The fallback is a perceptual reference; bit identity is claimed only
for two BeginFrame runs in the pinned Docker image on the same host. Both paths
must use the same scenario/clip file (matching `source_sha256`) before a
cross-platform comparison is meaningful.

```sh
# Same-host Docker/BeginFrame acceptance: whole-file SHA-256 must match.
node tools/video/compare-captures.mjs \
  tools/video/clips/docker-a/master.mkv \
  tools/video/clips/docker-b/master.mkv

# Cross-platform/fallback acceptance: every aligned frame must be >= 0.999 SSIM.
node tools/video/compare-captures.mjs --mode perceptual --threshold 0.999 \
  tools/video/clips/docker-a/master.mkv \
  tools/video/clips/macos/master.mkv
```

Perceptual comparison first requires identical dimensions, frame rate, and
decoded frame count, then reports both minimum per-frame and mean SSIM. For
0.5x or 0.25x editorial slow motion, recapture at 120 or 240 virtual fps; do
not synthesize missing frames.

## Pinned Docker BeginFrame path

The image pins Playwright/headless-shell, ffmpeg, software raster, and
fontconfig fallbacks. Capture pages use the same copied OFL web fonts on macOS
and Linux, with `ready()` rejecting if either font cannot load. Build from the
root:

```sh
docker build -f tools/video/Dockerfile -t snaketron-capture .
```

Build `client/pkg` on the host first, keep the dev server running, then on
Docker Desktop (macOS/Windows) run:

```sh
# `host.docker.internal` needs an accepted Host header. Run this instead of
# the localhost-only server command above while doing Docker captures.
cd client/web
npx webpack serve --config webpack.config.js --host 0.0.0.0 --allowed-hosts all --port 3000
```

```sh
docker run --rm --init \
  -v "$PWD:/workspace" \
  -v /workspace/client/web/node_modules \
  snaketron-capture \
  --url http://host.docker.internal:3000 \
  --scenario demolition-cutoff \
  --capture-vfps 60 \
  --out tools/video/clips/docker-a
```

The nested anonymous volume retains the image's pinned dependencies while the
outer mount supplies the current source and freshly built `client/pkg`. On
Linux use `--network host` and `--url http://127.0.0.1:3000` instead. Repeat
into `docker-b`, then run the exact comparison above. `meta.json` must report
`capture_path: beginframe` and `deterministic_scope:
same-machine-software-raster`; an explicit headless-shell failure aborts rather
than silently falling back.
