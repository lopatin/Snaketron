# ffmpeg recipe notes

Keep these patterns inside deterministic scripts. Escape commas in timeline expressions.

## Timing and joins

- Normalize every stream with `fps=<fps>,settb=AVTB,setpts=PTS-STARTPTS` before joining.
- Build a speed interval with `trim=start=S:end=E,setpts=(PTS-STARTPTS)/RATE`; concatenate intervals in source order.
- For a transition, calculate `offset = incoming.global_start` after prior overlap and beat-snap adjustments. Pair `xfade` with `acrossfade` at the same duration.
- Use concat for a hard cut. Do not emulate a hard cut with a one-frame dissolve.

## Effects

- Shake: overscale, then animate crop `x` and `y` with two non-harmonic sines multiplied by exponential decay.
- Punch-in: dynamically scale and center-crop. Avoid `zoompan` for impacts because integer rounding can wobble.
- RGB split: gate `rgbashift` with `enable=between(t,START,END)`.
- Full glow extension: `split`, isolate bright luma, `gblur`, then screen-blend over the untouched branch. Keep the built-in lightweight `unsharp` recipe for previews.
- Grain/vignette: gate `noise` and `vignette`; avoid crushing game-grid detail.
- LUT: apply `lut3d` while still RGB. Use the identity LUT as the safe default.
- Letterbox: overlay top and bottom `drawbox` bands; do not resize gameplay to create bars.

## Audio

Normalize sources with `aresample=48000,aformat=sample_fmts=fltp:channel_layouts=stereo`. Delay SFX in their global output timebase. Combine the SFX bus without normalization, use it as the sidechain for music, then mix with timeline audio. Run `loudnorm=I=-14:TP=-1.5:LRA=11` last.

## Color and delivery

Keep captures and cached segment masters RGB. Perform the only RGB→YUV conversion in final assembly with `scale=out_color_matrix=bt709,format=yuv420p`. Tag `color_primaries`, `color_trc`, and `colorspace` as `bt709`; encode H.264 CRF 18 slow with `+faststart`. Use CRF 28 veryfast and burned-in timecode for previews.
