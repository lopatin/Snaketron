/**
 * Produces the star head track with the same death-hold policy as
 * ScenarioCueTrack::follow_head in the WASM player.
 *
 * Scenario follow cameras hold the latest death for the rest of the script.
 * Highlight cameras ignore deaths before the selected focus tick so a death
 * in pre-roll cannot pin a later, respawned payoff to the old crash cell.
 */
export function buildStarHeadFrames(
  cueTrack,
  starSnakeId,
  frameCount,
  frameMs,
  scenarioDurationMs,
  cameraFocusTick = null,
  virtualFrameTimesMs = null,
) {
  if (!Number.isInteger(starSnakeId) || starSnakeId < 0) return [];
  const heads = cueTrack.heads ?? [];
  const hasHighlightFocus = Number.isInteger(cameraFocusTick) && cameraFocusTick >= 0;
  const deaths = (cueTrack.deaths ?? [])
    .filter((death) => (
      death.snake_id === starSnakeId &&
      (!hasHighlightFocus || death.tick >= cameraFocusTick)
    ))
    .sort((left, right) => left.tick - right.tick);
  const frames = [];
  let headIndex = -1;
  let deathIndex = -1;

  for (let frame = 0; frame < frameCount; frame += 1) {
    const masterMs = (frame + 1) * frameMs;
    const mappedVirtualMs = virtualFrameTimesMs?.[frame] ?? masterMs;
    const virtualMs = Math.min(scenarioDurationMs, Math.max(0, mappedVirtualMs));
    const tick = Math.min(
      cueTrack.end_tick,
      cueTrack.start_tick + Math.floor(virtualMs / cueTrack.tick_duration_ms),
    );
    while (headIndex + 1 < heads.length && heads[headIndex + 1].tick <= tick) headIndex += 1;
    while (deathIndex + 1 < deaths.length && deaths[deathIndex + 1].tick <= tick) deathIndex += 1;
    const snake = heads[headIndex]?.snakes?.find((entry) => entry.snake_id === starSnakeId) ?? null;
    // Match Rust's `find(...).and_then(hold_position)`: a latest applicable
    // death without a hold cell falls back to the frame's live/dead head.
    const held = deathIndex >= 0 ? deaths[deathIndex].hold_position : null;
    frames.push({
      frame,
      master_seconds: masterMs / 1000,
      virtual_ms: virtualMs,
      tick,
      head: held ?? snake?.head ?? null,
      is_alive: held ? false : snake?.is_alive ?? false,
    });
  }
  return frames;
}
// Do not set Playwright's `animations: "disabled"` here. That option fast-forwards
// finite CSS animations and transitions, which makes the virtual-time fallback
// render different celebration frames from HeadlessExperimental.beginFrame.
export const FALLBACK_SCREENSHOT_OPTIONS = Object.freeze({ type: 'png' });

// Keep canvas glyph rasterization on a grayscale, unhinted path in both the
// macOS fallback and pinned Linux shell. Together with the capture-only OFL
// FontFace assets, this removes platform-specific CoreText/FreeType LCD and
// hinting deltas without changing the live application.
export const CAPTURE_BROWSER_ARGS = Object.freeze([
  '--deterministic-mode',
  '--disable-gpu',
  '--disable-dev-shm-usage',
  '--disable-font-subpixel-positioning',
  '--disable-lcd-text',
  '--font-render-hinting=none',
  '--hide-scrollbars',
  '--force-device-scale-factor=1',
]);
