import React, {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from 'react';
import { flushSync } from 'react-dom';
import type {
  ArenaRotation,
  BoostConfig,
  ComboConfig,
  HighlightClip,
  Position,
  ScenarioAddons,
  ScenarioScript,
} from '../types';
import {
  CRASH_EXPLOSION_SPRITE_URL,
  drawCrashExplosions,
  syncPredictedCrashExplosions,
  type CrashExplosion,
  type PredictedCrashCue,
} from '../utils/crashExplosion';
import { buildBoostHudView, type BoostHudView } from '../utils/boostHud';
import { buildComboHudView, type ComboHudView } from '../utils/comboHud';
import {
  createScoreEffectRuntime,
  drawScoreEffects,
  resetScoreEffects,
  syncPredictedScoreEffects,
  type PredictedGoalCue,
} from '../utils/scoreEffects';
import {
  advanceScenarioVirtualTime,
  formatScenarioTimecode,
  scenarioTimingFromHighlight,
  scenarioTimeScaleAt,
  scenarioTimingFromPresentation,
  scenarioViewerDurationMs,
  scenarioViewerElapsedMs,
} from '../utils/scenarioPlayback';
import { loadScenarioSprite } from '../utils/scenarioAssets';
import { initWasm } from '../wasm';
import BoostMeter from './BoostMeter';
import ComboCallout from './ComboCallout';
import './GameArena.css';
import './ScenarioCanvas.css';

type ScenarioPlayerInstance = InstanceType<
  Awaited<ReturnType<typeof initWasm>>['ScenarioPlayer']
>;

export type ScenarioPlaybackStatus =
  | 'loading'
  | 'paused'
  | 'playing'
  | 'complete'
  | 'error';

interface ScenarioFrameSnake {
  snake_id: number;
  user_id: number | null;
  head: Position | null;
  is_alive: boolean;
  food: number;
  boost: {
    charge_ms: number;
    active: boolean;
    intent?: boolean;
  };
  combo: {
    chain_count: number;
    remaining_ms: number;
  };
}

interface ScenarioFrameMeta {
  elapsed_ms: number;
  tick: number;
  predicted_tick: number;
  committed_tick: number;
  tick_duration_ms: number;
  arena_width: number;
  arena_height: number;
  rotation: number;
  star_snake_id: number | null;
  addons: ScenarioAddons;
  boost_config: BoostConfig | null;
  combo_config: ComboConfig;
  cues: readonly PredictedCrashCue[];
  goals: readonly PredictedGoalCue[];
  snakes: readonly ScenarioFrameSnake[];
}

export interface ScenarioPickupCue {
  tick: number;
  sequence: number;
  snake_id: number;
  position: Position;
  points: number;
  combo_chain: number;
  combo_remaining_ms_before: number;
  boost_active: boolean;
}

export interface ScenarioCueTrack {
  tick_duration_ms: number;
  start_tick: number;
  end_tick: number;
  crashes: readonly PredictedCrashCue[];
  goals: readonly PredictedGoalCue[];
  pickups: readonly ScenarioPickupCue[];
  deaths: readonly unknown[];
  heads: readonly unknown[];
}

export interface ScenarioCanvasFrame {
  elapsedMs: number;
  durationMs: number;
  renderedTick: number;
  status: ScenarioPlaybackStatus;
}

export interface ScenarioCanvasHandle {
  ready(): Promise<void>;
  play(): Promise<void>;
  pause(): Promise<void>;
  replay(): Promise<void>;
  seekTo(elapsedMs: number): Promise<void>;
  stepMs(deltaMs: number): Promise<void>;
  stepViewerMs(deltaMs: number): Promise<void>;
  elapsedMs(): number;
  durationMs(): number;
  viewerDurationMs(): number;
  viewerMsForSourceMs(sourceMs: number): number;
  sourceMsForViewerMs(viewerMs: number): number;
  renderedTick(): number;
  cueTrack(): ScenarioCueTrack | null;
}

export type ScenarioCanvasSource =
  | { kind: 'script'; script: ScenarioScript | string }
  | { kind: 'highlight'; clip: HighlightClip | string };

interface ScenarioCanvasBaseProps {
  /** Overrides all authored speed segments when provided. */
  timeScale?: number;
  /** Source flags remain authoritative unless explicitly overridden here. */
  addons?: Partial<ScenarioAddons> | false;
  autoPlay?: boolean;
  loop?: boolean;
  controls?: boolean;
  aspectRatio?: number;
  label?: string;
  className?: string;
  onReady?: () => void;
  onFrame?: (frame: ScenarioCanvasFrame) => void;
  onError?: (error: Error) => void;
}

export type ScenarioCanvasProps = ScenarioCanvasBaseProps & (
  | { source: ScenarioCanvasSource; script?: never; clip?: never }
  | { source?: never; script: ScenarioScript | string; clip?: never }
  | { source?: never; script?: never; clip: HighlightClip | string }
);

interface ScenarioViewState {
  elapsedMs: number;
  durationMs: number;
  renderedTick: number;
  status: ScenarioPlaybackStatus;
  error: string | null;
}

interface ScenarioAddonView {
  boost: BoostHudView | null;
  combo: ComboHudView | null;
  comboVisible: boolean;
  pickupIdentity: string;
  comboAnimationElapsedMs?: number;
}

interface ReadyWaiter {
  resolve: () => void;
  reject: (error: Error) => void;
}

const EMPTY_ADDON_VIEW: ScenarioAddonView = {
  boost: null,
  combo: null,
  comboVisible: false,
  pickupIdentity: 'none',
};

const normalizeError = (value: unknown): Error => (
  value instanceof Error
    ? value
    : new Error(typeof value === 'string' ? value : 'Scenario playback failed')
);

const serializePlaybackPayload = (payload: ScenarioScript | HighlightClip | string): string => {
  if (typeof payload === 'string') {
    return payload;
  }
  return JSON.stringify(payload, (_key, value: unknown) => (
    typeof value === 'bigint' ? Number(value) : value
  ));
};

interface ResolvedScenarioCanvasSource {
  kind: ScenarioCanvasSource['kind'];
  json: string;
  id: string;
  presentation: Partial<ScenarioScript['presentation']>;
}

const parseScenarioIdentity = (scriptJson: string): {
  id: string;
  presentation: Partial<ScenarioScript['presentation']>;
} => {
  try {
    const parsed = JSON.parse(scriptJson) as Partial<ScenarioScript>;
    return {
      id: typeof parsed.id === 'string' && parsed.id ? parsed.id : 'scenario',
      presentation: parsed.presentation ?? {},
    };
  } catch {
    return { id: 'scenario', presentation: {} };
  }
};

const parseHighlightIdentity = (clipJson: string): Omit<
  ResolvedScenarioCanvasSource,
  'kind' | 'json'
> => {
  try {
    const clip = JSON.parse(clipJson) as HighlightClip;
    const timing = scenarioTimingFromHighlight(clip);
    return {
      id: `highlight-${clip.game_id}`,
      presentation: {
        default_time_scale: timing.defaultTimeScale,
        segments: [...timing.segments],
        star_snake_id: clip.star_snake_id,
        rotation: clip.presentation.rotation,
        addons: { combo_callout: true, boost_meter: true },
      },
    };
  } catch {
    return {
      id: 'highlight',
      presentation: {},
    };
  }
};

const resolveScenarioCanvasSource = (
  source: ScenarioCanvasSource | undefined,
  script: ScenarioScript | string | undefined,
  clip: HighlightClip | string | undefined,
): ResolvedScenarioCanvasSource => {
  const input: ScenarioCanvasSource = source ?? (
    clip !== undefined
      ? { kind: 'highlight', clip }
      : { kind: 'script', script: script as ScenarioScript | string }
  );
  const json = serializePlaybackPayload(
    input.kind === 'highlight' ? input.clip : input.script,
  );
  const identity = input.kind === 'highlight'
    ? parseHighlightIdentity(json)
    : parseScenarioIdentity(json);
  return { kind: input.kind, json, ...identity };
};

const boundedElapsed = (value: number, durationMs: number): number => (
  Math.min(
    Math.max(0, Number.isFinite(durationMs) ? durationMs : 0),
    Math.max(0, Number.isFinite(value) ? value : 0),
  )
);

const isArenaRotation = (value: number): value is ArenaRotation => (
  value === 0 || value === 90 || value === 180 || value === 270
);

const ScenarioCanvas = forwardRef<ScenarioCanvasHandle, ScenarioCanvasProps>(({
  source,
  script,
  clip,
  timeScale,
  addons,
  autoPlay = true,
  loop = false,
  controls = true,
  aspectRatio = 16 / 10,
  label,
  className = '',
  onReady,
  onFrame,
  onError,
}, forwardedRef) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const runtimeHandleRef = useRef<ScenarioCanvasHandle | null>(null);
  const timeScaleRef = useRef(timeScale);
  const loopRef = useRef(loop);
  const addonsRef = useRef(addons);
  const onReadyRef = useRef(onReady);
  const onFrameRef = useRef(onFrame);
  const onErrorRef = useRef(onError);
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(() => (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ));
  const [explicitMotion, setExplicitMotion] = useState(false);
  const [view, setView] = useState<ScenarioViewState>({
    elapsedMs: 0,
    durationMs: 0,
    renderedTick: 0,
    status: 'loading',
    error: null,
  });
  const [addonView, setAddonView] = useState<ScenarioAddonView>(EMPTY_ADDON_VIEW);

  timeScaleRef.current = timeScale;
  loopRef.current = loop;
  addonsRef.current = addons;
  onReadyRef.current = onReady;
  onFrameRef.current = onFrame;
  onErrorRef.current = onError;

  const playbackSource = useMemo(
    () => resolveScenarioCanvasSource(source, script, clip),
    [clip, script, source],
  );
  const identity = playbackSource;
  const timing = useMemo(
    () => scenarioTimingFromPresentation(identity.presentation),
    [playbackSource.json, playbackSource.kind],
  );
  const accessibleLabel = label ?? `${identity.id.replace(/-/g, ' ')} replay`;

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') {
      return undefined;
    }
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = () => setPrefersReducedMotion(query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);

  useImperativeHandle(forwardedRef, () => ({
    ready: () => runtimeHandleRef.current?.ready() ?? Promise.reject(
      new Error('Scenario player is not mounted'),
    ),
    play: () => runtimeHandleRef.current?.play() ?? Promise.reject(
      new Error('Scenario player is not mounted'),
    ),
    pause: () => runtimeHandleRef.current?.pause() ?? Promise.resolve(),
    replay: () => runtimeHandleRef.current?.replay() ?? Promise.reject(
      new Error('Scenario player is not mounted'),
    ),
    seekTo: (elapsedMs) => runtimeHandleRef.current?.seekTo(elapsedMs) ?? Promise.reject(
      new Error('Scenario player is not mounted'),
    ),
    stepMs: (deltaMs) => runtimeHandleRef.current?.stepMs(deltaMs) ?? Promise.reject(
      new Error('Scenario player is not mounted'),
    ),
    stepViewerMs: (deltaMs) => runtimeHandleRef.current?.stepViewerMs(deltaMs) ?? Promise.reject(
      new Error('Scenario player is not mounted'),
    ),
    elapsedMs: () => runtimeHandleRef.current?.elapsedMs() ?? 0,
    durationMs: () => runtimeHandleRef.current?.durationMs() ?? 0,
    viewerDurationMs: () => runtimeHandleRef.current?.viewerDurationMs() ?? 0,
    viewerMsForSourceMs: (sourceMs) => (
      runtimeHandleRef.current?.viewerMsForSourceMs(sourceMs) ?? 0
    ),
    sourceMsForViewerMs: (viewerMs) => (
      runtimeHandleRef.current?.sourceMsForViewerMs(viewerMs) ?? 0
    ),
    renderedTick: () => runtimeHandleRef.current?.renderedTick() ?? 0,
    cueTrack: () => runtimeHandleRef.current?.cueTrack() ?? null,
  }), []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return undefined;
    }

    let disposed = false;
    let ready = false;
    let failure: Error | null = null;
    let player: ScenarioPlayerInstance | null = null;
    let cueTrack: ScenarioCueTrack | null = null;
    let durationMs = 0;
    let elapsedMs = 0;
    let renderedTick = 0;
    let playing = false;
    let explicitMotionEnabled = false;
    let animationFrame = 0;
    let bootstrapFrame = 0;
    let lastViewerNow: number | null = null;
    let crashSprite: HTMLImageElement | null = null;
    const readyWaiters: ReadyWaiter[] = [];
    const crashes: CrashExplosion[] = [];
    const seenCrashIds = new Set<string>();
    const scoreRuntime = createScoreEffectRuntime();
    const captureMode = document.documentElement.dataset.scenarioCapture === 'true';
    let captureComboIdentity = 'none';
    let captureComboStartedAtMs = 0;

    setExplicitMotion(false);
    setAddonView(EMPTY_ADDON_VIEW);
    setView({
      elapsedMs: 0,
      durationMs: 0,
      renderedTick: 0,
      status: 'loading',
      error: null,
    });
    canvas.dataset.playback = 'loading';
    delete canvas.dataset.ready;

    const effectiveReducedMotion = () => (
      prefersReducedMotion && !explicitMotionEnabled
    );

    const resolveReadyWaiters = () => {
      readyWaiters.splice(0).forEach(({ resolve }) => resolve());
    };

    const rejectReadyWaiters = (error: Error) => {
      readyWaiters.splice(0).forEach(({ reject }) => reject(error));
    };

    const waitUntilReady = (): Promise<void> => {
      if (ready) {
        return Promise.resolve();
      }
      if (failure) {
        return Promise.reject(failure);
      }
      return new Promise<void>((resolve, reject) => {
        readyWaiters.push({ resolve, reject });
      });
    };

    const updateStatus = (status: ScenarioPlaybackStatus, error: string | null = null) => {
      canvas.dataset.playback = status;
      setView((current) => ({ ...current, status, error }));
    };

    const fail = (value: unknown) => {
      if (disposed) {
        return;
      }
      const error = normalizeError(value);
      failure = error;
      playing = false;
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
        animationFrame = 0;
      }
      updateStatus('error', error.message);
      rejectReadyWaiters(error);
      onErrorRef.current?.(error);
      console.error('ScenarioCanvas playback failed', identity.id, error);
    };

    const sizeCanvas = (): boolean => {
      const bounds = canvas.getBoundingClientRect();
      if (bounds.width <= 0 || bounds.height <= 0) {
        return false;
      }
      const pixelRatio = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.max(1, Math.round(bounds.width * pixelRatio));
      const height = Math.max(1, Math.round(bounds.height * pixelRatio));
      if (canvas.width !== width) {
        canvas.width = width;
      }
      if (canvas.height !== height) {
        canvas.height = height;
      }
      return true;
    };

    const resetVisualEffects = () => {
      crashes.length = 0;
      seenCrashIds.clear();
      resetScoreEffects(scoreRuntime);
      captureComboIdentity = 'none';
      captureComboStartedAtMs = elapsedMs;
    };

    const addonFlags = (meta: ScenarioFrameMeta): ScenarioAddons => {
      if (addonsRef.current === false) {
        return { combo_callout: false, boost_meter: false };
      }
      return {
        combo_callout: addonsRef.current?.combo_callout ?? meta.addons.combo_callout,
        boost_meter: addonsRef.current?.boost_meter ?? meta.addons.boost_meter,
      };
    };

    const buildAddonView = (meta: ScenarioFrameMeta): ScenarioAddonView => {
      const flags = addonFlags(meta);
      const starSnakeId = meta.star_snake_id ?? meta.snakes[0]?.snake_id ?? null;
      const starSnake = starSnakeId === null
        ? null
        : meta.snakes.find((snake) => snake.snake_id === starSnakeId) ?? null;
      const latestPickup = starSnakeId === null
        ? null
        : [...(cueTrack?.pickups ?? [])]
          .reverse()
          .find((pickup) => pickup.snake_id === starSnakeId && pickup.tick <= meta.tick) ?? null;
      const combo = flags.combo_callout && starSnake
        ? buildComboHudView(meta.combo_config, starSnake)
        : null;
      const boost = flags.boost_meter && meta.boost_config && starSnake
        ? buildBoostHudView(meta.boost_config, starSnake, false, false)
        : null;
      const pickupIdentity = latestPickup
        ? `${latestPickup.tick}:${latestPickup.sequence}`
        : 'none';
      if (captureMode && pickupIdentity !== captureComboIdentity) {
        captureComboIdentity = pickupIdentity;
        captureComboStartedAtMs = elapsedMs;
      }

      return {
        boost,
        combo,
        comboVisible: Boolean(combo?.active),
        pickupIdentity,
        comboAnimationElapsedMs: captureMode && pickupIdentity !== 'none'
          ? elapsedMs - captureComboStartedAtMs
          : undefined,
      };
    };

    const commitFrame = (
      meta: ScenarioFrameMeta,
      nextAddonView: ScenarioAddonView,
      syncDom: boolean,
    ) => {
      const nextView: ScenarioViewState = {
        elapsedMs,
        durationMs,
        renderedTick,
        status: playing ? 'playing' : elapsedMs >= durationMs ? 'complete' : 'paused',
        error: null,
      };
      const commit = () => {
        setView(nextView);
        setAddonView(nextAddonView);
      };
      if (syncDom) {
        flushSync(commit);
      } else {
        commit();
      }
      onFrameRef.current?.({
        elapsedMs,
        durationMs,
        renderedTick,
        status: nextView.status,
      });
      canvas.dataset.elapsedMs = Math.floor(elapsedMs).toString();
      canvas.dataset.renderedTick = renderedTick.toString();
      canvas.dataset.playback = nextView.status;
      canvas.dataset.motion = effectiveReducedMotion() ? 'reduced' : 'animated';
      canvas.dataset.scenario = identity.id;
      void meta;
    };

    const renderAt = (requestedElapsedMs: number, syncDom = false): void => {
      if (disposed || !player || !sizeCanvas()) {
        return;
      }
      const nextElapsedMs = boundedElapsed(requestedElapsedMs, durationMs);
      if (nextElapsedMs < elapsedMs) {
        resetVisualEffects();
      }
      elapsedMs = nextElapsedMs;

      const meta = JSON.parse(
        player.frameMetaJson(Math.floor(elapsedMs)),
      ) as ScenarioFrameMeta;
      renderedTick = meta.tick;
      const rotation: ArenaRotation = isArenaRotation(meta.rotation)
        ? meta.rotation
        : 0;
      const visualState = {
        predicted_tick: meta.predicted_tick,
        committed_tick: meta.committed_tick,
        tick_duration_ms: meta.tick_duration_ms,
        cues: meta.cues,
        goals: meta.goals,
      };
      syncPredictedCrashExplosions(
        crashes,
        seenCrashIds,
        `scenario:${identity.id}`,
        visualState,
        elapsedMs,
      );
      syncPredictedScoreEffects(
        scoreRuntime,
        `scenario:${identity.id}`,
        visualState,
        elapsedMs,
      );

      player.renderFrameWithEffects(
        Math.floor(elapsedMs),
        canvas,
        (scratchCanvas: HTMLCanvasElement, scratchCellSize: number) => {
          const context = scratchCanvas.getContext('2d');
          if (!context) {
            return;
          }
          drawScoreEffects(context, scoreRuntime, {
            nowMs: elapsedMs,
            cellSize: scratchCellSize,
            arenaWidth: meta.arena_width,
            arenaHeight: meta.arena_height,
            rotation,
            localTeamId: null,
            reducedMotion: effectiveReducedMotion(),
          });
        },
        (scratchCanvas: HTMLCanvasElement, scratchCellSize: number) => {
          const context = scratchCanvas.getContext('2d');
          if (!context) {
            return;
          }
          drawCrashExplosions(
            context,
            crashSprite,
            crashes,
            elapsedMs,
            scratchCellSize,
            meta.arena_width,
            meta.arena_height,
            rotation,
            effectiveReducedMotion(),
          );
        },
      );

      commitFrame(meta, buildAddonView(meta), syncDom);
    };

    const schedule = () => {
      if (!disposed && playing && !document.hidden && !animationFrame) {
        animationFrame = window.requestAnimationFrame(draw);
      }
    };

    const draw = (viewerNow: number) => {
      animationFrame = 0;
      if (disposed || !playing || document.hidden || !player) {
        return;
      }
      if (lastViewerNow === null) {
        lastViewerNow = viewerNow;
      }
      const viewerDeltaMs = Math.max(0, viewerNow - lastViewerNow);
      lastViewerNow = viewerNow;
      const nextElapsedMs = advanceScenarioVirtualTime(
        elapsedMs,
        viewerDeltaMs,
        durationMs,
        timing,
        timeScaleRef.current,
      );

      try {
        renderAt(nextElapsedMs);
      } catch (error) {
        fail(error);
        return;
      }

      if (nextElapsedMs >= durationMs) {
        if (loopRef.current) {
          resetVisualEffects();
          elapsedMs = 0;
          lastViewerNow = viewerNow;
          try {
            renderAt(0);
          } catch (error) {
            fail(error);
            return;
          }
        } else {
          playing = false;
          updateStatus('complete');
          return;
        }
      }
      schedule();
    };

    const play = async (): Promise<void> => {
      await waitUntilReady();
      if (disposed || !player) {
        return;
      }
      if (prefersReducedMotion && !explicitMotionEnabled) {
        explicitMotionEnabled = true;
        setExplicitMotion(true);
      }
      if (elapsedMs >= durationMs) {
        resetVisualEffects();
        elapsedMs = 0;
        renderAt(0);
      }
      playing = true;
      lastViewerNow = null;
      updateStatus('playing');
      schedule();
    };

    const pause = async (): Promise<void> => {
      if (!ready && !failure) {
        await waitUntilReady();
      }
      playing = false;
      lastViewerNow = null;
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
        animationFrame = 0;
      }
      if (!disposed && !failure) {
        updateStatus(elapsedMs >= durationMs ? 'complete' : 'paused');
      }
    };

    const replay = async (): Promise<void> => {
      await waitUntilReady();
      if (prefersReducedMotion && !explicitMotionEnabled) {
        explicitMotionEnabled = true;
        setExplicitMotion(true);
      }
      resetVisualEffects();
      elapsedMs = 0;
      playing = true;
      lastViewerNow = null;
      renderAt(0);
      updateStatus('playing');
      schedule();
    };

    const seekTo = async (nextElapsedMs: number): Promise<void> => {
      await waitUntilReady();
      lastViewerNow = null;
      renderAt(nextElapsedMs);
      if (elapsedMs >= durationMs && playing && !loopRef.current) {
        playing = false;
        updateStatus('complete');
      }
    };

    const stepTo = async (nextElapsedMs: number): Promise<void> => {
      await waitUntilReady();
      playing = false;
      lastViewerNow = null;
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
        animationFrame = 0;
      }
      renderAt(nextElapsedMs, true);
      // flushSync commits the React tree, and this layout read instantiates any
      // CSS transitions before the external capture clock advances. The combo
      // flourish itself is scrubbed from scenario time, so an event landing on
      // a frame boundary cannot inherit a scheduler-dependent animation age.
      if (captureMode) {
        void canvas.closest('.scenario-canvas')?.getBoundingClientRect();
      }
      await Promise.resolve();
    };

    const stepMs = async (deltaMs: number): Promise<void> => stepTo(
      elapsedMs + Math.max(0, Number.isFinite(deltaMs) ? deltaMs : 0),
    );

    const stepViewerMs = async (deltaMs: number): Promise<void> => {
      await waitUntilReady();
      await stepTo(advanceScenarioVirtualTime(
        elapsedMs,
        deltaMs,
        durationMs,
        timing,
        timeScaleRef.current,
      ));
    };

    const runtimeHandle: ScenarioCanvasHandle = {
      ready: waitUntilReady,
      play,
      pause,
      replay,
      seekTo,
      stepMs,
      stepViewerMs,
      elapsedMs: () => elapsedMs,
      durationMs: () => durationMs,
      viewerDurationMs: () => scenarioViewerDurationMs(
        durationMs,
        timing,
        timeScaleRef.current,
      ),
      viewerMsForSourceMs: (sourceMs) => scenarioViewerElapsedMs(
        sourceMs,
        durationMs,
        timing,
        timeScaleRef.current,
      ),
      sourceMsForViewerMs: (viewerMs) => advanceScenarioVirtualTime(
        0,
        viewerMs,
        durationMs,
        timing,
        timeScaleRef.current,
      ),
      renderedTick: () => player?.renderedTick() ?? renderedTick,
      cueTrack: () => cueTrack,
    };
    runtimeHandleRef.current = runtimeHandle;

    const handleVisibility = () => {
      lastViewerNow = null;
      if (document.hidden) {
        if (animationFrame) {
          window.cancelAnimationFrame(animationFrame);
          animationFrame = 0;
        }
        return;
      }
      if (playing) {
        schedule();
      } else if (player) {
        try {
          renderAt(elapsedMs);
        } catch (error) {
          fail(error);
        }
      }
    };

    const observer = new ResizeObserver(() => {
      if (!player || disposed) {
        return;
      }
      try {
        renderAt(elapsedMs);
      } catch (error) {
        fail(error);
      }
    });
    observer.observe(canvas);
    document.addEventListener('visibilitychange', handleVisibility);

    const handleContextLoss = (event: Event) => {
      event.preventDefault();
      fail(new Error('Replay canvas context was lost'));
    };
    canvas.addEventListener('contextlost', handleContextLoss);

    const sprite = new Image();
    sprite.decoding = 'async';
    crashSprite = sprite;
    const spriteReady = loadScenarioSprite(sprite, CRASH_EXPLOSION_SPRITE_URL);
    const fontsReady = Promise.all([
      document.fonts?.ready ?? Promise.resolve(),
      window.__SNAKETRON_CAPTURE_FONTS_READY__ ?? Promise.resolve(),
    ]);

    void Promise.all([initWasm(), spriteReady, fontsReady])
      .then(([wasm]) => {
        if (disposed) {
          return;
        }
        player = playbackSource.kind === 'highlight'
          ? wasm.ScenarioPlayer.fromHighlightClip(playbackSource.json)
          : new wasm.ScenarioPlayer(playbackSource.json);
        durationMs = player.durationMs();
        cueTrack = JSON.parse(player.cueTrackJson()) as ScenarioCueTrack;

        const poseInitialFrame = () => {
          bootstrapFrame = 0;
          if (disposed || !player) {
            return;
          }
          if (!sizeCanvas()) {
            bootstrapFrame = window.requestAnimationFrame(poseInitialFrame);
            return;
          }
          elapsedMs = prefersReducedMotion ? player.posterMs() : 0;
          try {
            renderAt(elapsedMs, true);
          } catch (error) {
            fail(error);
            return;
          }
          ready = true;
          canvas.dataset.ready = 'true';
          resolveReadyWaiters();
          onReadyRef.current?.();

          if (autoPlay && !prefersReducedMotion) {
            playing = true;
            lastViewerNow = null;
            updateStatus('playing');
            schedule();
          } else {
            updateStatus('paused');
          }
        };
        poseInitialFrame();
      })
      .catch(fail);

    return () => {
      disposed = true;
      playing = false;
      observer.disconnect();
      document.removeEventListener('visibilitychange', handleVisibility);
      canvas.removeEventListener('contextlost', handleContextLoss);
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
      }
      if (bootstrapFrame) {
        window.cancelAnimationFrame(bootstrapFrame);
      }
      crashSprite = null;
      resetVisualEffects();
      player?.free();
      rejectReadyWaiters(new Error('Scenario player was disposed'));
      if (runtimeHandleRef.current === runtimeHandle) {
        runtimeHandleRef.current = null;
      }
    };
  }, [autoPlay, identity.id, playbackSource.json, playbackSource.kind, prefersReducedMotion, timing]);

  const activeTimeScale = scenarioTimeScaleAt(
    timing,
    view.elapsedMs,
    timeScale,
  );
  const progress = view.durationMs > 0
    ? Math.min(1, Math.max(0, view.elapsedMs / view.durationMs))
    : 0;
  const isReady = view.status !== 'loading' && view.status !== 'error';
  const motionMode = prefersReducedMotion && !explicitMotion
    ? 'reduced'
    : explicitMotion ? 'explicit' : 'animated';

  return (
    <section
      className={`scenario-canvas${className ? ` ${className}` : ''}`}
      aria-label={accessibleLabel}
      data-testid="scenario-canvas"
      data-scenario={identity.id}
      data-playback={view.status}
      data-motion={motionMode}
    >
      <div className="scenario-canvas__viewport" style={{ aspectRatio }}>
        <canvas
          ref={canvasRef}
          className="scenario-canvas__surface"
          data-testid="scenario-canvas-surface"
          aria-hidden="true"
        />

        {addonView.combo && (
          <ComboCallout
            hud={addonView.combo}
            isVisible={addonView.comboVisible}
            pickupIdentity={addonView.pickupIdentity}
            animationElapsedMs={addonView.comboAnimationElapsedMs}
          />
        )}

        {view.status === 'loading' && (
          <div className="scenario-canvas__message" role="status">
            <span className="scenario-canvas__loader" aria-hidden="true" />
            Loading replay
          </div>
        )}

        {view.status === 'error' && (
          <div className="scenario-canvas__message is-error" role="alert">
            <strong>Replay unavailable</strong>
            <span>{view.error ?? 'The scenario could not be rendered.'}</span>
          </div>
        )}

        {prefersReducedMotion && !explicitMotion && isReady && (
          <button
            type="button"
            className="scenario-canvas__poster-play"
            onClick={() => void runtimeHandleRef.current?.play()}
            aria-label="Play replay animation"
          >
            <span aria-hidden="true">▶</span>
            Play replay
          </button>
        )}
      </div>

      {addonView.boost && (
        <div className="scenario-canvas__boost" data-testid="scenario-boost-addon">
          <BoostMeter
            mode="display"
            hud={addonView.boost}
            location="scenario-replay"
          />
        </div>
      )}

      {controls && (
        <div
          className="scenario-canvas__rail"
          data-testid="scenario-playback-rail"
          style={{ '--scenario-progress': progress } as React.CSSProperties}
        >
          <button
            type="button"
            className="scenario-canvas__transport"
            onClick={() => {
              if (view.status === 'playing') {
                void runtimeHandleRef.current?.pause();
              } else {
                void runtimeHandleRef.current?.play();
              }
            }}
            disabled={!isReady}
            aria-label={view.status === 'playing' ? 'Pause replay' : 'Play replay'}
            data-testid="scenario-play-toggle"
          >
            <span aria-hidden="true">{view.status === 'playing' ? 'Ⅱ' : '▶'}</span>
          </button>
          <time className="scenario-canvas__time is-current">
            {formatScenarioTimecode(view.elapsedMs)}
          </time>
          <input
            type="range"
            className="scenario-canvas__scrubber"
            min={0}
            max={Math.max(1, Math.floor(view.durationMs))}
            step={1}
            value={Math.min(Math.floor(view.elapsedMs), Math.max(1, Math.floor(view.durationMs)))}
            onChange={(event) => {
              void runtimeHandleRef.current?.seekTo(Number(event.currentTarget.value));
            }}
            disabled={!isReady}
            aria-label="Replay position"
            aria-valuetext={`${formatScenarioTimecode(view.elapsedMs)} of ${formatScenarioTimecode(view.durationMs)}`}
            data-testid="scenario-scrubber"
          />
          <time className="scenario-canvas__time is-duration">
            {formatScenarioTimecode(view.durationMs)}
          </time>
          <span className="scenario-canvas__rate" aria-label={`Playback speed ${activeTimeScale} times`}>
            {activeTimeScale.toFixed(activeTimeScale % 1 === 0 ? 0 : 2)}×
          </span>
          <button
            type="button"
            className="scenario-canvas__transport is-replay"
            onClick={() => void runtimeHandleRef.current?.replay()}
            disabled={!isReady}
            aria-label="Replay from the beginning"
            data-testid="scenario-replay"
          >
            <span aria-hidden="true">↺</span>
          </button>
        </div>
      )}

      <span className="sr-only" role="status" aria-live="polite" aria-atomic="true">
        Replay {view.status}.
      </span>
    </section>
  );
});

ScenarioCanvas.displayName = 'ScenarioCanvas';

export default ScenarioCanvas;
