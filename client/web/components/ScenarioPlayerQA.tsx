import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  SCENARIO_FIXTURES,
  scenarioFixtureById,
} from '../scenarios';
import type {
  ScenarioCanvasFrame,
  ScenarioCanvasHandle,
  ScenarioCanvasSource,
  ScenarioCueTrack,
} from './ScenarioCanvas';
import ScenarioCanvas from './ScenarioCanvas';
import './ScenarioCanvas.css';

const QA_TIME_SCALES = [0.1, 0.25, 0.5, 1, 2, 4] as const;

export interface SnaketronCaptureApi {
  ready(): Promise<void>;
  stepMs(deltaMs: number): Promise<void>;
  stepViewerMs(deltaMs: number): Promise<void>;
  durationMs(): number;
  viewerDurationMs(): number;
  viewerMsForSourceMs(sourceMs: number): number;
  sourceMsForViewerMs(viewerMs: number): number;
  starSnakeId(): number | null;
  renderedTick(): number;
  cueTrack(): ScenarioCueTrack | null;
}

interface ScenarioPlayerQAProps {
  captureMode?: boolean;
}

const parseTimeScale = (raw: string | null): number | undefined => {
  if (raw === null || raw.trim() === '') {
    return undefined;
  }
  const value = Number(raw);
  return Number.isFinite(value)
    ? Math.min(4, Math.max(0.1, value))
    : undefined;
};

const sourceStarSnakeId = (source: ScenarioCanvasSource): number | null => {
  try {
    const raw = source.kind === 'highlight' ? source.clip : source.script;
    const parsed = typeof raw === 'string' ? JSON.parse(raw) as any : raw as any;
    const candidate = source.kind === 'highlight'
      ? parsed.star_snake_id
      : parsed.presentation?.star_snake_id;
    return Number.isInteger(candidate) && candidate >= 0 ? candidate : null;
  } catch {
    return null;
  }
};

const ScenarioPlayerQA: React.FC<ScenarioPlayerQAProps> = ({
  captureMode = false,
}) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const playerRef = useRef<ScenarioCanvasHandle>(null);
  const fixture = scenarioFixtureById(searchParams.get('scenario'));
  const injectedSource = captureMode
    ? window.__SNAKETRON_CAPTURE_SOURCE__
    : undefined;
  const playbackSource = useMemo<ScenarioCanvasSource>(() => (
    injectedSource ?? {
      kind: 'script',
      script: fixture.script,
    }
  ), [fixture, injectedSource]);
  const starSnakeId = useMemo(
    () => sourceStarSnakeId(playbackSource),
    [playbackSource],
  );
  const timeScale = parseTimeScale(searchParams.get('timeScale'));
  const [frame, setFrame] = useState<ScenarioCanvasFrame>({
    elapsedMs: 0,
    durationMs: 0,
    renderedTick: 0,
    status: 'loading',
  });

  const updateQuery = (key: string, value: string | null) => {
    const next = new URLSearchParams(searchParams);
    if (value === null) {
      next.delete(key);
    } else {
      next.set(key, value);
    }
    setSearchParams(next, { replace: true });
  };

  useEffect(() => {
    if (!captureMode) {
      return undefined;
    }

    let disposed = false;
    const player = async (): Promise<ScenarioCanvasHandle> => {
      while (!disposed && playerRef.current === null) {
        await new Promise<void>((resolve) => window.requestAnimationFrame(() => resolve()));
      }
      if (disposed || !playerRef.current) {
        throw new Error('Scenario capture surface was disposed');
      }
      return playerRef.current;
    };

    const captureApi: SnaketronCaptureApi = {
      ready: async () => (await player()).ready(),
      stepMs: async (deltaMs) => (await player()).stepMs(deltaMs),
      stepViewerMs: async (deltaMs) => (await player()).stepViewerMs(deltaMs),
      durationMs: () => playerRef.current?.durationMs() ?? 0,
      viewerDurationMs: () => playerRef.current?.viewerDurationMs() ?? 0,
      viewerMsForSourceMs: (sourceMs) => (
        playerRef.current?.viewerMsForSourceMs(sourceMs) ?? 0
      ),
      sourceMsForViewerMs: (viewerMs) => (
        playerRef.current?.sourceMsForViewerMs(viewerMs) ?? 0
      ),
      starSnakeId: () => starSnakeId,
      renderedTick: () => playerRef.current?.renderedTick() ?? 0,
      cueTrack: () => playerRef.current?.cueTrack() ?? null,
    };
    window.__SNAKETRON_CAPTURE__ = captureApi;
    window.__scenarioCapture = captureApi;

    return () => {
      disposed = true;
      if (window.__SNAKETRON_CAPTURE__ === captureApi) {
        delete window.__SNAKETRON_CAPTURE__;
      }
      if (window.__scenarioCapture === captureApi) {
        delete window.__scenarioCapture;
      }
      delete document.documentElement.dataset.scenarioCaptureReady;
    };
  }, [captureMode, fixture.id, starSnakeId]);

  const player = (
    <ScenarioCanvas
      key={injectedSource ? `injected-${searchParams.get('scenario') ?? 'source'}` : fixture.id}
      ref={playerRef}
      source={playbackSource}
      timeScale={timeScale}
      autoPlay={!captureMode}
      loop={!captureMode}
      controls={!captureMode}
      aspectRatio={captureMode ? 16 / 9 : 16 / 10}
      label={`${fixture.label} deterministic replay`}
      onReady={() => {
        if (captureMode) {
          document.documentElement.dataset.scenarioCaptureReady = 'true';
        }
      }}
      onFrame={setFrame}
    />
  );

  if (captureMode) {
    return (
      <main
        className="scenario-capture"
        aria-label={`${fixture.label} capture surface`}
        data-testid="scenario-capture"
      >
        {player}
      </main>
    );
  }

  return (
    <main className="scenario-qa" data-testid="scenario-player-qa">
      <div className="scenario-qa__shell">
        <header className="scenario-qa__masthead">
          <div>
            <p className="scenario-qa__eyebrow">Deterministic replay lab</p>
            <h1>Scenario desk</h1>
            <p className="scenario-qa__intro">
              The real game engine, advanced by a viewer clock. Scrub, slow the
              cut, and compare every checked broadcast fixture without a socket.
            </p>
          </div>
          <div className="scenario-qa__status" aria-live="polite">
            <span>State</span><strong>{frame.status}</strong>
            <span>Tick</span><strong>{frame.renderedTick}</strong>
            <span>Scale</span><strong>{timeScale ?? 'authored'}×</strong>
          </div>
        </header>

        <div className="scenario-qa__deck">
          {player}

          <aside className="scenario-qa__sidecar" aria-label="Replay controls">
            <section className="scenario-qa__control-group">
              <span className="scenario-qa__control-label">Checked scenario</span>
              <div className="scenario-qa__scenario-list">
                {SCENARIO_FIXTURES.map((candidate) => (
                  <button
                    key={candidate.id}
                    type="button"
                    className="scenario-qa__scenario-button"
                    aria-pressed={candidate.id === fixture.id}
                    onClick={() => updateQuery('scenario', candidate.id)}
                  >
                    {candidate.label}
                  </button>
                ))}
              </div>
            </section>

            <section className="scenario-qa__control-group">
              <span className="scenario-qa__control-label">Viewer speed</span>
              <div className="scenario-qa__rate-list">
                {QA_TIME_SCALES.map((rate) => (
                  <button
                    key={rate}
                    type="button"
                    className="scenario-qa__rate-button"
                    aria-pressed={timeScale === rate}
                    onClick={() => updateQuery('timeScale', String(rate))}
                  >
                    {rate}×
                  </button>
                ))}
              </div>
            </section>

            <p className="scenario-qa__fixture-copy">
              <strong>{fixture.callout}</strong> {fixture.summary}
            </p>
          </aside>
        </div>
      </div>
    </main>
  );
};

declare global {
  interface Window {
    __SNAKETRON_CAPTURE__?: SnaketronCaptureApi;
    __scenarioCapture?: SnaketronCaptureApi;
    __SNAKETRON_CAPTURE_SOURCE__?: ScenarioCanvasSource;
  }
}

export default ScenarioPlayerQA;
