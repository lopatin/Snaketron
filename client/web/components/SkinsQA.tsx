import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { getWasm, initWasm, whenSkinAssetsSettle } from '../wasm';
import {
  DEFAULT_SKIN_REF,
  readSkinCatalog,
  type SkinCatalogEntry,
} from '../utils/equippedSkin';

/**
 * The skin contact sheet.
 *
 * Every tile is painted by the real renderer against the same fixture corpus
 * the golden traces and the conformance suite use, so "it looked right here"
 * and "the tests pass" are claims about the same pictures. That is the whole
 * point of the route: an agent authoring a skin can look at it.
 *
 * It also carries the only skin selector that ships — writing the local
 * preference the arena reads at join, which is what exercises the
 * client → server → cosmetic-map path end to end without a player-facing UI.
 */

interface PoseFixture {
  name: string;
  cellsWide: number;
  cellsHigh: number;
}

interface Fixtures {
  poses: PoseFixture[];
  roles: string[];
  cellSizes: number[];
  animSamples: number[];
}

const TILE_CELL_SIZE = 14;
/** Two cells of margin so a boosting snake's band is never clipped. */
const TILE_MARGIN_CELLS = 2;

/**
 * Tiles are sized from the pose, not from a constant.
 *
 * The corpus now spans 1 to 33 cells, and a fixed tile sized for the old
 * corpus silently cropped the long ones — which is the exact failure a contact
 * sheet exists to catch rather than introduce.
 */
const tileSize = (pose: PoseFixture | undefined) => ({
  width: ((pose?.cellsWide ?? 11) + TILE_MARGIN_CELLS) * TILE_CELL_SIZE,
  height: ((pose?.cellsHigh ?? 11) + TILE_MARGIN_CELLS) * TILE_CELL_SIZE,
});

interface TileProps {
  skinRef: string;
  pose: string;
  size: { width: number; height: number };
  role: string;
  boostActive?: boolean;
  dead?: boolean;
  animMs?: number;
  reducedMotion?: boolean;
  /**
   * Drive this tile from the real animation-frame clock instead of painting
   * one fixed sample. Repaints the canvas directly rather than through React
   * state, so a wall of live tiles costs canvas work and nothing else.
   */
  live?: boolean;
  label: string;
  testId: string;
}

const FixtureTile: React.FC<TileProps> = ({
  skinRef,
  pose,
  size,
  role,
  boostActive = false,
  dead = false,
  animMs = 0,
  reducedMotion = true,
  live = false,
  label,
  testId,
}) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wasm = getWasm();
    if (!canvas || !wasm) {
      return;
    }

    const paint = (clock: number) => {
      try {
        wasm.renderSkinFixture(
          canvas,
          skinRef,
          pose,
          role,
          TILE_CELL_SIZE,
          boostActive,
          dead,
          clock,
          // A live tile exists to show motion; holding it still would defeat
          // the point. The section's play control is the honest opt-out.
          live ? false : reducedMotion,
        );
        setError(null);
      } catch (cause) {
        setError(String(cause));
      }
    };

    if (!live) {
      paint(animMs);
      // A textured skin requests its pixels on that first paint. A still tile
      // would otherwise hold the flat coat for good — and a contact sheet of
      // flat coats is exactly the kind of quiet wrong answer this route exists
      // to prevent. Resolves immediately once the textures are in.
      let stale = false;
      void whenSkinAssetsSettle().then(() => {
        if (!stale) {
          paint(animMs);
        }
      });
      return () => {
        stale = true;
      };
    }

    let frame = requestAnimationFrame(function loop(now: number) {
      paint(now);
      frame = requestAnimationFrame(loop);
    });
    return () => cancelAnimationFrame(frame);
  }, [skinRef, pose, role, boostActive, dead, animMs, reducedMotion, live]);

  return (
    <figure className="skins-qa-tile" data-testid={testId}>
      <canvas
        ref={canvasRef}
        width={size.width}
        height={size.height}
        aria-label={label}
        role="img"
      />
      <figcaption>{error ?? label}</figcaption>
    </figure>
  );
};

const SkinsQA: React.FC = () => {
  const [ready, setReady] = useState(false);
  const [catalog, setCatalog] = useState<SkinCatalogEntry[]>([]);
  const [fixtures, setFixtures] = useState<Fixtures | null>(null);
  const [skinRef, setSkinRef] = useState(DEFAULT_SKIN_REF);
  // Someone who has asked the OS for less motion gets a paused sheet they can
  // start themselves, rather than a wall of moving snakes.
  const [playing, setPlaying] = useState(
    () =>
      typeof window === 'undefined' ||
      !window.matchMedia('(prefers-reduced-motion: reduce)').matches,
  );

  useEffect(() => {
    let cancelled = false;
    initWasm()
      .then((wasm) => {
        if (cancelled) {
          return;
        }
        setCatalog(readSkinCatalog());
        setFixtures(JSON.parse(wasm.skinFixtures()) as Fixtures);
        setReady(true);
      })
      .catch((error) => console.error('skins QA failed to load wasm:', error));
    return () => {
      cancelled = true;
    };
  }, []);

  // Which skin this sheet is previewing, and nothing more: it is a renderer
  // harness, not an equip control. Equipping writes to the account, which is
  // the only place it is recorded.
  const select = useCallback((next: string) => {
    setSkinRef(next);
  }, []);

  const sizeOf = useCallback(
    (name: string) =>
      tileSize(fixtures?.poses.find((entry) => entry.name === name)),
    [fixtures],
  );

  const swatches = useMemo(() => {
    const wasm = getWasm();
    if (!wasm || !fixtures) {
      return [];
    }
    // Resolved by the renderer from the same role names the tiles use, so the
    // strip cannot drift from the snakes above it.
    return fixtures.roles.map((role) => {
      try {
        const colors = JSON.parse(wasm.skinColorsForRole(skinRef, role));
        return { role, ...colors };
      } catch {
        return {
          role,
          fill: '#cccccc',
          outline: '#999999',
          label: '#000000',
          swatch: '#cccccc',
        };
      }
    });
  }, [fixtures, skinRef]);

  if (!ready || !fixtures) {
    return <main className="skins-qa">Loading the renderer…</main>;
  }

  return (
    <main className="skins-qa" data-testid="skins-qa">
      <h1>Skins</h1>

      <section aria-labelledby="skins-qa-select">
        <h2 id="skins-qa-select">Selected skin</h2>
        <p className="skins-qa-note">
          Saved locally and sent when you join a match. The server checks it
          against its catalogue before anyone else&rsquo;s client sees it.
        </p>
        <div className="skins-qa-catalog">
          {catalog.map((entry) => (
            <button
              key={entry.id}
              type="button"
              data-testid={`skin-select-${entry.id}`}
              aria-pressed={entry.id === skinRef}
              className={entry.id === skinRef ? 'is-selected' : undefined}
              onClick={() => select(entry.id)}
            >
              {entry.name}
              <small>{entry.id}</small>
            </button>
          ))}
        </div>
      </section>

      <section aria-labelledby="skins-qa-live">
        <h2 id="skins-qa-live">Live</h2>
        <p className="skins-qa-note">
          Running off the real animation-frame clock, exactly as the arena
          drives it. This is the only place motion is actually visible — the
          film strip below is fixed samples, which is what makes it capturable.
        </p>
        <div className="skins-qa-controls">
          <button
            type="button"
            data-testid="skins-qa-play"
            aria-pressed={playing}
            onClick={() => setPlaying((was) => !was)}
          >
            {playing ? 'Pause' : 'Play'}
          </button>
        </div>
        <div className="skins-qa-grid">
          <FixtureTile
            testId="fixture-live-long"
            skinRef={skinRef}
            pose="longer_than_head_gradient"
            size={sizeOf('longer_than_head_gradient')}
            role="own"
            live={playing}
            label="long body"
          />
          <FixtureTile
            testId="fixture-live-turning"
            skinRef={skinRef}
            pose="zigzag"
            size={sizeOf('zigzag')}
            role="own"
            live={playing}
            label="turning"
          />
          <FixtureTile
            testId="fixture-live-boost"
            skinRef={skinRef}
            pose="longer_than_head_gradient"
            size={sizeOf('longer_than_head_gradient')}
            role="enemy"
            boostActive
            live={playing}
            label="opponent, boosting"
          />
        </div>
      </section>

      <section aria-labelledby="skins-qa-roles">
        <h2 id="skins-qa-roles">Every role</h2>
        <div className="skins-qa-grid">
          {fixtures.roles.map((role) => (
            <FixtureTile
              key={role}
              testId={`fixture-role-${role}`}
              skinRef={skinRef}
              pose="single_corner"
              size={sizeOf('single_corner')}
              role={role}
              label={role}
            />
          ))}
        </div>
      </section>

      <section aria-labelledby="skins-qa-poses">
        <h2 id="skins-qa-poses">Every pose</h2>
        <div className="skins-qa-grid">
          {fixtures.poses.map((pose) => (
            <FixtureTile
              key={pose.name}
              testId={`fixture-pose-${pose.name}`}
              skinRef={skinRef}
              pose={pose.name}
              size={tileSize(pose)}
              role="own"
              label={pose.name}
            />
          ))}
        </div>
      </section>

      <section aria-labelledby="skins-qa-states">
        <h2 id="skins-qa-states">States</h2>
        <div className="skins-qa-grid">
          <FixtureTile
            testId="fixture-state-boost"
            skinRef={skinRef}
            pose="single_corner"
            size={sizeOf('single_corner')}
            role="own"
            boostActive
            label="boosting"
          />
          <FixtureTile
            testId="fixture-state-dead"
            skinRef={skinRef}
            pose="single_corner"
            size={sizeOf('single_corner')}
            role="own"
            dead
            label="dead (always the shared corpse)"
          />
          <FixtureTile
            testId="fixture-state-single-cell"
            skinRef={skinRef}
            pose="single_cell"
            size={sizeOf('single_cell')}
            role="own"
            label="single cell"
          />
          <FixtureTile
            testId="fixture-state-single-cell-boost"
            skinRef={skinRef}
            pose="single_cell"
            size={sizeOf('single_cell')}
            role="own"
            boostActive
            label="single cell, boosting"
          />
        </div>
      </section>

      <section aria-labelledby="skins-qa-anim">
        <h2 id="skins-qa-anim">Animation film strip</h2>
        <p className="skins-qa-note">
          Fixed clock samples — these never move, by design, so a screenshot of
          this section is reproducible. A still skin shows four identical
          frames; an animated one shows its cycle laid out side by side. The
          body is the longest fixture on purpose: a wave with crests ten cells
          apart says nothing on a six-cell snake. The last tile is the
          reduced-motion pose every skin has to hold.
        </p>
        <div className="skins-qa-grid">
          {fixtures.animSamples.map((animMs) => (
            <FixtureTile
              key={animMs}
              testId={`fixture-anim-${animMs}`}
              skinRef={skinRef}
              pose="longer_than_head_gradient"
              size={sizeOf('longer_than_head_gradient')}
              role="own"
              animMs={animMs}
              reducedMotion={false}
              label={`${animMs} ms`}
            />
          ))}
          <FixtureTile
            testId="fixture-anim-reduced"
            skinRef={skinRef}
            pose="longer_than_head_gradient"
            size={sizeOf('longer_than_head_gradient')}
            role="own"
            animMs={fixtures.animSamples[fixtures.animSamples.length - 1] ?? 0}
            reducedMotion
            label="reduced motion"
          />
        </div>
      </section>

      <section aria-labelledby="skins-qa-swatches">
        <h2 id="skins-qa-swatches">Reported colours</h2>
        <p className="skins-qa-note">
          What surfaces that cannot draw a snake get instead — the results-table
          pill, CSS variables, label ink.
        </p>
        <ul className="skins-qa-swatches" data-testid="skin-swatches">
          {swatches.map((entry) => (
            <li key={entry.role}>
              <span
                className="skins-qa-chip"
                style={{ background: entry.swatch, borderColor: entry.outline }}
              />
              <code>{entry.role}</code>
              <code>{entry.swatch}</code>
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
};

export default SkinsQA;
