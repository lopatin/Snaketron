import React, { useEffect, useRef, useState } from 'react';
import { initWasm, getWasm } from '../wasm';

/**
 * The pixel comparator.
 *
 * `specs/skin-shading-prd.md` section 12 rules out the obvious design — a Rust
 * rasterizer replaying the recorded op stream — and the reason is measured, not
 * aesthetic: 20-45% of a snake's pixels sit on an antialiased edge, and any
 * independent rasterizer disagrees with Skia across that whole band by 5-30
 * levels. A checker that cannot reach a 1/255 tolerance cannot prove the one
 * thing it exists to prove.
 *
 * So both lowerings render **in the same browser**, through the same Skia, and
 * the diff is `getImageData`. Every remaining difference is then a real one.
 *
 * This route has no visual design on purpose. It is a harness: Playwright
 * drives `window.__skinParity` and reads numbers back. Anything worth looking
 * at is on `/qa/skins`.
 */

export interface RenderSpec {
  skin: string;
  pose: string;
  role?: string;
  cellSize?: number;
  boost?: boolean;
  dead?: boolean;
  animMs?: number;
  reducedMotion?: boolean;
}

interface MeasureWindow {
  x: number;
  y: number;
  width: number;
  height: number;
  overhangPx: number;
}

export interface DiffReport {
  /** The pixels the tolerance is measured over. */
  window: MeasureWindow;
  pixels: number;
  /** Largest per-channel absolute difference anywhere in the window. */
  maxDelta: number;
  /** Pixels where some channel differs by more than 1. */
  over1: number;
  /** Pixels where some channel differs by more than 4. */
  over4: number;
  /** Share of the window within the 1-level band. */
  within1: number;
  /** Whether this pair meets section 12's cross-grammar tolerance. */
  passes: boolean;
}

/** Section 12: max per-channel delta <= 1 over >= 99.9%, and <= 4 everywhere. */
export const TOLERANCE = { within1: 0.999, everywhere: 4 };

const DEFAULT_CELL_SIZE = 15;
/** Room for the widest overhang any skin can report, at any cell size. */
const MARGIN_PX = 24;

const SkinParityQA: React.FC = () => {
  const slotA = useRef<HTMLCanvasElement | null>(null);
  const slotB = useRef<HTMLCanvasElement | null>(null);
  const sheetRef = useRef<HTMLCanvasElement | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    let cancelled = false;
    initWasm()
      .then(() => !cancelled && setReady(true))
      .catch((error) =>
        console.error('skin parity harness failed to load wasm:', error),
      );
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!ready) {
      return;
    }
    const wasm = getWasm();
    const canvases = { a: slotA.current, b: slotB.current };
    if (!wasm || !canvases.a || !canvases.b || !sheetRef.current) {
      return;
    }

    const fixtures = JSON.parse(wasm.skinFixtures());
    const poseByName = new Map<string, { cellsWide: number; cellsHigh: number }>(
      fixtures.poses.map((pose: any) => [pose.name, pose]),
    );

    const sizeFor = (spec: RenderSpec) => {
      const pose = poseByName.get(spec.pose);
      const cell = spec.cellSize ?? DEFAULT_CELL_SIZE;
      return {
        width: Math.ceil((pose?.cellsWide ?? 11) * cell) + MARGIN_PX,
        height: Math.ceil((pose?.cellsHigh ?? 11) * cell) + MARGIN_PX,
      };
    };

    /**
     * Pin a canvas to one rasterizer, before anything draws on it.
     *
     * Chrome moves a canvas from GPU to CPU rasterization once it decides you
     * are reading it back, and the two backends do not antialias identically.
     * Left alone, that meant slot A — which every comparison reads — drifted
     * onto the software path while slot B stayed on the GPU, and the
     * comparator reported a snake as differing from *itself* by up to 61
     * levels across 240 pixels. Claiming the `willReadFrequently` hint here,
     * before `renderSkinFixture` calls `getContext` from Rust, puts every
     * surface in this harness on the same rasterizer for its whole life.
     */
    const pinnedContext = (canvas: HTMLCanvasElement) => {
      const context = canvas.getContext('2d', {
        willReadFrequently: true,
        colorSpace: 'srgb',
      });
      if (!context) {
        throw new Error('parity canvas has no 2d context');
      }
      return context;
    };

    pinnedContext(canvases.a!);
    pinnedContext(canvases.b!);
    pinnedContext(sheetRef.current!);

    const paint = (canvas: HTMLCanvasElement, spec: RenderSpec) => {
      const { width, height } = sizeFor(spec);
      // Resizing clears the canvas, which is what guarantees slot B carries
      // nothing from a previous comparison.
      canvas.width = width;
      canvas.height = height;
      wasm.renderSkinFixture(
        canvas,
        spec.skin,
        spec.pose,
        spec.role ?? 'own',
        spec.cellSize ?? DEFAULT_CELL_SIZE,
        spec.boost ?? false,
        spec.dead ?? false,
        spec.animMs ?? 0,
        spec.reducedMotion ?? true,
      );
      return { width, height };
    };

    const measureWindow = (spec: RenderSpec): MeasureWindow =>
      JSON.parse(
        wasm.skinFixtureBounds(
          spec.skin,
          spec.pose,
          spec.cellSize ?? DEFAULT_CELL_SIZE,
          spec.boost ?? false,
        ),
      );

    const imageData = (
      canvas: HTMLCanvasElement,
      area: MeasureWindow,
    ): ImageData => {
      return pinnedContext(canvas).getImageData(
        area.x,
        area.y,
        Math.min(area.width, canvas.width - area.x),
        Math.min(area.height, canvas.height - area.y),
      );
    };

    const diff = (
      left: ImageData,
      right: ImageData,
      area: MeasureWindow,
    ): DiffReport => {
      if (left.data.length !== right.data.length) {
        throw new Error(
          `parity windows disagree: ${left.width}x${left.height} vs ${right.width}x${right.height}`,
        );
      }
      let maxDelta = 0;
      let over1 = 0;
      let over4 = 0;
      const pixels = left.width * left.height;
      for (let index = 0; index < left.data.length; index += 4) {
        let worst = 0;
        for (let channel = 0; channel < 4; channel += 1) {
          const delta = Math.abs(
            left.data[index + channel] - right.data[index + channel],
          );
          if (delta > worst) {
            worst = delta;
          }
        }
        if (worst > maxDelta) {
          maxDelta = worst;
        }
        if (worst > 1) {
          over1 += 1;
          if (worst > 4) {
            over4 += 1;
          }
        }
      }
      const within1 = pixels === 0 ? 1 : (pixels - over1) / pixels;
      return {
        window: area,
        pixels,
        maxDelta,
        over1,
        over4,
        within1,
        passes:
          within1 >= TOLERANCE.within1 && maxDelta <= TOLERANCE.everywhere,
      };
    };

    /**
     * Decode a baseline PNG **without colour management**.
     *
     * `new Image()` lets the browser colour-convert on decode, and drawing the
     * result back onto a canvas shifts saturated colours: measured here, the
     * Boost band's `#fff200` came back 74 levels off, which reads as "the skin
     * changed" when nothing changed but the decode path. `createImageBitmap`
     * with `colorSpaceConversion: 'none'` is the only way to get the stored
     * bytes back, and a comparator that cannot round-trip its own output is
     * worse than no comparator at all.
     */
    const loadImage = async (dataUrl: string): Promise<ImageBitmap> => {
      const blob = await (await fetch(dataUrl)).blob();
      return createImageBitmap(blob, {
        colorSpaceConversion: 'none',
        premultiplyAlpha: 'none',
      });
    };

    (window as any).__skinParity = {
      ready: true,
      fixtures,

      /** Paint one spec into a slot and hand back its size. */
      render(slot: 'a' | 'b', spec: RenderSpec) {
        return paint(canvases[slot]!, spec);
      },

      /**
       * Section 11's frame-time budget, measured where it lives.
       *
       * The op and allocation budgets are native tests in `skin::perf`; frame
       * time is a property of Skia, so it can only be measured here.
       */
      perfSmoke(frames: number) {
        return JSON.parse(wasm.skinPerfSmoke(canvases.b!, frames));
      },

      /** One spec as a lossless PNG data URL — the baseline artifact. */
      capture(spec: RenderSpec) {
        paint(canvases.a!, spec);
        return canvases.a!.toDataURL('image/png');
      },

      /**
       * Two lowerings, one browser, one diff. This is the section 12
       * comparator.
       */
      compare(left: RenderSpec, right: RenderSpec): DiffReport {
        paint(canvases.a!, left);
        paint(canvases.b!, right);
        // The window comes from the *left* spec: a comparison is always
        // "does this reproduce that", and the reference decides where to look.
        const area = measureWindow(left);
        return diff(
          imageData(canvases.a!, area),
          imageData(canvases.b!, area),
          area,
        );
      },

      /**
       * Every spec stacked into one image.
       *
       * One sheet per skin rather than one PNG per fixture: six committed
       * images review as six pictures and diff as six diffs, where 120 loose
       * files review as a number. The manifest carries each tile's offset, so
       * the comparison below is still per-fixture and still measured over the
       * section 12 window rather than over the whole sheet.
       */
      captureSheet(specs: RenderSpec[]) {
        const sheet = sheetRef.current!;
        const tiles = specs.map((spec) => ({ spec, ...sizeFor(spec) }));
        sheet.width = tiles.reduce((wide, tile) => Math.max(wide, tile.width), 1);
        sheet.height = tiles.reduce((tall, tile) => tall + tile.height, 0);
        const context = pinnedContext(sheet);

        let offsetY = 0;
        const manifest = tiles.map((tile) => {
          paint(canvases.a!, tile.spec);
          // `putImageData`, never `drawImage`. drawImage composites the source
          // and is free to colour-manage it; measured here it shifted band
          // edges by up to 4 levels, which is inside the tolerance this suite
          // is supposed to be measuring and would have quietly eaten a real
          // regression of the same size. putImageData writes the bytes.
          context.putImageData(
            imageData(canvases.a!, {
              x: 0,
              y: 0,
              width: tile.width,
              height: tile.height,
              overhangPx: 0,
            }),
            0,
            offsetY,
          );
          const entry = { ...tile, y: offsetY };
          offsetY += tile.height;
          return entry;
        });

        return { png: sheet.toDataURL('image/png'), tiles: manifest };
      },

      /**
       * Re-render every spec and compare it against its tile in a committed
       * sheet. Returns one report per fixture, worst first.
       */
      async compareToSheet(specs: RenderSpec[], sheetPng: string) {
        const image = await loadImage(sheetPng);
        // A baseline recorded over a different fixture matrix is not a pixel
        // regression, but it looks exactly like one: every tile after the
        // first missing row reads garbage. Say what actually happened.
        const expected = specs.reduce((tall, spec) => tall + sizeFor(spec).height, 0);
        if (image.height !== expected) {
          throw new Error(
            `baseline sheet is ${image.height}px tall but this fixture matrix ` +
              `needs ${expected}px (${specs.length} tiles). The corpus changed ` +
              'since the baseline was recorded — re-record it with ' +
              'SKIN_BASELINE_BLESS=1 npm run test:skins rather than reading this ' +
              'as a skin regression.',
          );
        }
        const sheet = sheetRef.current!;
        sheet.width = image.width;
        sheet.height = image.height;
        const context = pinnedContext(sheet);
        context.drawImage(image, 0, 0);

        const reports: (DiffReport & { spec: RenderSpec })[] = [];
        let offsetY = 0;
        for (const spec of specs) {
          const { height } = paint(canvases.a!, spec);
          const area = measureWindow(spec);
          // The baseline tile sits `offsetY` further down the sheet; shift the
          // read window to match rather than re-cropping the fresh render.
          const baselineArea = { ...area, y: area.y + offsetY };
          reports.push({
            spec,
            ...diff(
              imageData(canvases.a!, area),
              imageData(sheet, baselineArea),
              area,
            ),
          });
          offsetY += height;
        }
        reports.sort((left, right) => right.over1 - left.over1);
        return reports;
      },
    };
  }, [ready]);

  return (
    <main data-testid={ready ? 'skin-parity-ready' : 'skin-parity-loading'}>
      <h1>Skin parity harness</h1>
      <p>
        Drive this from Playwright via <code>window.__skinParity</code>. It has
        no design because nothing here is meant to be looked at.
      </p>
      <canvas ref={slotA} data-testid="parity-a" width={64} height={64} />
      <canvas ref={slotB} data-testid="parity-b" width={64} height={64} />
      <canvas ref={sheetRef} data-testid="parity-sheet" width={64} height={64} />
    </main>
  );
};

export default SkinParityQA;
