import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import { getWasm } from '../wasm';

/**
 * Composing a skin from a description.
 *
 * The loop is the point: ask for something, get a handful of real skins back,
 * keep the ones worth keeping, say what to change, and go round again until
 * one of them is worth opening. Every variation on screen is a document that
 * has already passed the validator, so "start editing" cannot drop an author
 * into an error they did not make.
 *
 * Nothing here calls a model. The variations come from the schema's own
 * vocabulary, in wasm, which is why a round costs a millisecond instead of a
 * queue and a quota — see `skin_schema::generate`.
 */

interface Variation {
  document: Record<string, unknown>;
  suggestedName: string;
  motif: string;
  seed: number;
}

interface GenerateSkinModalProps {
  onClose: () => void;
  /** Hand a chosen skin to the Builder, with the name to put in its field. */
  onUse: (document: Record<string, unknown>, name: string) => void;
}

/** How many skins a round offers. Enough to choose from, few enough to scan. */
const ROUND = 6;
const PREVIEW_POSE = 'straight_18';
const PREVIEW_CELL = 11;
const PREVIEW_PAD = 5;

/**
 * The colours a reference image is mostly made of.
 *
 * A picture is a far better palette brief than a sentence, and reading one
 * needs no model: shrink it, bucket the pixels coarsely, and take the buckets
 * with the most in them. Near-white and near-black are skipped because almost
 * every photograph is full of both and neither says anything about the colour
 * the author had in mind.
 */
const paletteFromImage = (image: HTMLImageElement): string[] => {
  const canvas = document.createElement('canvas');
  const side = 64;
  canvas.width = side;
  canvas.height = side;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (!ctx) {
    return [];
  }
  ctx.drawImage(image, 0, 0, side, side);
  const { data } = ctx.getImageData(0, 0, side, side);

  const buckets = new Map<string, { count: number; r: number; g: number; b: number }>();
  for (let i = 0; i < data.length; i += 4) {
    const [r, g, b, a] = [data[i], data[i + 1], data[i + 2], data[i + 3]];
    if (a < 128) continue;
    const max = Math.max(r, g, b);
    const min = Math.min(r, g, b);
    if (max > 244 || max < 26) continue;
    // Near-grey pixels carry no hue, and a photograph is mostly those.
    if (max - min < 18) continue;
    const key = `${r >> 5}:${g >> 5}:${b >> 5}`;
    const held = buckets.get(key) ?? { count: 0, r: 0, g: 0, b: 0 };
    held.count += 1;
    held.r += r;
    held.g += g;
    held.b += b;
    buckets.set(key, held);
  }

  return [...buckets.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 5)
    .map((bucket) => {
      const hex = (value: number) =>
        Math.round(value / bucket.count)
          .toString(16)
          .padStart(2, '0');
      return `#${hex(bucket.r)}${hex(bucket.g)}${hex(bucket.b)}`;
    });
};

/** One variation, painted by the renderer that will paint the real thing. */
const VariationPreview: React.FC<{ handle: string; document: unknown }> = ({
  handle,
  document: doc,
}) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [box, setBox] = useState({ w: 220, h: 110, cropW: 220, cropH: 30, x: 0, y: 60 });

  useEffect(() => {
    const wasm = getWasm();
    if (!wasm) {
      return;
    }
    try {
      wasm.registerDraftSkin(handle, JSON.stringify(doc));
      const bounds = JSON.parse(
        wasm.skinFixtureBounds(handle, PREVIEW_POSE, PREVIEW_CELL, false),
      ) as { x: number; y: number; width: number; height: number };
      setBox({
        w: Math.ceil(bounds.x + bounds.width + PREVIEW_PAD),
        h: Math.ceil(bounds.y + bounds.height + PREVIEW_PAD),
        cropW: Math.ceil(bounds.width + PREVIEW_PAD * 2),
        cropH: Math.ceil(bounds.height + PREVIEW_PAD * 2),
        x: Math.round(bounds.x - PREVIEW_PAD),
        y: Math.round(bounds.y - PREVIEW_PAD),
      });
    } catch {
      // A variation that will not register is one the generator should not
      // have returned; its card stays blank rather than taking the modal down.
    }
  }, [handle, doc]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wasm = getWasm();
    if (!canvas || !wasm) {
      return;
    }
    let frame = requestAnimationFrame(function loop(now: number) {
      try {
        wasm.renderSkinFixture(canvas, handle, PREVIEW_POSE, 'ffa3', PREVIEW_CELL, false, false, now, false);
      } catch {
        // Leave the last good frame up.
      }
      frame = requestAnimationFrame(loop);
    });
    return () => cancelAnimationFrame(frame);
  }, [handle, box.w, box.h]);

  return (
    <span className="generate-preview" style={{ width: box.cropW, height: box.cropH }}>
      <canvas
        ref={canvasRef}
        width={box.w}
        height={box.h}
        style={{ marginLeft: -box.x, marginTop: -box.y }}
        aria-hidden="true"
      />
    </span>
  );
};

const GenerateSkinModal: React.FC<GenerateSkinModalProps> = ({ onClose, onUse }) => {
  const titleId = useId();
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const fileRef = useRef<HTMLInputElement | null>(null);

  const [prompt, setPrompt] = useState('');
  const [guidance, setGuidance] = useState('');
  const [referenceColors, setReferenceColors] = useState<string[]>([]);
  const [referenceName, setReferenceName] = useState<string | null>(null);
  const [variations, setVariations] = useState<Variation[]>([]);
  const [kept, setKept] = useState<Set<number>>(new Set());
  const [round, setRound] = useState(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    dialogRef.current?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const run = useCallback(
    (liked: Variation[], nextRound: number) => {
      const wasm = getWasm();
      if (!wasm) {
        setError('The renderer is still loading.');
        return;
      }
      try {
        const brief = JSON.stringify({
          prompt,
          guidance,
          referenceColors,
          liked: liked.map((each) => each.document),
        });
        // The round number is the seed, so "regenerate" walks forward through
        // fresh sets rather than reshuffling the same one.
        const produced = JSON.parse(
          wasm.generateSkins(brief, ROUND, nextRound * 7919 + 13),
        ) as Variation[];
        if (produced.length === 0) {
          setError('Nothing came back for that. Try different words.');
          return;
        }
        setVariations(produced);
        setKept(new Set());
        setRound(nextRound);
        setError(null);
      } catch (cause) {
        setError(String(cause));
      }
    },
    [guidance, prompt, referenceColors],
  );

  const onPickImage = useCallback((file: File | undefined) => {
    if (!file) {
      return;
    }
    const url = URL.createObjectURL(file);
    const image = new Image();
    image.onload = () => {
      setReferenceColors(paletteFromImage(image));
      setReferenceName(file.name);
      URL.revokeObjectURL(url);
    };
    image.onerror = () => {
      setError('That file did not read as an image.');
      URL.revokeObjectURL(url);
    };
    image.src = url;
  }, []);

  const toggle = (index: number) =>
    setKept((held) => {
      const next = new Set(held);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });

  const keptList = [...kept].map((index) => variations[index]).filter(Boolean);
  const onlyOne = keptList.length === 1 ? keptList[0] : null;
  const canGenerate = prompt.trim().length > 0 || referenceColors.length > 0;

  return (
    <div className="generate-backdrop" data-testid="generate-backdrop">
      <div
        ref={dialogRef}
        className="generate-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        data-testid="generate-modal"
      >
        {/* The skewed segments the match-break dialog opens with, standing in
            for rounds here: one filled per round generated. */}
        <div className="generate-rule" aria-hidden="true">
          {Array.from({ length: 6 }, (_, index) => (
            <span key={index} className={index < round ? 'is-resolved' : ''} />
          ))}
        </div>

        <span className="generate-kicker">Skin builder</span>
        <h2 id={titleId}>Generate</h2>
        <p>Describe a skin, or hand it an image to take colours from.</p>

        <button
          type="button"
          className="generate-close"
          onClick={onClose}
          aria-label="Close"
        >
          <span aria-hidden="true">×</span>
        </button>

        <div className="generate-body">
          <textarea
            className="generate-prompt"
            value={prompt}
            onChange={(event) => setPrompt(event.target.value)}
            placeholder="molten lava, arctic frost, neon cyber…"
            rows={2}
            maxLength={400}
            aria-label="Describe the skin"
            autoFocus
          />

          <div className="generate-reference">
            <button
              type="button"
              className="game-shell-button is-small"
              onClick={() => fileRef.current?.click()}
            >
              {referenceName ? 'Change image' : 'Reference image'}
            </button>
            <input
              ref={fileRef}
              type="file"
              accept="image/*"
              hidden
              onChange={(event) => onPickImage(event.target.files?.[0])}
            />
            {referenceColors.length > 0 ? (
              <>
                <span className="generate-swatches" aria-label="Colours read from the reference">
                  {referenceColors.map((hex) => (
                    <i key={hex} style={{ background: hex }} />
                  ))}
                </span>
                <button
                  type="button"
                  className="generate-clear"
                  onClick={() => {
                    setReferenceColors([]);
                    setReferenceName(null);
                  }}
                >
                  Clear
                </button>
              </>
            ) : null}
          </div>

          {variations.length > 0 ? (
            <>
              <div className="generate-grid">
                {variations.map((variation, index) => (
                  <button
                    type="button"
                    key={`${round}-${variation.seed}`}
                    className={`generate-option${kept.has(index) ? ' is-kept' : ''}`}
                    aria-pressed={kept.has(index)}
                    onClick={() => toggle(index)}
                    onDoubleClick={() => onUse(variation.document, variation.suggestedName)}
                  >
                    <VariationPreview
                      handle={`draft:gen-${round}-${variation.seed}`}
                      document={variation.document}
                    />
                    <span className="generate-option-name">{variation.suggestedName}</span>
                  </button>
                ))}
              </div>
              <input
                className="generate-guidance"
                value={guidance}
                onChange={(event) => setGuidance(event.target.value)}
                placeholder="Anything to change? darker, faster, no stripes…"
                maxLength={200}
                aria-label="Guidance for the next round"
              />
            </>
          ) : null}

          {error ? <p className="generate-error">{error}</p> : null}
        </div>

        <div className="generate-status">
          <span>
            {variations.length === 0
              ? 'No rounds yet'
              : keptList.length > 0
                ? `${keptList.length} kept`
                : 'Keep the ones you like'}
          </span>
          <div className="generate-actions">
            <button
              type="button"
              className="game-shell-button"
              disabled={!canGenerate}
              onClick={() => run(keptList, round + 1)}
            >
              {variations.length === 0 ? 'Generate' : 'Generate again'}
            </button>
            <button
              type="button"
              className="game-shell-button is-primary"
              disabled={!onlyOne}
              title={onlyOne ? undefined : 'Keep exactly one to open it'}
              onClick={() => onlyOne && onUse(onlyOne.document, onlyOne.suggestedName)}
            >
              Edit this one
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default GenerateSkinModal;
