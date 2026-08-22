import React, { useCallback, useEffect, useRef, useState } from 'react';
import { api, isApiError } from '../services/api';
import type { Texture } from '../types/generated/Texture';
import type { GenerationJob } from '../types/generated/GenerationJob';
import type { TextureDescriptor } from '../utils/skinTextures';

/**
 * Choosing the art a texture layer wears.
 *
 * Three sources, one list: the pieces the client ships, the ones this account
 * has made, and — through the two buttons — a new one, either handed over or
 * asked for. They land in the same place because to a document they are the
 * same thing: a name, a reference, and a kind.
 *
 * Generation and upload are both jobs rather than requests, so the progress
 * shown here is real: the worker writes a stage per transition, and this polls
 * for it. A finished job puts its texture at the top of the list and selects
 * it, because the only reason anyone pressed the button was to use the result.
 */

interface BuiltinTexture {
  id: string;
  label: string;
  kind: string;
  contentRef: string;
}

/** One row of the popover: what to draw, what to call it, what it selects. */
interface Choice {
  id: string;
  label: string;
  kind: string;
  contentRef: string;
  descriptor?: TextureDescriptor;
  /** The image to show in the swatch. */
  preview: string;
  mine: boolean;
}

/**
 * The smallest rung of a stored texture is the right thumbnail: it is the one
 * already sized for something small, and it is a fraction of the canonical
 * variant's bytes.
 */
const thumbnailOf = (texture: Texture): string => {
  const smallest = [...texture.variants].sort(
    (a, b) => a.texelsPerCell - b.texelsPerCell,
  )[0];
  if (!smallest) {
    return '';
  }
  const variantRef = `sha256:${smallest.sha256}`;
  return `${api.baseUrl}/api/textures/variants/${encodeURIComponent(variantRef)}.png`;
};

/** Convert the private library row to the sanitized descriptor SkinDoc keeps. */
const descriptorOf = (texture: Texture): TextureDescriptor => ({
  kind: texture.kind,
  ...(texture.repeatCells !== null &&
  Number.isInteger(texture.repeatCells) &&
  texture.repeatCells > 0
    ? { body_columns: texture.repeatCells }
    : {}),
  ...(texture.rows === null ? {} : { frame_rows: texture.rows }),
  variants: texture.variants.map((variant) => {
    const contentRef = `sha256:${variant.sha256}`;
    return {
      content_ref: contentRef,
      url: `/api/textures/variants/${contentRef}.png`,
      width_px: variant.widthPx,
      height_px: variant.heightPx,
      bytes: variant.bytes,
      texels_per_cell: variant.texelsPerCell,
    };
  }),
});

interface TexturePickerProps {
  /** The name the layer currently references. */
  value: string;
  builtins: BuiltinTexture[];
  /** Declare and select a texture: name, wire reference, kind. */
  onChoose: (
    name: string,
    contentRef: string,
    kind: string,
    descriptor?: TextureDescriptor,
  ) => void;
}

/** How often to ask a running job where it has got to. */
const POLL_MS = 1500;

/** What each stage means to somebody watching it. */
const STAGE_WORDS: Record<string, string> = {
  queued: 'Waiting for a worker',
  generating: 'Asking the model',
  repairing: 'Fixing the joins',
  validating: 'Measuring it',
  done: 'Done',
  failed: 'Failed',
};

const TexturePicker: React.FC<TexturePickerProps> = ({ value, builtins, onChoose }) => {
  const [mine, setMine] = useState<Texture[]>([]);
  const [job, setJob] = useState<GenerationJob | null>(null);
  const [prompt, setPrompt] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const fileRef = useRef<HTMLInputElement | null>(null);
  const popoverRef = useRef<HTMLDivElement | null>(null);

  const refresh = useCallback(async () => {
    try {
      const { textures } = await api.listTextures();
      setMine(textures);
      return textures;
    } catch {
      // An account with no textures, or a deployment that stores none. The
      // built-ins still work, so this is not worth an error.
      return [];
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // The prompt box remembers what made the current texture, which is the whole
  // reason the server keeps `lastPrompt`: iterating on a generation should be
  // one edit rather than a retype.
  useEffect(() => {
    const chosen = mine.find((each) => each.contentRef === value || `${each.textureId}` === value);
    if (chosen?.lastPrompt) {
      setPrompt(chosen.lastPrompt);
    }
  }, [mine, value]);

  // Poll a running job. A terminal state stops the timer and, when it worked,
  // selects what it produced.
  useEffect(() => {
    if (!job || job.state === 'done' || job.state === 'failed') {
      return;
    }
    const timer = setTimeout(async () => {
      try {
        const next = await api.getGenerationJob(job.jobId);
        setJob(next);
        if (next.state === 'done') {
          const textures = await refresh();
          const made = textures.find((each) => each.textureId === next.textureId);
          if (made) {
            onChoose(
              `texture-${made.textureId}`,
              made.contentRef,
              made.kind,
              descriptorOf(made),
            );
          }
        }
        if (next.state === 'failed') {
          setError(next.detail ?? 'The job failed.');
        }
      } catch (cause) {
        setError(isApiError(cause) ? cause.message : String(cause));
      }
    }, POLL_MS);
    return () => clearTimeout(timer);
  }, [job, onChoose, refresh]);

  const start = useCallback(
    async (run: () => Promise<{ jobId: string }>) => {
      setBusy(true);
      setError(null);
      try {
        const accepted = await run();
        setJob({
          jobId: accepted.jobId,
          state: 'queued',
        } as GenerationJob);
      } catch (cause) {
        setError(isApiError(cause) ? cause.message : String(cause));
      } finally {
        setBusy(false);
      }
    },
    [],
  );

  const running = job !== null && job.state !== 'done' && job.state !== 'failed';

  // Shipped art and the author's own, in one list. A document does not care
  // which a texture came from, so neither does this.
  const choices: Choice[] = [
    ...builtins.map((art) => ({
      id: art.id,
      label: art.label,
      kind: art.kind,
      contentRef: art.contentRef,
      // Built from the id, which `docv2::builtin_texture` files each piece
      // under; a test there pins the convention this relies on.
      preview: `images/skins/${art.id}.png`,
      mine: false,
    })),
    ...mine.map((texture) => ({
      id: `texture-${texture.textureId}`,
      label: texture.lastPrompt ?? `Texture ${texture.textureId}`,
      kind: texture.kind,
      contentRef: texture.contentRef,
      preview: thumbnailOf(texture),
      descriptor: descriptorOf(texture),
      mine: true,
    })),
  ];
  const chosen = choices.find((each) => each.id === value);
  const needle = search.trim().toLowerCase();
  const shown = needle
    ? choices.filter((each) => each.label.toLowerCase().includes(needle))
    : choices;

  // Close on Escape or on a click that lands outside. Both are what a popover
  // is expected to do, and neither is worth surprising anyone by omitting.
  useEffect(() => {
    if (!open) {
      return;
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setOpen(false);
      }
    };
    const onClick = (event: MouseEvent) => {
      if (!popoverRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    window.addEventListener('keydown', onKey);
    // Deferred: the click that opened this would otherwise close it again.
    const timer = setTimeout(() => window.addEventListener('mousedown', onClick), 0);
    return () => {
      window.removeEventListener('keydown', onKey);
      window.removeEventListener('mousedown', onClick);
      clearTimeout(timer);
    };
  }, [open]);

  return (
    <div className="texture-picker" ref={popoverRef}>
      <div className="texture-picker-anchor">
      <button
        type="button"
        className="texture-picker-current"
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => {
          setSearch('');
          setOpen((was) => !was);
        }}
      >
        {chosen ? (
          <img className="texture-swatch" src={chosen.preview} alt="" />
        ) : (
          <span className="texture-swatch is-empty" aria-hidden="true" />
        )}
        <span className="texture-picker-name">{chosen?.label ?? 'Choose a texture'}</span>
        <span className="texture-picker-caret" aria-hidden="true">▾</span>
      </button>

      {open ? (
        <div className="texture-popover" role="listbox">
          <input
            className="texture-popover-search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search textures…"
            aria-label="Search textures"
            autoFocus
          />
          <div className="texture-popover-list">
            {shown.length === 0 ? (
              <p className="texture-popover-empty">Nothing matches that.</p>
            ) : (
              shown.map((choice) => (
                <button
                  type="button"
                  key={choice.id}
                  role="option"
                  aria-selected={choice.id === value}
                  className={`texture-option${choice.id === value ? ' is-chosen' : ''}`}
                  onClick={() => {
                    // Close first. Choosing re-renders the whole inspector,
                    // and a state update queued behind that was arriving too
                    // late to be seen — the popover stayed open over the
                    // selection it had just made.
                    setOpen(false);
                    onChoose(
                      choice.id,
                      choice.contentRef,
                      choice.kind,
                      choice.descriptor,
                    );
                  }}
                >
                  <img className="texture-swatch" src={choice.preview} alt="" loading="lazy" />
                  <span className="texture-option-name">{choice.label}</span>
                  {choice.mine ? <span className="texture-option-tag">yours</span> : null}
                </button>
              ))
            )}
          </div>
        </div>
      ) : null}
      </div>

      <div className="texture-picker-make">
        <input
          className="texture-picker-prompt"
          value={prompt}
          onChange={(event) => setPrompt(event.target.value)}
          placeholder="Describe a texture to generate…"
          maxLength={400}
          disabled={running}
          aria-label="Texture prompt"
        />
        <button
          type="button"
          className="game-shell-button is-small"
          disabled={running || busy || prompt.trim().length === 0}
          onClick={() =>
            void start(() =>
              api.generateTexture({
                kind: 'coat',
                prompt: prompt.trim(),
                referenceTextureIds: mine
                  .filter((each) => `texture-${each.textureId}` === value)
                  .map((each) => each.textureId),
              }),
            )
          }
        >
          Generate
        </button>
        <button
          type="button"
          className="game-shell-button is-small"
          disabled={running || busy}
          onClick={() => fileRef.current?.click()}
        >
          Upload
        </button>
        <input
          ref={fileRef}
          type="file"
          accept="image/png"
          hidden
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) {
              void start(() => api.uploadTexture(file, 'coat', prompt.trim() || file.name));
            }
          }}
        />
      </div>

      {/* Only while something is happening. A job that finished said so by
          selecting its texture; leaving "Done" on screen afterwards is a
          status for a question nobody is still asking. */}
      {running ? (
        <p className="texture-picker-stage">
          <span className="texture-picker-spinner" aria-hidden="true" />
          {STAGE_WORDS[job.state] ?? job.state}
        </p>
      ) : null}
      {error ? <p className="texture-picker-error">{error}</p> : null}
    </div>
  );
};

export default TexturePicker;
