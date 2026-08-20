import React, { useCallback, useEffect, useRef, useState } from 'react';
import { api, isApiError } from '../services/api';
import type { Texture } from '../types/generated/Texture';
import type { GenerationJob } from '../types/generated/GenerationJob';

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

interface TexturePickerProps {
  /** The name the layer currently references. */
  value: string;
  builtins: BuiltinTexture[];
  /** Declare and select a texture: name, wire reference, kind. */
  onChoose: (name: string, contentRef: string, kind: string) => void;
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
  const fileRef = useRef<HTMLInputElement | null>(null);

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
            onChoose(`texture-${made.textureId}`, made.contentRef, made.kind);
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

  return (
    <div className="texture-picker">
      <select
        className="texture-picker-select"
        value={value}
        onChange={(event) => {
          const id = event.target.value;
          const builtin = builtins.find((each) => each.id === id);
          if (builtin) {
            onChoose(builtin.id, builtin.contentRef, builtin.kind);
            return;
          }
          const own = mine.find((each) => `texture-${each.textureId}` === id);
          if (own) {
            onChoose(id, own.contentRef, own.kind);
          }
        }}
      >
        <optgroup label="Included">
          {builtins.map((art) => (
            <option key={art.id} value={art.id}>
              {art.label}
            </option>
          ))}
        </optgroup>
        {mine.length > 0 ? (
          <optgroup label="Yours">
            {mine.map((texture) => (
              <option key={texture.textureId} value={`texture-${texture.textureId}`}>
                {texture.lastPrompt ?? `Texture ${texture.textureId}`}
              </option>
            ))}
          </optgroup>
        ) : null}
      </select>

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
                // Whatever is selected now is what "more like this" means.
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

      {job ? (
        <p className={`texture-picker-stage${job.state === 'failed' ? ' is-failed' : ''}`}>
          {running ? <span className="texture-picker-spinner" aria-hidden="true" /> : null}
          {STAGE_WORDS[job.state] ?? job.state}
        </p>
      ) : null}
      {error ? <p className="texture-picker-error">{error}</p> : null}
    </div>
  );
};

export default TexturePicker;
