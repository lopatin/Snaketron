import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { getWasm } from '../wasm';

/**
 * Live editing for the sprite-sheet skins, on `/qa/skins`.
 *
 * The point is to close the loop between *looking* at a skin and *changing*
 * it. Before this, tuning a drift rate meant editing Rust, rebuilding wasm, and
 * reloading — long enough that nobody tried more than two values.
 *
 * Two mechanisms, deliberately separate:
 *
 * - **Preview** calls `setSkinTuning`, which rebuilds that skin inside wasm.
 *   The Live section repaints every frame, so a change shows immediately; the
 *   fixed-sample sections repaint when `revision` bumps.
 * - **Save** posts to the dev server, which writes
 *   `client/design/sprites/tuning.json` — the file `skin::sprite` compiles in.
 *   So a preview that is never saved dies with the page, and a saved one is
 *   what the next build ships.
 *
 * Only properties that can change without rebuilding the art are editable.
 * Rotation and repeat length are baked into the PNG by `sprite_sheet.py`, so
 * they are shown read-only rather than given a slider that quietly does
 * nothing.
 */

export interface SheetTuning {
  id: string;
  name: string;
  anim_speed: number;
  drift_cells: number;
  rotation_degrees: number;
  repeat_cells: number;
  repeats: boolean;
}

interface Props {
  skinRef: string;
  /** Bumped whenever a preview lands, so fixed-sample tiles repaint. */
  onChanged: () => void;
}

type SaveState = 'idle' | 'saving' | 'saved' | 'error';

type Field = 'anim_speed' | 'drift_cells';

interface Control {
  key: Field;
  label: string;
  hint: string;
  min: number;
  max: number;
  step: number;
}

/** The slider can only sit inside its own range; the value need not. */
const clampToSlider = (control: Control, value: number) =>
  Math.min(control.max, Math.max(control.min, value));

const CONTROLS: Control[] = [
  {
    key: 'anim_speed',
    label: 'Animation speed',
    hint: 'How fast the sheet plays its rows. 1 is one row per 90ms.',
    min: 0.05,
    max: 4,
    step: 0.05,
  },
  {
    key: 'drift_cells',
    label: 'Drift',
    hint: 'Cells the pattern slides per cycle. Negative travels toward the head.',
    min: -8,
    max: 8,
    step: 0.1,
  },
];

const SkinTuningSidebar: React.FC<Props> = ({ skinRef, onChanged }) => {
  const [sheets, setSheets] = useState<SheetTuning[]>([]);
  const [edited, setEdited] = useState<Record<string, SheetTuning>>({});
  const [save, setSave] = useState<SaveState>('idle');
  // What is in the number boxes while they are being typed in. Kept apart
  // from the applied value so a half-typed "-" or "1." is not read as a
  // number, snapped, and fought over with the cursor.
  const [draft, setDraft] = useState<Partial<Record<Field, string>>>({});
  // Whether a human has actually touched these controls yet.
  //
  // Chrome restores form-control values on reload and fires change events for
  // them, so the page would come back with the last session's slider positions,
  // a spurious "unsaved changes", and — worst — those stale values previewed
  // over the real ones. `autocomplete="off"` is not honoured for restore, and a
  // unique `name` does not help either because the match is by control index.
  //
  // A pointer or key event always *precedes* a real change and never
  // accompanies a restored one, so this tells the two apart exactly rather than
  // guessing with a timeout.
  const touched = useRef(false);

  useEffect(() => {
    const wasm = getWasm();
    if (!wasm?.readSkinTuning) {
      return;
    }
    try {
      setSheets(JSON.parse(wasm.readSkinTuning()) as SheetTuning[]);
    } catch {
      setSheets([]);
    }
  }, []);

  const current = useMemo(
    () => edited[skinRef] ?? sheets.find((sheet) => sheet.id === skinRef),
    [edited, sheets, skinRef],
  );

  const apply = useCallback(
    (next: SheetTuning) => {
      if (!touched.current) {
        return;
      }
      const wasm = getWasm();
      // The preview is the wasm rebuild, not a React re-render: without this
      // the sliders would move and the snakes would not.
      wasm?.setSkinTuning?.(next.id, next.anim_speed, next.drift_cells);
      setEdited((all) => ({ ...all, [next.id]: next }));
      setSave('idle');
      onChanged();
    },
    [onChanged],
  );

  // The draft is only ever what is *displayed*; the value is applied the
  // moment the text parses. Deferring the apply to blur looked tidier and was
  // wrong twice over: it read `draft` through a stale closure, so a blur that
  // arrived before React re-rendered committed nothing, and it withheld the
  // preview until focus left — which is the opposite of the point.
  const type = useCallback(
    (key: Field, text: string) => {
      setDraft((all) => ({ ...all, [key]: text }));
      const value = Number(text);
      // A blank or half-typed box ("-", "1.") keeps the last applied value
      // rather than writing a NaN into a source rectangle.
      if (current && text.trim() !== '' && Number.isFinite(value)) {
        apply({ ...current, [key]: value });
      }
    },
    [apply, current],
  );

  // Let the box snap back to the applied value once the author is done with it.
  const settle = useCallback((key: Field) => {
    setDraft((all) => ({ ...all, [key]: undefined }));
  }, []);

  const commit = useCallback(async () => {
    const body = Object.fromEntries(
      Object.values(edited).map((sheet) => [
        sheet.id,
        { anim_speed: sheet.anim_speed, drift_cells: sheet.drift_cells },
      ]),
    );
    if (!Object.keys(body).length) {
      return;
    }
    setSave('saving');
    try {
      const response = await fetch('/qa/skin-tuning', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      setSave(response.ok ? 'saved' : 'error');
    } catch {
      setSave('error');
    }
  }, [edited]);

  const revert = useCallback(() => {
    for (const sheet of sheets) {
      if (edited[sheet.id]) {
        getWasm()?.setSkinTuning?.(sheet.id, sheet.anim_speed, sheet.drift_cells);
      }
    }
    setEdited({});
    setDraft({});
    setSave('idle');
    onChanged();
  }, [edited, sheets, onChanged]);

  const dirty = Object.keys(edited).length > 0;

  return (
    <aside
      className="skins-qa-tuning"
      data-testid="skin-tuning"
      onPointerDownCapture={() => {
        touched.current = true;
      }}
      onKeyDownCapture={() => {
        touched.current = true;
      }}
    >
      <h2>Properties</h2>
      {!current ? (
        <p className="skins-qa-note">
          {sheets.length
            ? 'This skin is not a sprite sheet, so it has nothing to tune here. Pick one of the Living / Stars and Stripes / Race Livery skins.'
            : 'No sprite-sheet skins are registered.'}
        </p>
      ) : (
        <>
          <p className="skins-qa-note">
            Editing <strong>{current.name}</strong>. Changes preview
            immediately; <em>Save</em> writes them to
            <code> client/design/sprites/tuning.json</code>, which the next build
            compiles in.
          </p>

          {CONTROLS.map((control) => (
            <label key={control.key} className="skins-qa-field">
              <span>
                {control.label}
                <output data-testid={`tuning-value-${control.key}`}>
                  {current[control.key].toFixed(2)}
                </output>
              </span>
              <span className="skins-qa-inputs">
                <input
                  type="range"
                  autoComplete="off"
                  data-testid={`tuning-${control.key}`}
                  min={control.min}
                  max={control.max}
                  step={control.step}
                  value={clampToSlider(control, current[control.key])}
                  onChange={(event) =>
                    apply({ ...current, [control.key]: Number(event.target.value) })
                  }
                />
                <input
                  type="number"
                  // Deliberately unbounded. A slider has to pick a range to be
                  // draggable at all, and that range is a guess about what is
                  // useful — not a statement about what is legal. Typing a
                  // value outside it is how you find out the guess was wrong,
                  // so the box takes anything finite and the slider just pins
                  // to its end.
                  autoComplete="off"
                  data-testid={`tuning-number-${control.key}`}
                  step={control.step}
                  value={draft[control.key] ?? String(current[control.key])}
                  onChange={(event) => type(control.key, event.target.value)}
                  onBlur={() => settle(control.key)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') settle(control.key);
                  }}
                />
              </span>
              <small>{control.hint}</small>
            </label>
          ))}

          <dl className="skins-qa-baked">
            <dt>Rotation</dt>
            <dd>{current.rotation_degrees}&deg;</dd>
            <dt>Repeat</dt>
            <dd>
              {current.repeats
                ? `${current.repeat_cells} cells`
                : `${current.repeat_cells} cells, drawn once`}
            </dd>
          </dl>
          <p className="skins-qa-note">
            Both are baked into the PNG. Change them with
            <code> sprite_sheet.py --rotate</code> and rebuild the sheet.
          </p>

          <div className="skins-qa-tuning-actions">
            <button
              type="button"
              data-testid="tuning-save"
              onClick={commit}
              disabled={!dirty || save === 'saving'}
            >
              {save === 'saving' ? 'Saving…' : 'Save'}
            </button>
            <button
              type="button"
              data-testid="tuning-revert"
              onClick={revert}
              disabled={!dirty}
            >
              Revert
            </button>
          </div>
          <p className="skins-qa-note" data-testid="tuning-status">
            {save === 'saved' && 'Written to tuning.json.'}
            {save === 'error' &&
              'Could not write. This only works under `npm start`.'}
            {save === 'idle' && dirty && 'Unsaved changes.'}
          </p>
        </>
      )}
    </aside>
  );
};

export default SkinTuningSidebar;
