import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import { getWasm } from '../wasm';
import { formatBux } from '../utils/walletChip';
import SnakeBuxIcon from './SnakeBuxIcon';

/**
 * One skin, at length.
 *
 * A row in a list can say what a skin is called and roughly what colour it is.
 * It cannot show what the skin *does* — how it turns, what it looks like as
 * the enemy, whether the shine travels — and that is most of what someone
 * decides on. So the row opens this: the same renderer, several poses, several
 * roles, moving.
 */

const DEMO_CELL = 13;
const PREVIEW_PAD = 10;

interface Shot {
  pose: string;
  role: string;
  label: string;
}

/**
 * What to show, in the order it answers questions.
 *
 * Yours first — the reason anyone buys a skin is how it looks to them — then
 * the same skin as the enemy, which is the thing a still swatch can never
 * tell you, then the awkward bodies where a design either holds up or does
 * not.
 */
const SHOTS: Shot[] = [
  { pose: 'longer_than_head_gradient', role: 'own', label: 'Yours' },
  { pose: 'straight_18', role: 'enemy', label: 'As the enemy' },
  { pose: 'wide_u_turn', role: 'own', label: 'Turning' },
  { pose: 'zigzag', role: 'own', label: 'Zig-zag' },
];

/** One canvas, cropped to its own pose and always moving. */
const DemoShot: React.FC<{ skinRef: string; shot: Shot; revision: number }> = ({
  skinRef,
  shot,
  revision,
}) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [layout, setLayout] = useState({
    canvasWidth: 320,
    canvasHeight: 120,
    cropWidth: 320,
    cropHeight: 44,
    offsetX: 0,
    offsetY: 54,
  });

  useEffect(() => {
    const wasm = getWasm();
    if (!wasm) {
      return;
    }
    try {
      const bounds = JSON.parse(
        wasm.skinFixtureBounds(skinRef, shot.pose, DEMO_CELL, false),
      ) as { x: number; y: number; width: number; height: number };
      setLayout({
        canvasWidth: Math.ceil(bounds.x + bounds.width + PREVIEW_PAD),
        canvasHeight: Math.ceil(bounds.y + bounds.height + PREVIEW_PAD),
        cropWidth: Math.ceil(bounds.width + PREVIEW_PAD * 2),
        cropHeight: Math.ceil(bounds.height + PREVIEW_PAD * 2),
        offsetX: Math.round(bounds.x - PREVIEW_PAD),
        offsetY: Math.round(bounds.y - PREVIEW_PAD),
      });
    } catch {
      // Keep the previous framing rather than collapsing the row.
    }
  }, [skinRef, shot.pose, revision]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wasm = getWasm();
    if (!canvas || !wasm) {
      return;
    }
    let frame = requestAnimationFrame(function loop(now: number) {
      try {
        wasm.renderSkinFixture(
          canvas,
          skinRef,
          shot.pose,
          shot.role,
          DEMO_CELL,
          false,
          false,
          now,
          false,
        );
      } catch {
        // Leave the last good frame up.
      }
      frame = requestAnimationFrame(loop);
    });
    return () => cancelAnimationFrame(frame);
  }, [skinRef, shot.pose, shot.role, revision, layout.canvasWidth, layout.canvasHeight]);

  return (
    <figure className="skin-shot">
      <span
        className="skin-shot-crop"
        style={{ width: layout.cropWidth, height: layout.cropHeight }}
      >
        <canvas
          ref={canvasRef}
          width={layout.canvasWidth}
          height={layout.canvasHeight}
          style={{ marginLeft: -layout.offsetX, marginTop: -layout.offsetY }}
          role="img"
          aria-label={`${shot.label} preview`}
        />
      </span>
      <figcaption>{shot.label}</figcaption>
    </figure>
  );
};

/**
 * One skin, as this page needs it.
 *
 * A view model rather than the wire type, because two different things end up
 * here: a player-authored skin, which has an author and a price and a count of
 * who holds it, and a built-in, which has none of those and never will —
 * everyone has every built-in from the moment they load the page. Modelling
 * both as the authored shape would mean inventing an owner count for something
 * that has no such concept, and a made-up number on a page whose whole job is
 * to inform is worse than an absent one.
 */
export interface SkinView {
  reference: string;
  name: string;
  priceBux: number;
  /** The document reference to paint, which is not the skin's own reference. */
  previewRef?: string;
  /** Who made it. Absent for the ones that ship with the game. */
  creatorName?: string;
  owned: boolean;
  /** Present only for authored skins; a built-in has nobody to count. */
  stats?: { ownerCount: number; wearerCount: number };
  /** Present only for authored skins, and only the creator may edit. */
  editableSkinId?: number;
}

interface SkinModalProps {
  skin: SkinView;
  registryRevision: number;
  balanceBux: number;
  isEquipped: boolean;
  busy: boolean;
  onClose: () => void;
  onGet: () => void;
  onEquip: () => void;
  onEdit: () => void;
  onTopUp: () => void;
}

const plural = (count: number, one: string, many: string): string =>
  `${count.toLocaleString()} ${count === 1 ? one : many}`;

const SkinModal: React.FC<SkinModalProps> = ({
  skin,
  registryRevision,
  balanceBux,
  isEquipped,
  busy,
  onClose,
  onGet,
  onEquip,
  onEdit,
  onTopUp,
}) => {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const titleId = useId();
  const affordable = balanceBux >= skin.priceBux;

  useEffect(() => {
    dialogRef.current?.focus();
  }, []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const paintRef = skin.previewRef ?? skin.reference;

  const action = useCallback(() => {
    if (isEquipped) {
      return null;
    }
    if (skin.owned) {
      return (
        <button
          type="button"
          className="game-shell-button is-primary skin-cta"
          disabled={busy}
          onClick={onEquip}
          data-testid="skin-modal-equip"
        >
          Equip
        </button>
      );
    }
    if (skin.priceBux > 0 && !affordable) {
      return (
        <button
          type="button"
          className="game-shell-button is-primary skin-cta"
          onClick={onTopUp}
          data-testid="skin-modal-topup"
        >
          <SnakeBuxIcon size={16} />
          Get more Snake Bux
        </button>
      );
    }
    return (
      <button
        type="button"
        className="game-shell-button is-primary skin-cta"
        disabled={busy}
        onClick={onGet}
        data-testid="skin-modal-get"
      >
        {skin.priceBux > 0 ? (
          <>
            <SnakeBuxIcon size={16} />
            {`Get for ${formatBux(skin.priceBux)}`}
          </>
        ) : (
          'Get it free'
        )}
      </button>
    );
  }, [affordable, busy, isEquipped, onEquip, onGet, onTopUp, skin.owned, skin.priceBux]);

  return (
    <div className="shop-backdrop" data-testid="skin-backdrop" onClick={onClose}>
      <div
        ref={dialogRef}
        className="shop-dialog is-skin"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        data-testid="skin-modal"
        onClick={(event) => event.stopPropagation()}
      >
        <span className="shop-kicker">
          {skin.creatorName ? `by ${skin.creatorName}` : 'Snaketron'}
        </span>
        <h2 id={titleId}>{skin.name}</h2>

        <button type="button" className="shop-close" onClick={onClose} aria-label="Close">
          <span aria-hidden="true">×</span>
        </button>

        <div className="skin-shots">
          {SHOTS.map((shot) => (
            <DemoShot
              key={`${shot.pose}-${shot.role}`}
              skinRef={paintRef}
              shot={shot}
              revision={registryRevision}
            />
          ))}
        </div>

        <dl className="skin-stats">
          {skin.stats ? (
            <>
              <div>
                <dt>Owned by</dt>
                <dd>{plural(skin.stats.ownerCount, 'player', 'players')}</dd>
              </div>
              <div>
                <dt>Wearing now</dt>
                <dd>{plural(skin.stats.wearerCount, 'player', 'players')}</dd>
              </div>
            </>
          ) : (
            <div>
              <dt>Where it came from</dt>
              <dd>Ships with the game</dd>
            </div>
          )}
          <div>
            <dt>Price</dt>
            <dd className="skin-stat-price">
              {skin.priceBux === 0 ? (
                'Free'
              ) : (
                <>
                  <SnakeBuxIcon size={15} />
                  {formatBux(skin.priceBux)}
                </>
              )}
            </dd>
          </div>
        </dl>

        {skin.priceBux > 0 && !skin.owned && !affordable ? (
          <p className="shop-shortfall" data-testid="skin-modal-shortfall">
            {`You have ${formatBux(balanceBux)}. `}
            <strong>{`${formatBux(skin.priceBux - balanceBux)} more and it is yours.`}</strong>
          </p>
        ) : null}

        <div className="skin-actions">
          {isEquipped ? (
            <span className="skins-equipped-badge" data-testid="skin-modal-equipped">
              Equipped
            </span>
          ) : null}
          {action()}
          {skin.editableSkinId !== undefined ? (
            <button
              type="button"
              className="game-shell-button"
              onClick={onEdit}
              data-testid="skin-modal-edit"
            >
              Edit
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
};

export default SkinModal;
