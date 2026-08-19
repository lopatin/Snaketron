import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { AccountModalView } from './AccountModal';
import { HomeHeader } from './HomeHeader';
import { SocialFooter } from './SocialFooter';
import { useAuth } from '../contexts/AuthContext';
import { api, isApiError } from '../services/api';
import { getWasm, initWasm, whenSkinAssetsSettle } from '../wasm';
import type { CatalogEntry, SkinSummary } from '../types/generated';
import { Link } from 'react-router-dom';
import {
  DEFAULT_SKIN_REF,
  readBasePreference,
  readSkinPreference,
  writeBasePreference,
  writeSkinPreference,
} from '../utils/skinPreference';

/**
 * The Skins page.
 *
 * Browsing is open to anyone — this is a shop window as much as a picker — but
 * equipping needs an account, because the whole point of equipping is that the
 * choice is stored somewhere match preparation can read it and hand it to every
 * other player. A signed-out visitor's choice would only ever be visible to
 * themselves, which is what the page exists to stop being true.
 */

interface SkinsPageProps {
  onOpenAuth: () => void;
  onOpenAccount: (view: AccountModalView) => void;
}

/** The pose that reads best as "a snake wearing this": long, and horizontal. */
const SNAKE_PREVIEW_POSE = 'longer_than_head_gradient';
const SNAKE_PREVIEW_CELL = 16;
/** Breathing room around the painted snake, so a boosting band is never clipped. */
const PREVIEW_PAD_PX = 10;

/**
 * How one preview canvas is sized and cropped.
 *
 * Two rectangles, because they are genuinely different: the canvas has to be
 * big enough to contain the pose at its own arena coordinates, and the crop is
 * the part of it worth showing.
 */
interface PreviewLayout {
  canvasWidth: number;
  canvasHeight: number;
  cropWidth: number;
  cropHeight: number;
  offsetX: number;
  offsetY: number;
}

const FALLBACK_PREVIEW_LAYOUT: PreviewLayout = {
  canvasWidth: 356,
  canvasHeight: 96,
  cropWidth: 356,
  cropHeight: 40,
  offsetX: 0,
  offsetY: 54,
};

/**
 * The frozen frame every still preview paints.
 *
 * A constant rather than zero so animated skins are caught mid-cycle and show
 * something characteristic — a skin frozen at the start of its cycle can look
 * identical to a static one.
 */
const FROZEN_ANIM_MS = 640;

const prefersReducedMotion = (): boolean =>
  typeof window !== 'undefined' &&
  typeof window.matchMedia === 'function' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches;

interface SnakePreviewProps {
  skinRef: string;
  animate: boolean;
  label: string;
}

/**
 * One snake, painted by the real renderer.
 *
 * Still by default and animating while hovered, which is the whole interaction:
 * a wall of moving snakes is unreadable, and a wall of frozen ones hides half of
 * what an animated skin is. The live loop repaints the canvas directly rather
 * than through React state, so hovering costs canvas work and nothing else.
 */
const SnakePreview: React.FC<SnakePreviewProps> = ({ skinRef, animate, label }) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [layout, setLayout] = useState<PreviewLayout>(FALLBACK_PREVIEW_LAYOUT);

  // Fixture poses are painted at their own arena coordinates, not at the
  // origin: the long pose sits four cells down, so a canvas sized to the
  // snake's own height would paint it entirely off the bottom. The canvas is
  // therefore sized to reach the pose, and the wrapper crops back to it.
  //
  // Overhang is per-skin, so the crop is measured per-skin too — a fixed inset
  // would clip the widest contours, which is the exact failure a preview
  // exists to catch rather than cause.
  useEffect(() => {
    const wasm = getWasm();
    if (!wasm) {
      return;
    }
    try {
      const bounds = JSON.parse(
        wasm.skinFixtureBounds(skinRef, SNAKE_PREVIEW_POSE, SNAKE_PREVIEW_CELL, false),
      ) as { x: number; y: number; width: number; height: number };
      setLayout({
        canvasWidth: Math.ceil(bounds.x + bounds.width + PREVIEW_PAD_PX),
        canvasHeight: Math.ceil(bounds.y + bounds.height + PREVIEW_PAD_PX),
        cropWidth: Math.ceil(bounds.width + PREVIEW_PAD_PX * 2),
        cropHeight: Math.ceil(bounds.height + PREVIEW_PAD_PX * 2),
        offsetX: Math.round(bounds.x - PREVIEW_PAD_PX),
        offsetY: Math.round(bounds.y - PREVIEW_PAD_PX),
      });
    } catch {
      // A preview at slightly the wrong size beats no preview.
      setLayout(FALLBACK_PREVIEW_LAYOUT);
    }
  }, [skinRef]);

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
          SNAKE_PREVIEW_POSE,
          'own',
          SNAKE_PREVIEW_CELL,
          false,
          false,
          clock,
          // A hovered preview exists to show motion; holding it still would
          // defeat the point of hovering.
          !animate,
        );
      } catch {
        // An unpaintable skin leaves the previous frame up rather than
        // throwing during render.
      }
    };

    if (!animate) {
      paint(FROZEN_ANIM_MS);
      // A textured skin requests its pixels on that first paint, so a still
      // preview would hold the flat coat for good without this second one.
      let stale = false;
      void whenSkinAssetsSettle().then(() => {
        if (!stale) {
          paint(FROZEN_ANIM_MS);
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
  }, [skinRef, animate, layout.canvasWidth, layout.canvasHeight]);

  return (
    <div
      className="skins-preview-crop"
      style={{ width: layout.cropWidth, height: layout.cropHeight }}
    >
      <canvas
        ref={canvasRef}
        className="skins-preview-canvas"
        width={layout.canvasWidth}
        height={layout.canvasHeight}
        style={{ marginLeft: -layout.offsetX, marginTop: -layout.offsetY }}
        role="img"
        aria-label={label}
      />
    </div>
  );
};

interface BasePreviewProps {
  skinRef: string;
  label: string;
}

/**
 * One base theme, as the two halves a player actually sees: their own end of
 * the arena and the opponent's. Painted through the skin's own `base_theme()`
 * so no arena colour is mirrored into TypeScript.
 *
 * Base dressing carries no animation of its own, so unlike a snake preview
 * there is nothing for hover to start.
 */
const BasePreview: React.FC<BasePreviewProps> = ({ skinRef, label }) => {
  const ownRef = useRef<HTMLCanvasElement | null>(null);
  const enemyRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const wasm = getWasm();
    if (!wasm) {
      return;
    }
    for (const [canvas, own] of [
      [ownRef.current, true],
      [enemyRef.current, false],
    ] as const) {
      if (!canvas) {
        continue;
      }
      try {
        wasm.renderSkinBase(canvas, skinRef, own);
      } catch {
        // As with snake previews: leave the last good paint rather than throw.
      }
    }
  }, [skinRef]);

  return (
    <div className="skins-base-preview" role="img" aria-label={label}>
      <canvas ref={ownRef} width={120} height={72} />
      <canvas ref={enemyRef} width={120} height={72} />
    </div>
  );
};

type Slot = 'snake' | 'base';

interface SkinRowProps {
  entry: CatalogEntry;
  slot: Slot;
  isEquipped: boolean;
  canEquip: boolean;
  isBusy: boolean;
  onEquip: (reference: string) => void;
  /** Present only for a priced skin the viewer does not own yet. */
  onBuy?: () => void;
}

const SkinRow: React.FC<SkinRowProps> = ({
  entry,
  slot,
  isEquipped,
  canEquip,
  isBusy,
  onEquip,
  onBuy,
}) => {
  const [hovered, setHovered] = useState(false);
  const reduceMotion = useMemo(prefersReducedMotion, []);
  const animate = hovered && !reduceMotion;

  return (
    <li
      className={`skins-row ${isEquipped ? 'is-equipped' : ''}`}
      data-testid={`skin-row-${slot}-${entry.reference}`}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocus={() => setHovered(true)}
      onBlur={() => setHovered(false)}
    >
      <div className="skins-row-preview">
        {slot === 'snake' ? (
          <SnakePreview
            skinRef={entry.reference}
            animate={animate}
            label={`${entry.name} snake skin`}
          />
        ) : (
          <BasePreview skinRef={entry.reference} label={`${entry.name} base skin`} />
        )}
      </div>

      <div className="skins-row-meta">
        <span className="skins-row-name">{entry.name}</span>
        <span className="skins-row-price">
          {entry.priceBux === 0 ? 'Free' : `${entry.priceBux} BB`}
        </span>
      </div>

      <div className="skins-row-action">
        {isEquipped ? (
          <span className="skins-equipped-badge" data-testid={`skin-equipped-${slot}`}>
            Equipped
          </span>
        ) : onBuy ? (
          <button
            type="button"
            className="skins-equip-button"
            disabled={isBusy}
            onClick={onBuy}
            data-testid={`skin-buy-${entry.reference}`}
          >
            {canEquip ? `Buy · ${entry.priceBux} BB` : 'Sign in to buy'}
          </button>
        ) : (
          <button
            type="button"
            className="skins-equip-button"
            disabled={isBusy}
            onClick={() => onEquip(entry.reference)}
            data-testid={`skin-equip-${slot}-${entry.reference}`}
          >
            {canEquip ? 'Equip' : 'Sign in to equip'}
          </button>
        )}
      </div>
    </li>
  );
};

const SkinsPage: React.FC<SkinsPageProps> = ({ onOpenAuth, onOpenAccount }) => {
  const { user, logout } = useAuth();
  const [ready, setReady] = useState(false);
  const [snakeSkins, setSnakeSkins] = useState<CatalogEntry[]>([]);
  const [baseSkins, setBaseSkins] = useState<CatalogEntry[]>([]);
  const [equippedSkin, setEquippedSkin] = useState<string>(DEFAULT_SKIN_REF);
  const [equippedBase, setEquippedBase] = useState<string | null>(null);
  const [busySlot, setBusySlot] = useState<Slot | null>(null);
  /** Player-authored entries, kept beside the merged list so a row can find
   *  the skin id a purchase needs. */
  const [authored, setAuthored] = useState<SkinSummary[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void initWasm().then(() => {
      if (!cancelled) {
        setReady(true);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    // Built-ins and player-authored skins are two different lists on the
    // server — one is compiled in, the other is stored — and one list to a
    // player. Authored skins go first: they are the new thing, and a catalogue
    // that buries them under nineteen built-ins is not much of a shop window.
    void Promise.all([
      api.browseSkins('snake'),
      api.browseSkins('base'),
      api.browseAuthoredSkins('snake').catch(() => ({ skins: [], cursor: null })),
    ])
      .then(([snakes, bases, authored]) => {
        if (cancelled) {
          return;
        }
        setSnakeSkins([
          ...authored.skins.map(
            (skin): CatalogEntry => ({
              reference: skin.reference,
              name: skin.name,
              kind: 'snake',
              priceBux: skin.priceBux,
            }),
          ),
          ...snakes.skins,
        ]);
        setAuthored(authored.skins);
        setBaseSkins(bases.skins);
      })
      .catch(() => {
        if (!cancelled) {
          setError('The catalogue could not be loaded. Try again in a moment.');
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // The account is the authority on what is equipped; local storage is the
  // echo that makes the page correct before the account resolves, and that
  // carries a signed-out visitor's choice at all.
  useEffect(() => {
    setEquippedSkin(readSkinPreference());
    setEquippedBase(readBasePreference());
  }, [user?.id]);

  const equip = useCallback(
    async (slot: Slot, reference: string) => {
      setError(null);

      // Paint the choice immediately. Equipping is a cosmetic preference, and
      // making the player wait on a round trip to see their own pick is worse
      // than briefly showing a state the server has not confirmed.
      const previousSkin = equippedSkin;
      const previousBase = equippedBase;
      if (slot === 'snake') {
        setEquippedSkin(reference);
        writeSkinPreference(reference);
      } else {
        setEquippedBase(reference);
        writeBasePreference(reference);
      }

      if (!user) {
        // No account to write to. The local choice stands so the arena paints
        // it, and the prompt explains why nobody else will see it.
        onOpenAuth();
        return;
      }

      setBusySlot(slot);
      try {
        const equipment = await api.setEquipment(
          slot === 'snake'
            ? { selectedSkin: reference }
            : { selectedBase: `base:${reference}` },
        );
        setEquippedSkin(equipment.selectedSkin ?? DEFAULT_SKIN_REF);
        setEquippedBase(
          equipment.selectedBase?.startsWith('base:')
            ? equipment.selectedBase.slice('base:'.length)
            : null,
        );
      } catch (cause) {
        // Put the previous choice back rather than leaving the page claiming
        // something the server rejected.
        setEquippedSkin(previousSkin);
        setEquippedBase(previousBase);
        writeSkinPreference(previousSkin);
        writeBasePreference(previousBase);
        setError(
          isApiError(cause) ? cause.message : 'That skin could not be equipped.',
        );
      } finally {
        setBusySlot(null);
      }
    },
    [equippedBase, equippedSkin, onOpenAuth, user],
  );

  const buy = useCallback(
    async (skin: SkinSummary) => {
      if (!user) {
        onOpenAuth();
        return;
      }
      setError(null);
      try {
        // A fresh key per attempt, so a retry of *this* attempt is free and a
        // second deliberate purchase is a second purchase.
        const result = await api.purchaseSkin(
          skin.skinId,
          skin.priceBux,
          crypto.randomUUID(),
        );
        if (result.outcome === 'purchased' || result.outcome === 'alreadyOwned') {
          await equip('snake', skin.reference);
        }
      } catch (cause) {
        setError(
          isApiError(cause)
            ? cause.message
            : 'That skin could not be bought right now.',
        );
      }
    },
    [equip, onOpenAuth, user],
  );


  return (
    <div className="home-page skins-page">
      <HomeHeader
        activePage="skins"
        currentUser={user}
        lobbyMembers={[]}
        hasLobby={false}
        onInvite={() => {}}
        onJoinGame={() => {}}
        onLeaveLobby={() => {}}
        onAuthClick={onOpenAuth}
        onOpenAccount={onOpenAccount}
        onLogout={logout}
      />

      <main className="skins-main">
        <div className="skins-intro">
          <h1 className="skins-title">SKINS</h1>
          <p className="skins-subtitle">
            Pick how your snake looks to everyone else, and how your side of the
            arena looks to you.
          </p>
          <Link className="skins-create-link" to="/skins/builder">
            Make your own
          </Link>
        </div>

        {error ? (
          <p className="skins-error" role="alert">
            {error}
          </p>
        ) : null}

        <div className="skins-columns">
          <section className="skins-column skins-column-snakes" aria-labelledby="skins-snakes-heading">
            <h2 id="skins-snakes-heading" className="skins-column-heading">
              Snake skins
            </h2>
            <ul className="skins-list" data-testid="snake-skin-list">
              {ready
                ? snakeSkins.map((entry) => (
                    <SkinRow
                      key={entry.reference}
                      entry={entry}
                      slot="snake"
                      isEquipped={entry.reference === equippedSkin}
                      canEquip={Boolean(user)}
                      isBusy={busySlot === 'snake'}
                      onEquip={(reference) => void equip('snake', reference)}
                      onBuy={(() => {
                        const match = authored.find(
                          (candidate) => candidate.reference === entry.reference,
                        );
                        // Built-ins are free and already everyone's; only a
                        // priced authored skin needs buying.
                        return match && match.priceBux > 0
                          ? () => void buy(match)
                          : undefined;
                      })()}
                    />
                  ))
                : null}
            </ul>
          </section>

          <section className="skins-column skins-column-bases" aria-labelledby="skins-bases-heading">
            <h2 id="skins-bases-heading" className="skins-column-heading">
              Base skins
            </h2>
            <p className="skins-column-note">
              Base dressing themes the arena you are looking at. It is yours
              alone — other players see their own.
            </p>
            <ul className="skins-list" data-testid="base-skin-list">
              {ready
                ? baseSkins.map((entry) => (
                    <SkinRow
                      key={entry.reference}
                      entry={entry}
                      slot="base"
                      isEquipped={entry.reference === equippedBase}
                      canEquip={Boolean(user)}
                      isBusy={busySlot === 'base'}
                      onEquip={(reference) => void equip('base', reference)}
                    />
                  ))
                : null}
            </ul>
          </section>
        </div>
      </main>

      <SocialFooter />
    </div>
  );
};

export default SkinsPage;
