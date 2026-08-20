import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { AccountModalView } from './AccountModal';
import { HomeHeader } from './HomeHeader';
import { SocialFooter } from './SocialFooter';
import { useAuth } from '../contexts/AuthContext';
import { api, isApiError } from '../services/api';
import { useWallet } from '../contexts/WalletContext';
import { coerceBalance, purchaseMessage } from '../utils/walletChip';
import { getWasm, initWasm, whenSkinAssetsSettle } from '../wasm';
import { ensureAuthoredSkins } from '../utils/authoredSkins';
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

/**
 * How big one cell is drawn in a browse preview.
 *
 * Responsive rather than fixed, because the crop is measured in canvas pixels:
 * scaling the canvas with CSS would scale the drawing but not the negative
 * margins that crop it, so the snake would slide out of its own window. Drawing
 * smaller keeps the arithmetic exact at every width.
 */
const wideCell = 16;
const narrowCell = 9;

const previewCellSize = (): number =>
  typeof window !== 'undefined' && window.innerWidth < 700 ? narrowCell : wideCell;
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
  /** Changes when the registry gains a document, prompting a repaint. */
  revision?: number;
}

/**
 * One snake, painted by the real renderer.
 *
 * Still by default and animating while hovered, which is the whole interaction:
 * a wall of moving snakes is unreadable, and a wall of frozen ones hides half of
 * what an animated skin is. The live loop repaints the canvas directly rather
 * than through React state, so hovering costs canvas work and nothing else.
 */
const SnakePreview: React.FC<SnakePreviewProps> = ({
  skinRef,
  animate,
  label,
  revision = 0,
}) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [cell, setCell] = useState(previewCellSize);

  useEffect(() => {
    const onResize = () => setCell(previewCellSize());
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);
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
        wasm.skinFixtureBounds(skinRef, SNAKE_PREVIEW_POSE, cell, false),
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
  }, [skinRef, revision, cell]);

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
          cell,
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
  }, [skinRef, animate, revision, cell, layout.canvasWidth, layout.canvasHeight]);

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
  /**
   * What the renderer should resolve for the preview.
   *
   * A built-in is painted by its catalogue id, but an authored skin is painted
   * by the hash of its document — its `skin:<id>` reference means nothing to
   * the registry, and passing it would silently draw classic for every
   * player-made skin on the page.
   */
  previewRef?: string;
  /** Who made it, for player-authored skins. Built-ins have no byline. */
  byline?: string;
  /**
   * A word about this skin's standing, for skins you made.
   *
   * Only yours can be listed while unpublished, so without this a private
   * draft and a published skin look identical on a page that is showing you
   * both — and the difference is exactly what you would want to know.
   */
  status?: string;
  slot: Slot;
  isEquipped: boolean;
  canEquip: boolean;
  isBusy: boolean;
  onEquip: (reference: string) => void;
  /** Present only for a priced skin the viewer does not own yet. */
  onBuy?: () => void;
  /** Bumped when a document this row was waiting on has compiled. */
  registryRevision?: number;
}

const SkinRow: React.FC<SkinRowProps> = ({
  entry,
  previewRef,
  byline,
  status,
  slot,
  isEquipped,
  canEquip,
  isBusy,
  onEquip,
  onBuy,
  registryRevision = 0,
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
            skinRef={previewRef ?? entry.reference}
            animate={animate}
            revision={registryRevision}
            label={`${entry.name} snake skin`}
          />
        ) : (
          <BasePreview
            skinRef={previewRef ?? entry.reference}
            label={`${entry.name} base skin`}
          />
        )}
      </div>

      <div className="skins-row-meta">
        <span className="skins-row-name">{entry.name}</span>
        {byline ? <span className="skins-row-byline">by {byline}</span> : null}
        {status ? <span className="skins-row-status">{status}</span> : null}
        <span className={`skins-row-price${entry.priceBux > 0 ? ' is-priced' : ''}`}>
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
            className="game-shell-button is-primary"
            disabled={isBusy}
            onClick={onBuy}
            data-testid={`skin-buy-${entry.reference}`}
          >
            {canEquip ? `Buy · ${entry.priceBux} BB` : 'Sign in to buy'}
          </button>
        ) : (
          <button
            type="button"
            className="game-shell-button"
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
  const { applyBalance } = useWallet();
  const [ready, setReady] = useState(false);
  const [snakeSkins, setSnakeSkins] = useState<CatalogEntry[]>([]);
  const [baseSkins, setBaseSkins] = useState<CatalogEntry[]>([]);
  const [equippedSkin, setEquippedSkin] = useState<string>(DEFAULT_SKIN_REF);
  const [equippedBase, setEquippedBase] = useState<string | null>(null);
  const [busySlot, setBusySlot] = useState<Slot | null>(null);
  /** Player-authored entries, kept beside the merged list so a row can find
   *  the skin id a purchase needs. */
  const [authored, setAuthored] = useState<SkinSummary[]>([]);
  /** Bumped when an authored document compiles, so still previews repaint. */
  const [registryRevision, setRegistryRevision] = useState(0);
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
    const empty = { skins: [] as SkinSummary[], cursor: null };
    // Three lists, because they answer three different questions and only the
    // server can answer any of them: what this build ships, what other players
    // have published, and what *you* have. The last one is the reason a page
    // like this exists at all — a skin you made but have not published is
    // invisible to the published list by design, so a page built only from
    // that list can never show you your own work.
    void Promise.all([
      api.browseSkins('snake'),
      api.browseSkins('base'),
      api.browseAuthoredSkins('snake').catch(() => empty),
      user ? api.browseAuthoredSkins('snake', 'mine').catch(() => empty) : Promise.resolve(empty),
    ])
      .then(([snakes, bases, published, mine]) => {
        if (cancelled) {
          return;
        }
        // `mine` is listed by creator rather than by slot, so anything that is
        // not a snake is dropped here rather than being drawn as one.
        const merged: SkinSummary[] = mine.skins.filter((skin) => skin.kind === 'snake');
        for (const skin of published.skins) {
          if (!merged.some((held) => held.reference === skin.reference)) {
            merged.push(skin);
          }
        }

        // Yours first, then everyone else's, then the built-ins: a catalogue
        // that buries what you made under nineteen shipped skins is not much
        // of a wardrobe.
        setSnakeSkins([
          ...merged.map(
            (skin): CatalogEntry => ({
              reference: skin.reference,
              name: skin.name,
              kind: 'snake',
              priceBux: skin.priceBux,
            }),
          ),
          ...snakes.skins,
        ]);
        setAuthored(merged);
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
  }, [user]);

  // Painting an authored skin means having its document. This waits for both
  // halves — the list, and the wasm module to register into — because the
  // browse response routinely lands first and there is nothing to register
  // into yet when it does.
  useEffect(() => {
    if (!ready || authored.length === 0) {
      return;
    }
    let cancelled = false;
    void ensureAuthoredSkins(
      Object.fromEntries(
        authored
          .map((skin, index) => [index, skin.contentRef])
          .filter(([, reference]) => typeof reference === 'string'),
      ) as Record<number, string>,
    ).then(() => {
      // Rows paint once, so they need telling when a document they were
      // waiting on has compiled.
      if (!cancelled) {
        setRegistryRevision((current) => current + 1);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [authored, ready]);

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
        // The server's number, always — only `purchased` actually debits, and
        // any outcome can race a credit landing from a payment provider.
        // Subtracting the price locally would be right most of the time, which
        // is the worst kind of wrong for money.
        applyBalance(coerceBalance(result.balanceBux));
        if (result.outcome === 'purchased' || result.outcome === 'alreadyOwned') {
          await equip('snake', skin.reference);
        }
      } catch (cause) {
        // A refused purchase answers 402 or 409 with a `PurchaseResult` body
        // and no `error` or `message` key at all, so the generic reader fell
        // through to "Request failed" — which is what a player short of Bux
        // was being told. It also carries the balance, which is worth taking.
        if (isApiError(cause) && (cause.response.status === 402 || cause.response.status === 409)) {
          const body = cause.response.data as {
            outcome?: string;
            balanceBux?: unknown;
            actualPriceBux?: number | null;
          };
          applyBalance(coerceBalance(body.balanceBux));
          setError(purchaseMessage(body.outcome, body.actualPriceBux));
          return;
        }
        setError(
          isApiError(cause)
            ? cause.message
            : 'That skin could not be bought right now.',
        );
      }
    },
    [applyBalance, equip, onOpenAuth, user],
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
          <div className="skins-intro-actions">
            <Link
              className="game-shell-button is-primary skins-make-own"
              to="/skins/builder"
            >
              <span className="skins-make-own-icon" aria-hidden="true">
                +
              </span>
              Make your own
            </Link>
          </div>
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
                      previewRef={
                        authored.find(
                          (candidate) => candidate.reference === entry.reference,
                        )?.contentRef ?? undefined
                      }
                      byline={(() => {
                        const match = authored.find(
                          (candidate) => candidate.reference === entry.reference,
                        );
                        // "by you" on your own skin is noise; the status chip
                        // beside it already says whose it is.
                        return match && match.creatorUserId !== user?.id
                          ? (match.creatorUsername ?? undefined)
                          : undefined;
                      })()}
                      status={(() => {
                        const match = authored.find(
                          (candidate) => candidate.reference === entry.reference,
                        );
                        if (!match || match.creatorUserId !== user?.id) {
                          return undefined;
                        }
                        return match.publication === 'published'
                          ? 'Yours · published'
                          : match.pendingRevision !== null
                            ? 'Yours · in review'
                            : 'Yours · private';
                      })()}
                      registryRevision={registryRevision}
                      isBusy={busySlot === 'snake'}
                      onEquip={(reference) => void equip('snake', reference)}
                      onBuy={(() => {
                        const match = authored.find(
                          (candidate) => candidate.reference === entry.reference,
                        );
                        // Built-ins are free and already everyone's; only a
                        // priced authored skin somebody else made needs
                        // buying. Your own is yours at any price.
                        return match &&
                          match.priceBux > 0 &&
                          match.creatorUserId !== user?.id
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
