import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { AccountModalView } from './AccountModal';
import { HomeHeader } from './HomeHeader';
import { SocialFooter } from './SocialFooter';
import { useAuth } from '../contexts/AuthContext';
import { api, isApiError } from '../services/api';
import { analytics } from '../services/analytics';
import { useWallet } from '../contexts/WalletContext';
import { BUX_UNIT, coerceBalance, purchaseMessage } from '../utils/walletChip';
import { getWasm, initWasm, whenSkinAssetsSettle } from '../wasm';
import { ensureAuthoredSkins } from '../utils/authoredSkins';
import SnakeBuxIcon from './SnakeBuxIcon';
import SkinModal from './SkinModal';
import type { SkinView } from './SkinModal';
import GetSkinModal from './GetSkinModal';
import WalletModal from './WalletModal';
import SkinToast from './SkinToast';
import type { CatalogEntry, SkinSummary } from '../types/generated';
import { Link, useNavigate } from 'react-router-dom';
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
/** The tint an equipped row wears, and therefore what its canvas paints. */
const EQUIPPED_FIELD = '#eff6ff';

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
  /**
   * What to paint behind the snake, when the row is not on the usual white.
   *
   * The canvas is opaque — it has to be, because a body crossing itself paints
   * the gap in this colour — so a tinted row that did not pass its tint here
   * would show as a white block around the art.
   */
  field?: string;
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
  field,
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
          field,
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
  }, [skinRef, animate, revision, cell, field, layout.canvasWidth, layout.canvasHeight]);

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
  /** The authored record, when the row is one. Built-ins have none. */
  summary?: SkinSummary;
  slot: Slot;
  isEquipped: boolean;
  canEquip: boolean;
  isBusy: boolean;
  onEquip: (reference: string) => void;
  /** Acquire it. Absent for built-ins, which everyone already has. */
  onGet?: () => void;
  /** Open the skin's own page. Absent for built-ins, which have none. */
  onOpen?: () => void;
  /** Bumped when a document this row was waiting on has compiled. */
  registryRevision?: number;
}

const SkinRow: React.FC<SkinRowProps> = ({
  entry,
  previewRef,
  byline,
  summary,
  slot,
  isEquipped,
  canEquip,
  isBusy,
  onEquip,
  onGet,
  onOpen,
  registryRevision = 0,
}) => {
  const [hovered, setHovered] = useState(false);
  const reduceMotion = useMemo(prefersReducedMotion, []);
  const animate = hovered && !reduceMotion;
  // Built-ins ship with the client and belong to everybody; only an authored
  // skin has to be got before it can be worn.
  const owned = summary ? summary.owned : true;
  const locked = summary ? summary.publication !== 'published' : false;

  return (
    <li
      className={`skins-row ${isEquipped ? 'is-equipped' : ''} ${onOpen ? 'is-openable' : ''}`}
      data-testid={`skin-row-${slot}-${entry.reference}`}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocus={() => setHovered(true)}
      onBlur={() => setHovered(false)}
    >
      {/* The whole row opens the skin's page — except the action, which sits
          above it and does its own thing. A button rather than a click handler
          on the <li>, so it is reachable and announced like the control it is. */}
      {onOpen ? (
        <button
          type="button"
          className="skins-row-open"
          onClick={onOpen}
          aria-label={`About ${entry.name}`}
          data-testid={`skin-open-${entry.reference}`}
        />
      ) : null}

      <div className="skins-row-preview">
        {slot === 'snake' ? (
          <SnakePreview
            skinRef={previewRef ?? entry.reference}
            animate={animate}
            revision={registryRevision}
            label={`${entry.name} snake skin`}
            field={isEquipped ? EQUIPPED_FIELD : undefined}
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
        <span className={`skins-row-price${entry.priceBux > 0 ? ' is-priced' : ''}`}>
          {/* A skin nobody else can see yet says so with a lock rather than a
              word: it qualifies the price, and it belongs beside it. */}
          {locked ? (
            <svg
              className="skins-row-lock"
              viewBox="0 0 24 24"
              width="10"
              height="10"
              fill="currentColor"
              role="img"
              aria-label="Not published"
            >
              <path d="M12 1.6a4.9 4.9 0 0 0-4.9 4.9V9H6.2A1.7 1.7 0 0 0 4.5 10.7v9.1a1.7 1.7 0 0 0 1.7 1.7h11.6a1.7 1.7 0 0 0 1.7-1.7v-9.1A1.7 1.7 0 0 0 17.8 9h-.9V6.5A4.9 4.9 0 0 0 12 1.6zm0 2.2a2.7 2.7 0 0 1 2.7 2.7V9H9.3V6.5A2.7 2.7 0 0 1 12 3.8z" />
            </svg>
          ) : null}
          {entry.priceBux === 0 ? 'Free' : `${entry.priceBux} ${BUX_UNIT}`}
        </span>
      </div>

      <div className="skins-row-action">
        {isEquipped ? (
          <span className="skins-equipped-badge" data-testid={`skin-equipped-${slot}`}>
            Equipped
          </span>
        ) : owned ? (
          <button
            type="button"
            className="game-shell-button skins-row-cta"
            disabled={isBusy}
            onClick={() => onEquip(entry.reference)}
            data-testid={`skin-equip-${slot}-${entry.reference}`}
          >
            {canEquip ? 'Equip' : 'Sign in to equip'}
          </button>
        ) : (
          <button
            type="button"
            className="game-shell-button is-primary skins-row-cta"
            disabled={isBusy}
            onClick={onGet}
            data-testid={`skin-get-${entry.reference}`}
          >
            {entry.priceBux > 0 ? (
              <>
                <SnakeBuxIcon size={14} />
                {entry.priceBux}
              </>
            ) : (
              'Get'
            )}
          </button>
        )}
      </div>
    </li>
  );
};

const SkinsPage: React.FC<SkinsPageProps> = ({ onOpenAuth, onOpenAccount }) => {
  const { user, logout } = useAuth();
  const { applyBalance, balanceBux } = useWallet();
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
  const navigate = useNavigate();
  /** The skin whose page is open, if any. */
  const [viewing, setViewing] = useState<SkinView | null>(null);
  /** The skin awaiting a spend confirmation. */
  const [getting, setGetting] = useState<SkinSummary | null>(null);
  /** Whether the wallet is open, and what it is being opened *for*. */
  const [wallet, setWallet] = useState<{ name: string; priceBux: number } | null | false>(false);
  const [toast, setToast] = useState<{ message: string; coin: boolean } | null>(null);
  const [busySkin, setBusySkin] = useState<number | null>(null);
  const [getError, setGetError] = useState<string | null>(null);
  /** Bumped to re-ask the server what the viewer owns. */
  const [ownershipRevision, setOwnershipRevision] = useState(0);

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
  }, [user, ownershipRevision]);

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

  /**
   * Acquire a skin, priced or not.
   *
   * One route for both, because the server's purchase path already treats a
   * price of zero as a grant — and because "get" should mean the same thing to
   * a player whichever it is. What differs is only whether they were asked to
   * confirm first, which is decided before this runs.
   */
  const acquire = useCallback(
    async (skin: SkinSummary): Promise<boolean> => {
      if (!user) {
        onOpenAuth();
        return false;
      }
      setError(null);
      setGetError(null);
      setBusySkin(skin.skinId);
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
        if (result.outcome === 'purchased') {
          // Only this outcome debits. `alreadyOwned` moves no Bux, and
          // reporting it would put a free acquisition into the economy
          // dashboards as though it had cost something.
          analytics.trackCurrencySpent(skin.priceBux, 'skin', skin.reference);
        }
        if (result.outcome === 'purchased' || result.outcome === 'alreadyOwned') {
          // Re-ask what the viewer owns before equipping: wearing is gated on
          // holding, so the page has to learn about the grant it just earned.
          setOwnershipRevision((count) => count + 1);
          await equip('snake', skin.reference);
          setToast({
            message:
              skin.priceBux > 0
                ? `${skin.name} is yours. You are wearing it.`
                : `${skin.name} added. You are wearing it.`,
            coin: skin.priceBux > 0,
          });
          return true;
        }
        setGetError(purchaseMessage(result.outcome, result.actualPriceBux));
        return false;
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
          setGetError(purchaseMessage(body.outcome, body.actualPriceBux));
          return false;
        }
        const message = isApiError(cause)
          ? cause.message
          : 'That did not go through. Try again in a moment.';
        setGetError(message);
        setError(message);
        return false;
      } finally {
        setBusySkin(null);
      }
    },
    [applyBalance, equip, onOpenAuth, user],
  );

  /**
   * What pressing "get" does, which depends on what it costs.
   *
   * Free is immediate: a confirmation that cannot cost anything is a step, not
   * a safeguard. Priced opens the sums first, and a price beyond the balance
   * opens the wallet rather than refusing — being short of Bux is a thing to
   * fix, not an error to report.
   */
  const startGet = useCallback(
    (skin: SkinSummary) => {
      if (!user) {
        onOpenAuth();
        return;
      }
      if (skin.priceBux === 0) {
        void acquire(skin);
        return;
      }
      if ((balanceBux ?? 0) < skin.priceBux) {
        setWallet({ name: skin.name, priceBux: skin.priceBux });
        return;
      }
      setGetError(null);
      setGetting(skin);
    },
    [acquire, balanceBux, onOpenAuth, user],
  );


  /** The authored record behind a row, if the row is one. Built-ins have none. */
  const summaryFor = useCallback(
    (reference: string): SkinSummary | undefined =>
      authored.find((candidate) => candidate.reference === reference),
    [authored],
  );

  /**
   * Turn a row into the thing its page is about.
   *
   * Every snake skin has a page, built-ins included — a row that opens for
   * three of its entries and not the rest is not a list you can learn. What
   * differs is what there is to say: a built-in has no author, no price and
   * nobody to count, so it says so rather than showing zeroes.
   */
  const viewOf = useCallback(
    (entry: CatalogEntry): SkinView => {
      const match = summaryFor(entry.reference);
      if (!match) {
        return {
          reference: entry.reference,
          name: entry.name,
          priceBux: 0,
          owned: true,
        };
      }
      return {
        reference: match.reference,
        name: match.name,
        priceBux: match.priceBux,
        previewRef: match.contentRef ?? undefined,
        creatorName:
          match.creatorUserId === user?.id
            ? undefined
            : (match.creatorUsername ?? undefined),
        owned: match.owned,
        stats: { ownerCount: match.ownerCount, wearerCount: match.wearerCount },
        editableSkinId: match.creatorUserId === user?.id ? match.skinId : undefined,
      };
    },
    [summaryFor, user?.id],
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
                        const match = summaryFor(entry.reference);
                        // "by you" on your own skin is noise; you know.
                        return match && match.creatorUserId !== user?.id
                          ? (match.creatorUsername ?? undefined)
                          : undefined;
                      })()}
                      summary={summaryFor(entry.reference)}
                      registryRevision={registryRevision}
                      isBusy={busySlot === 'snake' || busySkin !== null}
                      onEquip={(reference) => void equip('snake', reference)}
                      onGet={(() => {
                        const match = summaryFor(entry.reference);
                        return match ? () => startGet(match) : undefined;
                      })()}
                      onOpen={() => setViewing(viewOf(entry))}
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

      {viewing ? (
        <SkinModal
          skin={viewing}
          registryRevision={registryRevision}
          balanceBux={balanceBux ?? 0}
          isEquipped={viewing.reference === equippedSkin}
          busy={busySkin === viewing.editableSkinId}
          onClose={() => setViewing(null)}
          onGet={() => {
            const match = summaryFor(viewing.reference);
            setViewing(null);
            if (match) {
              startGet(match);
            }
          }}
          onEquip={() => {
            void equip('snake', viewing.reference);
            setViewing(null);
          }}
          onEdit={() => {
            if (viewing.editableSkinId !== undefined) {
              navigate(`/skins/builder/${viewing.editableSkinId}`);
            }
          }}
          onTopUp={() => {
            setViewing(null);
            setWallet({ name: viewing.name, priceBux: viewing.priceBux });
          }}
        />
      ) : null}

      {getting ? (
        <GetSkinModal
          skin={getting}
          balanceBux={balanceBux ?? 0}
          busy={busySkin === getting.skinId}
          error={getError}
          preview={
            <SnakePreview
              skinRef={getting.contentRef ?? getting.reference}
              animate
              revision={registryRevision}
              label={`${getting.name} snake skin`}
            />
          }
          onConfirm={() => {
            void acquire(getting).then((done) => {
              if (done) {
                setGetting(null);
              }
            });
          }}
          onTopUp={() => {
            setGetting(null);
            setWallet({ name: getting.name, priceBux: getting.priceBux });
          }}
          onClose={() => setGetting(null)}
        />
      ) : null}

      {wallet !== false ? (
        <WalletModal shortfallFor={wallet} onClose={() => setWallet(false)} />
      ) : null}

      {toast ? (
        <SkinToast message={toast.message} coin={toast.coin} onDone={() => setToast(null)} />
      ) : null}

      <SocialFooter />
    </div>
  );
};

export default SkinsPage;
