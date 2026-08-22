import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import { api, isApiError } from '../services/api';
import { useWallet } from '../contexts/WalletContext';
import { formatBux } from '../utils/walletChip';
import SnakeBuxIcon from './SnakeBuxIcon';
import type { BuxPack } from '../types/generated';

/**
 * The wallet: what you have, and how to get more.
 *
 * Reached from the balance in the header and from anywhere a price turns out
 * to be more than the balance. Those are the same screen on purpose — someone
 * who came here because a skin cost more than they had should not have to
 * work out that "top up" and "the number in the corner" are the same subject.
 */

interface WalletModalProps {
  onClose: () => void;
  /**
   * What the player was trying to afford when they arrived, if anything.
   *
   * Turns an abstract shop into a errand with an end: the packs that would
   * cover it are marked, so the choice is "enough for this" rather than four
   * numbers to compare against a fifth.
   */
  shortfallFor?: { name: string; priceBux: number } | null;
}

const priceLabel = (cents: number): string =>
  `$${(cents / 100).toFixed(cents % 100 === 0 ? 0 : 2)}`;

const WalletModal: React.FC<WalletModalProps> = ({ onClose, shortfallFor = null }) => {
  const { balanceBux, refresh } = useWallet();
  const [packs, setPacks] = useState<BuxPack[]>([]);
  const [busySku, setBusySku] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const titleId = useId();

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

  useEffect(() => {
    let cancelled = false;
    void api
      .buxPacks()
      .then((found) => {
        if (!cancelled) {
          setPacks(found);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setError('The shop could not be loaded. Try again in a moment.');
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const buy = useCallback(
    async (pack: BuxPack) => {
      setBusySku(pack.sku);
      setError(null);
      try {
        const checkout = await api.buxCheckoutToken(pack.sku);
        // The provider hosts the payment itself; this hands off and comes back
        // through the settlement webhook, so the balance is refreshed on return
        // rather than guessed at here.
        window.open(
          `https://secure.xsolla.com/paystation4/?token=${encodeURIComponent(checkout.token)}`,
          '_blank',
          'noopener,noreferrer',
        );
        await refresh();
      } catch (cause) {
        setError(
          isApiError(cause)
            ? cause.message
            : 'That did not go through. Try again in a moment.',
        );
      } finally {
        setBusySku(null);
      }
    },
    [refresh],
  );

  const balance = balanceBux ?? 0;
  const short = shortfallFor ? Math.max(0, shortfallFor.priceBux - balance) : 0;

  return (
    <div className="shop-backdrop" data-testid="wallet-backdrop">
      <div
        ref={dialogRef}
        className="shop-dialog is-wallet"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        data-testid="wallet-modal"
      >
        <span className="shop-kicker">Wallet</span>
        <h2 id={titleId}>Snakebux</h2>

        <button type="button" className="shop-close" onClick={onClose} aria-label="Close">
          <span aria-hidden="true">×</span>
        </button>

        <div className="shop-balance" data-testid="wallet-balance">
          <SnakeBuxIcon size={40} />
          <strong>{formatBux(balance)}</strong>
          <span>in your wallet</span>
        </div>

        {shortfallFor ? (
          <p className="shop-shortfall" data-testid="wallet-shortfall">
            {`${shortfallFor.name} costs ${formatBux(shortfallFor.priceBux)}. `}
            <strong>{`You need ${formatBux(short)} more.`}</strong>
          </p>
        ) : null}

        {error ? (
          <p className="shop-error" role="alert">
            {error}
          </p>
        ) : null}

        <ul className="shop-packs">
          {packs.map((pack) => {
            // Marked, not filtered: seeing the one that just covers it beside
            // the bigger ones is the choice, and hiding the rest would make it
            // for them.
            const covers = shortfallFor ? balance + pack.bux >= shortfallFor.priceBux : false;
            return (
              <li key={pack.sku}>
                <button
                  type="button"
                  className={`shop-pack${covers ? ' is-enough' : ''}`}
                  disabled={busySku !== null}
                  onClick={() => void buy(pack)}
                  data-testid={`wallet-pack-${pack.sku}`}
                >
                  <SnakeBuxIcon size={30} />
                  <span className="shop-pack-bux">{formatBux(pack.bux)}</span>
                  <span className="shop-pack-price">
                    {busySku === pack.sku ? 'Opening…' : priceLabel(pack.priceUsdCents)}
                  </span>
                  {covers ? <span className="shop-pack-flag">Enough</span> : null}
                </button>
              </li>
            );
          })}
        </ul>

        <p className="shop-note">
          Payment is handled by our payment provider. Your balance updates once it
          settles.
        </p>
      </div>
    </div>
  );
};

export default WalletModal;
