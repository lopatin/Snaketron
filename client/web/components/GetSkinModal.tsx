import React, { useEffect, useId, useRef } from 'react';
import { formatBux } from '../utils/walletChip';
import SnakeBuxIcon from './SnakeBuxIcon';
import type { SkinSummary } from '../types/generated';

/**
 * The confirmation for spending Snakebux.
 *
 * Deliberately small and deliberately arithmetic: what you have, what it
 * costs, what you are left with. Someone who pressed "get" on a priced skin
 * has already decided they want it; what they have not necessarily worked out
 * is whether they can afford it and what it leaves them with, and that is the
 * only thing this screen is for. Free skins never reach it — a confirmation
 * that costs nothing to accept is a step, not a safeguard.
 */

interface GetSkinModalProps {
  skin: SkinSummary;
  balanceBux: number;
  preview: React.ReactNode;
  busy: boolean;
  error: string | null;
  onConfirm: () => void;
  /**
   * Open the wallet. Absent when there is no way to buy Snakebux in this
   * build; the shortfall is then stated without offering a fix that is not
   * available.
   */
  onTopUp?: () => void;
  onClose: () => void;
}

const GetSkinModal: React.FC<GetSkinModalProps> = ({
  skin,
  balanceBux,
  preview,
  busy,
  error,
  onConfirm,
  onTopUp,
  onClose,
}) => {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const titleId = useId();
  const remaining = balanceBux - skin.priceBux;
  const affordable = remaining >= 0;

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

  return (
    <div className="shop-backdrop" data-testid="get-backdrop" onClick={onClose}>
      <div
        ref={dialogRef}
        className="shop-dialog is-get"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        data-testid="get-modal"
        onClick={(event) => event.stopPropagation()}
      >
        <span className="shop-kicker">Confirm</span>
        <h2 id={titleId}>{skin.name}</h2>

        <button type="button" className="shop-close" onClick={onClose} aria-label="Close">
          <span aria-hidden="true">×</span>
        </button>

        <div className="get-preview">{preview}</div>

        <dl className="get-sums">
          <div>
            <dt>Your balance</dt>
            <dd>
              <SnakeBuxIcon size={15} />
              {formatBux(balanceBux)}
            </dd>
          </div>
          <div>
            <dt>This skin</dt>
            <dd className="is-cost">
              <SnakeBuxIcon size={15} />
              {`−${formatBux(skin.priceBux)}`}
            </dd>
          </div>
          <div className="get-sums-total">
            <dt>Left after</dt>
            <dd>
              <SnakeBuxIcon size={15} />
              {formatBux(Math.max(0, remaining))}
            </dd>
          </div>
        </dl>

        {error ? (
          <p className="shop-error" role="alert">
            {error}
          </p>
        ) : null}

        <div className="get-actions">
          <button type="button" className="game-shell-button" onClick={onClose}>
            Cancel
          </button>
          {affordable ? (
            <button
              type="button"
              className="game-shell-button is-primary"
              disabled={busy}
              onClick={onConfirm}
              data-testid="get-confirm"
            >
              {busy ? 'Getting…' : 'Confirm'}
            </button>
          ) : onTopUp ? (
            <button
              type="button"
              className="game-shell-button is-primary"
              onClick={onTopUp}
              data-testid="get-topup"
            >
              <SnakeBuxIcon size={16} />
              Get more Snakebux
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
};

export default GetSkinModal;
