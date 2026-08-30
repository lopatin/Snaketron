import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { LinkIcon, ShareIcon } from './Icons';
import {
  buildGameShareUrl,
  buildShareContent,
  buildShareTargets,
  canUseNativeShare,
} from '../utils/shareGame';

/**
 * Share control for one match. Renders as a single button that opens a small
 * popover with the permanent link, a copy action, the OS share sheet where the
 * browser has one, and plain web-intent links for the networks people actually
 * post game results to.
 *
 * The link is `/g/:gameId`, which keeps working forever — the summary behind
 * it has no retention deadline — so a share posted today still resolves years
 * from now, and the same path serves crawlers a real Open Graph card.
 */
export interface ShareGameProps {
  gameId: number | string | null | undefined;
  /**
   * One-line match result, used as the accompanying text.
   *
   * It is read by whoever the link is sent to, so it has to be written for
   * them: the sharer's own voice, or a neutral third person — never the
   * second-person copy the results card addresses its own reader in.
   */
  headline?: string | null;
  /** `compact` is the icon-only form used inside the in-match HUD. */
  variant?: 'default' | 'compact';
  className?: string;
  label?: string;
  /**
   * Button class for surfaces that already have a button vocabulary.
   *
   * The results card and the HUD rail both speak `game-shell-button`, and a
   * share control that invents its own border and radius next to a Main menu
   * button reads as a different kind of thing. The standalone match page has
   * no such vocabulary, so it keeps the default.
   */
  triggerClassName?: string;
}

type CopyState = 'idle' | 'copied' | 'failed';

const COPY_RESET_MS = 2000;

const IS_EMBEDDED_BUILD =
  process.env.ITCH_BUILD === 'true' || process.env.CRAZYGAMES_BUILD === 'true';

export const ShareGame: React.FC<ShareGameProps> = ({
  gameId,
  headline,
  variant = 'default',
  className = '',
  label = 'Share',
  triggerClassName,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [copyState, setCopyState] = useState<CopyState>('idle');
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const copyResetRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const shareUrl = useMemo(
    () =>
      buildGameShareUrl({
        gameId,
        origin: typeof window === 'undefined' ? null : window.location.origin,
        isEmbeddedBuild: IS_EMBEDDED_BUILD,
      }),
    [gameId],
  );
  const content = useMemo(
    () => (shareUrl ? buildShareContent(shareUrl, headline) : null),
    [headline, shareUrl],
  );
  const targets = useMemo(() => (content ? buildShareTargets(content) : []), [content]);
  const nativeShareAvailable = canUseNativeShare();

  useEffect(
    () => () => {
      if (copyResetRef.current) {
        clearTimeout(copyResetRef.current);
      }
    },
    [],
  );

  // Reset the acknowledgement when the popover closes, so reopening never
  // shows a stale "Copied" from a previous match.
  useEffect(() => {
    if (isOpen) {
      return;
    }
    if (copyResetRef.current) {
      clearTimeout(copyResetRef.current);
      copyResetRef.current = null;
    }
    setCopyState('idle');
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) {
      return undefined;
    }
    const onPointerDown = (event: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsOpen(false);
        triggerRef.current?.focus();
      }
    };
    document.addEventListener('mousedown', onPointerDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onPointerDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [isOpen]);

  const handleCopy = useCallback(async () => {
    if (!shareUrl) {
      return;
    }
    if (copyResetRef.current) {
      clearTimeout(copyResetRef.current);
    }
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error('Clipboard API unavailable');
      }
      await navigator.clipboard.writeText(shareUrl);
      setCopyState('copied');
    } catch {
      setCopyState('failed');
    }
    copyResetRef.current = setTimeout(() => setCopyState('idle'), COPY_RESET_MS);
  }, [shareUrl]);

  const handleNativeShare = useCallback(async () => {
    if (!content) {
      return;
    }
    try {
      await navigator.share({ title: content.title, text: content.text, url: content.url });
      setIsOpen(false);
    } catch {
      // A dismissed share sheet is a normal outcome, not an error worth
      // reporting; the popover stays open so the copy action is still there.
    }
  }, [content]);

  if (!shareUrl || !content) {
    return null;
  }

  return (
    <div ref={rootRef} className={`share-game ${className}`.trim()} data-testid="share-game">
      <button
        ref={triggerRef}
        type="button"
        className={
          triggerClassName
            ?? `share-game-trigger${variant === 'compact' ? ' is-compact' : ''}`
        }
        onClick={() => setIsOpen((open) => !open)}
        aria-expanded={isOpen}
        aria-haspopup="dialog"
        aria-label={variant === 'compact' ? 'Share this match' : undefined}
        data-testid="share-game-trigger"
      >
        <ShareIcon className="share-game-icon" />
        {variant === 'default' && <span>{label}</span>}
      </button>

      {isOpen && (
        <div className="share-game-popover" role="dialog" aria-label="Share this match">
          <p className="share-game-heading">Share this match</p>
          <p className="share-game-note">This link is permanent.</p>

          <div className="share-game-link-row">
            <span className="share-game-link" title={shareUrl}>
              {shareUrl}
            </span>
            <button
              type="button"
              className="share-game-copy"
              onClick={handleCopy}
              data-testid="share-game-copy"
            >
              <LinkIcon className="share-game-icon" />
              <span className="share-game-copy-label">
                {/* Reserves the width of the longest label so the button does
                    not resize under the cursor when the state changes. */}
                <span className="share-game-copy-sizer" aria-hidden="true">
                  Try again
                </span>
                <span>
                  {copyState === 'copied' ? 'Copied' : copyState === 'failed' ? 'Try again' : 'Copy'}
                </span>
              </span>
            </button>
          </div>

          {nativeShareAvailable && (
            <button
              type="button"
              className="share-game-native"
              onClick={handleNativeShare}
              data-testid="share-game-native"
            >
              <ShareIcon className="share-game-icon" />
              <span>Share via…</span>
            </button>
          )}

          <div className="share-game-targets">
            {targets.map((target) => (
              <a
                key={target.id}
                className="share-game-target"
                href={target.href}
                target="_blank"
                rel="noopener noreferrer"
                data-testid={`share-game-target-${target.id}`}
              >
                {target.label}
              </a>
            ))}
          </div>

          <p className="sr-only" aria-live="polite">
            {copyState === 'copied'
              ? 'Match link copied.'
              : copyState === 'failed'
                ? 'Could not copy the match link.'
                : ''}
          </p>
        </div>
      )}
    </div>
  );
};

export default ShareGame;
