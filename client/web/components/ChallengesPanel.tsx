import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useWebSocket } from '../contexts/WebSocketContext';
import {
  challengeOutcomeLabel,
  secondsRemaining,
  visibleChallenges,
} from '../utils/challengePresentation';
import { ChallengeIcon } from './Icons';

/**
 * Incoming and outgoing challenges, as a minimizable dock in the bottom-right
 * corner — the same trigger/panel shape as lobby and game chat, so it reads as
 * part of the same family of floating panels.
 *
 * It renders nothing at all when there is nothing pending. A permanently
 * parked empty panel would be one more thing occupying a corner that already
 * holds chat.
 */

export const ChallengesPanel: React.FC = () => {
  const { challenges, challengeError, respondToChallenge, cancelChallenge, dismissChallengeError } =
    useWebSocket();
  // Starts collapsed. Only a challenge addressed *at you* is worth taking the
  // screen for; your own outgoing challenge is something you already know
  // about, and on a phone an auto-opened card lands on the play button.
  const [isExpanded, setIsExpanded] = useState(false);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const triggerRef = useRef<HTMLButtonElement>(null);
  const collapseRef = useRef<HTMLButtonElement>(null);
  const previousPendingRef = useRef(0);

  const visible = useMemo(() => visibleChallenges(challenges, nowMs), [challenges, nowMs]);
  const pendingIncoming = visible.incoming.filter((challenge) => challenge.state === 'pending');
  const hasAnything = visible.incoming.length + visible.outgoing.length > 0;

  // Countdown, ticked only while something is on screen so an idle tab is not
  // re-rendering once a second forever.
  useEffect(() => {
    if (!hasAnything) {
      return undefined;
    }
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [hasAnything]);

  // A new incoming challenge opens the panel: one nobody sees is one that
  // expires. Unlike chat this is not chatter — it is addressed at you and it
  // lapses in two minutes. Reconnecting into an existing challenge counts as
  // new, because the count starts at zero.
  useEffect(() => {
    if (pendingIncoming.length > previousPendingRef.current) {
      setIsExpanded(true);
    }
    previousPendingRef.current = pendingIncoming.length;
  }, [pendingIncoming.length]);

  // An error the player cannot see is an action that silently did nothing.
  useEffect(() => {
    if (challengeError) {
      setIsExpanded(true);
    }
  }, [challengeError]);

  // A failure raised by this panel's own buttons has to be visible from this
  // panel: the roster strip that otherwise shows it is hidden during a match,
  // which is exactly when Accept and Decline are still reachable.
  if (!hasAnything && !challengeError) {
    return null;
  }

  const badgeCount = pendingIncoming.length;

  return (
    <div
      className={`challenges-dock${isExpanded ? ' is-expanded' : ''}`}
      data-testid="challenges-dock"
    >
      {!isExpanded && (
        <button
          ref={triggerRef}
          type="button"
          className={`home-chat-trigger challenges-trigger${badgeCount > 0 ? ' has-unread' : ''}`}
          onClick={() => setIsExpanded(true)}
          aria-expanded={false}
          aria-label={
            badgeCount > 0
              ? `Open challenges, ${badgeCount} waiting`
              : 'Open challenges'
          }
          data-testid="challenges-trigger"
        >
          <span className={`home-chat-icon${badgeCount > 0 ? ' has-unread' : ''}`}>
            <ChallengeIcon className="h-4 w-4" />
            {badgeCount > 0 && (
              <span className="absolute -top-1 -right-1 rounded bg-red-500 text-white text-[10px] font-bold px-1.5 py-0.5">
                {badgeCount > 99 ? '99+' : badgeCount}
              </span>
            )}
          </span>
          <span className="home-chat-label">Challenges</span>
        </button>
      )}

      {isExpanded && (
        <div className="home-chat-panel challenges-panel" role="region" aria-label="Challenges">
          <div className="home-chat-panel-header">
            <div className="home-chat-panel-title">
              <span
                className={`home-chat-panel-status${badgeCount > 0 ? ' is-active' : ''}`}
                aria-hidden="true"
              />
              Challenges
            </div>
            <button
              ref={collapseRef}
              type="button"
              className="home-chat-collapse"
              onClick={() => setIsExpanded(false)}
              aria-label="Minimize challenges"
              aria-expanded
              data-testid="challenges-collapse"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.8"
                className="h-4 w-4"
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 15h12" />
              </svg>
            </button>
          </div>

          {challengeError && (
            <p className="challenges-error" role="alert">
              <span>{challengeError}</span>
              <button type="button" onClick={dismissChallengeError} aria-label="Dismiss">
                ✕
              </button>
            </p>
          )}

          <ul className="challenges-list">
            {visible.incoming.map((challenge) => {
              const outcome = challengeOutcomeLabel(challenge);
              return (
                <li
                  key={challenge.challenge_id}
                  className={`challenge-row is-incoming${outcome ? ' is-resolved' : ''}`}
                  data-testid={`challenge-${challenge.challenge_id}`}
                >
                  <div className="challenge-copy">
                    <span className="challenge-name">{challenge.from_username}</span>
                    <span className="challenge-detail">
                      {outcome ?? `challenges you · ${secondsRemaining(challenge, nowMs)}s`}
                    </span>
                  </div>
                  {!outcome && (
                    <div className="challenge-actions">
                      <button
                        type="button"
                        className="challenge-action is-accept"
                        onClick={() => respondToChallenge(challenge.challenge_id, true)}
                        data-testid={`challenge-accept-${challenge.challenge_id}`}
                      >
                        Accept
                      </button>
                      <button
                        type="button"
                        className="challenge-action is-decline"
                        onClick={() => respondToChallenge(challenge.challenge_id, false)}
                        data-testid={`challenge-decline-${challenge.challenge_id}`}
                      >
                        Decline
                      </button>
                    </div>
                  )}
                </li>
              );
            })}

            {visible.outgoing.map((challenge) => {
              const outcome = challengeOutcomeLabel(challenge);
              return (
                <li
                  key={challenge.challenge_id}
                  className={`challenge-row is-outgoing${outcome ? ' is-resolved' : ''}`}
                  data-testid={`challenge-${challenge.challenge_id}`}
                >
                  <div className="challenge-copy">
                    <span className="challenge-name">{challenge.to_username}</span>
                    <span className="challenge-detail">
                      {outcome ?? `waiting · ${secondsRemaining(challenge, nowMs)}s`}
                    </span>
                  </div>
                  {!outcome && (
                    <div className="challenge-actions">
                      <button
                        type="button"
                        className="challenge-action is-decline"
                        onClick={() => cancelChallenge(challenge.challenge_id)}
                        data-testid={`challenge-cancel-${challenge.challenge_id}`}
                      >
                        Cancel
                      </button>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
};

export default ChallengesPanel;
