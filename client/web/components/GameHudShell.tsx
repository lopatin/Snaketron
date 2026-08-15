import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { GameState, QueueMode } from '../types';
import { buildMatchPresentation } from '../utils/gamePresentation';
import type { MatchRatingState } from '../utils/ratingReveal';
import type { MatchHighlightState } from '../utils/highlightPresentation';
import GameOverCard from './GameOverCard';
import MatchRosterBand from './MatchRosterBand';
import Scoreboard from './Scoreboard';

/**
 * Beat between the match ending and the results card taking over the screen.
 * Long enough to see how it ended, short enough not to feel like a stall.
 */
const SCORE_CARD_REVEAL_DELAY_MS = 1600;

export interface GameHudShellProps {
  gameState: GameState | null;
  isVisible: boolean;
  arenaWidth: number;
  currentUserId?: number;
  queueMode?: QueueMode;
  rating?: MatchRatingState;
  highlight: MatchHighlightState;
  onMenu: () => void;
  onPlayAgain: () => void;
  playAgainDisabled?: boolean;
  utilityHost?: HTMLElement | null;
}

const GameHudShell: React.FC<GameHudShellProps> = ({
  gameState,
  isVisible,
  arenaWidth,
  currentUserId,
  queueMode,
  rating,
  highlight,
  onMenu,
  onPlayAgain,
  playAgainDisabled = false,
  utilityHost = null,
}) => {
  const presentation = useMemo(() => (
    gameState ? buildMatchPresentation(gameState, currentUserId, queueMode) : null
  ), [currentUserId, gameState, queueMode]);
  const [scoreCardOpen, setScoreCardOpen] = useState(false);
  const priorMatchRef = useRef<{ key: string; complete: boolean } | null>(null);
  const matchKey = gameState
    ? `${gameState.start_ms}:${gameState.game_code ?? ''}`
    : 'none';

  useEffect(() => {
    const complete = presentation?.isComplete ?? false;
    const prior = priorMatchRef.current;
    let timer: number | undefined;
    if (!prior || prior.key !== matchKey) {
      // First sight of this match. An already-finished game (a rejoin, a
      // spectator arriving late) has no ending to register, so it opens flat.
      setScoreCardOpen(complete);
    } else if (complete && !prior.complete) {
      // The match just ended under the player. Let the final moment land
      // before the results card covers the arena — cutting straight to the
      // modal reads as though the game was interrupted rather than finished.
      timer = window.setTimeout(() => setScoreCardOpen(true), SCORE_CARD_REVEAL_DELAY_MS);
    } else if (!complete) {
      setScoreCardOpen(false);
    }
    priorMatchRef.current = { key: matchKey, complete };
    return () => {
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [matchKey, presentation?.isComplete]);

  if (!presentation || !gameState) {
    return null;
  }

  const utilityRail = (
    <div
      className="game-hud-utility-row"
      data-testid="game-utility-rail"
    >
      <MatchRosterBand
        presentation={presentation}
        isVisible={isVisible}
        onMenu={onMenu}
        onScoreCard={() => setScoreCardOpen((open) => !open)}
        scoreCardOpen={scoreCardOpen}
      />
    </div>
  );

  return (
    <>
      <div
        className={`game-hud-shell${isVisible ? ' is-visible' : ''}`}
        style={{ '--game-arena-panel-width': `${arenaWidth}px` } as React.CSSProperties}
        data-testid="game-hud-shell"
      >
        <Scoreboard
          gameState={gameState}
          isVisible={isVisible}
          currentUserId={currentUserId}
          queueMode={queueMode}
          onMenu={presentation.isComplete ? onMenu : undefined}
        />
      </div>

      {utilityHost ? createPortal(utilityRail, utilityHost) : null}

      <GameOverCard
        open={scoreCardOpen && presentation.isComplete}
        gameId={matchKey}
        presentation={presentation}
        rating={rating}
        highlight={highlight}
        onDismiss={() => setScoreCardOpen(false)}
        onMenu={onMenu}
        onPlayAgain={onPlayAgain}
        playAgainDisabled={playAgainDisabled}
      />
    </>
  );
};

export default GameHudShell;
