import { useEffect, useState } from 'react';
import { api } from '../services/api';
import {
  HIGHLIGHT_POLL_INTERVAL_MS,
  HIGHLIGHT_POLL_MAX_ATTEMPTS,
  isCompatibleHighlightClip,
  type MatchHighlightState,
} from '../utils/highlightPresentation';

/** Bounded post-match poll for the single server-selected public highlight. */
export const useHighlight = (
  gameId: string,
  isComplete: boolean,
): MatchHighlightState => {
  const [state, setState] = useState<MatchHighlightState>({ phase: 'idle' });

  useEffect(() => {
    if (!isComplete || !/^\d+$/.test(gameId) || Number(gameId) <= 0) {
      setState({ phase: 'idle' });
      return undefined;
    }

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let controller: AbortController | null = null;
    let sawNetworkFailure = false;
    setState({ phase: 'pending' });

    const finishUnavailable = (reason: 'absent' | 'incompatible' | 'network') => {
      if (!cancelled) setState({ phase: 'unavailable', reason });
    };

    const poll = (attempt: number) => {
      controller = new AbortController();
      api.getGameHighlight(gameId, controller.signal)
        .then((response) => {
          if (cancelled) return;
          if (response.status === 'ready') {
            if (
              !isCompatibleHighlightClip(response.play_of_the_game) ||
              response.play_of_the_game.game_id !== Number(gameId)
            ) {
              finishUnavailable('incompatible');
              return;
            }
            setState({ phase: 'ready', clip: response.play_of_the_game });
            return;
          }
          if (response.status === 'unavailable') {
            finishUnavailable('absent');
            return;
          }
          if (attempt >= HIGHLIGHT_POLL_MAX_ATTEMPTS) {
            finishUnavailable(sawNetworkFailure ? 'network' : 'absent');
            return;
          }
          timer = setTimeout(() => poll(attempt + 1), HIGHLIGHT_POLL_INTERVAL_MS);
        })
        .catch((error: unknown) => {
          if (cancelled || (error instanceof DOMException && error.name === 'AbortError')) return;
          sawNetworkFailure = true;
          if (attempt >= HIGHLIGHT_POLL_MAX_ATTEMPTS) {
            finishUnavailable('network');
            return;
          }
          timer = setTimeout(() => poll(attempt + 1), HIGHLIGHT_POLL_INTERVAL_MS);
        });
    };

    poll(1);
    return () => {
      cancelled = true;
      controller?.abort();
      if (timer !== null) clearTimeout(timer);
    };
  }, [gameId, isComplete]);

  return state;
};
