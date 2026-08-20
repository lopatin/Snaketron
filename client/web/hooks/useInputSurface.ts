import { useSyncExternalStore } from 'react';
import { crazyGames } from '../services/crazyGames';
import {
  COARSE_POINTER_QUERY,
  forcedInputSurface,
  resolveInputSurface,
  type InputSurface,
} from '../utils/inputSurface';

// One shared MediaQueryList: getSnapshot runs on every render of every
// consumer, and allocating a fresh list per call is both wasteful and against
// useSyncExternalStore's pure-snapshot expectations.
let coarsePointerMedia: MediaQueryList | null | undefined;

const coarsePointerQuery = (): MediaQueryList | null => {
  if (coarsePointerMedia === undefined) {
    coarsePointerMedia =
      typeof window === 'undefined' || typeof window.matchMedia !== 'function'
        ? null
        : window.matchMedia(COARSE_POINTER_QUERY);
  }
  return coarsePointerMedia;
};

const subscribe = (onStoreChange: () => void): (() => void) => {
  const media = coarsePointerQuery();
  media?.addEventListener('change', onStoreChange);
  const unsubscribeCrazyGames = crazyGames.subscribe(onStoreChange);
  return () => {
    media?.removeEventListener('change', onStoreChange);
    unsubscribeCrazyGames();
  };
};

const getSnapshot = (): InputSurface =>
  forcedInputSurface() ??
  resolveInputSurface(
    coarsePointerQuery()?.matches ?? false,
    crazyGames.getSnapshot(),
  );

const getServerSnapshot = (): InputSurface => 'keyboard';

/**
 * Reactive control-surface detection. Docking a tablet to a keyboard or the
 * CrazyGames SDK finishing init both re-resolve the surface live, so the
 * touch controls appear or retire without a reload.
 */
export function useInputSurface(): InputSurface {
  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
