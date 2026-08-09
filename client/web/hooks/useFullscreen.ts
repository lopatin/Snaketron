import { useSyncExternalStore } from 'react';
import {
  isFullscreenActive,
  isFullscreenSupported,
  subscribeToFullscreenChanges,
  toggleFullscreen,
} from '../utils/fullscreen';

export interface FullscreenState {
  /** Whether the platform can enter fullscreen at all (iPhones cannot). */
  supported: boolean;
  active: boolean;
  toggle: () => void;
}

const getSnapshot = (): boolean => isFullscreenActive();
const getServerSnapshot = (): boolean => false;

export function useFullscreen(): FullscreenState {
  const active = useSyncExternalStore(
    subscribeToFullscreenChanges,
    getSnapshot,
    getServerSnapshot,
  );
  return {
    supported: isFullscreenSupported(),
    active,
    toggle: () => {
      void toggleFullscreen();
    },
  };
}
