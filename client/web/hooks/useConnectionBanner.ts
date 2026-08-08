import { useEffect, useRef, useState } from 'react';
import {
  CONNECTION_BANNER_SHOW_DELAY_MS,
  connectionBannerHideDelayMs,
} from '../utils/connectionBanner';

/**
 * Asymmetric hysteresis for the connection badge: appear only after the client
 * has been unusable for a beat, then stay up long enough to be read.
 *
 * Deliberately not `useDebouncedValue`, which is a symmetric trailing-edge
 * debounce — it would delay the badge in both directions and so keep it on
 * screen for a fixed time after every recovery, however brief the outage.
 */
export function useConnectionBanner(isReady: boolean): boolean {
  const [visible, setVisible] = useState<boolean>(false);
  const shownAtRef = useRef<number | null>(null);

  useEffect(() => {
    if (!isReady && !visible) {
      const timer = setTimeout(() => {
        shownAtRef.current = Date.now();
        setVisible(true);
      }, CONNECTION_BANNER_SHOW_DELAY_MS);
      return () => clearTimeout(timer);
    }

    if (isReady && visible) {
      const timer = setTimeout(() => {
        shownAtRef.current = null;
        setVisible(false);
      }, connectionBannerHideDelayMs(shownAtRef.current, Date.now()));
      return () => clearTimeout(timer);
    }

    return undefined;
  }, [isReady, visible]);

  return visible;
}
