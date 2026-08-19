/**
 * A one-shot message handed to the home screen by whatever redirected there.
 *
 * Carried in react-router's `location.state` rather than a query parameter, so
 * it never survives a reload, a copy-pasted URL, or a share. The home screen
 * consumes it on mount and clears it, which means a refresh shows a clean
 * page rather than re-announcing something that already happened.
 */
export interface HomeNotice {
  /** Human-readable sentence, already resolved — no ids, no codes. */
  message: string;
  /**
   * Drives presentation only. `info` for ordinary outcomes the visitor asked
   * for, `error` for a request that could not be honored.
   */
  tone: 'info' | 'error';
}

/** Router state shape written by redirects that want to explain themselves. */
export interface HomeNoticeLocationState {
  homeNotice?: HomeNotice;
}

/**
 * Read a notice out of opaque router state.
 *
 * `location.state` is `unknown` and can hold anything a previous navigation
 * put there (`ProtectedRoute` writes `{ from }`), so this validates rather
 * than casts.
 */
export function readHomeNotice(state: unknown): HomeNotice | null {
  if (typeof state !== 'object' || state === null) {
    return null;
  }

  const candidate = (state as HomeNoticeLocationState).homeNotice;
  if (typeof candidate !== 'object' || candidate === null) {
    return null;
  }

  const { message, tone } = candidate as Partial<HomeNotice>;
  if (typeof message !== 'string' || message.trim().length === 0) {
    return null;
  }

  return {
    message,
    tone: tone === 'error' ? 'error' : 'info',
  };
}

/** Build the router state for `navigate('/', homeNoticeState(...))`. */
export function homeNoticeState(
  message: string,
  tone: HomeNotice['tone'] = 'info',
): { state: HomeNoticeLocationState; replace: true } {
  return { state: { homeNotice: { message, tone } }, replace: true };
}
