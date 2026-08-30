import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { api, AUTH_TOKEN_STORAGE_KEY, isApiError } from '../services/api';
import {
  CRAZY_GAMES_PREFERENCE_KEYS,
  applyCrazyGamesPreferences,
  clearLinkedCrazyGamesPreferences,
  crazyGamesPreferenceOwner,
  markCrazyGamesPreferencesOwnedBy,
  readCrazyGamesPreferences,
} from '../services/crazyGamesPreferences';
import { gameStorage, subscribeGameStorage } from '../services/gameStorage';
import { CrazyGamesAccountException } from '../services/crazyGames';
import { useCrazyGames } from './CrazyGamesContext';
import type { Equipment } from '../types/generated';
import {
  AuthContextType,
  CrazyGamesSessionStatus,
  User,
} from '../types';

const AuthContext = createContext<AuthContextType | null>(null);
const IS_CRAZY_GAMES_BUILD = process.env.CRAZYGAMES_BUILD === 'true';
const SESSION_RENEWAL_LEAD_MS = 5 * 60 * 1000;
const MIN_RENEWAL_DELAY_MS = 30 * 1000;
const PREFERENCE_SAVE_DELAY_MS = 750;
const PREFERENCE_RETRY_MAX_MS = 30 * 1000;
const ACCOUNT_REQUEST_TIMEOUT_MS = 15 * 1000;

const withTimeout = async <T,>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
};

interface AuthProviderProps {
  children: React.ReactNode;
}

const accountErrorMessage = (error: unknown): string => {
  if (error instanceof CrazyGamesAccountException) {
    if (error.code === 'sdkUnavailable' || error.code === 'userAccountUnavailable') {
      return 'CrazyGames account services are unavailable. Retry before playing.';
    }
    if (error.code === 'invalidToken') {
      return 'CrazyGames could not verify this account. Please retry.';
    }
  }
  if (isApiError(error)) {
    if (error.response.status === 401 || error.response.status === 403) {
      return 'CrazyGames could not verify this account. Please retry.';
    }
    if (error.response.status >= 500) {
      return 'Account progress is temporarily unavailable. Retry before playing.';
    }
  }
  return 'We could not connect your CrazyGames account. Retry before playing.';
};

const isGuestLinkConsentRequired = (error: unknown): boolean => {
  if (!isApiError(error) || error.response.status !== 409) {
    return false;
  }
  const data = error.response.data;
  return Boolean(
    data &&
    typeof data === 'object' &&
    (data as Record<string, unknown>).code === 'guestLinkConsentRequired',
  );
};

const fallbackGuestNickname = (): string =>
  `CGPlayer${Math.floor(1000 + Math.random() * 9000)}`;

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const {
    getUserToken,
    showAccountLinkPrompt,
    authChangeSequence,
    accountError: crazyGamesSdkAccountError,
  } = useCrazyGames();
  const [user, setUserState] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [crazyGamesSessionStatus, setCrazyGamesSessionStatusState] =
    useState<CrazyGamesSessionStatus>(
      IS_CRAZY_GAMES_BUILD ? 'resolving' : 'not-applicable',
    );
  const [crazyGamesSessionError, setCrazyGamesSessionError] = useState<string | null>(null);
  const [sessionExpiresAt, setSessionExpiresAt] = useState<number | null>(null);
  const [initialResolutionComplete, setInitialResolutionComplete] = useState(
    !IS_CRAZY_GAMES_BUILD,
  );
  const [crazyGamesAccountTransitionSequence, setCrazyGamesAccountTransitionSequence] =
    useState(0);
  const navigate = useNavigate();
  const location = useLocation();
  const isCrazyGamesPrivacyPage =
    IS_CRAZY_GAMES_BUILD && location.pathname === '/privacy';

  const userRef = useRef<User | null>(null);
  const statusRef = useRef<CrazyGamesSessionStatus>(crazyGamesSessionStatus);
  const generationRef = useRef(0);
  const resolutionFlightRef = useRef<Promise<void> | null>(null);
  const guestFlightRef = useRef<Promise<{ user: User; token: string }> | null>(null);
  const expiresAtRef = useRef<number | null>(null);
  const renewalRetryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const preferenceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const preferenceRetryRef = useRef(0);
  const applyingPreferencesRef = useRef(false);
  const resolveAccountRef = useRef<((background?: boolean) => Promise<void>) | null>(null);
  const observedInitialAuthChangeRef = useRef(authChangeSequence);
  const preferenceSaveInFlightRef = useRef(false);
  const preferenceDirtyRef = useRef(false);
  const preferenceRevisionRef = useRef(0);

  const setUser = useCallback((next: User | null) => {
    userRef.current = next;
    setUserState(next);
  }, []);

  const setCrazyGamesSessionStatus = useCallback((next: CrazyGamesSessionStatus) => {
    statusRef.current = next;
    setCrazyGamesSessionStatusState(next);
  }, []);

  const clearRenewalRetry = useCallback(() => {
    if (renewalRetryTimerRef.current) {
      clearTimeout(renewalRetryTimerRef.current);
      renewalRetryTimerRef.current = null;
    }
  }, []);

  const becomeCrazyGamesGuest = useCallback(async (expectedGeneration: number) => {
    // A portal sign-out must never resurrect a linked account from storage.
    // A server-verified guest token may be restored for same-tab
    // continuity; every other cached identity is discarded.
    const cachedToken = api.getAuthToken();
    let cachedGuest: User | null = null;
    let shouldClearToken = false;
    let shouldClearPreferences = false;
    if (cachedToken) {
      try {
        const cachedUser = await withTimeout(
          api.getCurrentUser(),
          ACCOUNT_REQUEST_TIMEOUT_MS,
          'Guest session check',
        );
        if (cachedUser.isGuest) {
          cachedGuest = { ...cachedUser, isGuest: true };
        } else {
          shouldClearToken = true;
          shouldClearPreferences = true;
        }
      } catch (error) {
        // A timeout, offline browser, or 5xx response is not evidence that a
        // stored guest belongs to a different person. Keep its token and
        // preferences dormant, fail closed, and let Retry verify it later.
        if (
          !isApiError(error) ||
          (error.response.status !== 401 && error.response.status !== 403)
        ) {
          throw error;
        }
        shouldClearToken = true;
        // An expired guest JWT is also unauthorized. Preserve ownerless
        // browser settings; only a recorded linked owner proves the snapshot
        // must be removed before guest play.
        shouldClearPreferences = crazyGamesPreferenceOwner() !== null;
      }
    }

    if (expectedGeneration !== generationRef.current) {
      return;
    }
    clearRenewalRetry();
    expiresAtRef.current = null;
    setSessionExpiresAt(null);
    clearLinkedCrazyGamesPreferences(shouldClearPreferences);
    if (shouldClearToken) api.setAuthToken(null);
    setUser(cachedGuest);

    setCrazyGamesSessionError(null);
    setCrazyGamesSessionStatus('guest');
    setLoading(false);
    setInitialResolutionComplete(true);
  }, [clearRenewalRetry, setCrazyGamesSessionStatus, setUser]);

  const scheduleRenewalRetry = useCallback(() => {
    clearRenewalRetry();
    renewalRetryTimerRef.current = setTimeout(() => {
      renewalRetryTimerRef.current = null;
      void resolveAccountRef.current?.(true);
    }, MIN_RENEWAL_DELAY_MS);
  }, [clearRenewalRetry]);

  const eligibleGuestInitialPreferences = useCallback(() => {
    // The exchange transaction is the authoritative guest eligibility gate:
    // the backend imports this optional snapshot only for GuestClaimed and
    // ignores it for Created/Returning. Avoid a preliminary /me request that
    // could fail transiently and silently drop a valid guest's settings.
    if (crazyGamesPreferenceOwner() !== null || !api.getAuthToken()) {
      return undefined;
    }
    const preferences = readCrazyGamesPreferences();
    return Object.keys(preferences).length > 0 ? preferences : undefined;
  }, []);

  const resolveCrazyGamesAccount = useCallback((background = false): Promise<void> => {
    if (!IS_CRAZY_GAMES_BUILD) {
      return Promise.resolve();
    }
    if (resolutionFlightRef.current) {
      return resolutionFlightRef.current;
    }

    const generation = ++generationRef.current;
    if (!background) {
      setLoading(true);
      setCrazyGamesSessionStatus('resolving');
      setCrazyGamesSessionError(null);
      setUser(null);
    }

    const flight = (async () => {
      try {
        // Always ask the SDK. portalUser is intentionally not consulted:
        // getUser() is display-only and can finish after token retrieval.
        const crazyGamesToken = await withTimeout(
          getUserToken(),
          ACCOUNT_REQUEST_TIMEOUT_MS,
          'CrazyGames sign-in',
        );
        const hasAttachedInternalSession = api.getAuthToken() !== null;
        let response;
        try {
          response = await withTimeout(
            api.exchangeCrazyGamesToken(
              crazyGamesToken,
              hasAttachedInternalSession ? 'check' : 'decline',
            ),
            ACCOUNT_REQUEST_TIMEOUT_MS,
            'CrazyGames account exchange',
          );
        } catch (error) {
          if (!hasAttachedInternalSession || !isGuestLinkConsentRequired(error)) {
            throw error;
          }

          const consent = await showAccountLinkPrompt();
          if (consent === null) {
            throw new Error('CrazyGames account linking was not completed');
          }

          if (consent === 'no') {
            response = await withTimeout(
              api.exchangeCrazyGamesToken(crazyGamesToken, 'decline'),
              ACCOUNT_REQUEST_TIMEOUT_MS,
              'CrazyGames account exchange',
            );
          } else if (consent === 'yes') {
            const initialPreferences = eligibleGuestInitialPreferences();
            try {
              response = await withTimeout(
                api.exchangeCrazyGamesToken(
                  crazyGamesToken,
                  'allow',
                  initialPreferences,
                ),
                ACCOUNT_REQUEST_TIMEOUT_MS,
                'CrazyGames account exchange',
              );
            } catch (preferenceError) {
              // Optional legacy preferences must never brick a consented
              // guest claim. Retry the same credential and consent without
              // the optional snapshot when only that payload is rejected.
              if (
                !initialPreferences ||
                !isApiError(preferenceError) ||
                preferenceError.response.status !== 400
              ) {
                throw preferenceError;
              }
              response = await withTimeout(
                api.exchangeCrazyGamesToken(crazyGamesToken, 'allow'),
                ACCOUNT_REQUEST_TIMEOUT_MS,
                'CrazyGames account exchange',
              );
            }
          } else {
            throw new Error('CrazyGames returned an invalid account-link decision');
          }
        }

        if (generation !== generationRef.current) {
          return;
        }
        if (
          !response ||
          typeof response.token !== 'string' ||
          response.token.length === 0 ||
          !response.user ||
          !Number.isSafeInteger(response.user.id) ||
          response.user.authSource !== 'crazygames' ||
          !Number.isFinite(response.expiresAt) ||
          !response.preferences ||
          typeof response.preferences !== 'object' ||
          Array.isArray(response.preferences)
        ) {
          throw new Error('CrazyGames exchange returned an invalid session');
        }

        if (
          background &&
          userRef.current &&
          response.user.id !== userRef.current.id
        ) {
          // Do not replace identity inside an active game if the SDK event was
          // delayed or absent. The mounted bridge performs leave/clear/reload.
          setCrazyGamesAccountTransitionSequence((value) => value + 1);
          return;
        }

        if (!background) {
          applyingPreferencesRef.current = true;
          try {
            applyCrazyGamesPreferences(response.preferences, true);
            markCrazyGamesPreferencesOwnedBy(response.user.id);
          } finally {
            applyingPreferencesRef.current = false;
          }
        }

        // The portal token is now out of scope and is never written to any
        // browser store. Only the ordinary internal Snaketron JWT is retained.
        api.setAuthToken(response.token);
        setUser({
          ...response.user,
          isGuest: false,
          authSource: 'crazygames',
          avatarUrl: response.user.avatarUrl ?? null,
        });
        expiresAtRef.current = response.expiresAt;
        setSessionExpiresAt(response.expiresAt);
        clearRenewalRetry();
        setCrazyGamesSessionError(null);
        setCrazyGamesSessionStatus('linked');
        setLoading(false);
        setInitialResolutionComplete(true);
      } catch (error) {
        if (generation !== generationRef.current) {
          return;
        }

        if (
          error instanceof CrazyGamesAccountException &&
          (error.code === 'userNotAuthenticated' || error.code === 'userAccountUnavailable')
        ) {
          try {
            await becomeCrazyGamesGuest(generation);
          } catch (guestError) {
            if (generation !== generationRef.current) {
              return;
            }
            setUser(null);
            expiresAtRef.current = null;
            setSessionExpiresAt(null);
            setCrazyGamesSessionError(
              'We could not safely restore your saved guest session. Retry account sync.',
            );
            setCrazyGamesSessionStatus('error');
            setLoading(false);
            setInitialResolutionComplete(true);
            console.warn('CrazyGames saved guest verification failed:', guestError);
          }
          return;
        }

        const expiryMs = (expiresAtRef.current ?? 0) * 1000;
        if (background && userRef.current && expiryMs > Date.now() + MIN_RENEWAL_DELAY_MS) {
          // Keep the still-valid internal session usable and retry well before
          // expiry. Once it is no longer valid, the next attempt fails closed.
          scheduleRenewalRetry();
          return;
        }

        // Keep the cached internal token dormant while the UI is fail-closed.
        // If it is an eligible guest, a manual retry can still promote it in
        // place; if it belongs to another linked user, the server ignores it
        // as a migration candidate and the verified portal identity wins.
        setUser(null);
        expiresAtRef.current = null;
        setSessionExpiresAt(null);
        setCrazyGamesSessionError(accountErrorMessage(error));
        setCrazyGamesSessionStatus('error');
        setLoading(false);
        setInitialResolutionComplete(true);
      }
    })();

    resolutionFlightRef.current = flight;
    const clearResolutionFlight = () => {
      if (resolutionFlightRef.current === flight) {
        resolutionFlightRef.current = null;
      }
    };
    void flight.then(clearResolutionFlight, clearResolutionFlight);
    return flight;
  }, [
    becomeCrazyGamesGuest,
    clearRenewalRetry,
    eligibleGuestInitialPreferences,
    getUserToken,
    scheduleRenewalRetry,
    showAccountLinkPrompt,
    setCrazyGamesSessionStatus,
    setUser,
  ]);

  useEffect(() => {
    resolveAccountRef.current = resolveCrazyGamesAccount;
  }, [resolveCrazyGamesAccount]);

  // The multiplayer bridge is intentionally not mounted during the initial
  // account gate. If login/logout changes in that narrow window, invalidate
  // the pending result and restart so the new provider identity always wins.
  useEffect(() => {
    if (authChangeSequence <= observedInitialAuthChangeRef.current) {
      return;
    }
    observedInitialAuthChangeRef.current = authChangeSequence;
    if (!IS_CRAZY_GAMES_BUILD) {
      return;
    }
    if (!initialResolutionComplete) {
      generationRef.current += 1;
      window.location.reload();
      return;
    }
    // The bridge may not exist yet on the exact render boundary where the
    // initial exchange finishes. Record a durable provider-owned transition;
    // its observer starts at zero so an event cannot disappear during mount.
    setCrazyGamesAccountTransitionSequence((value) => value + 1);
  }, [authChangeSequence, initialResolutionComplete]);

  // Resolve the platform identity before gameplay components may connect.
  useEffect(() => {
    if (IS_CRAZY_GAMES_BUILD) {
      // A direct privacy-policy visit must remain a passive document: do not
      // retrieve a portal token or open consent UI behind it. Navigating back
      // changes this dependency and starts the ordinary account gate.
      if (isCrazyGamesPrivacyPage) {
        return;
      }
      void resolveCrazyGamesAccount();
      return;
    }

    const fetchCurrentUser = async () => {
      const token = localStorage.getItem(AUTH_TOKEN_STORAGE_KEY);
      if (!token) {
        setLoading(false);
        return;
      }
      try {
        const currentUser = await api.getCurrentUser();
        setUser(currentUser);
      } catch (error) {
        console.error('Failed to fetch current user:', error);
        const status = isApiError(error) ? error.response.status : undefined;
        const isAbortError = error instanceof Error && error.name === 'AbortError';
        if (status === 401 || status === 403) {
          api.setAuthToken(null);
        } else if (isAbortError) {
          console.debug('Fetch aborted while loading user; keeping existing auth token');
        }
      } finally {
        setLoading(false);
      }
    };
    void fetchCurrentUser();
  }, [isCrazyGamesPrivacyPage, resolveCrazyGamesAccount, setUser]);

  /**
   * Drop the keys an older build kept a second copy of the equipped skin in.
   *
   * The account is now the only store, so these are dead weight — and worse
   * than dead: a browser that last held one account's choice would otherwise
   * keep offering it to whoever signs in next. Removing them is a one-way
   * step, which is the point.
   */
  useEffect(() => {
    for (const key of ['snaketron:skin:v1', 'snaketron:base:v1']) {
      gameStorage.removeItem(key);
    }
  }, []);

  // Renew the internal session while the platform can still mint a fresh
  // CrazyGames token. Token changes for the same user preserve lobby state.
  useEffect(() => {
    if (!IS_CRAZY_GAMES_BUILD || crazyGamesSessionStatus !== 'linked' || !sessionExpiresAt) {
      return;
    }
    const delay = Math.max(
      MIN_RENEWAL_DELAY_MS,
      sessionExpiresAt * 1000 - Date.now() - SESSION_RENEWAL_LEAD_MS,
    );
    const timer = setTimeout(() => {
      void resolveCrazyGamesAccount(true);
    }, delay);
    return () => clearTimeout(timer);
  }, [crazyGamesSessionStatus, resolveCrazyGamesAccount, sessionExpiresAt]);

  const flushPreferences = useCallback(async () => {
    preferenceTimerRef.current = null;
    if (
      preferenceSaveInFlightRef.current ||
      statusRef.current !== 'linked' ||
      !userRef.current
    ) {
      return;
    }
    preferenceSaveInFlightRef.current = true;
    preferenceDirtyRef.current = false;
    const requestedRevision = preferenceRevisionRef.current;
    const requestedUserId = userRef.current.id;
    let retryDelay: number | null = null;
    try {
      // Keep the actual fetch serialized until it settles. A synthetic timeout
      // would permit an older request to commit after a newer retry and roll
      // the server back despite client-side revision checks.
      const canonical = await api.saveCrazyGamesPreferences(
        readCrazyGamesPreferences(),
      );
      if (
        statusRef.current !== 'linked' ||
        userRef.current?.id !== requestedUserId
      ) {
        return;
      }
      preferenceRetryRef.current = 0;
      // Never let an older response overwrite a browser edit that happened
      // while this PUT was in flight. The dirty snapshot is sent next.
      if (
        requestedRevision === preferenceRevisionRef.current &&
        !preferenceDirtyRef.current
      ) {
        applyingPreferencesRef.current = true;
        try {
          applyCrazyGamesPreferences(canonical, true);
        } finally {
          applyingPreferencesRef.current = false;
        }
      }
    } catch (error) {
      if (
        statusRef.current !== 'linked' ||
        userRef.current?.id !== requestedUserId
      ) {
        return;
      }
      console.warn('CrazyGames preference save will retry:', error);
      preferenceDirtyRef.current = true;
      preferenceRetryRef.current += 1;
      retryDelay = Math.min(
        PREFERENCE_RETRY_MAX_MS,
        1000 * 2 ** Math.min(preferenceRetryRef.current, 5),
      );
    } finally {
      preferenceSaveInFlightRef.current = false;
      if (
        preferenceDirtyRef.current &&
        statusRef.current === 'linked' &&
        userRef.current?.id === requestedUserId
      ) {
        if (preferenceTimerRef.current) {
          clearTimeout(preferenceTimerRef.current);
        }
        preferenceTimerRef.current = setTimeout(() => {
          void flushPreferences();
        }, retryDelay ?? PREFERENCE_SAVE_DELAY_MS);
      }
    }
  }, []);

  useEffect(() => subscribeGameStorage((key) => {
    if (
      !IS_CRAZY_GAMES_BUILD ||
      statusRef.current !== 'linked' ||
      applyingPreferencesRef.current ||
      !CRAZY_GAMES_PREFERENCE_KEYS.has(key)
    ) {
      return;
    }
    preferenceDirtyRef.current = true;
    preferenceRevisionRef.current += 1;
    preferenceRetryRef.current = 0;
    if (preferenceSaveInFlightRef.current) {
      return;
    }
    if (preferenceTimerRef.current) {
      clearTimeout(preferenceTimerRef.current);
    }
    preferenceTimerRef.current = setTimeout(() => {
      void flushPreferences();
    }, PREFERENCE_SAVE_DELAY_MS);
  }), [flushPreferences]);

  useEffect(() => () => {
    clearRenewalRetry();
    if (preferenceTimerRef.current) {
      clearTimeout(preferenceTimerRef.current);
    }
  }, [clearRenewalRetry]);

  const login = useCallback(async (username: string, password: string) => {
    const data = await api.login(username, password);
    setUser(data.user);
  }, [setUser]);

  const register = useCallback(async (username: string, password: string | null) => {
    const data = await api.register(username, password || '');
    setUser(data.user);
  }, [setUser]);

  const createGuestSession = useCallback(async (nickname: string) => {
    const generation = generationRef.current;
    // Do not time out a state-creating request without abort/idempotency: its
    // late success would orphan one guest while a retry created another. This
    // also preserves the original website/itch behavior exactly.
    const data = await api.createGuest(nickname);
    if (
      IS_CRAZY_GAMES_BUILD &&
      (generation !== generationRef.current || statusRef.current !== 'guest')
    ) {
      throw new Error('CrazyGames identity changed while creating a guest session');
    }
    api.setAuthToken(data.token);
    const guestUser: User = { ...data.user, isGuest: true };
    setUser(guestUser);
    try {
      gameStorage.setItem('savedUsername', guestUser.username);
    } catch {
      // Storage is optional for guest display-name convenience.
    }
    return { user: guestUser, token: data.token };
  }, [setUser]);

  const ensurePlayableSession = useCallback(async (nickname?: string) => {
    if (IS_CRAZY_GAMES_BUILD) {
      const resolving = resolutionFlightRef.current;
      if (resolving) {
        await resolving;
      }
      if (statusRef.current === 'resolving') {
        throw new Error('CrazyGames account is still connecting.');
      }
      if (statusRef.current === 'error') {
        throw new Error('CrazyGames account progress is unavailable. Retry account sync first.');
      }
    }

    const currentUser = userRef.current;
    const currentToken = api.getAuthToken();
    if (currentUser && currentToken) {
      return { user: currentUser, token: currentToken };
    }

    if (IS_CRAZY_GAMES_BUILD && statusRef.current !== 'guest') {
      throw new Error('CrazyGames account is not ready for gameplay.');
    }
    if (guestFlightRef.current) {
      return guestFlightRef.current;
    }

    const normalizedNickname = nickname?.trim();
    const flight = createGuestSession(
      normalizedNickname && normalizedNickname.length >= 3
        ? normalizedNickname
        : fallbackGuestNickname(),
    );
    guestFlightRef.current = flight;
    const clearGuestFlight = () => {
      if (guestFlightRef.current === flight) {
        guestFlightRef.current = null;
      }
    };
    void flight.then(clearGuestFlight, clearGuestFlight);
    return flight;
  }, [createGuestSession]);

  const createGuest = useCallback(async (nickname: string) => {
    if (IS_CRAZY_GAMES_BUILD) {
      return ensurePlayableSession(nickname);
    }
    return createGuestSession(nickname);
  }, [createGuestSession, ensurePlayableSession]);

  /**
   * Record what the server now says this account is wearing.
   *
   * The account record is the only store for equipment, so the copy of it
   * held here is what the Skins page badges and the arena's dressing both
   * read. Updating it in place — rather than re-fetching the account — is
   * what makes an equip visible immediately without any second store to keep
   * in sync. Modelled on `updateGuestNickname`: nothing downstream keys an
   * effect on the whole `user` object where a refetch would be triggered.
   */
  const applyEquipment = useCallback((equipment: Equipment) => {
    setUserState((previous) => {
      if (!previous) {
        return previous;
      }
      const next = {
        ...previous,
        selectedSkin: equipment.selectedSkin,
        selectedBase: equipment.selectedBase,
      };
      userRef.current = next;
      return next;
    });
  }, []);

  const updateGuestNickname = useCallback((nickname: string) => {
    setUserState((previous) => {
      if (!previous) return previous;
      const next = { ...previous, username: nickname };
      userRef.current = next;
      return next;
    });
    try {
      gameStorage.setItem('savedUsername', nickname);
    } catch {
      // ignore storage errors
    }
  }, []);

  const logout = useCallback(() => {
    api.setAuthToken(null);
    setUser(null);
    if (IS_CRAZY_GAMES_BUILD) {
      setCrazyGamesSessionStatus('guest');
    }
    navigate('/');
  }, [navigate, setCrazyGamesSessionStatus, setUser]);

  const getToken = useCallback((): string | null => api.getAuthToken(), []);

  const retryCrazyGamesSession = useCallback(async () => {
    if (crazyGamesSdkAccountError?.code === 'sdkUnavailable') {
      // SDK bootstrap is deliberately single-shot so a timed-out init cannot
      // later register duplicate listeners. A full reload is the only safe
      // retry; the tab-scoped internal session survives it.
      window.location.reload();
      return;
    }
    await resolveCrazyGamesAccount(false);
  }, [crazyGamesSdkAccountError?.code, resolveCrazyGamesAccount]);

  const beginCrazyGamesAccountTransition = useCallback(() => {
    if (!IS_CRAZY_GAMES_BUILD) return;
    generationRef.current += 1;
    clearRenewalRetry();
    setLoading(true);
    setCrazyGamesSessionError(null);
    setCrazyGamesSessionStatus('resolving');
  }, [clearRenewalRetry, setCrazyGamesSessionStatus]);

  const value: AuthContextType = {
    user,
    loading,
    login,
    register,
    createGuest,
    ensurePlayableSession,
    updateGuestNickname,
    applyEquipment,
    logout,
    getToken,
    crazyGamesSessionStatus,
    crazyGamesSessionError,
    retryCrazyGamesSession,
    beginCrazyGamesAccountTransition,
    crazyGamesAccountTransitionSequence,
  };

  return (
    <AuthContext.Provider value={value}>
      {IS_CRAZY_GAMES_BUILD && !initialResolutionComplete && !isCrazyGamesPrivacyPage ? (
        <main className="min-h-screen flex items-center justify-center px-6" aria-busy="true">
          <div className="max-w-md border-2 border-black bg-white p-8 text-center shadow-[8px_8px_0_#000]">
            <div
              className="mx-auto mb-4 h-7 w-7 animate-spin rounded-full border-4 border-black/20 border-t-black"
              aria-hidden="true"
            />
            <h1 className="text-xl font-black uppercase tracking-1">Connecting your account</h1>
            <p className="mt-3 text-sm text-gray-600">
              Restoring your Snaketron progress from CrazyGames…
            </p>
          </div>
        </main>
      ) : children}
    </AuthContext.Provider>
  );
};
