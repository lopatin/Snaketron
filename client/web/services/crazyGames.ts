export type CrazyGamesEnvironment = 'local' | 'crazygames' | 'disabled';
export type CrazyGamesAdState = 'idle' | 'requesting' | 'playing';
export type CrazyGamesAdType = 'midgame' | 'rewarded';
export type CrazyGamesAccountStatus =
  | 'disabled'
  | 'checking'
  | 'authenticated'
  | 'signed-out'
  | 'unavailable'
  | 'error';
export type CrazyGamesAccountErrorCode =
  | 'userNotAuthenticated'
  | 'userAccountUnavailable'
  | 'sdkUnavailable'
  | 'invalidToken'
  | 'unknown';

export interface CrazyGamesAccountError {
  code: CrazyGamesAccountErrorCode;
  message: string;
}

export class CrazyGamesAccountException extends Error {
  readonly code: CrazyGamesAccountErrorCode;

  constructor(error: CrazyGamesAccountError) {
    super(error.message);
    this.name = 'CrazyGamesAccountException';
    this.code = error.code;
  }
}

export interface CrazyGamesPortalUser {
  __dangerousUserId: string;
  username: string;
  profilePictureUrl: string;
}

export interface CrazyGamesFriend {
  id: string;
  username: string;
  profilePictureUrl: string;
}

export interface CrazyGamesFriendsPage {
  friends: CrazyGamesFriend[];
  page: number;
  size: number;
  hasMore: boolean;
  total: number;
}

export interface CrazyGamesSystemInfo {
  countryCode?: string;
  locale?: string;
  device?: { type?: 'desktop' | 'tablet' | 'mobile' };
  os?: { name?: string; version?: string };
  browser?: { name?: string; version?: string };
  applicationType?: 'google_play_store' | 'apple_store' | 'pwa' | 'web' | string;
}

export interface CrazyGamesGameSettings {
  disableChat: boolean;
  muteAudio: boolean;
}

export type CrazyGamesInviteParams = Record<string, string>;

export interface CrazyGamesRoomUpdate {
  roomId?: string;
  isJoinable?: boolean;
  inviteParams?: CrazyGamesInviteParams;
}

export interface CrazyGamesDataModule {
  clear(): void;
  getItem(key: string): string | null;
  removeItem(key: string): void;
  setItem(key: string, value: string): void;
}

interface CrazyGamesAdError {
  code?: string;
  message?: string;
}

interface CrazyGamesSdk {
  init(): Promise<void>;
  environment?: string;
  ad: {
    requestAd(
      type: CrazyGamesAdType,
      callbacks: {
        adStarted: () => void;
        adFinished: () => void;
        adError: (error: CrazyGamesAdError) => void;
      },
    ): void;
    hasAdblock(): Promise<boolean>;
  };
  banner: {
    requestBanner(input: { id: string; width: number; height: number }): Promise<void>;
    requestResponsiveBanner(containerId: string): Promise<void>;
    clearBanner(containerId: string): void;
    clearAllBanners(): void;
  };
  game: {
    settings?: Partial<CrazyGamesGameSettings>;
    isInstantMultiplayer?: boolean;
    inviteParams?: CrazyGamesInviteParams | null;
    addSettingsChangeListener(listener: (settings: Partial<CrazyGamesGameSettings>) => void): void;
    removeSettingsChangeListener(listener: (settings: Partial<CrazyGamesGameSettings>) => void): void;
    addJoinRoomListener(listener: (params: CrazyGamesInviteParams) => void): void;
    removeJoinRoomListener(listener: (params: CrazyGamesInviteParams) => void): void;
    gameplayStart(): void;
    gameplayStop(): void;
    loadingStart(): void;
    loadingStop(): void;
    happytime(): void;
    reportGameCompletedPercentage(value: number): void;
    setGameContext(context: Record<string, string | number | boolean>): void;
    clearGameContext(): void;
    updateRoom(update: CrazyGamesRoomUpdate): void;
    leftRoom(): void;
    inviteLink(params: CrazyGamesInviteParams): string;
    getInviteParam(name: string): string | null;
  };
  user: {
    isUserAccountAvailable?: boolean;
    systemInfo?: CrazyGamesSystemInfo;
    getUser(): Promise<CrazyGamesPortalUser | null>;
    getUserToken(): Promise<string>;
    listFriends(input: { page: number; size: number }): Promise<CrazyGamesFriendsPage>;
    showAuthPrompt(): Promise<CrazyGamesPortalUser>;
    showAccountLinkPrompt(): Promise<{ response: 'yes' | 'no' }>;
    addAuthListener(listener: (user: CrazyGamesPortalUser | null) => void): void;
    removeAuthListener(listener: (user: CrazyGamesPortalUser | null) => void): void;
  };
  data: CrazyGamesDataModule;
}

interface CrazyGamesWindow extends Window {
  CrazyGames?: { SDK?: CrazyGamesSdk };
}

export interface CrazyGamesSnapshot {
  isCrazyGamesBuild: boolean;
  initialized: boolean;
  available: boolean;
  environment: CrazyGamesEnvironment;
  settings: CrazyGamesGameSettings;
  portalUser: CrazyGamesPortalUser | null;
  userAccountAvailable: boolean;
  accountStatus: CrazyGamesAccountStatus;
  accountError: CrazyGamesAccountError | null;
  authChangeSequence: number;
  systemInfo: CrazyGamesSystemInfo | null;
  isInstantMultiplayer: boolean;
  inviteParams: CrazyGamesInviteParams | null;
  inviteSequence: number;
  hasAdblock: boolean | null;
  adState: CrazyGamesAdState;
  lastAdError: CrazyGamesAdError | null;
  adsEnabled: boolean;
  dataEnabled: boolean;
  iapEnabled: false;
  leaderboardsEnabled: false;
  initializationError: string | null;
}

export interface CrazyGamesAdResult {
  status: 'finished' | 'error' | 'disabled';
  error?: CrazyGamesAdError;
}

const IS_CRAZY_GAMES_BUILD = process.env.CRAZYGAMES_BUILD === 'true';
const ARE_CRAZY_GAMES_ADS_ENABLED = process.env.CRAZYGAMES_ADS_ENABLED === 'true';
const IS_CRAZY_GAMES_DATA_ENABLED = process.env.CRAZYGAMES_DATA_ENABLED === 'true';
const SDK_INIT_TIMEOUT_MS = 15_000;
const AD_REQUEST_TIMEOUT_MS = 15_000;
const PROFILE_LOOKUP_DELAY_MS = 300;

const DEFAULT_SETTINGS: CrazyGamesGameSettings = {
  disableChat: false,
  muteAudio: false,
};

const normalizeEnvironment = (environment: string | undefined): CrazyGamesEnvironment => {
  if (environment === 'local' || environment === 'crazygames') {
    return environment;
  }
  return 'disabled';
};

const errorMessage = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }
  if (error && typeof error === 'object' && 'message' in error) {
    return String((error as { message?: unknown }).message ?? 'Unknown CrazyGames SDK error');
  }
  return String(error ?? 'Unknown CrazyGames SDK error');
};

const errorCode = (error: unknown): string => {
  if (error && typeof error === 'object' && 'code' in error) {
    return String((error as { code?: unknown }).code ?? '');
  }
  return '';
};

/** Normalize the SDK's Error/string/object variants without exposing them to callers. */
export const normalizeCrazyGamesAccountError = (error: unknown): CrazyGamesAccountError => {
  const message = errorMessage(error);
  const normalized = `${errorCode(error)} ${message}`.replace(/[\s_-]+/g, '').toLowerCase();

  if (normalized.includes('usernotauthenticated') || normalized.includes('notauthenticated')) {
    return { code: 'userNotAuthenticated', message };
  }
  if (normalized.includes('useraccountunavailable') || normalized.includes('accountunavailable')) {
    return { code: 'userAccountUnavailable', message };
  }
  if (normalized.includes('sdknotavailable') || normalized.includes('sdkunavailable')) {
    return { code: 'sdkUnavailable', message };
  }
  if (normalized.includes('invalidtoken') || normalized.includes('emptytoken')) {
    return { code: 'invalidToken', message };
  }
  return { code: 'unknown', message };
};

const normalizedSettings = (
  settings: Partial<CrazyGamesGameSettings> | undefined,
): CrazyGamesGameSettings => ({
  disableChat: settings?.disableChat === true,
  muteAudio: settings?.muteAudio === true,
});

/**
 * The CrazyGames SDK is deliberately isolated behind this adapter. Cosmetic,
 * ad, and room calls fail open. Account-token failures remain typed so the
 * authentication layer can avoid restoring the wrong player's cached session.
 */
class CrazyGamesService {
  private sdk: CrazyGamesSdk | null = null;
  private initialization: Promise<CrazyGamesSnapshot> | null = null;
  private listeners = new Set<(snapshot: CrazyGamesSnapshot) => void>();
  private gameplayActive = false;
  private loadingActive = false;
  private profileLookupScheduled = false;
  private accountLinkPromptActive = false;
  private userTokenFlight: Promise<string> | null = null;

  private snapshot: CrazyGamesSnapshot = {
    isCrazyGamesBuild: IS_CRAZY_GAMES_BUILD,
    initialized: !IS_CRAZY_GAMES_BUILD,
    available: false,
    environment: 'disabled',
    settings: DEFAULT_SETTINGS,
    portalUser: null,
    userAccountAvailable: false,
    accountStatus: IS_CRAZY_GAMES_BUILD ? 'checking' : 'disabled',
    accountError: null,
    authChangeSequence: 0,
    systemInfo: null,
    isInstantMultiplayer: false,
    inviteParams: null,
    inviteSequence: 0,
    hasAdblock: null,
    adState: 'idle',
    lastAdError: null,
    adsEnabled: IS_CRAZY_GAMES_BUILD && ARE_CRAZY_GAMES_ADS_ENABLED,
    dataEnabled: IS_CRAZY_GAMES_BUILD && IS_CRAZY_GAMES_DATA_ENABLED,
    iapEnabled: false,
    leaderboardsEnabled: false,
    initializationError: null,
  };

  private readonly settingsListener = (settings: Partial<CrazyGamesGameSettings>) => {
    this.updateSnapshot({ settings: normalizedSettings(settings) });
  };

  private readonly authListener = (user: CrazyGamesPortalUser | null) => {
    this.publishAuthChange(user);
  };

  private readonly roomJoinListener = (params: CrazyGamesInviteParams) => {
    this.publishInvite(params);
  };

  getSnapshot = (): CrazyGamesSnapshot => this.snapshot;

  subscribe = (listener: (snapshot: CrazyGamesSnapshot) => void): (() => void) => {
    this.listeners.add(listener);
    listener(this.snapshot);
    return () => this.listeners.delete(listener);
  };

  private updateSnapshot(update: Partial<CrazyGamesSnapshot>): void {
    this.snapshot = { ...this.snapshot, ...update };
    for (const listener of this.listeners) {
      listener(this.snapshot);
    }
  }

  private publishInvite(params: CrazyGamesInviteParams | null): void {
    if (!params) {
      return;
    }
    this.updateSnapshot({
      inviteParams: { ...params },
      inviteSequence: this.snapshot.inviteSequence + 1,
    });
  }

  private publishAuthChange(user: CrazyGamesPortalUser | null): void {
    this.updateSnapshot({
      portalUser: user,
      accountStatus: user ? 'authenticated' : 'signed-out',
      accountError: user ? null : {
        code: 'userNotAuthenticated',
        message: 'CrazyGames user signed out',
      },
      authChangeSequence: this.snapshot.authChangeSequence + 1,
    });
  }

  private schedulePortalProfileLookup(): void {
    const sdk = this.sdk;
    if (
      this.profileLookupScheduled ||
      !sdk ||
      !this.snapshot.userAccountAvailable ||
      this.snapshot.portalUser
    ) {
      return;
    }
    this.profileLookupScheduled = true;
    setTimeout(() => {
      // Account-token retrieval is the security-critical User-module call.
      // Only start this cosmetic lookup after it has resolved, never beside
      // it, since the portal may rate-limit or serialize User-module calls.
      if (this.sdk !== sdk || !this.canUseSdk(sdk) || this.snapshot.portalUser) {
        return;
      }
      if (this.accountLinkPromptActive) {
        this.profileLookupScheduled = false;
        this.schedulePortalProfileLookup();
        return;
      }
      void sdk.user.getUser()
        .then((portalUser) => {
          if (this.sdk === sdk && portalUser) {
            this.updateSnapshot({ portalUser });
          }
        })
        .catch((error) => {
          console.info('CrazyGames user profile unavailable:', errorMessage(error));
        });
    }, PROFILE_LOOKUP_DELAY_MS);
  }

  private canUseSdk(sdk: CrazyGamesSdk | null): sdk is CrazyGamesSdk {
    return Boolean(sdk && this.snapshot.available);
  }

  init = async (): Promise<CrazyGamesSnapshot> => {
    if (this.initialization) {
      return this.initialization;
    }

    this.initialization = this.initializeOnce();
    return this.initialization;
  };

  private async initializeOnce(): Promise<CrazyGamesSnapshot> {
    if (!IS_CRAZY_GAMES_BUILD || typeof window === 'undefined') {
      return this.snapshot;
    }

    const sdk = (window as CrazyGamesWindow).CrazyGames?.SDK;
    if (!sdk) {
      this.updateSnapshot({
        initialized: true,
        initializationError: 'CrazyGames SDK script was not available',
        accountStatus: 'unavailable',
        accountError: {
          code: 'sdkUnavailable',
          message: 'CrazyGames SDK script was not available',
        },
      });
      return this.snapshot;
    }

    try {
      let initTimeout: ReturnType<typeof setTimeout> | null = null;
      await Promise.race([
        sdk.init(),
        new Promise<void>((_resolve, reject) => {
          initTimeout = setTimeout(
            () => reject(new Error('CrazyGames SDK initialization timed out')),
            SDK_INIT_TIMEOUT_MS,
          );
        }),
      ]).finally(() => {
        if (initTimeout) {
          clearTimeout(initTimeout);
        }
      });
      this.sdk = sdk;
      const environment = normalizeEnvironment(sdk.environment);
      const available = environment !== 'disabled';
      this.updateSnapshot({
        initialized: true,
        available,
        environment,
        settings: available ? normalizedSettings(sdk.game.settings) : DEFAULT_SETTINGS,
        userAccountAvailable: available && sdk.user.isUserAccountAvailable === true,
        accountStatus: available
          ? sdk.user.isUserAccountAvailable === true ? 'checking' : 'unavailable'
          : 'unavailable',
        accountError: available && sdk.user.isUserAccountAvailable === true
          ? null
          : {
              // A loaded SDK can deliberately report a disabled environment
              // on affiliate/non-CrazyGames embeds. That is a supported
              // guest-only state, not a failed SDK bootstrap.
              code: 'userAccountUnavailable',
              message: available
                ? 'CrazyGames user accounts are unavailable in this embed'
                : 'CrazyGames account services are unavailable in this embed',
            },
        systemInfo: available ? sdk.user.systemInfo ?? null : null,
        isInstantMultiplayer: available && sdk.game.isInstantMultiplayer === true,
        initializationError: null,
      });

      if (!available) {
        return this.snapshot;
      }

      sdk.game.addSettingsChangeListener(this.settingsListener);
      sdk.game.addJoinRoomListener(this.roomJoinListener);
      sdk.user.addAuthListener(this.authListener);
      this.publishInvite(sdk.game.inviteParams ?? null);

      void sdk.ad.hasAdblock()
        .then((hasAdblock) => this.updateSnapshot({ hasAdblock }))
        .catch((error) => {
          console.info('CrazyGames adblock detection unavailable:', errorMessage(error));
        });

      return this.snapshot;
    } catch (error) {
      console.error('CrazyGames SDK initialization failed:', error);
      this.sdk = null;
      this.updateSnapshot({
        initialized: true,
        available: false,
        environment: 'disabled',
        initializationError: errorMessage(error),
        accountStatus: 'error',
        accountError: normalizeCrazyGamesAccountError(error),
      });
      return this.snapshot;
    }
  }

  getDataModule = (): CrazyGamesDataModule | null => {
    if (!this.snapshot.dataEnabled || !this.canUseSdk(this.sdk)) {
      return null;
    }
    return this.sdk.data;
  };

  loadingStart = (): void => {
    if (!this.canUseSdk(this.sdk) || this.loadingActive) {
      return;
    }
    try {
      this.sdk.game.loadingStart();
      this.loadingActive = true;
    } catch (error) {
      console.info('CrazyGames loadingStart unavailable:', errorMessage(error));
    }
  };

  loadingStop = (): void => {
    if (!this.canUseSdk(this.sdk) || !this.loadingActive) {
      return;
    }
    try {
      this.sdk.game.loadingStop();
    } catch (error) {
      console.info('CrazyGames loadingStop unavailable:', errorMessage(error));
    } finally {
      this.loadingActive = false;
    }
  };

  gameplayStart = (): void => {
    if (!this.canUseSdk(this.sdk) || this.gameplayActive) {
      return;
    }
    try {
      this.sdk.game.gameplayStart();
      this.gameplayActive = true;
    } catch (error) {
      console.info('CrazyGames gameplayStart unavailable:', errorMessage(error));
    }
  };

  gameplayStop = (): void => {
    if (!this.canUseSdk(this.sdk) || !this.gameplayActive) {
      return;
    }
    try {
      this.sdk.game.gameplayStop();
    } catch (error) {
      console.info('CrazyGames gameplayStop unavailable:', errorMessage(error));
    } finally {
      this.gameplayActive = false;
    }
  };

  setGameContext = (context: Record<string, string | number | boolean>): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.game.setGameContext(context);
    } catch (error) {
      console.info('CrazyGames game context unavailable:', errorMessage(error));
    }
  };

  clearGameContext = (): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.game.clearGameContext();
    } catch (error) {
      console.info('CrazyGames clear game context unavailable:', errorMessage(error));
    }
  };

  happyTime = (): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.game.happytime();
    } catch (error) {
      console.info('CrazyGames happytime unavailable:', errorMessage(error));
    }
  };

  reportGameCompletedPercentage = (value: number): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.game.reportGameCompletedPercentage(Math.max(0, Math.min(100, value)));
    } catch (error) {
      console.info('CrazyGames completion reporting unavailable:', errorMessage(error));
    }
  };

  updateRoom = (update: CrazyGamesRoomUpdate): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.game.updateRoom(update);
    } catch (error) {
      console.info('CrazyGames room update unavailable:', errorMessage(error));
    }
  };

  leftRoom = (): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.game.leftRoom();
    } catch (error) {
      console.info('CrazyGames room teardown unavailable:', errorMessage(error));
    }
  };

  inviteLink = (params: CrazyGamesInviteParams): string | null => {
    if (!this.canUseSdk(this.sdk)) {
      return null;
    }
    try {
      return this.sdk.game.inviteLink(params);
    } catch (error) {
      console.info('CrazyGames invite link unavailable:', errorMessage(error));
      return null;
    }
  };

  showAuthPrompt = async (): Promise<CrazyGamesPortalUser | null> => {
    if (!this.canUseSdk(this.sdk) || !this.snapshot.userAccountAvailable) {
      return null;
    }
    try {
      const user = await this.sdk.user.showAuthPrompt();
      // Some SDK environments emit the auth listener and some only resolve
      // the prompt. Publishing here makes both paths observable; consumers
      // collapse duplicate events into one hard reload.
      this.publishAuthChange(user);
      return user;
    } catch (error) {
      const message = errorMessage(error);
      if (!message.toLowerCase().includes('cancel')) {
        console.info('CrazyGames authentication prompt unavailable:', message);
      }
      return null;
    }
  };

  getUserToken = (): Promise<string> => {
    if (this.userTokenFlight) {
      return this.userTokenFlight;
    }
    const flight = this.requestUserToken();
    this.userTokenFlight = flight;
    const clearFlight = () => {
      if (this.userTokenFlight === flight) {
        this.userTokenFlight = null;
      }
    };
    // Clear only when the actual SDK promise settles. Auth-layer timeouts may
    // stop waiting, but Retry must never start a concurrent User-module call.
    void flight.then(clearFlight, clearFlight);
    return flight;
  };

  private async requestUserToken(): Promise<string> {
    if (!this.canUseSdk(this.sdk)) {
      const initializedGuestOnlyEmbed = Boolean(
        this.sdk &&
        this.snapshot.initialized &&
        this.snapshot.initializationError === null &&
        this.snapshot.environment === 'disabled',
      );
      const error: CrazyGamesAccountError = {
        code: initializedGuestOnlyEmbed ? 'userAccountUnavailable' : 'sdkUnavailable',
        message: initializedGuestOnlyEmbed
          ? 'CrazyGames account services are unavailable in this embed'
          : this.snapshot.initializationError ?? 'CrazyGames SDK is unavailable',
      };
      this.updateSnapshot({ accountStatus: 'unavailable', accountError: error });
      throw new CrazyGamesAccountException(error);
    }
    if (!this.snapshot.userAccountAvailable) {
      const error: CrazyGamesAccountError = {
        code: 'userAccountUnavailable',
        message: 'CrazyGames user accounts are unavailable in this embed',
      };
      this.updateSnapshot({ accountStatus: 'unavailable', accountError: error });
      throw new CrazyGamesAccountException(error);
    }
    this.updateSnapshot({ accountStatus: 'checking', accountError: null });
    try {
      const token = await this.sdk.user.getUserToken();
      if (typeof token !== 'string' || token.trim() === '') {
        throw new CrazyGamesAccountException({
          code: 'invalidToken',
          message: 'CrazyGames returned an empty user token',
        });
      }
      this.updateSnapshot({ accountStatus: 'authenticated', accountError: null });
      this.schedulePortalProfileLookup();
      return token;
    } catch (error) {
      const normalized = error instanceof CrazyGamesAccountException
        ? { code: error.code, message: error.message }
        : normalizeCrazyGamesAccountError(error);
      const accountStatus: CrazyGamesAccountStatus = normalized.code === 'userNotAuthenticated'
        ? 'signed-out'
        : normalized.code === 'userAccountUnavailable' || normalized.code === 'sdkUnavailable'
          ? 'unavailable'
          : 'error';
      this.updateSnapshot({ accountStatus, accountError: normalized });
      if (normalized.code !== 'userNotAuthenticated') {
        console.info('CrazyGames user token unavailable:', normalized.message);
      }
      throw new CrazyGamesAccountException(normalized);
    }
  }

  listFriends = async (page = 1, size = 50): Promise<CrazyGamesFriendsPage | null> => {
    if (!this.canUseSdk(this.sdk) || !this.snapshot.portalUser) {
      return null;
    }
    try {
      return await this.sdk.user.listFriends({
        page: Math.max(1, Math.trunc(page)),
        size: Math.max(1, Math.min(50, Math.trunc(size))),
      });
    } catch (error) {
      console.info('CrazyGames friends unavailable:', errorMessage(error));
      return null;
    }
  };

  showAccountLinkPrompt = async (): Promise<'yes' | 'no' | null> => {
    if (!this.canUseSdk(this.sdk) || !this.snapshot.userAccountAvailable) {
      return null;
    }
    this.accountLinkPromptActive = true;
    try {
      const response = (await this.sdk.user.showAccountLinkPrompt()).response;
      return response === 'yes' || response === 'no' ? response : null;
    } catch (error) {
      console.info('CrazyGames account link prompt unavailable:', errorMessage(error));
      return null;
    } finally {
      this.accountLinkPromptActive = false;
    }
  };

  requestAd = (type: CrazyGamesAdType): Promise<CrazyGamesAdResult> => {
    const sdk = this.sdk;
    if (!this.snapshot.adsEnabled || !this.canUseSdk(sdk)) {
      return Promise.resolve({ status: 'disabled' });
    }
    if (this.snapshot.adState !== 'idle') {
      return Promise.resolve({
        status: 'error',
        error: { code: 'requestInProgress', message: 'An ad request is already active' },
      });
    }

    this.updateSnapshot({ adState: 'requesting', lastAdError: null });
    return new Promise<CrazyGamesAdResult>((resolve) => {
      let settled = false;
      let requestTimeout: ReturnType<typeof setTimeout> | null = null;
      const finish = (result: CrazyGamesAdResult) => {
        if (settled) {
          return;
        }
        settled = true;
        if (requestTimeout) {
          clearTimeout(requestTimeout);
          requestTimeout = null;
        }
        this.updateSnapshot({
          adState: 'idle',
          lastAdError: result.status === 'error' ? result.error ?? null : null,
        });
        resolve(result);
      };

      requestTimeout = setTimeout(() => finish({
        status: 'error',
        error: { code: 'requestTimeout', message: 'Advertisement request timed out' },
      }), AD_REQUEST_TIMEOUT_MS);

      try {
        sdk.ad.requestAd(type, {
          adStarted: () => {
            if (!settled) {
              if (requestTimeout) {
                clearTimeout(requestTimeout);
                requestTimeout = null;
              }
              this.gameplayStop();
              this.updateSnapshot({ adState: 'playing' });
            }
          },
          adFinished: () => finish({ status: 'finished' }),
          adError: (error) => finish({ status: 'error', error }),
        });
      } catch (error) {
        finish({
          status: 'error',
          error: { code: 'other', message: errorMessage(error) },
        });
      }
    });
  };

  requestBanner = async (input: { id: string; width: number; height: number }): Promise<boolean> => {
    if (!this.snapshot.adsEnabled || !this.canUseSdk(this.sdk)) {
      return false;
    }
    try {
      await this.sdk.banner.requestBanner(input);
      return true;
    } catch (error) {
      console.info('CrazyGames banner unavailable:', errorMessage(error));
      return false;
    }
  };

  requestResponsiveBanner = async (containerId: string): Promise<boolean> => {
    if (!this.snapshot.adsEnabled || !this.canUseSdk(this.sdk)) {
      return false;
    }
    try {
      await this.sdk.banner.requestResponsiveBanner(containerId);
      return true;
    } catch (error) {
      console.info('CrazyGames responsive banner unavailable:', errorMessage(error));
      return false;
    }
  };

  clearBanner = (containerId: string): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.banner.clearBanner(containerId);
    } catch (error) {
      console.info('CrazyGames clear banner unavailable:', errorMessage(error));
    }
  };

  clearAllBanners = (): void => {
    if (!this.canUseSdk(this.sdk)) {
      return;
    }
    try {
      this.sdk.banner.clearAllBanners();
    } catch (error) {
      console.info('CrazyGames clear banners unavailable:', errorMessage(error));
    }
  };
}

export const crazyGames = new CrazyGamesService();

/** Convert a mutable portal display name into Snaketron's guest nickname rules. */
export const crazyGamesGuestNickname = (username?: string): string => {
  const normalized = (username ?? '')
    .normalize('NFKC')
    .replace(/[^\p{L}\p{N}_-]+/gu, '_')
    .replace(/^[_-]+|[_-]+$/g, '')
    .slice(0, 20);

  if (normalized.length >= 3) {
    return normalized;
  }
  return `CGPlayer${Math.floor(1000 + Math.random() * 9000)}`;
};
