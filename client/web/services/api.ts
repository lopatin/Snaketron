import {
  UserInfo,
  AuthResponse,
  CreateGuestResponse,
  CheckUsernameResult,
  LeaderboardResponse,
  SeasonsResponse,
  UserRankingResponse,
  MatchHistoryPage,
  PublicRuntimeConfig,
  RuntimeConfig,
  RuntimeConfigAuditPage,
  RuntimeConfigRecord,
  UpdateRuntimeConfigRequest,
} from '../types';
import type { CheckUsernameResponse } from '../types/generated';
import { getOrCreateAnonId } from '../utils/anonId';
import type { PlayerLobbyResponse } from '../types/generated';
import type { BuxPack, CheckoutToken } from '../types/generated';
import type { NewsTickerResponse } from '../types/generated';
import type { HighlightClip } from '../types/generated';
import type { PublicGameResponse } from '../types/generated';
import type { Texture } from '../types/generated/Texture';
import type { JobAccepted } from '../types/generated/JobAccepted';
import type { GenerationJob } from '../types/generated/GenerationJob';
import type { TextureListResponse } from '../types/generated/TextureListResponse';
import type {
  BrowseResponse,
  Equipment,
  PurchaseResult,
  SkinKind,
  SkinListResponse,
  SkinSummary,
  Wallet,
} from '../types/generated';
import {
  exactPublicationRequest,
  exactSkinUpdate,
  type UpdateSkinRequest,
} from '../utils/skinApiContracts';

/**
 * An equip request.
 *
 * Three-valued per slot, matching the server: omit a slot to leave it alone,
 * pass `null` to clear it back to the default look, pass a reference to equip.
 * Written by hand rather than generated because ts-rs flattens Rust's
 * `Option<Option<T>>` into a single nullable, losing exactly the distinction
 * this type exists to carry.
 */
export interface EquipRequest {
  selectedSkin?: string | null;
  selectedBase?: string | null;
}

/** Error thrown by `API.request` for a non-2xx response. */
export interface ApiError {
  response: { data: unknown; status: number };
  message: string;
}

export function isApiError(error: unknown): error is ApiError {
  return (
    typeof error === 'object' &&
    error !== null &&
    'response' in error &&
    typeof (error as ApiError).response?.status === 'number'
  );
}

function errorMessage(data: unknown): string {
  if (data && typeof data === 'object') {
    const record = data as Record<string, unknown>;
    if (typeof record.error === 'string') return record.error;
    if (typeof record.message === 'string') return record.message;
  }
  return 'Request failed';
}

interface RequestOptions extends RequestInit {
  headers?: Record<string, string>;
}

export interface CrazyGamesPreferences {
  tutorialSeen?: Record<string, boolean>;
  lobbyPreferences?: {
    selectedModes: string[];
    competitive: boolean;
  };
  boostInputMode?: 'hold' | 'toggle';
}

export type CrazyGamesGuestPromotion = 'check' | 'allow' | 'decline';

export type CrazyGamesExchangeResolution = 'created' | 'guestClaimed' | 'returning';

export interface CrazyGamesExchangeUser extends UserInfo {
  authSource: 'crazygames';
  avatarUrl?: string | null;
}

export interface CrazyGamesExchangeResponse {
  token: string;
  expiresAt: number;
  resolution: CrazyGamesExchangeResolution;
  user: CrazyGamesExchangeUser;
  preferences: CrazyGamesPreferences;
}

export type GameHighlightResponse =
  | { status: 'pending' }
  | { status: 'ready'; play_of_the_game: HighlightClip }
  | { status: 'unavailable' };
export interface AdminHistoryFilters {
  cursor?: string | null;
  limit?: number;
}

// Portal sessions are intentionally isolated from first-party username/
// password sessions. This key stores only Snaketron's internal JWT; the
// short-lived CrazyGames token is never persisted.
export const AUTH_TOKEN_STORAGE_KEY = process.env.CRAZYGAMES_BUILD === 'true'
  ? 'snaketron:crazygames:session-token'
  : 'token';
const IS_CRAZY_GAMES_BUILD = process.env.CRAZYGAMES_BUILD === 'true';

class API {
  private baseURL: string;
  private crazyGamesMemoryToken: string | null = null;
  private crazyGamesTokenLoaded = false;

  constructor() {
    // Base API host; endpoints below include the /api prefix explicitly
    const envUrl = process.env.REACT_APP_API_URL?.replace(/\/+$/, '');
    this.baseURL = envUrl || 'http://localhost:8080';
    // The wasm renderer builds texture URLs itself — a skin's atlas is loaded
    // by the compiled skin, not by anything on this side — and it has no way
    // to read the build's environment. Publishing what we resolved keeps the
    // two from disagreeing about where the API is, which origin-relative URLs
    // get wrong on every deployment that serves the app from a different host.
    (globalThis as Record<string, unknown>).__snaketronApiOrigin = this.baseURL;
  }

  private getToken(): string | null {
    if (!IS_CRAZY_GAMES_BUILD) {
      return localStorage.getItem(AUTH_TOKEN_STORAGE_KEY);
    }
    if (this.crazyGamesTokenLoaded) {
      return this.crazyGamesMemoryToken;
    }
    this.crazyGamesTokenLoaded = true;
    try {
      this.crazyGamesMemoryToken = sessionStorage.getItem(AUTH_TOKEN_STORAGE_KEY);
    } catch {
      // In-memory auth still works when an embed blocks Web Storage.
    }
    if (this.crazyGamesMemoryToken) {
      return this.crazyGamesMemoryToken;
    }
    // One-time migration from the earlier shared-tab pilot. All new reads and
    // writes are tab-scoped so another portal account cannot replace this
    // tab's bearer token.
    try {
      const legacyToken = localStorage.getItem(AUTH_TOKEN_STORAGE_KEY);
      if (legacyToken) {
        this.crazyGamesMemoryToken = legacyToken;
        try {
          sessionStorage.setItem(AUTH_TOKEN_STORAGE_KEY, legacyToken);
        } catch {
          // Memory remains authoritative for this page lifetime.
        }
      }
      localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
    } catch {
      // Shared storage is optional and is never used as the live CG source.
    }
    return this.crazyGamesMemoryToken;
  }

  setAuthToken(token: string | null): void {
    if (!IS_CRAZY_GAMES_BUILD) {
      if (token) {
        localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, token);
      } else {
        localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
      }
      return;
    }

    this.crazyGamesTokenLoaded = true;
    this.crazyGamesMemoryToken = token;
    try {
      if (token) {
        sessionStorage.setItem(AUTH_TOKEN_STORAGE_KEY, token);
      } else {
        sessionStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
      }
    } catch {
      // Memory remains authoritative until the next full page load.
    }
    try {
      localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
    } catch {
      // Best-effort cleanup of the shared-tab legacy key.
    }
  }

  getAuthToken(): string | null {
    return this.getToken();
  }

  // T must be specified by the caller (typically a generated wire DTO). The
  // response JSON lands in `unknown` and is asserted to T at this single
  // boundary rather than defaulting every call to `any`.
  async request<T>(endpoint: string, options: RequestOptions = {}): Promise<T> {
    const url = `${this.baseURL}${endpoint}`;
    // A multipart body writes its own content type, boundary and all. Setting
    // `application/json` over the top of it — which the default here would —
    // makes the server refuse a body it can otherwise read perfectly well.
    const sendsForm = options.body instanceof FormData;
    const config: RequestOptions = {
      ...options,
      headers: {
        ...(sendsForm ? {} : { 'Content-Type': 'application/json' }),
        ...options.headers,
      },
    };

    const token = this.getToken();
    if (token && config.headers) {
      config.headers.Authorization = `Bearer ${token}`;
    }

    // Advisory analytics identifier. Sent on every request, including
    // unauthenticated ones, because the top of the signup funnel happens
    // before any token exists. The server never uses it for authorization.
    if (config.headers) {
      config.headers['x-snaketron-anon-id'] = getOrCreateAnonId();
    }

    const response = await fetch(url, config);
    const data: unknown = await response.json();

    if (!response.ok) {
      const error: ApiError = {
        response: { data, status: response.status },
        message: errorMessage(data),
      };
      throw error;
    }

    return data as T;
  }

  /**
   * Resolve a `/play/<username>` invite link to the lobby that player is in.
   *
   * Anonymous, because the link is followed before the visitor has an account.
   * A failed request is reported as `notFound` rather than thrown: the caller
   * always has to explain the outcome to a visitor either way, and there is
   * nothing useful for them to do differently about a 500.
   */
  async getPlayerLobby(username: string): Promise<PlayerLobbyResponse> {
    try {
      return await this.request<PlayerLobbyResponse>(
        `/api/players/${encodeURIComponent(username)}/lobby`,
      );
    } catch {
      return { username, lobbyCode: null, status: 'notFound' };
    }
  }

  async login(username: string, password: string): Promise<AuthResponse> {
    const data = await this.request<AuthResponse>('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    this.setAuthToken(data.token);
    return data;
  }

  async register(username: string, password: string): Promise<AuthResponse> {
    const data = await this.request<AuthResponse>('/api/auth/register', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    this.setAuthToken(data.token);
    return data;
  }

  async createGuest(nickname: string): Promise<CreateGuestResponse> {
    return this.request<CreateGuestResponse>('/api/auth/guest', {
      method: 'POST',
      body: JSON.stringify({ nickname }),
    });
  }

  /**
   * Exchange a short-lived CrazyGames JWT without ever persisting it. The
   * optional internal bearer attached by request() lets the server inspect or
   * promote an eligible guest according to the caller's explicit consent.
   */
  async exchangeCrazyGamesToken(
    token: string,
    guestPromotion: CrazyGamesGuestPromotion,
    initialPreferences?: CrazyGamesPreferences,
  ): Promise<CrazyGamesExchangeResponse> {
    return this.request<CrazyGamesExchangeResponse>('/api/auth/crazygames/exchange', {
      method: 'POST',
      body: JSON.stringify({
        token,
        guestPromotion,
        ...(initialPreferences ? { initialPreferences } : {}),
      }),
    });
  }

  async saveCrazyGamesPreferences(
    preferences: CrazyGamesPreferences,
  ): Promise<CrazyGamesPreferences> {
    const response = await this.request<{ preferences: CrazyGamesPreferences }>(
      '/api/auth/crazygames/preferences', {
      method: 'PUT',
      body: JSON.stringify(preferences),
      },
    );
    return response.preferences;
  }

  async checkUsername(username: string): Promise<CheckUsernameResult> {
    try {
      const response = await this.request<CheckUsernameResponse>('/api/auth/check-username', {
        method: 'POST',
        body: JSON.stringify({ username }),
      });

      return {
        available: response.available,
        // The server's response has no requiresPassword field; the UI expects
        // one, so it is defaulted false here. See CheckUsernameResult.
        requiresPassword: false,
        errors: response.errors,
      };
    } catch {
      // Return a safe default on error
      return {
        available: false,
        requiresPassword: false,
        errors: [],
      };
    }
  }

  async getCurrentUser(): Promise<UserInfo & { isAdmin?: boolean }> {
    return this.request<UserInfo & { isAdmin?: boolean }>('/api/auth/me');
  }

  async getNewsTicker(): Promise<NewsTickerResponse> {
    return this.request<NewsTickerResponse>('/api/news');
  }

  /** Public-by-design replay metadata. A bearer is still attached when one is
   * available, including tab-scoped CrazyGames sessions. */
  async getGameHighlight(
    gameId: string,
    signal?: AbortSignal,
  ): Promise<GameHighlightResponse> {
    return this.request<GameHighlightResponse>(
      `/api/games/${encodeURIComponent(gameId)}/highlight`,
      { signal },
    );
  }

  /**
   * The permanent public summary of a finished match. Anonymous by design —
   * this is what a shared link resolves to, and it must work for someone who
   * has never played.
   */
  async getPublicGameSummary(
    gameId: string,
    signal?: AbortSignal,
  ): Promise<PublicGameResponse> {
    return this.request<PublicGameResponse>(
      `/api/games/${encodeURIComponent(gameId)}/summary`,
      { signal },
    );
  }

  async getLeaderboard(
    queueMode: 'quickmatch' | 'competitive',
    gameType: 'solo' | 'duel' | '2v2' | 'ffa',
    season?: number,
    limit?: number,
    offset?: number,
    region?: string
  ): Promise<LeaderboardResponse> {
    const params = new URLSearchParams({
      queue_mode: queueMode,
      game_type: gameType,
    });

    if (season !== undefined) params.append('season', season.toString());
    if (limit !== undefined) params.append('limit', limit.toString());
    if (offset !== undefined) params.append('offset', offset.toString());
    if (region) params.append('region', region);

    return this.request<LeaderboardResponse>(`/api/leaderboard?${params.toString()}`);
  }

  async getSeasons(): Promise<SeasonsResponse> {
    return this.request<SeasonsResponse>('/api/seasons');
  }

  async getMyRanking(
    queueMode: 'quickmatch' | 'competitive',
    gameType: 'solo' | 'duel' | '2v2' | 'ffa',
    season?: number,
    region?: string
  ): Promise<UserRankingResponse> {
    const params = new URLSearchParams({
      queue_mode: queueMode,
      game_type: gameType,
    });

    if (season !== undefined) params.append('season', season.toString());
    if (region) params.append('region', region);

    return this.request<UserRankingResponse>(`/api/leaderboard/me?${params.toString()}`);
  }

  /**
   * Any player's ranking in one region. Standing is already public — the
   * leaderboard publishes the same MMR next to the username — so this needs
   * no auth. It exists for surfaces that know a user id but are not that
   * user, such as the Play of the Game caption naming whoever earned it.
   */
  async getUserRanking(
    userId: number,
    queueMode: 'quickmatch' | 'competitive',
    gameType: 'solo' | 'duel' | '2v2' | 'ffa',
    season?: number,
    region?: string
  ): Promise<UserRankingResponse> {
    const params = new URLSearchParams({
      queue_mode: queueMode,
      game_type: gameType,
    });

    if (season !== undefined) params.append('season', season.toString());
    if (region) params.append('region', region);

    return this.request<UserRankingResponse>(
      `/api/leaderboard/users/${userId}?${params.toString()}`
    );
  }

  async getMatchHistory(cursor?: string | null, limit = 12): Promise<MatchHistoryPage> {
    const params = new URLSearchParams({ limit: limit.toString() });
    if (cursor) params.set('cursor', cursor);
    return this.request<MatchHistoryPage>(`/api/history?${params.toString()}`);
  }

  async getAdminMatchHistory(filters: AdminHistoryFilters = {}): Promise<MatchHistoryPage> {
    const params = new URLSearchParams({ limit: String(filters.limit ?? 25) });
    if (filters.cursor) params.set('cursor', filters.cursor);
    return this.request<MatchHistoryPage>(`/api/admin/history?${params.toString()}`);
  }

  async getRuntimeConfig(): Promise<PublicRuntimeConfig> {
    return this.request<PublicRuntimeConfig>('/api/config');
  }

  async getAdminRuntimeConfig(): Promise<RuntimeConfigRecord> {
    return this.request<RuntimeConfigRecord>('/api/admin/config');
  }

  async updateAdminRuntimeConfig(
    config: RuntimeConfig,
    expectedVersion: number,
  ): Promise<RuntimeConfigRecord> {
    const request: UpdateRuntimeConfigRequest = { config, expectedVersion };
    return this.request<RuntimeConfigRecord>('/api/admin/config', {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async getAdminRuntimeConfigAudit(
    cursor?: string | null,
    limit = 20,
  ): Promise<RuntimeConfigAuditPage> {
    const params = new URLSearchParams({ limit: limit.toString() });
    if (cursor) params.set('cursor', cursor);
    return this.request<RuntimeConfigAuditPage>(`/api/admin/config/audit?${params.toString()}`);
  }

  /** The catalogue. Needs no account — browsing is open to anyone. */
  async browseSkins(kind: SkinKind = 'snake'): Promise<BrowseResponse> {
    return this.request<BrowseResponse>(`/api/skins?kind=${kind}`);
  }

  /**
   * Record what the signed-in player is wearing.
   *
   * Slots are addressed independently: omit a slot to leave it alone, pass
   * `null` to clear it back to the default look. Returns both slots as they
   * now stand, so a caller that changed one still learns the whole state.
   */
  async setEquipment(request: EquipRequest): Promise<Equipment> {
    return this.request<Equipment>('/api/users/me/equipped', {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  /** What a player can buy Snakebux in. */
  async buxPacks(): Promise<BuxPack[]> {
    return this.request<BuxPack[]>('/api/wallet/packs');
  }

  /** Mint a checkout token for one pack; the provider hosts the rest. */
  async buxCheckoutToken(sku: string): Promise<CheckoutToken> {
    return this.request<CheckoutToken>('/api/wallet/xsolla/checkout-token', {
      method: 'POST',
      body: JSON.stringify({ sku }),
    });
  }

  /** Published player-authored skins, newest first. */
  async browseAuthoredSkins(
    kind: SkinKind = 'snake',
    filter: 'published' | 'mine' = 'published',
  ): Promise<SkinListResponse> {
    return this.request<SkinListResponse>(`/api/skins/browse?kind=${kind}&filter=${filter}`);
  }

  /**
   * Buy a skin.
   *
   * `expectedPriceBux` is what the buyer was shown; the server conditions on
   * it, so a price that moved between the dialog and this call comes back a
   * 409 rather than charging a surprise.
   */
  async purchaseSkin(
    skinId: number,
    expectedPriceBux: number,
    idempotencyKey: string,
  ): Promise<PurchaseResult> {
    return this.request<PurchaseResult>(`/api/skins/${skinId}/purchase`, {
      method: 'POST',
      body: JSON.stringify({ idempotencyKey, expectedPriceBux }),
    });
  }

  async getWallet(): Promise<Wallet> {
    return this.request<Wallet>('/api/wallet');
  }

  /**
   * One skin's document, by the reference that names it.
   *
   * Fetched by content reference rather than by skin id, because that is what
   * a reference *is*: the bytes are immutable, so this response can be cached
   * hard, and a revision an author has since replaced still resolves for the
   * replay that recorded it.
   */
  async getSkinDocument(contentRef: string): Promise<unknown> {
    return this.request<unknown>(`/api/skins/by-ref/${encodeURIComponent(contentRef)}`);
  }

  /** The textures this account owns, newest first. */
  /** Where this client talks to, so callers can build asset URLs. */
  get baseUrl(): string {
    return this.baseURL;
  }

  async listTextures(): Promise<TextureListResponse> {
    return this.request<TextureListResponse>('/api/textures');
  }

  /**
   * Hand over art you already have.
   *
   * The one route in this API that takes bytes rather than JSON. `request`
   * spots the `FormData` and stands back from the content type, because the
   * browser has to write the multipart boundary itself.
   */
  async uploadTexture(file: File, kind: string, subject?: string): Promise<JobAccepted> {
    const form = new FormData();
    form.append('kind', kind);
    if (subject) {
      form.append('subject', subject);
    }
    form.append('file', file);
    return this.request<JobAccepted>('/api/textures', { method: 'POST', body: form });
  }

  /** Ask a model for one, with optional references. */
  async generateTexture(request: {
    kind: string;
    prompt: string;
    referenceTextureIds?: number[];
  }): Promise<JobAccepted> {
    return this.request<JobAccepted>('/api/textures/generate', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  /** Where a texture job has got to. */
  async getGenerationJob(jobId: string): Promise<GenerationJob> {
    return this.request<GenerationJob>(`/api/generation-jobs/${encodeURIComponent(jobId)}`);
  }

  /** Create a skin from a document the editor has already compiled. */
  async createSkin(request: { name: string; document: unknown; kind?: 'snake' | 'base' }): Promise<SkinSummary> {
    return this.request<SkinSummary>('/api/skins', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  /** Append a revision, rename, or re-price. */
  async updateSkin(
    skinId: number,
    request: UpdateSkinRequest,
  ): Promise<SkinSummary> {
    return this.request<SkinSummary>(`/api/skins/${skinId}`, {
      method: 'PUT',
      body: JSON.stringify(exactSkinUpdate(request)),
    });
  }

  /** Ask an admin to look at one exact immutable revision. */
  async requestSkinPublication(skinId: number, revision: number, contentRef: string): Promise<void> {
    await this.request<unknown>(`/api/skins/${skinId}/publish-request`, {
      method: 'POST',
      body: JSON.stringify(exactPublicationRequest(revision, contentRef)),
    });
  }

  /** Everything waiting on a reviewer, oldest first. */
  async getSkinReviewQueue(): Promise<SkinListResponse> {
    return this.request<SkinListResponse>('/api/admin/skins');
  }

  /**
   * Decide a skin.
   *
   * Publishing names one revision — by default the one review was asked about,
   * because the creator's head may have moved since they submitted it.
   */
  async setSkinPublication(
    skinId: number,
    publication: 'published' | 'unpublished' | 'disabled' | 'private',
    options: { revision?: number; reason?: string } = {},
  ): Promise<SkinSummary> {
    return this.request<SkinSummary>(`/api/admin/skins/${skinId}/status`, {
      method: 'PUT',
      body: JSON.stringify({ publication, ...options }),
    });
  }
}

export const api = new API();
