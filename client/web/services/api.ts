import {
  UserInfo,
  AuthResponse,
  CreateGuestResponse,
  CheckUsernameResult,
  LeaderboardResponse,
  SeasonsResponse,
  UserRankingResponse,
} from '../types';
import type { CheckUsernameResponse } from '../types/generated';
import type { NewsTickerResponse } from '../types/generated';

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
    const config: RequestOptions = {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    };

    const token = this.getToken();
    if (token && config.headers) {
      config.headers.Authorization = `Bearer ${token}`;
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

  async getCurrentUser(): Promise<UserInfo> {
    return this.request<UserInfo>('/api/auth/me');
  }

  async getNewsTicker(): Promise<NewsTickerResponse> {
    return this.request<NewsTickerResponse>('/api/news');
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
}

export const api = new API();
