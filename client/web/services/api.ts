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
import { getOrCreateAnonId } from '../utils/anonId';

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

class API {
  private baseURL: string;

  constructor() {
    // Base API host; endpoints below include the /api prefix explicitly
    const envUrl = process.env.REACT_APP_API_URL?.replace(/\/+$/, '');
    this.baseURL = envUrl || 'http://localhost:8080';
  }

  private getToken(): string | null {
    return localStorage.getItem('token');
  }

  setAuthToken(token: string | null): void {
    if (token) {
      localStorage.setItem('token', token);
    } else {
      localStorage.removeItem('token');
    }
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
    const data = await this.request<CreateGuestResponse>('/api/auth/guest', {
      method: 'POST',
      body: JSON.stringify({ nickname }),
    });
    this.setAuthToken(data.token);
    return data;
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
