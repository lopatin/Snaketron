import { User, LoginResponse, CheckUsernameResponse, CreateGuestResponse, LeaderboardResponse, SeasonsResponse, UserRankingResponse } from '../types';

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

  async request<T = any>(endpoint: string, options: RequestOptions = {}): Promise<T> {
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
    const data = await response.json();
    
    if (!response.ok) {
      throw { 
        response: { 
          data, 
          status: response.status 
        },
        message: data.error || data.message || 'Request failed'
      };
    }

    return data as T;
  }

  async login(username: string, password: string): Promise<LoginResponse> {
    const data = await this.request<LoginResponse>('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    this.setAuthToken(data.token);
    return data;
  }

  async register(username: string, password: string): Promise<LoginResponse> {
    const data = await this.request<LoginResponse>('/api/auth/register', {
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

  async checkUsername(username: string): Promise<CheckUsernameResponse> {
    try {
      const response = await this.request<CheckUsernameResponse>('/api/auth/check-username', {
        method: 'POST',
        body: JSON.stringify({ username }),
      });
      
      // Enhanced response to include password requirement info
      return {
        available: response.available,
        requiresPassword: response.requiresPassword || false,
        errors: response.errors || []
      };
    } catch (error) {
      // Return a safe default on error
      return {
        available: false,
        requiresPassword: false,
        errors: []
      };
    }
  }

  async getCurrentUser(): Promise<User> {
    return this.request<User>('/api/auth/me');
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
