import { User, LoginResponse, CheckUsernameResponse } from '../types';

interface RequestOptions extends RequestInit {
  headers?: Record<string, string>;
}

class API {
  private baseURL: string;
  private token: string | null;

  constructor() {
    // Use environment variable if available, otherwise default to localhost for development
    this.baseURL = process.env.REACT_APP_API_URL || 'http://localhost:3001';
    this.token = localStorage.getItem('token');
  }

  setAuthToken(token: string | null): void {
    this.token = token;
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

    if (this.token && config.headers) {
      config.headers.Authorization = `Bearer ${this.token}`;
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
}

export const api = new API();