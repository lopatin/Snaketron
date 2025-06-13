class API {
  constructor() {
    // In webpack, process.env needs to be defined in the webpack config
    // For now, we'll use a simple fallback
    this.baseURL = 'http://localhost:3001';
    this.token = localStorage.getItem('token');
  }

  setAuthToken(token) {
    this.token = token;
    if (token) {
      localStorage.setItem('token', token);
    } else {
      localStorage.removeItem('token');
    }
  }

  async request(endpoint, options = {}) {
    const url = `${this.baseURL}${endpoint}`;
    const config = {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    };

    if (this.token) {
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

    return data;
  }

  async login(username, password) {
    const data = await this.request('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    this.setAuthToken(data.token);
    return data;
  }

  async register(username, password) {
    const data = await this.request('/api/auth/register', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    this.setAuthToken(data.token);
    return data;
  }

  async checkUsername(username) {
    try {
      const response = await this.request('/api/auth/check-username', {
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
        available: null,
        requiresPassword: false,
        errors: []
      };
    }
  }

  async getCurrentUser() {
    return this.request('/api/auth/me');
  }
}

export const api = new API();