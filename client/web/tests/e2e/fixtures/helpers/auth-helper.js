class AuthHelper {
  constructor(page) {
    this.page = page;
    this.apiUrl = process.env.API_URL || 'http://localhost:8080';
  }

  async register(username, password = 'testpassword123') {
    // Make a direct API call to register
    const response = await this.page.request.post(`${this.apiUrl}/api/auth/register`, {
      data: {
        username,
        password
      }
    });

    if (!response.ok()) {
      const error = await response.json();
      throw new Error(`Registration failed: ${error.error || 'Unknown error'}`);
    }

    const data = await response.json();
    return data;
  }

  async login(username, password = 'testpassword123') {
    // Make a direct API call to login
    const response = await this.page.request.post(`${this.apiUrl}/api/auth/login`, {
      data: {
        username,
        password
      }
    });

    if (!response.ok()) {
      const error = await response.json();
      throw new Error(`Login failed: ${error.error || 'Unknown error'}`);
    }

    const data = await response.json();
    return data;
  }

  async authenticateUser(username = null) {
    // Generate a unique username if not provided
    const testUsername = username || `test_user_${Date.now()}`;
    
    try {
      // Try to register first
      const authData = await this.register(testUsername);
      
      // Set the token in localStorage
      await this.page.evaluate((token) => {
        localStorage.setItem('token', token);
      }, authData.token);
      
      return authData;
    } catch (error) {
      // If registration fails (user might exist), try login
      if (error.message.includes('already exists')) {
        const authData = await this.login(testUsername);
        
        // Set the token in localStorage
        await this.page.evaluate((token) => {
          localStorage.setItem('token', token);
        }, authData.token);
        
        return authData;
      }
      throw error;
    }
  }

  async logout() {
    await this.page.evaluate(() => {
      localStorage.removeItem('token');
    });
  }

  async isAuthenticated() {
    return await this.page.evaluate(() => {
      return localStorage.getItem('token') !== null;
    });
  }
}

module.exports = { AuthHelper };