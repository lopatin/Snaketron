import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../services/api';
import { AuthContextType, User } from '../types';

const AuthContext = createContext<AuthContextType | null>(null);

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

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  // Check if user is logged in on mount
  useEffect(() => {
    const token = localStorage.getItem('token');
    if (token) {
      fetchCurrentUser();
    } else {
      setLoading(false);
    }
  }, []);

  const fetchCurrentUser = async () => {
    try {
      const userInfo = await api.getCurrentUser();
      setUser(userInfo);
    } catch (err) {
      console.error('Failed to fetch current user:', err);
      const status = (err as any)?.response?.status;
      const isAuthError = status === 401 || status === 403;
      const isAbortError = err instanceof Error && err.name === 'AbortError';

      // Only clear the token for real auth failures, not for transient/aborted requests
      if (isAuthError) {
        api.setAuthToken(null);
      } else if (isAbortError) {
        console.debug('Fetch aborted while loading user; keeping existing auth token');
      }
    } finally {
      setLoading(false);
    }
  };

  const login = useCallback(async (username: string, password: string) => {
    try {
      // If currently logged in as a guest, clear the guest session first
      if (user?.isGuest) {
        console.log('Logging out guest user before full login');
        api.setAuthToken(null);
        setUser(null);
        // Wait a brief moment for state to settle
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      const data = await api.login(username, password);
      setUser(data.user);
    } catch (err) {
      throw err;
    }
  }, [user]);

  const register = useCallback(async (username: string, password: string | null) => {
    try {
      // If currently logged in as a guest, clear the guest session first
      if (user?.isGuest) {
        console.log('Logging out guest user before registration');
        api.setAuthToken(null);
        setUser(null);
        // Wait a brief moment for state to settle
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      // Support guest registration (no password)
      const data = await api.register(username, password || '');
      setUser(data.user);
    } catch (err) {
      throw err;
    }
  }, [user]);

  const createGuest = useCallback(async (nickname: string) => {
    try {
      const data = await api.createGuest(nickname);
      const guestUser: User = { ...data.user, isGuest: true };
      setUser(guestUser);
      try {
        localStorage.setItem('savedUsername', guestUser.username);
      } catch {
        // ignore storage errors
      }
      return { user: guestUser, token: data.token };
    } catch (err) {
      throw err;
    }
  }, []);

  const updateGuestNickname = useCallback((nickname: string) => {
    setUser(prev => {
      if (!prev) {
        return prev;
      }
      return { ...prev, username: nickname };
    });

    try {
      localStorage.setItem('savedUsername', nickname);
    } catch {
      // ignore storage errors
    }
  }, []);

  const logout = useCallback(() => {
    api.setAuthToken(null);
    setUser(null);
    navigate('/');
  }, [navigate]);

  const getToken = useCallback((): string | null => {
    return localStorage.getItem('token');
  }, []);

  const value: AuthContextType = {
    user,
    loading,
    login,
    register,
    createGuest,
    updateGuestNickname,
    logout,
    getToken,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};
