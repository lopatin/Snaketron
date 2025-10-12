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
      // Token is invalid or expired
      api.setAuthToken(null);
    } finally {
      setLoading(false);
    }
  };

  const login = useCallback(async (username: string, password: string) => {
    try {
      const data = await api.login(username, password);
      setUser(data.user);
    } catch (err) {
      throw err;
    }
  }, []);

  const register = useCallback(async (username: string, password: string | null) => {
    try {
      // Support guest registration (no password)
      const data = await api.register(username, password || '');
      setUser(data.user);
    } catch (err) {
      throw err;
    }
  }, []);

  const createGuest = useCallback(async (nickname: string) => {
    try {
      const data = await api.createGuest(nickname);
      setUser({ ...data.user, isGuest: true });
    } catch (err) {
      throw err;
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
    logout,
    getToken,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};