import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../services/api.js';

const AuthContext = createContext(null);

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
};

const API_BASE_URL = 'http://localhost:3001';

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  // Check if user is logged in on mount
  useEffect(() => {
    const token = localStorage.getItem('token');
    if (token) {
      fetchCurrentUser(token);
    } else {
      setLoading(false);
    }
  }, []);

  const fetchCurrentUser = async (token) => {
    try {
      api.setAuthToken(token);
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

  const login = useCallback(async (username, password) => {
    setError(null);
    try {
      const data = await api.login(username, password);
      setUser(data.user);
      return { success: true };
    } catch (err) {
      const errorMessage = err.message || 'Login failed';
      setError(errorMessage);
      throw err;
    }
  }, []);

  const register = useCallback(async (username, password) => {
    setError(null);
    try {
      // Support guest registration (no password)
      const data = await api.register(username, password || '');
      setUser(data.user);
      return { success: true };
    } catch (err) {
      const errorMessage = err.message || 'Registration failed';
      setError(errorMessage);
      throw err;
    }
  }, []);

  const logout = useCallback(() => {
    api.setAuthToken(null);
    setUser(null);
    navigate('/');
  }, [navigate]);

  const getToken = useCallback(() => {
    return localStorage.getItem('token');
  }, []);

  const value = {
    user,
    loading,
    error,
    login,
    register,
    logout,
    getToken,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};