import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext.jsx';
import { useDebounce } from '../hooks/useDebounce.js';
import { api } from '../services/api.js';
import { CheckIcon, XIcon } from './Icons.jsx';
import Spinner from './Spinner.jsx';

function AuthPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { login, register } = useAuth();
  const [isLogin, setIsLogin] = useState(true);
  const [formData, setFormData] = useState({
    username: '',
    password: '',
    confirmPassword: ''
  });
  const [errors, setErrors] = useState({});
  const [loading, setLoading] = useState(false);
  const [checkingUsername, setCheckingUsername] = useState(false);
  const [usernameAvailable, setUsernameAvailable] = useState(null);
  const [usernameError, setUsernameError] = useState(null);

  // Debounced username availability check
  const checkUsername = useDebounce(async (username) => {
    if (!username || username.length < 3) {
      setUsernameAvailable(null);
      setUsernameError(username.length > 0 ? "Username must be at least 3 characters" : null);
      return;
    }

    setCheckingUsername(true);
    setUsernameError(null);
    
    try {
      const response = await api.checkUsername(username);
      setUsernameAvailable(response.available);
      if (!response.available && response.errors?.length > 0) {
        setUsernameError(response.errors[0]);
      }
    } catch (error) {
      console.error('Username check failed:', error);
      setUsernameAvailable(null);
    } finally {
      setCheckingUsername(false);
    }
  }, 500);

  const handleInputChange = (field, value) => {
    setFormData(prev => ({ ...prev, [field]: value }));
    setErrors(prev => ({ ...prev, [field]: null }));

    if (field === 'username' && !isLogin) {
      checkUsername(value);
    }
  };

  const validateForm = () => {
    const newErrors = {};

    if (!formData.username) {
      newErrors.username = 'Username is required';
    }

    if (!formData.password) {
      newErrors.password = 'Password is required';
    } else if (formData.password.length < 6) {
      newErrors.password = 'Password must be at least 6 characters';
    }

    if (!isLogin) {
      if (!formData.confirmPassword) {
        newErrors.confirmPassword = 'Please confirm your password';
      } else if (formData.password !== formData.confirmPassword) {
        newErrors.confirmPassword = 'Passwords do not match';
      }

      if (usernameAvailable === false) {
        newErrors.username = usernameError || 'Username is already taken';
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    setLoading(true);

    try {
      if (isLogin) {
        await login(formData.username, formData.password);
      } else {
        await register(formData.username, formData.password);
      }
      
      // Navigate to intended destination or home
      const from = location.state?.from?.pathname || '/';
      navigate(from, { replace: true });
    } catch (error) {
      setErrors({
        submit: error.response?.data?.message || error.message || 'An error occurred. Please try again.'
      });
      setLoading(false);
    }
  };

  const toggleMode = () => {
    setIsLogin(!isLogin);
    setFormData({ username: '', password: '', confirmPassword: '' });
    setErrors({});
    setUsernameAvailable(null);
    setUsernameError(null);
  };

  return (
    <div className="flex-1 p-8">
      <div className="max-w-xl mx-auto">
        <h1 className="panel-heading mb-6">
          {isLogin ? 'LOGIN' : 'CREATE ACCOUNT'}
        </h1>
        <div className="panel p-6">
          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Username Field */}
            <div>
              <label className="block text-sm font-bold uppercase tracking-1 mb-2">
                Username
              </label>
              <div className="relative">
                <input
                  type="text"
                  value={formData.username}
                  onChange={(e) => handleInputChange('username', e.target.value)}
                  className={`w-full px-4 py-2 text-sm border-2 rounded ${
                    errors.username ? 'border-red-600' : 'border-black-70'
                  }`}
                  placeholder="Enter username"
                  autoComplete={isLogin ? "username" : "new-username"}
                  disabled={loading}
                />
                {!isLogin && formData.username && (
                  <div className="absolute right-3 top-2.5">
                    {checkingUsername ? (
                      <Spinner className="w-4 h-4" />
                    ) : usernameAvailable === true ? (
                      <CheckIcon className="w-4 h-4 text-green-600" />
                    ) : usernameAvailable === false ? (
                      <XIcon className="w-4 h-4 text-red-600" />
                    ) : null}
                  </div>
                )}
              </div>
              {(errors.username || usernameError) && (
                <p className="text-red-600 text-xs mt-1">{errors.username || usernameError}</p>
              )}
              {!isLogin && !errors.username && !usernameError && (
                <p className="text-xs text-black-70 mt-1">
                  3-20 characters, letters, numbers, underscore and hyphen only
                </p>
              )}
            </div>

            {/* Password Field */}
            <div>
              <label className="block text-sm font-bold uppercase tracking-1 mb-2">
                Password
              </label>
              <input
                type="password"
                value={formData.password}
                onChange={(e) => handleInputChange('password', e.target.value)}
                className={`w-full px-4 py-2 text-sm border-2 rounded ${
                  errors.password ? 'border-red-600' : 'border-black-70'
                }`}
                placeholder="Enter password"
                autoComplete={isLogin ? "current-password" : "new-password"}
                disabled={loading}
              />
              {errors.password && (
                <p className="text-red-600 text-xs mt-1">{errors.password}</p>
              )}
            </div>

            {/* Confirm Password (Register only) */}
            {!isLogin && (
              <div>
                <label className="block text-sm font-bold uppercase tracking-1 mb-2">
                  Confirm Password
                </label>
                <input
                  type="password"
                  value={formData.confirmPassword}
                  onChange={(e) => handleInputChange('confirmPassword', e.target.value)}
                  className={`w-full px-4 py-2 text-sm border-2 rounded ${
                    errors.confirmPassword ? 'border-red-600' : 'border-black-70'
                  }`}
                  placeholder="Confirm password"
                  autoComplete="new-password"
                  disabled={loading}
                />
                {errors.confirmPassword && (
                  <p className="text-red-600 text-xs mt-1">{errors.confirmPassword}</p>
                )}
              </div>
            )}

            {/* Submit Error */}
            {errors.submit && (
              <div className="bg-red-50 border border-red-200 text-red-600 px-4 py-2 rounded text-sm">
                {errors.submit}
              </div>
            )}

            {/* Action Buttons */}
            <div className="flex gap-4 mt-8">
              <button
                type="button"
                onClick={() => navigate('/')}
                className="flex-1 btn-secondary"
                disabled={loading}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={loading || (!isLogin && usernameAvailable === false)}
                className="flex-1 btn-primary-straight"
              >
                {loading ? <Spinner className="w-4 h-4" /> : (isLogin ? 'Login' : 'Create Account')}
              </button>
            </div>

            {/* Toggle Login/Register */}
            <div className="text-center mt-4">
              <button
                type="button"
                onClick={toggleMode}
                className="text-sm text-black-70 hover:text-black transition-colors"
                disabled={loading}
              >
                {isLogin ? "Don't have an account? Register" : "Already have an account? Login"}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}

export default AuthPage;