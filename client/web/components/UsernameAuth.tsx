import React, { useState, useEffect, useRef } from 'react';
import { useAuth } from '../contexts/AuthContext.jsx';
import { useDebounce } from '../hooks/useDebounce.js';
import { api } from '../services/api.js';
import { CheckIcon, XIcon } from './Icons.jsx';
import Spinner from './Spinner.jsx';

function UsernameAuth({ onAuthenticated, className = '' }) {
  const { user, login, register } = useAuth();
  
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPasswordField, setShowPasswordField] = useState(false);
  const [requiresPassword, setRequiresPassword] = useState(false);
  const [checkingUsername, setCheckingUsername] = useState(false);
  const [usernameStatus, setUsernameStatus] = useState(null); // 'available', 'taken', 'exists'
  const [authError, setAuthError] = useState(null);
  const [isAuthenticating, setIsAuthenticating] = useState(false);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [hasAttemptedAuth, setHasAttemptedAuth] = useState(false);
  const passwordInputRef = useRef(null);

  // Load saved username on mount
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    if (savedUsername) {
      setUsername(savedUsername);
      checkUsernameStatus(savedUsername);
    }
    
    // If already logged in, mark as authenticated
    if (user) {
      setUsername(user.username);
      setIsAuthenticated(true);
      if (onAuthenticated) {
        onAuthenticated(user);
      }
    }
  }, [user, onAuthenticated]);

  // Auto-authenticate when conditions are met
  useEffect(() => {
    const shouldAutoAuth = 
      !isAuthenticated && 
      !isAuthenticating &&
      !hasAttemptedAuth &&
      !authError &&
      username.length >= 3 && 
      !checkingUsername &&
      ((usernameStatus === 'available') || 
       (usernameStatus === 'exists' && !requiresPassword) ||
       (usernameStatus === 'exists' && requiresPassword && password));

    if (shouldAutoAuth) {
      // Call authenticate function directly
      (async () => {
        if (!username || username.length < 3) {
          return;
        }

        setIsAuthenticating(true);
        setAuthError(null);
        setHasAttemptedAuth(true);

        try {
          if (usernameStatus === 'available') {
            // Register with optional password
            await register(username, password || null);
          } else if (usernameStatus === 'exists') {
            if (requiresPassword && !password) {
              // Don't show error, just wait for password
              setIsAuthenticating(false);
              setHasAttemptedAuth(false); // Allow retry when password is entered
              return;
            }
            // Login with password (or without if not required)
            await login(username, password);
          }

          // Save username for next time
          localStorage.setItem('savedUsername', username);
          setIsAuthenticated(true);
          
          if (onAuthenticated) {
            onAuthenticated({ username });
          }
        } catch (error) {
          setAuthError(error.message || 'Authentication failed');
          // Don't reset hasAttemptedAuth here to prevent retry loop
        } finally {
          setIsAuthenticating(false);
        }
      })();
    }
  }, [username, password, usernameStatus, requiresPassword, checkingUsername, isAuthenticated, isAuthenticating, hasAttemptedAuth, authError, register, login, onAuthenticated]);

  // Check username status with debounce
  const checkUsernameStatus = useDebounce(async (username) => {
    if (!username || username.length < 3) {
      setUsernameStatus(null);
      setRequiresPassword(false);
      setShowPasswordField(false);
      setIsAuthenticated(false);
      return;
    }

    // If already logged in with this username, don't check
    if (user && user.username === username) {
      setUsernameStatus('authenticated');
      setIsAuthenticated(true);
      return;
    }

    setCheckingUsername(true);
    setAuthError(null);
    
    try {
      const response = await api.checkUsername(username);
      if (response.available) {
        setUsernameStatus('available');
        setRequiresPassword(false);
        setShowPasswordField(true); // Show optional password field for new users
        setIsAuthenticated(false);
        // Focus password field when it appears
        setTimeout(() => passwordInputRef.current?.focus(), 100);
      } else {
        setUsernameStatus('exists');
        setRequiresPassword(response.requiresPassword || false);
        setShowPasswordField(true); // Always show password field for existing users
        setIsAuthenticated(false);
        if (response.requiresPassword) {
          // Focus password field if required
          setTimeout(() => passwordInputRef.current?.focus(), 100);
        }
      }
    } catch (error) {
      console.error('Username check failed:', error);
      setUsernameStatus(null);
    } finally {
      setCheckingUsername(false);
    }
  }, 500);

  const handleUsernameChange = (value) => {
    setUsername(value);
    setPassword('');
    setAuthError(null);
    setIsAuthenticated(false);
    setHasAttemptedAuth(false);
    checkUsernameStatus(value);
  };

  const handlePasswordChange = (value) => {
    setPassword(value);
    setAuthError(null);
    setHasAttemptedAuth(false);
  };


  return (
    <div className={`space-y-3 ${className}`}>
      {/* Username Field */}
      <div className="relative">
        <input
          type="text"
          value={username}
          onChange={(e) => handleUsernameChange(e.target.value)}
          className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
          placeholder="Username"
          disabled={isAuthenticated}
          autoFocus
        />
        {username && (
          <div className="absolute right-3 top-3.5">
            {checkingUsername || isAuthenticating ? (
              <Spinner className="w-5 h-5" />
            ) : isAuthenticated ? (
              <CheckIcon className="w-5 h-5 text-green-600" />
            ) : usernameStatus === 'available' ? (
              <div className="w-5 h-5 rounded-full bg-green-100" />
            ) : usernameStatus === 'exists' ? (
              <div className="w-5 h-5 rounded-full bg-yellow-100" />
            ) : null}
          </div>
        )}
      </div>

      {/* Password Field (shown for new users and existing users) */}
      {showPasswordField && !isAuthenticated && (
        <div className="relative">
          <input
            ref={passwordInputRef}
            type="password"
            value={password}
            onChange={(e) => handlePasswordChange(e.target.value)}
            className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
            placeholder={
              usernameStatus === 'available' 
                ? 'Password (optional)' 
                : requiresPassword 
                  ? 'Password' 
                  : 'Password (optional)'
            }
          />
        </div>
      )}

      {/* Status messages */}
      {authError && (
        <div className="space-y-2">
          <p className="text-red-600 text-sm">{authError}</p>
          <button
            onClick={() => {
              setAuthError(null);
              setHasAttemptedAuth(false);
            }}
            className="text-sm text-blue-600 hover:text-blue-800 underline"
          >
            Try again
          </button>
        </div>
      )}

      {isAuthenticated && (
        <p className="text-green-600 text-sm font-medium">
          ✓ Authenticated as {username}
        </p>
      )}
    </div>
  );
}

export default UsernameAuth;