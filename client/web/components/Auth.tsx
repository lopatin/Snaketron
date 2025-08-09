import React, { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useDebouncedValue } from '../hooks/useDebouncedValue';
import { api } from '../services/api';
import { UsernameStatus } from '../types';

function Auth() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { user, login, register } = useAuth();
  const { createSoloGame, currentGameId, createGame, isConnected } = useGameWebSocket();
  const [isProcessingAction, setIsProcessingAction] = useState(false);
  const [waitingForGameId, setWaitingForGameId] = useState(false);
  
  // Auth form state
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [authError, setAuthError] = useState<string | null>(null);
  const [isAuthenticating, setIsAuthenticating] = useState(false);
  const [checkingUsername, setCheckingUsername] = useState(false);
  const [usernameStatus, setUsernameStatus] = useState<UsernameStatus>(null);
  const [requiresPassword, setRequiresPassword] = useState(false);
  
  const debouncedUsername = useDebouncedValue(username, 500);
  const action = searchParams.get('action');

  // Load saved username on mount
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    if (savedUsername) {
      setUsername(savedUsername);
    }
  }, []);
  
  // Check username availability when it changes
  useEffect(() => {
    if (debouncedUsername && debouncedUsername.length >= 3) {
      setCheckingUsername(true);
      setUsernameStatus(null);
      
      api.checkUsername(debouncedUsername)
        .then(data => {
          setUsernameStatus(data.available ? 'available' : 'exists');
          setRequiresPassword(data.requiresPassword || false);
        })
        .catch(err => {
          console.error('Error checking username:', err);
          // On error, assume username is available (for development)
          setUsernameStatus('available');
          setRequiresPassword(false);
        })
        .finally(() => {
          setCheckingUsername(false);
        });
    } else {
      setUsernameStatus(null);
      setRequiresPassword(false);
    }
  }, [debouncedUsername]);

  // Get the title based on the action
  const getTitle = () => {
    switch (action) {
      case 'solo':
        return 'Solo Game';
      case 'quick-match':
        return 'Quick Match';
      case 'competitive':
        return 'Competitive';
      case 'custom':
        return 'Custom Game';
      default:
        return 'Sign In';
    }
  };

  // Determine which dynamic message to show (only one at a time, based on priority)
  const getDynamicMessage = (): { text: string; className: string } | null => {
    // Priority 1: Error messages
    if (authError) {
      return { text: authError, className: 'text-red-600 text-sm' };
    }
    
    // Priority 2: Authenticating
    if (isAuthenticating) {
      return { text: 'Authenticating...', className: 'text-sm text-gray-700' };
    }
    
    // Priority 3: Checking username
    if (username && username.length >= 3 && checkingUsername) {
      return { text: 'Checking username...', className: 'text-sm text-gray-700' };
    }
    
    // Priority 4: Username status messages
    if (username && username.length >= 3 && !checkingUsername) {
      if (usernameStatus === 'available') {
        return { text: `Create a new password for ${username}`, className: 'text-sm text-gray-700' };
      }
      if (usernameStatus === 'exists') {
        return { text: `Enter password for ${username}`, className: 'text-sm text-gray-700' };
      }
    }
    
    // Priority 5: Default message
    if (!username) {
      return { text: 'Enter your username or register a new one', className: 'text-sm text-gray-700' };
    }
    
    return null;
  };

  // Handle form submission
  const handleSubmit = async () => {
    if (!username || username.length < 3) {
      setAuthError('Please enter a username (at least 3 characters)');
      return;
    }

    if (!isConnected) {
      setAuthError('Not connected to game server');
      return;
    }

    setIsAuthenticating(true);
    setAuthError(null);

    try {
      // First, authenticate the user if not already authenticated
      if (!user) {
        // Check if username exists
        const checkData = await api.checkUsername(username);
        
        if (!checkData.available && checkData.requiresPassword && !password) {
          setAuthError('This username requires a password');
          setIsAuthenticating(false);
          return;
        }

        // Login or register
        if (!checkData.available) {
          await login(username, password);
        } else {
          await register(username, password || null);
        }
        
        // Save username for next time
        localStorage.setItem('savedUsername', username);
        
        // Wait a bit for the JWT token to be sent to WebSocket
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      // Now perform the action
      setIsProcessingAction(true);
      
      switch (action) {
        case 'solo':
          // Create a solo game
          setWaitingForGameId(true);
          createSoloGame();
          break;
        
        case 'quick-match':
          // Navigate to quick match selection
          navigate('/game-modes/quick-play');
          break;
        
        case 'competitive':
          // Navigate to competitive selection
          navigate('/game-modes/competitive');
          break;
        
        case 'custom':
          // Navigate to custom game creation
          navigate('/custom');
          break;
        
        default:
          // Just go back to home
          navigate('/');
          break;
      }
    } catch (error) {
      setAuthError((error as Error).message || 'Failed to authenticate');
      setIsAuthenticating(false);
      setIsProcessingAction(false);
    }
  };

  // If already authenticated on mount, perform the action
  useEffect(() => {
    if (user && !isProcessingAction && !waitingForGameId) {
      handleSubmit();
    }
  }, [user]);

  // Watch for game creation when waiting for solo game
  useEffect(() => {
    if (waitingForGameId && currentGameId) {
      navigate(`/play/${currentGameId}`);
      setWaitingForGameId(false);
      setIsProcessingAction(false);
    }
  }, [currentGameId, waitingForGameId, navigate]);

  return (
    <div className="flex-1 p-8">
      <div className="max-w-md mx-auto">
        <h1 className="panel-heading mb-6">{getTitle()}</h1>
        
        <div className="panel p-6">
          {!user ? (
            <>
              {/* Username Input */}
              <div className="space-y-2">
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="Username"
                  className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
                  disabled={isAuthenticating}
                  autoFocus
                />
                
                {/* Password Input */}
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Password"
                  className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
                  disabled={isAuthenticating}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && username && username.length >= 3 && password) {
                      handleSubmit();
                    }
                  }}
                />
                
                {/* Dynamic message below password */}
                <div className="auth-message ml-2 h-8 flex items-center">
                  {(() => {
                    const message = getDynamicMessage();
                    return message ? (
                      <p className={message.className}>{message.text}</p>
                    ) : null;
                  })()}
                </div>
              </div>
              
              {/* Submit Button */}
              <div className="mt-4">
                <button
                  onClick={handleSubmit}
                  disabled={isAuthenticating || !username || username.length < 3 || !password}
                  className={`w-full p-4 text-center border-2 rounded-lg transition-all font-black italic uppercase tracking-1 text-lg ${
                    username && username.length >= 3 && password && !isAuthenticating
                      ? 'border-black-70 bg-white hover:bg-gray-50 cursor-pointer text-black-70'
                      : 'border-gray-300 bg-gray-50 cursor-not-allowed opacity-50 text-gray-500'
                  }`}
                >
                  START GAME
                </button>
              </div>
            </>
          ) : (
            <div className="text-center">
              {isProcessingAction || waitingForGameId ? (
                <p className="text-gray-600">Starting game...</p>
              ) : (
                <p className="text-gray-600">Redirecting...</p>
              )}
            </div>
          )}
          
          {/* Back button */}
          <div className="mt-6 pt-6 border-t-2 border-gray-200">
            <button
              onClick={() => navigate('/')}
              className="text-sm text-gray-600 hover:text-black transition-colors"
            >
              ← Back to menu
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Auth;