import React, { useEffect, useState, useRef } from 'react';
import { BrowserRouter, Route, useNavigate, Link } from 'react-router-dom';
import './index.css';
import CustomGameCreator from './components/CustomGameCreator';
import GameLobby from './components/GameLobby';
import GameArena from './components/GameArena';
import JoinGameModal from './components/JoinGameModal';
import ProtectedRoute from './components/ProtectedRoute';
import GameModeSelector from './components/GameModeSelector';
import AnimatedRoutes from './components/AnimatedRoutes';
import { WebSocketProvider, useWebSocket } from './contexts/WebSocketContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';

function Header() {
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [showUserDropdown, setShowUserDropdown] = useState(false);
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const dropdownRef = useRef<HTMLDivElement>(null);
  
  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !(dropdownRef.current as HTMLElement).contains(event.target as Node)) {
        setShowUserDropdown(false);
      }
    }
    
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);
  
  return (
    <>
      <header className="bg-white border-t-3 border-b-3 border-white py-5 pb-[18px] site-header">
        <div className="max-w-6xl mx-auto px-5 flex justify-between items-center">
          <div className="flex items-center gap-12">
            <Link to="/">
              <img src="/SnaketronLogo.png" alt="Snaketron" className="h-6 w-auto opacity-80" />
            </Link>
            <nav className="flex gap-6">
              <Link to="/" className="text-black-70 font-black italic uppercase tracking-1 opacity-100 underline underline-offset-6">Play</Link>
              <a href="#" className="text-black-70 font-black italic uppercase tracking-1 opacity-70 hover:opacity-100 transition-opacity">Leaderboards</a>
              <a href="#" className="text-black-70 font-black italic uppercase tracking-1 opacity-70 hover:opacity-100 transition-opacity">Spectate</a>
              <a 
                href="#" 
                onClick={(e) => {
                  e.preventDefault();
                  setShowJoinModal(true);
                }}
                className="text-black-70 font-black italic uppercase tracking-1 opacity-70 hover:opacity-100 transition-opacity"
              >
                Join
              </a>
            </nav>
          </div>
          <div className="flex items-center gap-4">
            <select className="text-sm text-black-70 font-bold uppercase tracking-1 bg-transparent border border-black-70 rounded px-3 py-1 cursor-pointer hover:bg-gray-50 transition-colors">
              <option>US East</option>
              <option>US West</option>
              <option>Europe</option>
              <option>Asia</option>
            </select>
            {user && (
              <div className="relative" ref={dropdownRef}>
                <button 
                  onClick={() => setShowUserDropdown(!showUserDropdown)}
                  className="flex items-center gap-1 text-sm text-black-70 font-bold uppercase cursor-pointer hover:opacity-70 transition-opacity"
                >
                  {user.username}
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {showUserDropdown && (
                  <div className="absolute right-0 mt-1 bg-white rounded-lg main-menu-button z-50" style={{ minWidth: '120px' }}>
                    <button 
                      onClick={() => {
                        // TODO: Navigate to profile
                        setShowUserDropdown(false);
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-black-70 hover:underline transition-all cursor-pointer"
                      style={{ boxShadow: 'inset 0 -0.5px 0 0 rgba(0, 0, 0, 0.1)' }}
                    >
                      Profile
                    </button>
                    <button 
                      onClick={() => {
                        logout();
                        setShowUserDropdown(false);
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-black-70 hover:underline transition-all cursor-pointer"
                    >
                      Logout
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </header>
      
      {/* Join Game Modal */}
      <JoinGameModal 
        isOpen={showJoinModal} 
        onClose={() => setShowJoinModal(false)} 
      />
    </>
  );
}

function GameCanvas() {
  return (
    <canvas width="900" height="500" className="block max-w-full h-auto border border-gray-100" />
  );
}

function Home() {
  const navigate = useNavigate();
  
  return (
    <>
      <div className="flex justify-center items-center mt-10">
        <div className="flex gap-4">
          {/* Column 1: Quick Match */}
          <div className="-skew-x-[10deg]">
            <button 
              data-testid="quick-play-button"
              onClick={() => navigate('/game-modes/quick-play')}
              className="h-[110px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button">
              <span className="skew-x-[10deg]">QUICK MATCH</span>
            </button>
          </div>
          
          {/* Column 2: Competitive */}
          <div className="-skew-x-[10deg]">
            <button 
              onClick={() => navigate('/game-modes/competitive')}
              className="h-[110px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button">
              <span className="skew-x-[10deg]">COMPETITIVE</span>
            </button>
          </div>
          
          {/* Column 3: Solo & Custom Game */}
          <div className="-skew-x-[10deg] flex flex-col gap-[18px]">
            <button 
              onClick={() => navigate('/game-modes/solo')}
              className="h-[45px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button">
              <span className="skew-x-[10deg]">SOLO</span>
            </button>
            <button 
              data-testid="custom-game-button"
              onClick={() => navigate('/custom')}
              className="h-[45px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button"
            >
              <span className="skew-x-[10deg]">CUSTOM GAME</span>
            </button>
          </div>
        </div>
      </div>
      
      <main className="flex-1 flex justify-center items-center text-center p-5">
        <GameCanvas />
      </main>
    </>
  );
}

function AppContent() {
  const { connect, sendMessage } = useWebSocket();
  const { user, getToken, loading } = useAuth();
  const [wsConnected, setWsConnected] = useState(false);
  
  useEffect(() => {
    // Connect to WebSocket server running in Docker container
    // Send authentication token when connection is established
    connect('ws://localhost:8080/ws', () => {
      console.log('WebSocket connected, checking for auth token...');
      setWsConnected(true);
      const token = getToken();
      if (token) {
        console.log('Sending authentication token on connection');
        sendMessage({ Token: token });
      } else {
        console.log('No auth token found');
      }
    });
  }, [connect, getToken, sendMessage]);
  
  // Also send token when user logs in after WebSocket is already connected
  useEffect(() => {
    if (wsConnected && user) {
      const token = getToken();
      if (token) {
        console.log('User logged in, sending token to existing WebSocket connection');
        sendMessage({ Token: token });
      }
    }
  }, [wsConnected, user, getToken, sendMessage]);

  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <AnimatedRoutes>
        <Route path="/" element={<Home />} />
        <Route path="/game-modes/:category" element={<GameModeSelector />} />
        <Route path="/custom" element={<CustomGameCreator />} />
        <Route path="/game/:gameCode" element={
          <ProtectedRoute>
            <GameLobby />
          </ProtectedRoute>
        } />
        <Route path="/play/:gameId" element={
          <ProtectedRoute>
            <GameArena />
          </ProtectedRoute>
        } />
      </AnimatedRoutes>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <WebSocketProvider>
          <AppContent />
        </WebSocketProvider>
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;