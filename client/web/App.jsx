import React, { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route, useNavigate, Link } from 'react-router-dom';
import './index.css';
import CustomGameCreator from './components/CustomGameCreator.jsx';
import GameLobby from './components/GameLobby.jsx';
import JoinGameModal from './components/JoinGameModal.jsx';
import AuthPage from './components/AuthPage.jsx';
import ProtectedRoute from './components/ProtectedRoute.jsx';
import GameModeSelector from './components/GameModeSelector.jsx';
import { WebSocketProvider, useWebSocket } from './contexts/WebSocketContext.jsx';
import { AuthProvider, useAuth } from './contexts/AuthContext.jsx';

function Header() {
  const [showJoinModal, setShowJoinModal] = useState(false);
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  
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
            <select className="text-black-70 font-bold uppercase tracking-1 bg-transparent border border-black-70 rounded px-3 py-1 cursor-pointer hover:bg-gray-50 transition-colors">
              <option>US East</option>
              <option>US West</option>
              <option>Europe</option>
              <option>Asia</option>
            </select>
            {user ? (
              <div className="flex items-center gap-3">
                <span className="text-black-70 font-bold uppercase">{user.username}</span>
                <button 
                  onClick={logout}
                  className="text-black-70 font-bold uppercase tracking-1 hover:opacity-70 transition-opacity"
                >
                  Logout
                </button>
              </div>
            ) : (
              <button 
                onClick={() => navigate('/auth')}
                className="text-black-70 font-bold uppercase tracking-1 border border-black-70 rounded px-3 py-1 hover:bg-gray-50 transition-colors"
              >
                Login
              </button>
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
  const { user, getToken } = useAuth();
  
  useEffect(() => {
    // Connect to WebSocket server running in Docker container
    connect('ws://localhost:8080/ws');
  }, [connect]);
  
  // Send JWT token when user logs in
  useEffect(() => {
    if (user) {
      const token = getToken();
      if (token) {
        sendMessage({ Token: token });
      }
    }
  }, [user, getToken, sendMessage]);

  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/auth" element={<AuthPage />} />
        <Route path="/game-modes/:category" element={<GameModeSelector />} />
        <Route path="/custom" element={
          <ProtectedRoute>
            <CustomGameCreator />
          </ProtectedRoute>
        } />
        <Route path="/game/:gameCode" element={
          <ProtectedRoute>
            <GameLobby />
          </ProtectedRoute>
        } />
        <Route path="/play/:gameId" element={
          <ProtectedRoute>
            <div className="flex-1 flex justify-center items-center">Game View - Coming Soon</div>
          </ProtectedRoute>
        } />
      </Routes>
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