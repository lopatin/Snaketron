import React, { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route, useNavigate } from 'react-router-dom';
import './index.css';
import CustomGameCreator from './components/CustomGameCreator.jsx';
import GameLobby from './components/GameLobby.jsx';
import JoinGameModal from './components/JoinGameModal.jsx';
import { WebSocketProvider, useWebSocket } from './contexts/WebSocketContext.jsx';

function Header() {
  const [showJoinModal, setShowJoinModal] = useState(false);
  
  return (
    <>
      <header className="bg-white border-t-3 border-b-3 border-white py-5 pb-[18px] site-header">
        <div className="max-w-6xl mx-auto px-5 flex justify-between items-center">
          <div className="flex items-center gap-12">
            <img src="/SnaketronLogo.png" alt="Snaketron" className="h-6 w-auto opacity-80" />
            <nav className="flex gap-6">
              <a href="#" className="text-black-70 font-black italic uppercase tracking-1 opacity-100 underline underline-offset-6">Play</a>
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
          <div>
            <select className="text-black-70 font-bold uppercase tracking-1 bg-transparent border border-black-70 rounded px-3 py-1 cursor-pointer hover:bg-gray-50 transition-colors">
              <option>US East</option>
              <option>US West</option>
              <option>Europe</option>
              <option>Asia</option>
            </select>
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
              className="h-[110px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button">
              <span className="skew-x-[10deg]">QUICK MATCH</span>
            </button>
          </div>
          
          {/* Column 2: Competitive */}
          <div className="-skew-x-[10deg]">
            <button className="h-[110px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button">
              <span className="skew-x-[10deg]">COMPETITIVE</span>
            </button>
          </div>
          
          {/* Column 3: Solo & Custom Game */}
          <div className="-skew-x-[10deg] flex flex-col gap-[18px]">
            <button className="h-[45px] w-[240px] bg-white text-black-70 text-18 font-black italic uppercase tracking-1 cursor-pointer text-center rounded-lg flex items-center justify-center main-menu-button">
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
  const { connect } = useWebSocket();
  
  useEffect(() => {
    // Connect to WebSocket server running in Docker container
    connect('ws://localhost:8080/ws');
  }, [connect]);

  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/custom" element={<CustomGameCreator />} />
        <Route path="/game/:gameCode" element={<GameLobby />} />
        <Route path="/play/:gameId" element={<div className="flex-1 flex justify-center items-center">Game View - Coming Soon</div>} />
      </Routes>
    </div>
  );
}

function App() {
  return (
    <WebSocketProvider>
      <BrowserRouter>
        <AppContent />
      </BrowserRouter>
    </WebSocketProvider>
  );
}

export default App;