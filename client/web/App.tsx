import React from 'react';
import { BrowserRouter, Route } from 'react-router-dom';
import './index.css';
import Auth from './components/Auth';
import CustomGameCreator from './components/CustomGameCreator';
import GameLobby from './components/GameLobby';
import GameArena from './components/GameArena';
import ProtectedRoute from './components/ProtectedRoute';
import GameModeSelector from './components/GameModeSelector';
import AnimatedRoutes from './components/AnimatedRoutes';
import LobbyInvitePage from './components/LobbyInvitePage';
import { NewHome } from './components/NewHome';
import { Leaderboard } from './components/Leaderboard';
import { MatchmakingBanner } from './components/MatchmakingBanner';
import { WebSocketProvider } from './contexts/WebSocketContext';
import { AuthProvider } from './contexts/AuthContext';
import { UIProvider } from './contexts/UIContext';
import { LatencyProvider } from './contexts/LatencyContext';

function AppContent() {
  return (
    <div className="min-h-screen flex flex-col">
      <MatchmakingBanner />
      <AnimatedRoutes>
        <Route path="/" element={<NewHome />} />
        <Route path="/auth" element={<Auth />} />
        <Route path="/leaderboards" element={<Leaderboard />} />
        <Route path="/game-modes/:category" element={<GameModeSelector />} />
        <Route path="/custom" element={<CustomGameCreator />} />
        <Route path="/lobby/:lobbyCode" element={<LobbyInvitePage />} />
        <Route
          path="/game/:gameCode"
          element={
            <ProtectedRoute>
              <GameLobby />
            </ProtectedRoute>
          }
        />
        <Route
          path="/play/:gameId"
          element={
            <ProtectedRoute>
              <GameArena />
            </ProtectedRoute>
          }
        />
      </AnimatedRoutes>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <UIProvider>
          <LatencyProvider>
            <WebSocketProvider>
              <AppContent />
            </WebSocketProvider>
          </LatencyProvider>
        </UIProvider>
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;
