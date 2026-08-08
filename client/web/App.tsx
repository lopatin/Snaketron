import React, { useEffect, useState } from 'react';
import { BrowserRouter, HashRouter, Navigate, Route, matchPath, useLocation } from 'react-router-dom';
import './index.css';
import { AccountModal } from './components/AccountModal';
import type { AccountModalView } from './components/AccountModal';
import AuthModal from './components/AuthModal';
import CustomGameCreator from './components/CustomGameCreator';
import GameLobby from './components/GameLobby';
import GameArena from './components/GameArena';
import ProtectedRoute from './components/ProtectedRoute';
import GameModeSelector from './components/GameModeSelector';
import AnimatedRoutes from './components/AnimatedRoutes';
import LobbyInvitePage from './components/LobbyInvitePage';
import { NewHome } from './components/NewHome';
import { ArenaBackdrop, SHOW_BACKDROP_DURING_GAMEPLAY } from './components/ArenaBackdrop';
import { Leaderboard } from './components/Leaderboard';
import { MatchmakingBanner } from './components/MatchmakingBanner';
import { WebSocketProvider } from './contexts/WebSocketContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { UIProvider } from './contexts/UIContext';
import { LatencyProvider } from './contexts/LatencyContext';

function AppContent() {
  const location = useLocation();
  const { user } = useAuth();
  const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);
  const [accountModalView, setAccountModalView] = useState<AccountModalView | null>(null);
  const isGameArenaActive = matchPath('/play/:gameId', location.pathname) !== null;
  const showBackdrop = SHOW_BACKDROP_DURING_GAMEPLAY || !isGameArenaActive;

  useEffect(() => {
    if (location.pathname === '/auth') {
      setIsAuthModalOpen(true);
    }
  }, [location.pathname]);

  useEffect(() => {
    if (!user || user.isGuest) {
      setAccountModalView(null);
    }
  }, [user]);

  return (
    <div className="min-h-screen flex flex-col">
      {showBackdrop && <ArenaBackdrop />}
      <MatchmakingBanner />
      <AnimatedRoutes>
        <Route
          path="/"
          element={
            <NewHome
              onOpenAuth={() => setIsAuthModalOpen(true)}
              onOpenAccount={setAccountModalView}
            />
          }
        />
        <Route path="/auth" element={<Navigate to="/" replace />} />
        <Route
          path="/leaderboards"
          element={
            <Leaderboard
              onOpenAuth={() => setIsAuthModalOpen(true)}
              onOpenAccount={setAccountModalView}
            />
          }
        />
        <Route path="/profile" element={<Navigate to="/" replace />} />
        <Route path="/history" element={<Navigate to="/" replace />} />
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
      <AuthModal
        isOpen={isAuthModalOpen}
        onClose={() => setIsAuthModalOpen(false)}
      />
      <AccountModal
        view={accountModalView}
        user={user}
        onClose={() => setAccountModalView(null)}
      />
    </div>
  );
}

// The itch.io build is served from a static path with no History-API
// fallback, so client routes live in the URL hash there. The regular build
// keeps clean History-API URLs.
const Router = process.env.ITCH_BUILD === 'true' ? HashRouter : BrowserRouter;

function App() {
  return (
    <Router>
      <AuthProvider>
        <UIProvider>
          <LatencyProvider>
            <WebSocketProvider>
              <AppContent />
            </WebSocketProvider>
          </LatencyProvider>
        </UIProvider>
      </AuthProvider>
    </Router>
  );
}

export default App;
