import React, { useEffect, useState } from 'react';
import { BrowserRouter, Navigate, Route, matchPath, useLocation } from 'react-router-dom';
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
import { WebSocketProvider, useWebSocket } from './contexts/WebSocketContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { UIProvider } from './contexts/UIContext';
import { LatencyProvider } from './contexts/LatencyContext';

function AppContent() {
  const location = useLocation();
  const { user } = useAuth();
  const { clientUpdateRequired } = useWebSocket();
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

  if (clientUpdateRequired) {
    return (
      <main
        className="relative z-50 flex min-h-screen items-center justify-center bg-white px-6 text-center"
        role="alert"
        data-testid="client-update-required"
      >
        <div className="max-w-md border-4 border-black p-8 shadow-[8px_8px_0_#000]">
          <h1 className="text-3xl font-black uppercase">Client update required</h1>
          <p className="mt-4 text-sm font-semibold">
            Snaketron was updated. Reload to use the current gameplay protocol.
          </p>
          <button
            type="button"
            className="mt-6 border-2 border-black bg-black px-5 py-3 font-black uppercase text-white"
            onClick={() => window.location.reload()}
          >
            Reload now
          </button>
        </div>
      </main>
    );
  }

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
