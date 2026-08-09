import React, { useCallback, useEffect, useState } from 'react';
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
import { CrazyGamesProvider, useCrazyGames } from './contexts/CrazyGamesContext';
import { CrazyGamesAdOverlay, CrazyGamesBridge } from './components/CrazyGamesBridge';

// Design-review harness for the post-match rating reveal. Only reachable —
// and only bundled — outside production builds.
const RatingRevealQA = process.env.NODE_ENV !== 'production'
  ? React.lazy(() => import('./components/RatingRevealQA'))
  : null;

function AppContent() {
  const location = useLocation();
  const { user } = useAuth();
  const { isCrazyGamesBuild, showAuthPrompt } = useCrazyGames();
  const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);
  const [accountModalView, setAccountModalView] = useState<AccountModalView | null>(null);
  const isGameArenaActive = matchPath('/play/:gameId', location.pathname) !== null;
  const showBackdrop = SHOW_BACKDROP_DURING_GAMEPLAY || !isGameArenaActive;

  const handleOpenAuth = useCallback(() => {
    if (isCrazyGamesBuild) {
      void showAuthPrompt();
      return;
    }
    setIsAuthModalOpen(true);
  }, [isCrazyGamesBuild, showAuthPrompt]);

  useEffect(() => {
    if (location.pathname === '/auth' && !isCrazyGamesBuild) {
      setIsAuthModalOpen(true);
    }
  }, [isCrazyGamesBuild, location.pathname]);

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
              onOpenAuth={handleOpenAuth}
              onOpenAccount={setAccountModalView}
            />
          }
        />
        <Route path="/auth" element={<Navigate to="/" replace />} />
        <Route
          path="/leaderboards"
          element={
            <Leaderboard
              onOpenAuth={handleOpenAuth}
              onOpenAccount={setAccountModalView}
            />
          }
        />
        <Route path="/profile" element={<Navigate to="/" replace />} />
        <Route path="/history" element={<Navigate to="/" replace />} />
        <Route
          path="/game-modes/:category"
          element={isCrazyGamesBuild ? <Navigate to="/" replace /> : <GameModeSelector />}
        />
        <Route
          path="/custom"
          element={isCrazyGamesBuild ? <Navigate to="/" replace /> : <CustomGameCreator />}
        />
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
        {RatingRevealQA && (
          <Route
            path="/qa/rating-reveal"
            element={
              <React.Suspense fallback={null}>
                <RatingRevealQA />
              </React.Suspense>
            }
          />
        )}
      </AnimatedRoutes>
      <AuthModal
        isOpen={isAuthModalOpen && !isCrazyGamesBuild}
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

// Embedded static builds are served from deep paths with no History-API
// fallback, so client routes live in the URL hash there. The regular build
// keeps clean History-API URLs.
const Router = process.env.ITCH_BUILD === 'true' || process.env.CRAZYGAMES_BUILD === 'true'
  ? HashRouter
  : BrowserRouter;

function App() {
  return (
    <Router>
      <CrazyGamesProvider>
        <AuthProvider>
          <UIProvider>
            <LatencyProvider>
              <WebSocketProvider>
                <CrazyGamesBridge />
                <AppContent />
                <CrazyGamesAdOverlay />
              </WebSocketProvider>
            </LatencyProvider>
          </UIProvider>
        </AuthProvider>
      </CrazyGamesProvider>
    </Router>
  );
}

export default App;
