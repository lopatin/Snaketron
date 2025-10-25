import React, { useEffect, useRef } from 'react';
import { BrowserRouter, Route } from 'react-router-dom';
import './index.css';
import Auth from './components/Auth';
import CustomGameCreator from './components/CustomGameCreator';
import GameLobby from './components/GameLobby';
import GameArena from './components/GameArena';
import Queue from './components/Queue';
import ProtectedRoute from './components/ProtectedRoute';
import GameModeSelector from './components/GameModeSelector';
import AnimatedRoutes from './components/AnimatedRoutes';
import LobbyInvitePage from './components/LobbyInvitePage';
import { NewHome } from './components/NewHome';
import { WebSocketProvider, useWebSocket } from './contexts/WebSocketContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { UIProvider } from './contexts/UIContext';
import { LatencyProvider } from './contexts/LatencyContext';

function AppContent() {
  const { sendMessage, isConnected, disconnect } = useWebSocket();
  const { user, getToken } = useAuth();
  const tokenSentRef = useRef<boolean>(false);
  const previousUserRef = useRef<typeof user>(null);

  // Detect guest-to-full-user transition and reconnect WebSocket
  useEffect(() => {
    const wasGuest = previousUserRef.current?.isGuest;
    const isNowFullUser = user && !user.isGuest;

    if (wasGuest && isNowFullUser) {
      console.log('Guest transitioned to full user, reconnecting WebSocket');
      // Disconnect existing WebSocket to force reconnection with new token
      disconnect();
      tokenSentRef.current = false;
    }

    // Update previous user reference
    previousUserRef.current = user;
  }, [user, disconnect]);

  // Send authentication token when WebSocket connects
  useEffect(() => {
    if (isConnected && !tokenSentRef.current) {
      const token = getToken();
      if (token) {
        console.log('Sending authentication token on connection');
        sendMessage({ Token: token });
        tokenSentRef.current = true;
      } else {
        console.log('No auth token found');
      }
    }
  }, [isConnected, getToken, sendMessage]);

  // Also send token when user logs in after WebSocket is already connected
  useEffect(() => {
    if (isConnected && user && !tokenSentRef.current) {
      const token = getToken();
      if (token) {
        console.log('User logged in, sending token to existing WebSocket connection');
        sendMessage({ Token: token });
        tokenSentRef.current = true;
      }
    }
  }, [isConnected, user, getToken, sendMessage]);

  // Reset token sent flag when WebSocket disconnects
  useEffect(() => {
    if (!isConnected) {
      tokenSentRef.current = false;
      console.log('WebSocket disconnected, resetting token sent flag');
    }
  }, [isConnected]);

  return (
    <div className="min-h-screen flex flex-col">
      <AnimatedRoutes>
        <Route path="/" element={<NewHome />} />
        <Route path="/auth" element={<Auth />} />
        <Route path="/game-modes/:category" element={<GameModeSelector />} />
        <Route path="/custom" element={<CustomGameCreator />} />
        <Route path="/lobby/:lobbyCode" element={<LobbyInvitePage />} />
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
        <Route path="/queue" element={
          <ProtectedRoute>
            <Queue />
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
