import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import {
  WebSocketContextType,
  Lobby,
  LobbyMember,
  ChatMessage,
  ChatScope,
  LobbyPreferences,
  LobbyGameMode,
  LobbyState,
  User,
} from '../types';
import { clockSync } from '../utils/clockSync';
import { useLatency } from './LatencyContext';
import { useAuth } from './AuthContext';
import {
  detectBestRegion,
  fetchRegionMetadata,
  loadRegionPreference,
  saveRegionPreference,
} from '../utils/regionPreference';

interface WebSocketProviderProps {
  children: React.ReactNode;
}

interface MessageHandler {
  (message: { type: string; data: any }): void;
}

// Extend window interface for testing
declare global {
  interface Window {
    __wsInstance?: WebSocket;
    __wsContext?: WebSocketContextType;
  }
}

const WebSocketContext = createContext<WebSocketContextType | null>(null);

const LOBBY_STORAGE_KEY = 'snaketron:lastLobby';
const MAX_CHAT_HISTORY = 200;
const VALID_LOBBY_MODES: LobbyGameMode[] = ['duel', '2v2', 'solo', 'ffa'];
const VALID_LOBBY_STATES: LobbyState[] = ['waiting', 'queued', 'matched'];

const DEFAULT_LOBBY_PREFERENCES: LobbyPreferences = {
  selectedModes: ['duel'],
  competitive: false,
};

const normalizeLobbyPreferences = (payload: any): LobbyPreferences => {
  if (!payload || typeof payload !== 'object') {
    return DEFAULT_LOBBY_PREFERENCES;
  }

  const rawModes = Array.isArray(payload.selected_modes ?? payload.selectedModes)
    ? payload.selected_modes ?? payload.selectedModes
    : [];

  const normalized = new Set<LobbyGameMode>();
  for (const value of rawModes) {
    if (typeof value !== 'string') {
      continue;
    }
    const lower = value.trim().toLowerCase();
    if (VALID_LOBBY_MODES.includes(lower as LobbyGameMode)) {
      normalized.add(lower as LobbyGameMode);
    }
  }

  const ordered: LobbyGameMode[] = [];
  for (const mode of VALID_LOBBY_MODES) {
    if (normalized.has(mode)) {
      ordered.push(mode);
    }
  }

  const hasSelection = ordered.length > 0;
  return {
    selectedModes: hasSelection ? ordered : DEFAULT_LOBBY_PREFERENCES.selectedModes,
    competitive: Boolean(payload.competitive),
  };
};

interface StoredLobbyInfo {
  code: string;
  id?: number;
}

export const useWebSocket = (): WebSocketContextType => {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocket must be used within WebSocketProvider');
  }
  return context;
};

export const WebSocketProvider: React.FC<WebSocketProviderProps> = ({ children }) => {
  const [isConnected, setIsConnected] = useState(false);
  const [latencyMs, setLatencyMs] = useState<number>(0);
  const [currentRegionUrl, setCurrentRegionUrl] = useState<string | null>(null);
  const [currentLobby, setCurrentLobby] = useState<Lobby | null>(null);
  const [lobbyMembers, setLobbyMembers] = useState<LobbyMember[]>([]);
  const [lobbyChatMessages, setLobbyChatMessages] = useState<ChatMessage[]>([]);
  const [gameChatMessages, setGameChatMessages] = useState<ChatMessage[]>([]);
  const [lobbyPreferences, setLobbyPreferences] = useState<LobbyPreferences>(DEFAULT_LOBBY_PREFERENCES);
  const [isSessionAuthenticated, setIsSessionAuthenticated] = useState(false);
  const currentLobbyRef = useRef<Lobby | null>(null);
  const ws = useRef<WebSocket | null>(null);
  const messageHandlers = useRef<Map<string, MessageHandler[]>>(new Map());
  const reconnectTimeout = useRef<NodeJS.Timeout | null>(null);
  const onConnectCallback = useRef<(() => void) | null>(null);
  const syncRequestTimes = useRef<Map<number, number>>(new Map());
  const isInitializingRef = useRef(false);
  const storedLobbyRef = useRef<StoredLobbyInfo | null>(null);
  const hasLoadedStoredLobbyRef = useRef(false);
  const restoreInProgressRef = useRef(false);
  const lobbyChatLobbyIdRef = useRef<number | null>(null);
  const gameChatIdRef = useRef<number | null>(null);
  const { settings: latencySettings } = useLatency();
  const { user, getToken } = useAuth();
  const authHandshakeRef = useRef(false);
  const lastAuthTokenRef = useRef<string | null>(null);
  const previousUserRef = useRef<User | null>(null);

  const setAuthHandshakeState = useCallback((value: boolean) => {
    if (authHandshakeRef.current !== value) {
      authHandshakeRef.current = value;
      setIsSessionAuthenticated(value);
    }
  }, []);

  useEffect(() => {
    if (hasLoadedStoredLobbyRef.current) {
      return;
    }

    if (typeof window === 'undefined') {
      hasLoadedStoredLobbyRef.current = true;
      return;
    }

    try {
      const raw = window.localStorage.getItem(LOBBY_STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed.code === 'string' && parsed.code.trim()) {
          storedLobbyRef.current = {
            code: parsed.code.toUpperCase(),
            id: typeof parsed.id === 'number' ? parsed.id : undefined,
          };
        }
      }
    } catch (error) {
      console.warn('Failed to load stored lobby info, clearing persisted data', error);
      try {
        window.localStorage.removeItem(LOBBY_STORAGE_KEY);
      } catch {
        // Ignore removal errors
      }
    } finally {
      hasLoadedStoredLobbyRef.current = true;
    }
  }, []);

  const persistLobby = useCallback((lobby: { id: number; code: string }) => {
    storedLobbyRef.current = { id: lobby.id, code: lobby.code.toUpperCase() };

    if (typeof window === 'undefined') {
      return;
    }

    try {
      window.localStorage.setItem(
        LOBBY_STORAGE_KEY,
        JSON.stringify({ id: lobby.id, code: lobby.code.toUpperCase() })
      );
    } catch (error) {
      console.warn('Failed to persist lobby info', error);
    }
  }, []);

  const clearPersistedLobby = useCallback(() => {
    storedLobbyRef.current = null;

    if (typeof window === 'undefined') {
      return;
    }

    try {
      window.localStorage.removeItem(LOBBY_STORAGE_KEY);
    } catch (error) {
      console.warn('Failed to clear stored lobby info', error);
    }
  }, []);

  const resetLobbyState = useCallback(() => {
    setCurrentLobby(null);
    currentLobbyRef.current = null;
    setLobbyMembers([]);
    setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
    clearPersistedLobby();
  }, [clearPersistedLobby]);

  const isLobbyMissingReason = useCallback((reason: string) => {
    if (!reason || typeof reason !== 'string') {
      return false;
    }
    const normalized = reason.toLowerCase();
    if (!normalized.includes('lobby')) {
      return false;
    }
    return (
      normalized.includes('not found') ||
      normalized.includes('does not exist') ||
      normalized.includes('missing')
    );
  }, []);

  const connect = useCallback((url: string, onConnect?: () => void) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      console.log('WebSocket already connected');
      return;
    }

    // Store the onConnect callback
    if (onConnect) {
      onConnectCallback.current = onConnect;
    }

    try {
      ws.current = new WebSocket(url);
      setCurrentRegionUrl(url);

      ws.current.onopen = () => {
        console.log('WebSocket connected to:', url);
        setIsConnected(true);
        if (reconnectTimeout.current) {
          clearTimeout(reconnectTimeout.current);
          reconnectTimeout.current = null;
        }
        // Expose for testing
        if (typeof window !== 'undefined') {
          window.__wsInstance = ws.current || undefined;
        }
        
        // Set up clock sync callback
        clockSync.setOnSyncRequest((clientTime) => {
          if (ws.current?.readyState === WebSocket.OPEN) {
            syncRequestTimes.current.set(clientTime, clientTime);
            ws.current.send(JSON.stringify({
              Ping: { client_time: clientTime }
            }));
          }
        });

        // Start clock synchronization
        clockSync.start();
        
        // Call the onConnect callback if provided
        if (onConnectCallback.current) {
          onConnectCallback.current();
        }
      };

      ws.current.onclose = () => {
        console.log('WebSocket disconnected');
        setIsConnected(false);
        // Reset clock sync
        clockSync.reset();
        syncRequestTimes.current.clear();
        // Auto-reconnect after 2 seconds
        reconnectTimeout.current = setTimeout(() => {
          console.log('Attempting to reconnect...');
          connect(url, onConnect);
        }, 2000);
      };

      ws.current.onerror = (error: Event) => {
        console.error('WebSocket error:', error);
      };

      ws.current.onmessage = (event: MessageEvent) => {
        // Apply artificial receive delay if enabled
        const processMessage = () => {
          try {
            const rawMessage = JSON.parse(event.data);
            console.log('WebSocket message received:', rawMessage);

            // Handle Pong response carrying clock synchronization data
            if (rawMessage?.Pong) {
              const { client_time, server_time } = rawMessage.Pong;
              const t1 = syncRequestTimes.current.get(client_time);
              if (t1) {
                syncRequestTimes.current.delete(client_time);
                const t3 = Date.now();
                clockSync.processSyncResponse(t1, server_time, t3);
                const rtt = t3 - t1;
                setLatencyMs(Math.round(rtt / 2));
              }
              return;
            }

            let messageType: string | null = null;
            let messageData: any = undefined;

            if (typeof rawMessage === 'string') {
              messageType = rawMessage;
              messageData = null;
            } else if (rawMessage && typeof rawMessage === 'object') {
              const keys = Object.keys(rawMessage);
              if (keys.length === 1) {
                messageType = keys[0];
                messageData = rawMessage[messageType];
              }
            }

            if (!messageType) {
              console.warn('Unexpected WebSocket message shape', rawMessage);
              return;
            }

            const resolvedType = messageType as string;
            const handlers = messageHandlers.current.get(resolvedType) || [];
            handlers.forEach((handler: MessageHandler) => handler({ type: resolvedType, data: messageData }));
          } catch (error) {
            console.error('Failed to parse WebSocket message:', error);
          }
        };

        if (latencySettings.enabled && latencySettings.receiveDelayMs > 0) {
          console.log(`Applying artificial receive delay: ${latencySettings.receiveDelayMs}ms`);
          setTimeout(processMessage, latencySettings.receiveDelayMs);
        } else {
          processMessage();
        }
      };
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
    }
  }, []);

  const disconnect = useCallback(() => {
    if (reconnectTimeout.current) {
      clearTimeout(reconnectTimeout.current);
      reconnectTimeout.current = null;
    }
    syncRequestTimes.current.clear();
    if (ws.current) {
      ws.current.close();
      ws.current = null;
    }
  }, []);

  const connectToRegion = useCallback((wsUrl: string, options?: { regionId?: string; origin?: string }) => {
    console.log('Switching to region:', wsUrl);

    // Disconnect existing connection
    if (ws.current) {
      console.log('Closing existing WebSocket connection');
      ws.current.close();
      ws.current = null;
    }

    // Clear any pending reconnection
    if (reconnectTimeout.current) {
      clearTimeout(reconnectTimeout.current);
      reconnectTimeout.current = null;
    }

    if (options?.regionId) {
      saveRegionPreference({
        regionId: options.regionId,
        wsUrl,
        origin: options.origin,
        timestamp: Date.now(),
      });
    }

    // Connect to new region. Authentication is handled automatically on connection.
    connect(wsUrl, onConnectCallback.current || undefined);
  }, [connect]);

  const sendMessage = useCallback((message: any) => {
    const doSend = () => {
      if (ws.current?.readyState === WebSocket.OPEN) {
        ws.current.send(JSON.stringify(message));
        console.log('WebSocket message sent:', message);
      } else {
        console.error('WebSocket is not connected');
      }
    };

    if (latencySettings.enabled && latencySettings.sendDelayMs > 0) {
      console.log(`Applying artificial send delay: ${latencySettings.sendDelayMs}ms`);
      setTimeout(doSend, latencySettings.sendDelayMs);
    } else {
      doSend();
    }
  }, [latencySettings]);

  const authenticateConnection = useCallback(() => {
    if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
      return false;
    }

    const token = getToken();
    if (!token) {
      console.warn('No auth token available for WebSocket authentication');
      return false;
    }

    if (authHandshakeRef.current && lastAuthTokenRef.current === token) {
      return true;
    }

    console.log('Authenticating WebSocket connection');
    sendMessage({ Token: token });
    lastAuthTokenRef.current = token;
    setAuthHandshakeState(true);
    return true;
  }, [getToken, sendMessage, setAuthHandshakeState]);

  const sendChatMessage = useCallback((scope: ChatScope, message: string) => {
    const trimmed = message.trim();
    if (!trimmed) {
      return;
    }

    console.log(`Sending ${scope} chat message`, trimmed);
    sendMessage({ Chat: trimmed });
  }, [sendMessage]);

  const onMessage = useCallback((messageType: string, handler: MessageHandler) => {
    if (!messageHandlers.current.has(messageType)) {
      messageHandlers.current.set(messageType, []);
    }
    messageHandlers.current.get(messageType)!.push(handler);

    // Return cleanup function
    return () => {
      const handlers = messageHandlers.current.get(messageType) || [];
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
      }
    };
  }, []);

  useEffect(() => {
    if (!isConnected) {
      setAuthHandshakeState(false);
      lastAuthTokenRef.current = null;
      return;
    }

    authenticateConnection();
  }, [isConnected, authenticateConnection, setAuthHandshakeState]);

  useEffect(() => {
    const previous = previousUserRef.current;
    if (previous?.isGuest && user && !user.isGuest) {
      console.log('Guest transitioned to full user, reconnecting WebSocket');
      previousUserRef.current = user;
      disconnect();
      return;
    }

    previousUserRef.current = user;

    if (!isConnected || !isSessionAuthenticated) {
      return;
    }

    const token = getToken();
    if (!token) {
      setAuthHandshakeState(false);
      lastAuthTokenRef.current = null;
      return;
    }

    if (token !== lastAuthTokenRef.current) {
      setAuthHandshakeState(false);
      authenticateConnection();
    }
  }, [user, isConnected, isSessionAuthenticated, disconnect, getToken, authenticateConnection, setAuthHandshakeState]);

  // Auto-connect to the preferred or closest region on mount
  useEffect(() => {
    let cancelled = false;

    const ensureConnected = async () => {
      if (typeof window === 'undefined') {
        return;
      }

      if (ws.current && (ws.current.readyState === WebSocket.OPEN || ws.current.readyState === WebSocket.CONNECTING)) {
        return;
      }

      if (isInitializingRef.current) {
        return;
      }

      isInitializingRef.current = true;

      try {
        const storedPreference = loadRegionPreference();
        if (storedPreference?.regionId) {
          if (storedPreference.wsUrl) {
            if (!cancelled) {
              connectToRegion(storedPreference.wsUrl, {
                regionId: storedPreference.regionId,
                origin: storedPreference.origin,
              });
            }
            return;
          }

          try {
            const metadata = await fetchRegionMetadata();
            const matched = metadata.find(region => region.id === storedPreference.regionId);
            if (matched && !cancelled) {
              const repairedPreference = {
                regionId: matched.id,
                wsUrl: matched.ws_url,
                origin: matched.origin,
                timestamp: Date.now(),
              };
              saveRegionPreference(repairedPreference);
              connectToRegion(repairedPreference.wsUrl, {
                regionId: repairedPreference.regionId,
                origin: repairedPreference.origin,
              });
              return;
            }
          } catch (error) {
            console.error('Failed to repair legacy region preference:', error);
          }
        }

        const detected = await detectBestRegion();
        if (detected && !cancelled) {
          saveRegionPreference(detected.preference);
          connectToRegion(detected.preference.wsUrl!, {
            regionId: detected.preference.regionId,
            origin: detected.preference.origin,
          });
        }
      } finally {
        isInitializingRef.current = false;
      }
    };

    ensureConnected();

    return () => {
      cancelled = true;
    };
  }, [connectToRegion]);

  // Lobby methods
  const createLobby = useCallback(async () => {
    return new Promise<void>((resolve, reject) => {
      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      // Set up one-time handler for LobbyCreated message
      const cleanup = onMessage('LobbyCreated', (message: any) => {
        if (settled) {
          return;
        }

        const { lobby_id, lobby_code } = message.data;
        const normalizedCode = lobby_code.toUpperCase();
        const newLobby: Lobby = {
          id: lobby_id,
          code: normalizedCode,
          hostUserId: 0, // Will be set by LobbyUpdate
          region: '', // Will be set by LobbyUpdate
          state: 'waiting',
        };
        currentLobbyRef.current = newLobby;
        setCurrentLobby(newLobby);
        setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
        persistLobby({ id: lobby_id, code: normalizedCode });
        cleanup();
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        settled = true;
        resolve();
      });

      // Send CreateLobby message
      sendMessage('CreateLobby');

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        reject(new Error('Timeout waiting for lobby creation'));
      }, 5000);
    });
  }, [onMessage, sendMessage, persistLobby]);

  const joinLobby = useCallback(async (lobbyCode: string) => {
    const normalizedCode = lobbyCode.toUpperCase();
    return new Promise<void>((resolve, reject) => {
      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      const cleanupHandlers = () => {
        cleanupJoined();
        cleanupDenied();
        cleanupMismatch();
        cleanupUpdate();
        if (timeoutId) {
          clearTimeout(timeoutId);
          timeoutId = null;
        }
      };

      const handleSuccess = (lobbyId: number, hostUserId?: number) => {
        if (settled) {
          return;
        }
        settled = true;
        const joinedLobby: Lobby = {
          id: lobbyId,
          code: normalizedCode,
          hostUserId: hostUserId ?? 0, // Will be refined by future LobbyUpdate messages
          region: '', // Will be set by LobbyUpdate
          state: 'waiting',
        };
        currentLobbyRef.current = joinedLobby;
        setCurrentLobby(joinedLobby);
        setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
        persistLobby({ id: lobbyId, code: normalizedCode });
        cleanupHandlers();
        resolve();
      };

      // Set up handlers for possible responses
      let cleanupJoined = () => {};
      let cleanupDenied = () => {};
      let cleanupMismatch = () => {};
      let cleanupUpdate = () => {};

      cleanupJoined = onMessage('JoinedLobby', (message: any) => {
        if (settled) {
          return;
        }
        const { lobby_id } = message.data;
        handleSuccess(lobby_id);
      });

      cleanupDenied = onMessage('AccessDenied', (message: any) => {
        if (settled) {
          return;
        }
        const reason =
          typeof message?.data?.reason === 'string' ? message.data.reason : '';
        if (isLobbyMissingReason(reason)) {
          resetLobbyState();
        }
        settled = true;
        cleanupHandlers();
        reject(new Error(reason || 'Access denied'));
      });

      cleanupMismatch = onMessage('LobbyRegionMismatch', (message: any) => {
        const { target_region, ws_url, lobby_code: code } = message.data;
        console.log(`Lobby is in region ${target_region}, reconnecting to ${ws_url}`);

        if (settled) {
          return;
        }

        // Clean up handlers before reconnecting
        cleanupHandlers();

        // Reconnect to the correct region
        connectToRegion(ws_url, { regionId: target_region });

        // After reconnecting, retry joining
        // This will be handled by the onConnect callback
        setTimeout(() => {
          joinLobby(code).then(resolve).catch(reject);
        }, 1000);
      });

      cleanupUpdate = onMessage('LobbyUpdate', (message: any) => {
        if (settled) {
          return;
        }
        const { lobby_id, host_user_id } = message.data;
        handleSuccess(lobby_id, host_user_id);
      });

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanupHandlers();
        reject(new Error('Timeout waiting to join lobby'));
      }, 5000);

      sendMessage({ JoinLobby: { lobby_code: normalizedCode } });
    });
  }, [onMessage, sendMessage, connectToRegion, persistLobby, resetLobbyState, isLobbyMissingReason]);

  const leaveLobby = useCallback(async () => {
    return new Promise<void>((resolve, reject) => {
      if (!ws.current || ws.current.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      // Set up one-time handler for LeftLobby message
      const cleanup = onMessage('LeftLobby', () => {
        if (settled) {
          return;
        }
        resetLobbyState();
        cleanup();
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        settled = true;
        resolve();
      });

      // Send LeaveLobby message
      sendMessage('LeaveLobby');

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        reject(new Error('Timeout waiting to leave lobby'));
      }, 5000);
    });
  }, [onMessage, sendMessage, resetLobbyState]);

  const updateLobbyPreferences = useCallback(
    (preferences: LobbyPreferences) => {
      setLobbyPreferences(preferences);

      if (!currentLobbyRef.current) {
        return;
      }

      sendMessage({
        UpdateLobbyPreferences: {
          selected_modes: preferences.selectedModes,
          competitive: preferences.competitive,
        },
      });
    },
    [sendMessage],
  );

  useEffect(() => {
    const cleanup = onMessage('LobbyChatMessage', (message: any) => {
      const payload = message?.data ?? message?.LobbyChatMessage ?? message;
      if (!payload || typeof payload.message !== 'string') {
        return;
      }

      const lobbyId =
        typeof payload.lobby_id === 'number'
          ? payload.lobby_id
          : typeof payload.lobby_id === 'string'
            ? parseInt(payload.lobby_id, 10)
            : undefined;
      const timestampMs =
        typeof payload.timestamp_ms === 'number'
          ? payload.timestamp_ms
          : Date.now();
      const rawUsername =
        typeof payload.username === 'string' && payload.username.trim()
          ? payload.username.trim()
          : null;
      const rawUserId =
        typeof payload.user_id === 'number'
          ? payload.user_id
          : typeof payload.user_id === 'string'
            ? parseInt(payload.user_id, 10)
            : null;
      const messageId =
        typeof payload.message_id === 'string'
          ? payload.message_id
          : `${lobbyId ?? 'lobby'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

      const normalized: ChatMessage = {
        id: messageId,
        scope: 'lobby',
        lobbyId,
        gameId: undefined,
        userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
        username: rawUsername,
        message: payload.message,
        type: rawUsername ? 'user' : 'system',
        timestamp: new Date(timestampMs),
      };

      setLobbyChatMessages((previous) => {
        const isNewLobby = typeof lobbyId === 'number' && lobbyChatLobbyIdRef.current !== lobbyId;
        if (isNewLobby) {
          lobbyChatLobbyIdRef.current = lobbyId;
        }
        const base = isNewLobby ? [] : previous;
        const next = [...base, normalized];
        if (next.length > MAX_CHAT_HISTORY) {
          return next.slice(next.length - MAX_CHAT_HISTORY);
        }
        return next;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    return onMessage('AccessDenied', (message: any) => {
      const reason =
        typeof message?.data?.reason === 'string' ? message.data.reason : '';
      if (isLobbyMissingReason(reason)) {
        resetLobbyState();
      }
    });
  }, [onMessage, resetLobbyState, isLobbyMissingReason]);

  useEffect(() => {
    const cleanup = onMessage('LobbyChatHistory', (message: any) => {
      const payload = message?.data ?? message?.LobbyChatHistory ?? message;
      if (!payload) {
        return;
      }

      const lobbyIdRaw = payload.lobby_id;
      const lobbyId =
        typeof lobbyIdRaw === 'number'
          ? lobbyIdRaw
          : typeof lobbyIdRaw === 'string'
            ? parseInt(lobbyIdRaw, 10)
            : undefined;
      const messagesArray = Array.isArray(payload.messages) ? payload.messages : [];

      const normalized = messagesArray
        .map((entry: any) => {
          if (!entry || typeof entry.message !== 'string') {
            return null;
          }

          const timestampMs =
            typeof entry.timestamp_ms === 'number' ? entry.timestamp_ms : Date.now();
          const rawUsername =
            typeof entry.username === 'string' && entry.username.trim()
              ? entry.username.trim()
              : null;
          const rawUserId =
            typeof entry.user_id === 'number'
              ? entry.user_id
              : typeof entry.user_id === 'string'
                ? parseInt(entry.user_id, 10)
                : null;
          const messageId =
            typeof entry.message_id === 'string'
              ? entry.message_id
              : `${lobbyId ?? 'lobby'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

          const chatMessage: ChatMessage = {
            id: messageId,
            scope: 'lobby' as const,
            lobbyId,
            gameId: undefined,
            userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
            username: rawUsername,
            message: entry.message,
            type: rawUsername ? 'user' : 'system',
            timestamp: new Date(timestampMs),
          };
          return chatMessage;
        })
        .filter((entry): entry is ChatMessage => entry !== null)
        .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

      if (typeof lobbyId === 'number' && Number.isFinite(lobbyId)) {
        lobbyChatLobbyIdRef.current = lobbyId;
      } else {
        lobbyChatLobbyIdRef.current = null;
      }

      setLobbyChatMessages(() => {
        if (normalized.length > MAX_CHAT_HISTORY) {
          return normalized.slice(normalized.length - MAX_CHAT_HISTORY);
        }
        return normalized;
      });
    });

    return cleanup;
  }, [onMessage]);

  // Handle lobby updates
  useEffect(() => {
    const cleanup = onMessage('LobbyUpdate', (message: any) => {
      const payload = message?.data ?? message?.LobbyUpdate ?? message;
      if (!payload || typeof payload !== 'object') {
        return;
      }

      const lobbyId =
        typeof payload.lobby_id === 'number'
          ? payload.lobby_id
          : typeof payload.lobby_id === 'string'
            ? parseInt(payload.lobby_id, 10)
            : NaN;
      const lobbyCode =
        typeof payload.lobby_code === 'string' && payload.lobby_code.trim()
          ? payload.lobby_code.trim().toUpperCase()
          : null;

      const currentLobbySnapshot = currentLobbyRef.current;
      if (!currentLobbySnapshot) {
        return;
      }

      const matchesById =
        Number.isFinite(lobbyId) && currentLobbySnapshot.id === lobbyId;
      const matchesByCode =
        lobbyCode !== null &&
        typeof currentLobbySnapshot.code === 'string' &&
        currentLobbySnapshot.code.toUpperCase() === lobbyCode;

      if (!matchesById && !matchesByCode) {
        return;
      }

      const members = Array.isArray(payload.members) ? payload.members : [];
      setLobbyMembers(members);

      const hostUserId =
        typeof payload.host_user_id === 'number' ? payload.host_user_id : 0;
      const rawState =
        typeof payload.state === 'string' ? payload.state.trim().toLowerCase() : '';
      const lobbyState: LobbyState = VALID_LOBBY_STATES.includes(rawState as LobbyState)
        ? (rawState as LobbyState)
        : 'waiting';

      setLobbyPreferences(normalizeLobbyPreferences(payload.preferences));

      setCurrentLobby((previous) => {
        if (!previous) {
          return previous;
        }

        const previousMatchesById =
          Number.isFinite(lobbyId) && previous.id === lobbyId;
        const previousMatchesByCode =
          lobbyCode !== null &&
          typeof previous.code === 'string' &&
          previous.code.toUpperCase() === lobbyCode;

        if (!previousMatchesById && !previousMatchesByCode) {
          return previous;
        }

        const updatedLobby: Lobby = {
          ...previous,
          hostUserId,
          state: lobbyState,
        };
        currentLobbyRef.current = updatedLobby;
        return updatedLobby;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    const lobbyId = currentLobby?.id ?? null;

    if (lobbyId === null) {
      lobbyChatLobbyIdRef.current = null;
      setLobbyChatMessages([]);
      setLobbyPreferences(DEFAULT_LOBBY_PREFERENCES);
      return;
    }

    if (lobbyChatLobbyIdRef.current !== lobbyId) {
      lobbyChatLobbyIdRef.current = lobbyId;
      setLobbyChatMessages([]);
    }
  }, [currentLobby]);

  useEffect(() => {
    const cleanup = onMessage('GameChatHistory', (message: any) => {
      const payload = message?.data ?? message?.GameChatHistory ?? message;
      if (!payload) {
        return;
      }

      const gameIdRaw = payload.game_id;
      const gameId =
        typeof gameIdRaw === 'number'
          ? gameIdRaw
          : typeof gameIdRaw === 'string'
            ? parseInt(gameIdRaw, 10)
            : undefined;
      const messagesArray = Array.isArray(payload.messages) ? payload.messages : [];

      const normalized = messagesArray
        .map((entry: any) => {
          if (!entry || typeof entry.message !== 'string') {
            return null;
          }

          const timestampMs =
            typeof entry.timestamp_ms === 'number' ? entry.timestamp_ms : Date.now();
          const rawUsername =
            typeof entry.username === 'string' && entry.username.trim()
              ? entry.username.trim()
              : null;
          const rawUserId =
            typeof entry.user_id === 'number'
              ? entry.user_id
              : typeof entry.user_id === 'string'
                ? parseInt(entry.user_id, 10)
                : null;
          const messageId =
            typeof entry.message_id === 'string'
              ? entry.message_id
              : `${gameId ?? 'game'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

          const chatMessage: ChatMessage = {
            id: messageId,
            scope: 'game' as const,
            lobbyId: undefined,
            gameId,
            userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
            username: rawUsername,
            message: entry.message,
            type: rawUsername ? 'user' : 'system',
            timestamp: new Date(timestampMs),
          };
          return chatMessage;
        })
        .filter((entry): entry is ChatMessage => entry !== null)
        .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

      if (typeof gameId === 'number' && Number.isFinite(gameId)) {
        gameChatIdRef.current = gameId;
      } else {
        gameChatIdRef.current = null;
      }

      setGameChatMessages(() => {
        if (normalized.length > MAX_CHAT_HISTORY) {
          return normalized.slice(normalized.length - MAX_CHAT_HISTORY);
        }
        return normalized;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    const cleanup = onMessage('GameChatMessage', (message: any) => {
      const payload = message?.data ?? message?.GameChatMessage ?? message;
      if (!payload || typeof payload.message !== 'string') {
        return;
      }

      const gameId =
        typeof payload.game_id === 'number'
          ? payload.game_id
          : typeof payload.game_id === 'string'
            ? parseInt(payload.game_id, 10)
            : undefined;
      const timestampMs =
        typeof payload.timestamp_ms === 'number'
          ? payload.timestamp_ms
          : Date.now();
      const rawUsername =
        typeof payload.username === 'string' && payload.username.trim()
          ? payload.username.trim()
          : null;
      const rawUserId =
        typeof payload.user_id === 'number'
          ? payload.user_id
          : typeof payload.user_id === 'string'
            ? parseInt(payload.user_id, 10)
            : null;
      const messageId =
        typeof payload.message_id === 'string'
          ? payload.message_id
          : `${gameId ?? 'game'}-${timestampMs}-${Math.random().toString(36).slice(2, 8)}`;

      const normalized: ChatMessage = {
        id: messageId,
        scope: 'game',
        lobbyId: undefined,
        gameId,
        userId: Number.isFinite(rawUserId as number) ? (rawUserId as number) : null,
        username: rawUsername,
        message: payload.message,
        type: rawUsername ? 'user' : 'system',
        timestamp: new Date(timestampMs),
      };

      setGameChatMessages((previous) => {
        const isNewGame = typeof gameId === 'number' && gameChatIdRef.current !== gameId;
        if (isNewGame) {
          gameChatIdRef.current = gameId;
        }
        const base = isNewGame ? [] : previous;
        const next = [...base, normalized];
        if (next.length > MAX_CHAT_HISTORY) {
          return next.slice(next.length - MAX_CHAT_HISTORY);
        }
        return next;
      });
    });

    return cleanup;
  }, [onMessage]);

  useEffect(() => {
    const resetGameChat = (gameId: number | null) => {
      if (typeof gameId === 'number' && Number.isFinite(gameId)) {
        gameChatIdRef.current = gameId;
      } else {
        gameChatIdRef.current = null;
      }
      setGameChatMessages([]);
    };

    const extractGameId = (raw: any): number | null => {
      if (raw === null || raw === undefined) {
        return null;
      }
      if (typeof raw === 'number' && Number.isFinite(raw)) {
        return raw;
      }
      if (typeof raw === 'string' && raw.trim()) {
        const parsed = parseInt(raw, 10);
        return Number.isFinite(parsed) ? parsed : null;
      }
      if (typeof raw === 'object') {
        if (typeof raw.game_id === 'number') {
          return raw.game_id;
        }
        if (typeof raw.game_id === 'string') {
          const parsed = parseInt(raw.game_id, 10);
          return Number.isFinite(parsed) ? parsed : null;
        }
        if ('JoinGame' in raw) {
          return extractGameId((raw as any).JoinGame);
        }
        if ('data' in raw) {
          return extractGameId((raw as any).data);
        }
      }
      return null;
    };

    const cleanupJoin = onMessage('JoinGame', (message: any) => {
      const payload = message?.data ?? message?.JoinGame ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupCustomCreated = onMessage('CustomGameCreated', (message: any) => {
      const payload = message?.data ?? message?.CustomGameCreated ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupCustomJoined = onMessage('CustomGameJoined', (message: any) => {
      const payload = message?.data ?? message?.CustomGameJoined ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupSoloCreated = onMessage('SoloGameCreated', (message: any) => {
      const payload = message?.data ?? message?.SoloGameCreated ?? message;
      resetGameChat(extractGameId(payload));
    });

    const cleanupSpectator = onMessage('SpectatorJoined', () => {
      setGameChatMessages([]);
    });

    return () => {
      cleanupJoin();
      cleanupCustomCreated();
      cleanupCustomJoined();
      cleanupSoloCreated();
      cleanupSpectator();
    };
  }, [onMessage]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disconnect();
    };
  }, [disconnect]);

  useEffect(() => {
    if (!hasLoadedStoredLobbyRef.current) {
      return;
    }

    if (!isConnected || !isSessionAuthenticated) {
      return;
    }

    if (currentLobby) {
      return;
    }

    if (!storedLobbyRef.current || !storedLobbyRef.current.code) {
      return;
    }

    if (restoreInProgressRef.current) {
      return;
    }

    let cancelled = false;
    restoreInProgressRef.current = true;

    const attemptRestore = async () => {
      const { code } = storedLobbyRef.current!;

      try {
        console.log(`Attempting to restore lobby ${code}`);
        await joinLobby(code);
      } catch (error) {
        if (cancelled) {
          return;
        }

        const message = error instanceof Error ? error.message : String(error ?? 'unknown error');
        const normalizedMessage = message.toLowerCase();

        if (normalizedMessage.includes('access denied') || normalizedMessage.includes('not found')) {
          console.warn('Stored lobby is no longer valid, clearing persisted lobby info');
          resetLobbyState();
          return;
        }

        console.warn('Failed to restore lobby from storage, not retrying automatically:', message);
      }
    };

    attemptRestore()
      .catch(error => {
        console.error('Failed to restore lobby from storage:', error);
      })
      .finally(() => {
        restoreInProgressRef.current = false;
      });

    return () => {
      cancelled = true;
    };
  }, [isConnected, isSessionAuthenticated, currentLobby, joinLobby, resetLobbyState]);

  const value: WebSocketContextType = {
    isConnected,
    sendMessage,
    onMessage,
    connect,
    disconnect,
    connectToRegion,
    currentRegionUrl,
    latencyMs,
    currentLobby,
    lobbyMembers,
    lobbyChatMessages,
    gameChatMessages,
    lobbyPreferences,
    createLobby,
    joinLobby,
    leaveLobby,
    sendChatMessage,
    updateLobbyPreferences,
  };

  // Expose context for testing
  useEffect(() => {
    if (typeof window !== 'undefined') {
      window.__wsContext = value;
    }
  }, [value]);

  return (
    <WebSocketContext.Provider value={value}>
      {children}
    </WebSocketContext.Provider>
  );
};
