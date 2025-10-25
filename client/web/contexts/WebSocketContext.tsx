import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import { WebSocketContextType, Lobby, LobbyMember } from '../types';
import { clockSync } from '../utils/clockSync';
import { useLatency } from './LatencyContext';
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
  const ws = useRef<WebSocket | null>(null);
  const messageHandlers = useRef<Map<string, MessageHandler[]>>(new Map());
  const reconnectTimeout = useRef<NodeJS.Timeout | null>(null);
  const onConnectCallback = useRef<(() => void) | null>(null);
  const pingTimeout = useRef<NodeJS.Timeout | null>(null);
  const pingSentTime = useRef<number | null>(null);
  const syncRequestTimes = useRef<Map<number, number>>(new Map());
  const isInitializingRef = useRef(false);
  const storedLobbyRef = useRef<StoredLobbyInfo | null>(null);
  const hasLoadedStoredLobbyRef = useRef(false);
  const restoreInProgressRef = useRef(false);
  const { settings: latencySettings } = useLatency();

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
              ClockSyncRequest: { client_time: clientTime }
            }));
          }
        });
        
        // Start clock synchronization
        clockSync.start();
        
        // Keep legacy latency measurement for backward compatibility
        const measureLatency = () => {
          if (ws.current?.readyState === WebSocket.OPEN) {
            pingSentTime.current = Date.now();
            ws.current.send(JSON.stringify('Ping'));
            // Measure latency every 10 seconds (less frequent since we have clock sync)
            pingTimeout.current = setTimeout(measureLatency, 10000);
          }
        };
        // Delay initial ping to avoid conflict with clock sync
        setTimeout(measureLatency, 1000);
        
        // Call the onConnect callback if provided
        if (onConnectCallback.current) {
          onConnectCallback.current();
        }
      };

      ws.current.onclose = () => {
        console.log('WebSocket disconnected');
        setIsConnected(false);
        // Clear ping timeout
        if (pingTimeout.current) {
          clearTimeout(pingTimeout.current);
          pingTimeout.current = null;
        }
        // Reset clock sync
        clockSync.reset();
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

            // Handle Pong response for latency measurement
            if (rawMessage === 'Pong' && pingSentTime.current !== null) {
              const latency = Math.round((Date.now() - pingSentTime.current) / 2);
              setLatencyMs(latency);
              console.log('WebSocket latency:', latency, 'ms');
              pingSentTime.current = null;
              return;
            }

            // Handle clock sync response payloads
            if (rawMessage?.ClockSyncResponse) {
              const { client_time, server_time } = rawMessage.ClockSyncResponse;
              const t1 = syncRequestTimes.current.get(client_time);
              if (t1) {
                syncRequestTimes.current.delete(client_time);
                const t3 = Date.now();
                clockSync.processSyncResponse(t1, server_time, t3);
                // Update latency with clock sync RTT as well
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
    if (pingTimeout.current) {
      clearTimeout(pingTimeout.current);
      pingTimeout.current = null;
    }
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

    // Connect to new region
    // Note: We need to get the auth token and send it on connection
    // This is handled by the onConnectCallback in App.tsx
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
        setCurrentLobby({
          id: lobby_id,
          code: normalizedCode,
          hostUserId: 0, // Will be set by LobbyUpdate
          region: '', // Will be set by LobbyUpdate
        });
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
        setCurrentLobby({
          id: lobbyId,
          code: normalizedCode,
          hostUserId: hostUserId ?? 0, // Will be refined by future LobbyUpdate messages
          region: '', // Will be set by LobbyUpdate
        });
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
        settled = true;
        cleanupHandlers();
        reject(new Error(message.data.reason || 'Access denied'));
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

      // Send JoinLobbyByCode message
      sendMessage({ JoinLobbyByCode: { lobby_code: normalizedCode } });

      // Timeout after 5 seconds
      timeoutId = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        cleanupHandlers();
        reject(new Error('Timeout waiting to join lobby'));
      }, 5000);
    });
  }, [onMessage, sendMessage, connectToRegion, persistLobby]);

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
        setCurrentLobby(null);
        setLobbyMembers([]);
        clearPersistedLobby();
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
  }, [onMessage, sendMessage, clearPersistedLobby]);

  // Handle lobby updates
  useEffect(() => {
    const cleanup = onMessage('LobbyUpdate', (message: any) => {
      const { lobby_id, members, host_user_id } = message.data;

      // Update lobby members
      setLobbyMembers(members);

      // Update current lobby with host info
      if (currentLobby && currentLobby.id === lobby_id) {
        setCurrentLobby({
          ...currentLobby,
          hostUserId: host_user_id,
        });
      }
    });

    return cleanup;
  }, [onMessage, currentLobby]);

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

    if (!isConnected) {
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
      const maxAttempts = 3;

      for (let attempt = 0; attempt < maxAttempts && !cancelled; attempt += 1) {
        try {
          console.log(`Attempting to restore lobby ${code} (attempt ${attempt + 1})`);
          await joinLobby(code);
          return;
        } catch (error) {
          if (cancelled) {
            return;
          }

          const message =
            error instanceof Error ? error.message : String(error ?? 'unknown error');
          const normalizedMessage = message.toLowerCase();

          if (normalizedMessage.includes('access denied') || normalizedMessage.includes('not found')) {
            console.warn('Stored lobby is no longer valid, clearing persisted lobby info');
            clearPersistedLobby();
            return;
          }

          if (attempt < maxAttempts - 1) {
            const delayMs = 1000 * (attempt + 1);
            await new Promise(resolve => setTimeout(resolve, delayMs));
          }
        }
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
  }, [isConnected, currentLobby, joinLobby, clearPersistedLobby]);

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
    createLobby,
    joinLobby,
    leaveLobby,
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
