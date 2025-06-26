import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import { WebSocketContextType } from '../types';
import { clockSync } from '../utils/clockSync';
import { useLatency } from './LatencyContext';

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
  const ws = useRef<WebSocket | null>(null);
  const messageHandlers = useRef<Map<string, MessageHandler[]>>(new Map());
  const reconnectTimeout = useRef<NodeJS.Timeout | null>(null);
  const onConnectCallback = useRef<(() => void) | null>(null);
  const pingTimeout = useRef<NodeJS.Timeout | null>(null);
  const pingSentTime = useRef<number | null>(null);
  const syncRequestTimes = useRef<Map<number, number>>(new Map());
  const { settings: latencySettings } = useLatency();

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

      ws.current.onopen = () => {
        console.log('WebSocket connected');
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
            const message = JSON.parse(event.data);
            console.log('WebSocket message received:', message);

          // Handle Pong response for latency measurement
          if (message === 'Pong' && pingSentTime.current !== null) {
            const latency = Math.round((Date.now() - pingSentTime.current) / 2);
            setLatencyMs(latency);
            console.log('WebSocket latency:', latency, 'ms');
            pingSentTime.current = null;
          } else if (message.ClockSyncResponse) {
            // Handle clock sync response
            const { client_time, server_time } = message.ClockSyncResponse;
            const t1 = syncRequestTimes.current.get(client_time);
            if (t1) {
              syncRequestTimes.current.delete(client_time);
              const t3 = Date.now();
              clockSync.processSyncResponse(t1, server_time, t3);
              // Update latency with clock sync RTT as well
              const rtt = t3 - t1;
              setLatencyMs(Math.round(rtt / 2));
            }
          } else {
            // Extract message type from enum-style format
            const messageType = Object.keys(message)[0];
            const messageData = message[messageType];

            // Call registered handlers for this message type
            const handlers = messageHandlers.current.get(messageType) || [];
            handlers.forEach((handler: MessageHandler) => handler({type: messageType, data: messageData}));
          }
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

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disconnect();
    };
  }, [disconnect]);

  const value: WebSocketContextType = {
    isConnected,
    sendMessage,
    onMessage,
    connect,
    latencyMs,
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