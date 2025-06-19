import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';

const WebSocketContext = createContext(null);

export const useWebSocket = () => {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocket must be used within WebSocketProvider');
  }
  return context;
};

export const WebSocketProvider = ({ children }) => {
  const [isConnected, setIsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState(null);
  const ws = useRef(null);
  const messageHandlers = useRef(new Map());
  const reconnectTimeout = useRef(null);
  const onConnectCallback = useRef(null);

  const connect = useCallback((url, onConnect) => {
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
          window.__wsInstance = ws.current;
        }
        // Call the onConnect callback if provided
        if (onConnectCallback.current) {
          onConnectCallback.current();
        }
      };

      ws.current.onclose = () => {
        console.log('WebSocket disconnected');
        setIsConnected(false);
        // Auto-reconnect after 2 seconds
        reconnectTimeout.current = setTimeout(() => {
          console.log('Attempting to reconnect...');
          connect(url, onConnect);
        }, 2000);
      };

      ws.current.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      ws.current.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          console.log('WebSocket message received:', message);
          setLastMessage(message);
          
          // Extract message type from enum-style format
          const messageType = Object.keys(message)[0];
          const messageData = message[messageType];
          
          // Call registered handlers for this message type
          const handlers = messageHandlers.current.get(messageType) || [];
          handlers.forEach(handler => handler({ type: messageType, data: messageData }));
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error);
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
    if (ws.current) {
      ws.current.close();
      ws.current = null;
    }
  }, []);

  const sendMessage = useCallback((message) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify(message));
      console.log('WebSocket message sent:', message);
    } else {
      console.error('WebSocket is not connected');
    }
  }, []);

  const onMessage = useCallback((messageType, handler) => {
    if (!messageHandlers.current.has(messageType)) {
      messageHandlers.current.set(messageType, []);
    }
    messageHandlers.current.get(messageType).push(handler);

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

  const value = {
    isConnected,
    lastMessage,
    connect,
    disconnect,
    sendMessage,
    onMessage,
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