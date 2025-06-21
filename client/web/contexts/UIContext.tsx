import React, { createContext, useContext, useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';

interface UIContextType {
  isHeaderVisible: boolean;
  setHeaderVisible: (visible: boolean) => void;
}

const UIContext = createContext<UIContextType | undefined>(undefined);

export function UIProvider({ children }: { children: React.ReactNode }) {
  const [isHeaderVisible, setIsHeaderVisibleState] = useState(true);
  const location = useLocation();

  useEffect(() => {
    // Determine if header should be shown based on route
    const isGameArena = location.pathname.startsWith('/play/');
    
    if (isGameArena) {
      // Fade out header when entering game
      setIsHeaderVisibleState(false);
    } else {
      // Show header when leaving game
      setIsHeaderVisibleState(true);
    }
  }, [location]);

  const setHeaderVisible = (visible: boolean) => {
    setIsHeaderVisibleState(visible);
  };

  return (
    <UIContext.Provider value={{ isHeaderVisible, setHeaderVisible }}>
      {children}
    </UIContext.Provider>
  );
}

export function useUI() {
  const context = useContext(UIContext);
  if (!context) {
    throw new Error('useUI must be used within a UIProvider');
  }
  return context;
}