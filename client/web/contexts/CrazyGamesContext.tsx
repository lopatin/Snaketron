import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import {
  crazyGames,
  CrazyGamesFriendsPage,
  CrazyGamesPortalUser,
  CrazyGamesSnapshot,
} from '../services/crazyGames';

interface CrazyGamesContextValue extends CrazyGamesSnapshot {
  showAuthPrompt: () => Promise<CrazyGamesPortalUser | null>;
  getUserToken: () => Promise<string>;
  listFriends: (page?: number, size?: number) => Promise<CrazyGamesFriendsPage | null>;
  showAccountLinkPrompt: () => Promise<'yes' | 'no' | null>;
}

const CrazyGamesContext = createContext<CrazyGamesContextValue | null>(null);

export const CrazyGamesProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [snapshot, setSnapshot] = useState<CrazyGamesSnapshot>(() => crazyGames.getSnapshot());

  useEffect(() => crazyGames.subscribe(setSnapshot), []);

  const value = useMemo<CrazyGamesContextValue>(() => ({
    ...snapshot,
    showAuthPrompt: crazyGames.showAuthPrompt,
    getUserToken: crazyGames.getUserToken,
    listFriends: crazyGames.listFriends,
    showAccountLinkPrompt: crazyGames.showAccountLinkPrompt,
  }), [snapshot]);

  return (
    <CrazyGamesContext.Provider value={value}>
      {children}
    </CrazyGamesContext.Provider>
  );
};

export const useCrazyGames = (): CrazyGamesContextValue => {
  const context = useContext(CrazyGamesContext);
  if (!context) {
    throw new Error('useCrazyGames must be used within CrazyGamesProvider');
  }
  return context;
};
