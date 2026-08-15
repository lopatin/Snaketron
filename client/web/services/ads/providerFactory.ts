import { crazyGames } from '../crazyGames';
import { CrazyGamesAdProvider } from './providers/crazyGamesAdProvider';
import { NullAdProvider } from './providers/nullAdProvider';
import type { AdProvider } from './types';

export type AdProviderFactory = () => AdProvider;

const factories = new Map<string, AdProviderFactory>();

export const registerAdProvider = (id: string, factory: AdProviderFactory): (() => void) => {
  const normalized = id.trim().toLowerCase();
  factories.set(normalized, factory);
  return () => {
    if (factories.get(normalized) === factory) {
      factories.delete(normalized);
    }
  };
};

registerAdProvider('crazygames', () => (
  crazyGames.getSnapshot().isCrazyGamesBuild
    ? new CrazyGamesAdProvider()
    : new NullAdProvider('crazygames')
));
registerAdProvider('crazy_games', () => (
  crazyGames.getSnapshot().isCrazyGamesBuild
    ? new CrazyGamesAdProvider()
    : new NullAdProvider('crazy_games')
));

/** Unknown providers fail open; a future site adapter can register itself. */
export const createAdProvider = (id: string): AdProvider => {
  const normalized = id.trim().toLowerCase();
  return factories.get(normalized)?.() ?? new NullAdProvider(normalized || 'none');
};
