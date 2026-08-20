import assert from 'node:assert/strict';
import test from 'node:test';

test('CrazyGames preference memory stays authoritative when sessionStorage is blocked', async () => {
  const previousBuild = process.env.CRAZYGAMES_BUILD;
  const hadWindow = 'window' in globalThis;
  const previousWindow = (globalThis as any).window;
  const localValues = new Map<string, string>();
  let published = 0;

  process.env.CRAZYGAMES_BUILD = 'true';
  (globalThis as any).window = {
    localStorage: {
      getItem: (key: string) => localValues.get(key) ?? null,
      setItem: (key: string, value: string) => localValues.set(key, String(value)),
      removeItem: (key: string) => { localValues.delete(key); },
    },
    sessionStorage: {
      getItem: () => null,
      setItem: () => { throw new Error('blocked'); },
      removeItem: () => { throw new Error('blocked'); },
    },
  };

  try {
    const { gameStorage, subscribeGameStorage } = await import(
      `../../services/gameStorage.ts?cg-memory=${Date.now()}`
    );
    const unsubscribe = subscribeGameStorage(() => { published += 1; });

    gameStorage.setItem('lastLobbyPreferences', 'account-a');
    localValues.set('lastLobbyPreferences', 'account-b');
    assert.equal(gameStorage.getItem('lastLobbyPreferences'), 'account-a');
    assert.equal(published, 1);

    gameStorage.removeItem('lastLobbyPreferences');
    localValues.set('lastLobbyPreferences', 'stale-account-b');
    assert.equal(gameStorage.getItem('lastLobbyPreferences'), null);
    assert.equal(published, 2);
    unsubscribe();
  } finally {
    process.env.CRAZYGAMES_BUILD = previousBuild;
    if (hadWindow) {
      (globalThis as any).window = previousWindow;
    } else {
      delete (globalThis as any).window;
    }
  }
});
