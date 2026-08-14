const { test, expect } = require('@playwright/test');

test.use({ headless: true });
test.skip(
  process.env.SNAKETRON_CRAZYGAMES_E2E !== 'true',
  'Run against a CRAZYGAMES_BUILD=true bundle with SNAKETRON_CRAZYGAMES_E2E=true',
);

const appUrl = process.env.SNAKETRON_TEST_BASE_URL || 'http://127.0.0.1:3100';

const fulfillRuntimeConfig = route => route.fulfill({
  contentType: 'application/json',
  headers: { 'access-control-allow-origin': '*' },
  body: JSON.stringify({
    version: 1,
    announcement: { enabled: false, message: '' },
    ads: { postMatchEnabled: false, minimumIntervalMinutes: 10 },
  }),
});

const sdkScript = `
(() => {
  const portalUser = {
    __dangerousUserId: 'display-only-do-not-trust',
    username: 'Verified.Player',
    profilePictureUrl: 'https://example.test/avatar.png'
  };
  const listeners = { auth: null, join: null, settings: null };
  window.__cgTest = {
    tokenCalls: 0,
    linkPromptCalls: 0,
    leftRoomCalls: 0,
    emitAuth: user => listeners.auth?.(user ?? portalUser)
  };
  window.CrazyGames = { SDK: {
    environment: 'local',
    init: async () => {},
    ad: { hasAdblock: async () => false, requestAd: (_type, callbacks) => callbacks.adFinished() },
    banner: {
      requestBanner: async () => {}, requestResponsiveBanner: async () => {},
      clearBanner: () => {}, clearAllBanners: () => {}
    },
    data: { clear: () => {}, getItem: () => null, removeItem: () => {}, setItem: () => {} },
    game: {
      settings: { disableChat: false, muteAudio: false },
      isInstantMultiplayer: false,
      inviteParams: null,
      addSettingsChangeListener: listener => { listeners.settings = listener; },
      removeSettingsChangeListener: () => {},
      addJoinRoomListener: listener => { listeners.join = listener; },
      removeJoinRoomListener: () => {},
      gameplayStart: () => {}, gameplayStop: () => {},
      loadingStart: () => {}, loadingStop: () => {}, happytime: () => {},
      reportGameCompletedPercentage: () => {}, setGameContext: () => {}, clearGameContext: () => {},
      updateRoom: () => {},
      leftRoom: () => { window.__cgTest.leftRoomCalls += 1; },
      inviteLink: () => '', getInviteParam: () => null
    },
    user: {
      isUserAccountAvailable: true,
      systemInfo: { locale: 'en-US', device: { type: 'desktop' } },
      getUser: async () => new Promise(resolve => setTimeout(() => resolve(portalUser), 250)),
      getUserToken: async () => {
        window.__cgTest.tokenCalls += 1;
        return 'fresh-crazygames-jwt';
      },
      listFriends: async () => ({ friends: [], page: 1, size: 50, hasMore: false, total: 0 }),
      showAuthPrompt: async () => portalUser,
      showAccountLinkPrompt: async () => {
        window.__cgTest.linkPromptCalls += 1;
        const response = sessionStorage.getItem('__cg-link-response') || 'no';
        if (response === 'error') throw new Error('account link prompt failed');
        return { response };
      },
      addAuthListener: listener => { listeners.auth = listener; },
      removeAuthListener: () => {}
    }
  }};
})();
`;

const signedOutSdkScript = sdkScript.replace(
  "return 'fresh-crazygames-jwt';",
  "throw { code: 'userNotAuthenticated', message: 'The user is not authenticated' };",
);

const accountUnavailableSdkScript = sdkScript.replace(
  'isUserAccountAvailable: true,',
  'isUserAccountAvailable: false,',
);

const disabledEnvironmentSdkScript = sdkScript.replace(
  "environment: 'local',",
  "environment: 'disabled',",
);

const authRaceSdkScript = sdkScript.replace(
  "return 'fresh-crazygames-jwt';",
  "return sessionStorage.getItem('__cg-race-account') === 'b' ? 'crazygames-token-b' : 'crazygames-token-a';",
);

const authBoundarySdkScript = authRaceSdkScript.replace(
  '  window.CrazyGames = { SDK: {',
  `  const originalSetItem = Storage.prototype.setItem;
  Storage.prototype.setItem = function(key, value) {
    originalSetItem.call(this, key, value);
    if (
      key === 'snaketron:crazygames:session-token' &&
      value === 'internal-boundary-a' &&
      sessionStorage.getItem('__cg-boundary-fired') !== 'true'
    ) {
      originalSetItem.call(sessionStorage, '__cg-boundary-fired', 'true');
      originalSetItem.call(sessionStorage, '__cg-race-account', 'b');
      queueMicrotask(() => listeners.auth?.({
        __dangerousUserId: 'display-only-boundary-b',
        username: 'Boundary.B',
        profilePictureUrl: ''
      }));
    }
  };
  window.CrazyGames = { SDK: {`,
);

const interactiveSignInSdkScript = sdkScript
  .replace(
    "return 'fresh-crazygames-jwt';",
    "if (localStorage.getItem('__cg-signed-in') === 'true') return 'fresh-crazygames-jwt'; throw { code: 'userNotAuthenticated', message: 'The user is not authenticated' };",
  )
  .replace(
    'showAuthPrompt: async () => portalUser,',
    "showAuthPrompt: async () => { localStorage.setItem('__cg-signed-in', 'true'); return portalUser; },",
  )
  .replace(
    'leftRoom: () => { window.__cgTest.leftRoomCalls += 1; },',
    "leftRoom: () => { window.__cgTest.leftRoomCalls += 1; sessionStorage.setItem('__cg-left-room', String(Number(sessionStorage.getItem('__cg-left-room') || '0') + 1)); },",
  );

test('fresh CrazyGames identity wins before cached session or socket startup', async ({ page }) => {
  const requests = [];
  let exchangeAuthorization = null;
  let exchangeBody = null;
  let preferenceAuthorization = null;

  await page.addInitScript(() => {
    localStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'stale-internal-session');
    localStorage.setItem('lastLobbyPreferences', JSON.stringify({
      selectedModes: ['duel'],
      competitive: false,
    }));
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: sdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    requests.push(pathname);
    const headers = { 'access-control-allow-origin': '*' };

    if (pathname === '/api/auth/crazygames/exchange') {
      exchangeAuthorization = request.headers().authorization ?? null;
      exchangeBody = request.postDataJSON();
      await new Promise(resolve => setTimeout(resolve, 150));
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'verified-internal-session',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'returning',
          user: {
            id: 912,
            username: 'Verified.Player',
            mmr: 1120,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: 'https://example.test/avatar.png',
          },
          preferences: {
            tutorialSeen: { movement: true },
            lobbyPreferences: { selectedModes: ['solo'], competitive: true },
            boostInputMode: 'toggle',
          },
        }),
      });
    }
    if (pathname === '/api/auth/crazygames/preferences') {
      preferenceAuthorization = request.headers().authorization ?? null;
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({ preferences: request.postDataJSON() }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Connecting your account')).toBeVisible();
  await expect(page.getByText('Verified.Player', { exact: true })).toBeVisible();
  await expect(page.getByText(/progress saves automatically/i)).toBeVisible();

  expect(exchangeAuthorization).toBe('Bearer stale-internal-session');
  expect(exchangeBody).toEqual({
    token: 'fresh-crazygames-jwt',
    guestPromotion: 'check',
  });
  expect(requests.indexOf('/api/auth/crazygames/exchange')).toBeLessThan(
    requests.indexOf('/api/regions'),
  );
  expect(await page.evaluate(() => window.__cgTest.tokenCalls)).toBe(1);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(0);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('verified-internal-session');
  expect(await page.evaluate(() => (
    Object.values(localStorage).includes('fresh-crazygames-jwt') ||
    Object.values(sessionStorage).includes('fresh-crazygames-jwt')
  )))
    .toBe(false);
  // The provider's live preference state and storage must both reflect the
  // canonical backend response, not the pre-exchange browser snapshot.
  expect(await page.evaluate(() => window.__wsContext?.lobbyPreferences)).toEqual({
    selectedModes: ['solo'],
    competitive: true,
  });
  expect(await page.evaluate(() => JSON.parse(sessionStorage.getItem('lastLobbyPreferences'))))
    .toEqual({ selectedModes: ['solo'], competitive: true });

  // Simulate another tab replacing the old shared localStorage key. This
  // tab's API and socket identity remain bound to its in-memory/session token.
  await page.evaluate(() => {
    localStorage.setItem('snaketron:crazygames:session-token', 'other-tab-session');
    window.__wsContext.updateLobbyPreferences({
      selectedModes: ['ffa'],
      competitive: false,
    });
  });
  await expect.poll(() => preferenceAuthorization).toBe('Bearer verified-internal-session');
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('verified-internal-session');
});

test('an eligible guest is promoted only after explicit CrazyGames link consent', async ({ page }) => {
  const exchanges = [];

  await page.addInitScript(() => {
    localStorage.clear();
    sessionStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'saved-guest-session');
    localStorage.setItem('lastLobbyPreferences', JSON.stringify({
      selectedModes: ['ffa'],
      competitive: true,
    }));
    localStorage.setItem('snaketron:boost-input-mode:v1', 'toggle');
    sessionStorage.setItem('__cg-link-response', 'yes');
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: sdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      exchanges.push({
        authorization: request.headers().authorization ?? null,
        body: request.postDataJSON(),
      });
      if (exchanges.length === 1) {
        return route.fulfill({
          status: 409,
          contentType: 'application/json',
          headers,
          body: JSON.stringify({
            code: 'guestLinkConsentRequired',
            error: 'Confirm whether to keep guest progress',
          }),
        });
      }
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'claimed-guest-session',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'guestClaimed',
          user: {
            id: 920,
            username: 'Claimed.Guest',
            mmr: 1042,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: null,
          },
          preferences: {
            lobbyPreferences: { selectedModes: ['ffa'], competitive: true },
            boostInputMode: 'toggle',
          },
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Claimed.Guest', { exact: true })).toBeVisible();

  expect(exchanges).toEqual([
    {
      authorization: 'Bearer saved-guest-session',
      body: {
        token: 'fresh-crazygames-jwt',
        guestPromotion: 'check',
      },
    },
    {
      authorization: 'Bearer saved-guest-session',
      body: {
        token: 'fresh-crazygames-jwt',
        guestPromotion: 'allow',
        initialPreferences: {
          lobbyPreferences: { selectedModes: ['ffa'], competitive: true },
          boostInputMode: 'toggle',
        },
      },
    },
  ]);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(1);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('claimed-guest-session');
});

test('declining guest promotion creates the CrazyGames identity without sharing preferences', async ({ page }) => {
  const exchanges = [];

  await page.addInitScript(() => {
    localStorage.clear();
    sessionStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'declined-guest-session');
    localStorage.setItem('lastLobbyPreferences', JSON.stringify({
      selectedModes: ['duel'],
      competitive: false,
    }));
    sessionStorage.setItem('__cg-link-response', 'no');
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: sdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      exchanges.push({
        authorization: request.headers().authorization ?? null,
        body: request.postDataJSON(),
      });
      if (exchanges.length === 1) {
        return route.fulfill({
          status: 409,
          contentType: 'application/json',
          headers,
          body: JSON.stringify({ code: 'guestLinkConsentRequired' }),
        });
      }
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'new-crazygames-session',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'created',
          user: {
            id: 921,
            username: 'Separate.Player',
            mmr: 1000,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: null,
          },
          preferences: {},
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Separate.Player', { exact: true })).toBeVisible();

  expect(exchanges).toEqual([
    {
      authorization: 'Bearer declined-guest-session',
      body: {
        token: 'fresh-crazygames-jwt',
        guestPromotion: 'check',
      },
    },
    {
      authorization: 'Bearer declined-guest-session',
      body: {
        token: 'fresh-crazygames-jwt',
        guestPromotion: 'decline',
      },
    },
  ]);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(1);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('new-crazygames-session');
});

test('an unavailable account-link prompt fails closed and preserves the guest session', async ({ page }) => {
  const exchanges = [];

  await page.addInitScript(() => {
    localStorage.clear();
    sessionStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'retryable-guest-session');
    sessionStorage.setItem('__cg-link-response', 'error');
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: sdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      exchanges.push({
        authorization: request.headers().authorization ?? null,
        body: request.postDataJSON(),
      });
      return route.fulfill({
        status: 409,
        contentType: 'application/json',
        headers,
        body: JSON.stringify({ code: 'guestLinkConsentRequired' }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Progress connection failed')).toBeVisible();

  expect(exchanges).toEqual([{
    authorization: 'Bearer retryable-guest-session',
    body: {
      token: 'fresh-crazygames-jwt',
      guestPromotion: 'check',
    },
  }]);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(1);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('retryable-guest-session');
});

test('a direct privacy visit stays passive and resolves the account only after leaving', async ({ page }) => {
  const exchanges = [];

  await page.addInitScript(() => {
    localStorage.clear();
    sessionStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'privacy-guest-session');
    sessionStorage.setItem('__cg-link-response', 'no');
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: sdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      exchanges.push(request.postDataJSON());
      if (exchanges.length === 1) {
        return route.fulfill({
          status: 409,
          contentType: 'application/json',
          headers,
          body: JSON.stringify({ code: 'guestLinkConsentRequired' }),
        });
      }
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'privacy-return-session',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'created',
          user: {
            id: 922,
            username: 'Privacy.Return',
            mmr: 1000,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: null,
          },
          preferences: {},
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(`${appUrl}/#/privacy`);
  await expect(page.getByRole('heading', { name: 'Privacy on CrazyGames' })).toBeVisible();
  await page.waitForTimeout(500);
  expect(exchanges).toEqual([]);
  expect(await page.evaluate(() => window.__cgTest.tokenCalls)).toBe(0);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(0);

  await page.getByRole('link', { name: /Back to Snaketron/ }).click();
  await expect(page.getByText('Privacy.Return', { exact: true })).toBeVisible();
  expect(exchanges).toEqual([
    { token: 'fresh-crazygames-jwt', guestPromotion: 'check' },
    { token: 'fresh-crazygames-jwt', guestPromotion: 'decline' },
  ]);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(1);
});

test('preference writes stay serialized and a delayed response cannot overwrite a newer edit', async ({ page }) => {
  const putBodies = [];
  let concurrentPuts = 0;
  let maxConcurrentPuts = 0;
  let targetWriteStarted = false;
  let releaseFirstPut;
  let notifyFirstPut;
  const firstPutReleased = new Promise(resolve => { releaseFirstPut = resolve; });
  const firstPutStarted = new Promise(resolve => { notifyFirstPut = resolve; });

  await page.addInitScript(() => localStorage.clear());
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: sdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'preference-session',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'returning',
          user: {
            id: 919,
            username: 'Preference.Player',
            mmr: 1000,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: null,
          },
          preferences: {},
        }),
      });
    }
    if (pathname === '/api/auth/crazygames/preferences') {
      concurrentPuts += 1;
      maxConcurrentPuts = Math.max(maxConcurrentPuts, concurrentPuts);
      const body = request.postDataJSON();
      const isTargetWrite = body.lobbyPreferences?.selectedModes?.includes('2v2');
      if (isTargetWrite && !targetWriteStarted) {
        targetWriteStarted = true;
        putBodies.push(body);
        notifyFirstPut();
        await firstPutReleased;
      } else if (targetWriteStarted) {
        putBodies.push(body);
      }
      concurrentPuts -= 1;
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({ preferences: body }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Preference.Player', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: '2V2', exact: true }).click();
  await firstPutStarted;
  await page.getByRole('button', { name: '2V2', exact: true }).click();
  await page.getByRole('button', { name: 'FFA', exact: true }).click();
  await page.getByText('Competitive', { exact: true }).click();

  // Hold the real first fetch beyond the second edit's debounce window. No
  // second request may begin until this one actually settles.
  await page.waitForTimeout(900);
  expect(putBodies).toHaveLength(1);
  releaseFirstPut();
  await expect.poll(() => putBodies.length).toBe(2);
  expect(maxConcurrentPuts).toBe(1);
  expect(putBodies[0].lobbyPreferences).toEqual({
    selectedModes: ['duel', '2v2'],
    competitive: false,
  });
  expect(putBodies[1].lobbyPreferences).toEqual({
    selectedModes: ['duel', 'ffa'],
    competitive: true,
  });
  await expect.poll(() => page.evaluate(() => (
    JSON.parse(sessionStorage.getItem('lastLobbyPreferences'))
  ))).toEqual({ selectedModes: ['duel', 'ffa'], competitive: true });
});

test('signed-out CrazyGames users stay guest-capable without restoring a linked account', async ({ page }) => {
  let guestCalls = 0;
  const requests = [];

  await page.addInitScript(() => {
    localStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'previous-linked-session');
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: signedOutSdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    requests.push(pathname);
    const headers = { 'access-control-allow-origin': '*' };

    if (pathname === '/api/auth/me') {
      expect(request.headers().authorization).toBe('Bearer previous-linked-session');
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({ id: 111, username: 'Previous.Player', mmr: 1000, isGuest: false }),
      });
    }
    if (pathname === '/api/auth/guest') {
      guestCalls += 1;
      expect(request.headers().authorization).toBeUndefined();
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'new-guest-session',
          user: { id: 222, username: 'GuestPlayer', mmr: 1000, isGuest: true },
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Sign in with CrazyGames').first()).toBeVisible();
  expect(requests).not.toContain('/api/auth/crazygames/exchange');
  expect(guestCalls).toBe(0);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBeNull();

  await page.getByPlaceholder('Nickname').fill('GuestPlayer');
  await page.getByRole('button', { name: 'Start Game' }).click();
  await expect.poll(() => guestCalls).toBe(1);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('new-guest-session');
});

test('a transient guest verification failure preserves the saved guest for retry', async ({ page }) => {
  let meCalls = 0;
  let guestCreationCalls = 0;

  await page.addInitScript(() => {
    localStorage.clear();
    localStorage.setItem('snaketron:crazygames:session-token', 'saved-guest-session');
    localStorage.setItem('lastLobbyPreferences', JSON.stringify({
      selectedModes: ['ffa'],
      competitive: false,
    }));
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: signedOutSdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/me') {
      meCalls += 1;
      expect(request.headers().authorization).toBe('Bearer saved-guest-session');
      if (meCalls === 1) {
        return route.fulfill({
          status: 503,
          contentType: 'application/json',
          headers,
          body: JSON.stringify({ error: 'temporary outage' }),
        });
      }
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          id: 515,
          username: 'SavedGuest',
          mmr: 1000,
          isGuest: true,
        }),
      });
    }
    if (pathname === '/api/auth/guest') {
      guestCreationCalls += 1;
      return route.abort();
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Progress connection failed')).toBeVisible();
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('saved-guest-session');
  expect(await page.evaluate(() => JSON.parse(sessionStorage.getItem('lastLobbyPreferences'))))
    .toEqual({ selectedModes: ['ffa'], competitive: false });

  await page.getByRole('button', { name: 'Retry account sync' }).click();
  await expect(page.getByText('Sign in with CrazyGames').first()).toBeVisible();
  expect(meCalls).toBe(2);
  expect(guestCreationCalls).toBe(0);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('saved-guest-session');
  expect(await page.evaluate(() => JSON.parse(sessionStorage.getItem('lastLobbyPreferences'))))
    .toEqual({ selectedModes: ['ffa'], competitive: false });
});

test('embeds without CrazyGames account support remain fully guest-capable', async ({ page }) => {
  let exchangeCalls = 0;

  await page.addInitScript(() => localStorage.clear());
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: accountUnavailableSdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const pathname = new URL(route.request().url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      exchangeCalls += 1;
      return route.abort();
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Playing as guest').first()).toBeVisible();
  await expect(page.getByText('Playing as CrazyGames guest')).toBeVisible();
  expect(exchangeCalls).toBe(0);
  expect(await page.evaluate(() => window.__cgTest.tokenCalls)).toBe(0);
});

test('a successfully initialized disabled SDK environment remains fully guest-capable', async ({ page }) => {
  let exchangeCalls = 0;
  let guestCalls = 0;

  await page.addInitScript(() => {
    localStorage.clear();
    sessionStorage.clear();
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: disabledEnvironmentSdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      exchangeCalls += 1;
      return route.abort();
    }
    if (pathname === '/api/auth/guest') {
      guestCalls += 1;
      expect(request.headers().authorization).toBeUndefined();
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'disabled-environment-guest-session',
          user: {
            id: 923,
            username: 'AffiliateGuest',
            mmr: 1000,
            isGuest: true,
          },
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Playing as guest').first()).toBeVisible();
  await expect(page.getByText('Playing as CrazyGames guest')).toBeVisible();
  expect(exchangeCalls).toBe(0);
  expect(await page.evaluate(() => window.__cgTest.tokenCalls)).toBe(0);
  expect(await page.evaluate(() => window.__cgTest.linkPromptCalls)).toBe(0);

  await page.getByPlaceholder('Nickname').fill('AffiliateGuest');
  await page.getByRole('button', { name: 'Start Game' }).click();
  await expect.poll(() => guestCalls).toBe(1);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('disabled-environment-guest-session');
});

test('retry hard-reloads after SDK bootstrap is unavailable', async ({ page }) => {
  const missingSdkScript = `
    sessionStorage.setItem(
      '__cg-missing-sdk-loads',
      String(Number(sessionStorage.getItem('__cg-missing-sdk-loads') || '0') + 1)
    );
  `;
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: missingSdkScript,
  }));

  await page.goto(appUrl);
  await expect(page.getByText('Progress connection failed')).toBeVisible();
  expect(await page.evaluate(() => Number(sessionStorage.getItem('__cg-missing-sdk-loads'))))
    .toBe(1);

  await page.getByRole('button', { name: 'Retry account sync' }).click();
  await expect.poll(() => page.evaluate(() => (
    Number(sessionStorage.getItem('__cg-missing-sdk-loads'))
  ))).toBeGreaterThanOrEqual(2);
  await expect(page.getByText('Progress connection failed')).toBeVisible();
});

test('an auth event during the initial exchange invalidates the old account before render', async ({ page }) => {
  const exchangedTokens = [];
  let resolveFirstExchange;
  const firstExchangeStarted = new Promise(resolve => { resolveFirstExchange = resolve; });

  await page.addInitScript(() => {
    if (sessionStorage.getItem('__cg-race-initialized') !== 'true') {
      localStorage.clear();
      sessionStorage.setItem('__cg-race-initialized', 'true');
      sessionStorage.setItem('__cg-race-account', 'a');
    }
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: authRaceSdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      const { token } = request.postDataJSON();
      exchangedTokens.push(token);
      if (token === 'crazygames-token-a') {
        resolveFirstExchange();
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      const accountB = token === 'crazygames-token-b';
      try {
        return await route.fulfill({
          contentType: 'application/json',
          headers,
          body: JSON.stringify({
            token: accountB ? 'internal-account-b' : 'internal-account-a',
            expiresAt: Math.floor(Date.now() / 1000) + 3600,
            resolution: 'returning',
            user: {
              id: accountB ? 702 : 701,
              username: accountB ? 'Account.B' : 'Account.A',
              mmr: 1000,
              isGuest: false,
              authSource: 'crazygames',
              avatarUrl: null,
            },
            preferences: {},
          }),
        });
      } catch {
        // Reloading intentionally aborts the superseded account-A request.
        return undefined;
      }
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await firstExchangeStarted;
  await expect(page.getByText('Connecting your account')).toBeVisible();
  await page.evaluate(() => {
    sessionStorage.setItem('__cg-race-account', 'b');
    window.__cgTest.emitAuth({
      __dangerousUserId: 'display-only-account-b',
      username: 'Account.B',
      profilePictureUrl: '',
    });
  });

  await expect(page.getByText('Account.B', { exact: true })).toBeVisible();
  await expect(page.getByText('Account.A', { exact: true })).toHaveCount(0);
  expect(exchangedTokens).toContain('crazygames-token-a');
  expect(exchangedTokens).toContain('crazygames-token-b');
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('internal-account-b');
});

test('an auth event on the exchange-completion render boundary cannot be missed', async ({ page }) => {
  const exchangedTokens = [];

  await page.addInitScript(() => {
    if (sessionStorage.getItem('__cg-boundary-initialized') !== 'true') {
      localStorage.clear();
      sessionStorage.setItem('__cg-boundary-initialized', 'true');
      sessionStorage.setItem('__cg-race-account', 'a');
    }
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: authBoundarySdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/crazygames/exchange') {
      const { token } = request.postDataJSON();
      exchangedTokens.push(token);
      const accountB = token === 'crazygames-token-b';
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: accountB ? 'internal-boundary-b' : 'internal-boundary-a',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'returning',
          user: {
            id: accountB ? 802 : 801,
            username: accountB ? 'Boundary.B' : 'Boundary.A',
            mmr: 1000,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: null,
          },
          preferences: {},
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Boundary.B', { exact: true })).toBeVisible();
  await expect(page.getByText('Boundary.A', { exact: true })).toHaveCount(0);
  expect(exchangedTokens).toEqual([
    'crazygames-token-a',
    'crazygames-token-b',
  ]);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('internal-boundary-b');
});

test('an in-page CrazyGames sign-in clears portal room state and reloads into the linked account', async ({ page }) => {
  let guestCalls = 0;
  let exchangeCalls = 0;

  await page.addInitScript(() => {
    if (sessionStorage.getItem('__cg-test-initialized') !== 'true') {
      localStorage.clear();
      sessionStorage.setItem('__cg-test-initialized', 'true');
    }
  });
  await page.route('https://sdk.crazygames.com/crazygames-sdk-v3.js', route => route.fulfill({
    contentType: 'application/javascript',
    body: interactiveSignInSdkScript,
  }));
  await page.route('http://localhost:8080/api/**', async route => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/api/config') return fulfillRuntimeConfig(route);
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/auth/guest') {
      guestCalls += 1;
      return route.abort();
    }
    if (pathname === '/api/auth/crazygames/exchange') {
      exchangeCalls += 1;
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'linked-after-prompt',
          expiresAt: Math.floor(Date.now() / 1000) + 3600,
          resolution: 'created',
          user: {
            id: 333,
            username: 'Verified.Player',
            mmr: 1000,
            isGuest: false,
            authSource: 'crazygames',
            avatarUrl: 'https://example.test/avatar.png',
          },
          preferences: {},
        }),
      });
    }
    if (pathname === '/api/regions') {
      return route.fulfill({ contentType: 'application/json', headers, body: '[]' });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    return route.abort();
  });

  await page.goto(appUrl);
  await expect(page.getByText('Sign in with CrazyGames').first()).toBeVisible();
  await page.getByText('Sign in with CrazyGames').first().click();

  await expect(page.getByText('Verified.Player', { exact: true })).toBeVisible();
  expect(exchangeCalls).toBe(1);
  expect(guestCalls).toBe(0);
  expect(await page.evaluate(() => Number(sessionStorage.getItem('__cg-left-room') || '0')))
    .toBeGreaterThanOrEqual(1);
  expect(await page.evaluate(() => sessionStorage.getItem('snaketron:crazygames:session-token')))
    .toBe('linked-after-prompt');
});
