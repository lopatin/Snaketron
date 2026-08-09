import assert from 'node:assert/strict';
import test from 'node:test';
import {
  crazyGames,
  crazyGamesGuestNickname,
} from '../../services/crazyGames.ts';

type InviteParams = Record<string, string>;

const withInviteSdk = async (
  coldInviteParams: InviteParams | null,
  assertion: (input: {
    service: typeof crazyGames;
    emitJoinRoom: (params: InviteParams) => void;
  }) => Promise<void>,
) => {
  const previousBuild = process.env.CRAZYGAMES_BUILD;
  const previousAds = process.env.CRAZYGAMES_ADS_ENABLED;
  const previousData = process.env.CRAZYGAMES_DATA_ENABLED;
  const hadWindow = 'window' in globalThis;
  const previousWindow = (globalThis as any).window;
  let joinRoomListener: ((params: InviteParams) => void) | null = null;

  process.env.CRAZYGAMES_BUILD = 'true';
  process.env.CRAZYGAMES_ADS_ENABLED = 'false';
  process.env.CRAZYGAMES_DATA_ENABLED = 'false';

  const sdk = {
    environment: 'local',
    init: async () => {},
    ad: {
      hasAdblock: async () => false,
      requestAd: () => {},
    },
    banner: {
      requestBanner: async () => {},
      requestResponsiveBanner: async () => {},
      clearBanner: () => {},
      clearAllBanners: () => {},
    },
    data: {
      clear: () => {},
      getItem: () => null,
      removeItem: () => {},
      setItem: () => {},
    },
    game: {
      settings: { disableChat: false, muteAudio: false },
      isInstantMultiplayer: false,
      inviteParams: coldInviteParams,
      addSettingsChangeListener: () => {},
      removeSettingsChangeListener: () => {},
      addJoinRoomListener: (listener: (params: InviteParams) => void) => {
        joinRoomListener = listener;
      },
      removeJoinRoomListener: () => {},
      gameplayStart: () => {},
      gameplayStop: () => {},
      loadingStart: () => {},
      loadingStop: () => {},
      happytime: () => {},
      reportGameCompletedPercentage: () => {},
      setGameContext: () => {},
      clearGameContext: () => {},
      updateRoom: () => {},
      leftRoom: () => {},
      inviteLink: () => '',
      getInviteParam: () => null,
    },
    user: {
      isUserAccountAvailable: false,
      getUser: async () => null,
      getUserToken: async () => '',
      listFriends: async () => ({ friends: [], page: 1, size: 50, hasMore: false, total: 0 }),
      showAuthPrompt: async () => { throw new Error('not authenticated'); },
      showAccountLinkPrompt: async () => ({ response: 'no' as const }),
      addAuthListener: () => {},
      removeAuthListener: () => {},
    },
  };
  (globalThis as any).window = { CrazyGames: { SDK: sdk } };

  try {
    const enabledModule = await import(
      `../../services/crazyGames.ts?invite-flow=${Date.now()}-${Math.random()}`
    );
    const service = enabledModule.crazyGames as typeof crazyGames;
    await assertion({
      service,
      emitJoinRoom: (params) => {
        assert.ok(joinRoomListener, 'CrazyGames join-room listener was registered');
        joinRoomListener(params);
      },
    });
  } finally {
    if (hadWindow) {
      (globalThis as any).window = previousWindow;
    } else {
      delete (globalThis as any).window;
    }
    process.env.CRAZYGAMES_BUILD = previousBuild;
    process.env.CRAZYGAMES_ADS_ENABLED = previousAds;
    process.env.CRAZYGAMES_DATA_ENABLED = previousData;
  }
};

test('CrazyGames display names become valid, recognizable guest nicknames', () => {
  assert.equal(crazyGamesGuestNickname('  Portal.User / 42  '), 'Portal_User_42');
  assert.equal(crazyGamesGuestNickname('Élodie'), 'Élodie');
  assert.equal(crazyGamesGuestNickname('a'.repeat(40)), 'a'.repeat(20));
  assert.match(crazyGamesGuestNickname('x'), /^CGPlayer\d{4}$/);
});

test('the adapter fails open outside a CrazyGames build', async () => {
  const snapshot = await crazyGames.init();
  assert.equal(snapshot.isCrazyGamesBuild, false);
  assert.equal(snapshot.available, false);
  assert.equal(crazyGames.getDataModule(), null);
  assert.deepEqual(await crazyGames.requestAd('midgame'), { status: 'disabled' });
});

test('cold-start inviteParams are published for the invitation bridge', async () => {
  const sdkInviteParams = {
    lobbyCode: 'USE1-COLD',
    region: 'us-east-1',
  };

  await withInviteSdk(sdkInviteParams, async ({ service }) => {
    const observed: Array<{ sequence: number; params: InviteParams | null }> = [];
    const unsubscribe = service.subscribe((snapshot) => {
      observed.push({
        sequence: snapshot.inviteSequence,
        params: snapshot.inviteParams,
      });
    });

    const snapshot = await service.init();

    assert.equal(snapshot.available, true);
    assert.equal(snapshot.inviteSequence, 1);
    assert.deepEqual(snapshot.inviteParams, sdkInviteParams);
    assert.notEqual(snapshot.inviteParams, sdkInviteParams);
    assert.deepEqual([...new Set(observed.map(({ sequence }) => sequence))], [0, 1]);
    assert.deepEqual(observed.at(-1)?.params, sdkInviteParams);

    sdkInviteParams.lobbyCode = 'MUTATED';
    assert.equal(service.getSnapshot().inviteParams?.lobbyCode, 'USE1-COLD');
    unsubscribe();
  });
});

test('a warm addJoinRoomListener event publishes each accepted room invitation', async () => {
  await withInviteSdk(null, async ({ service, emitJoinRoom }) => {
    const snapshot = await service.init();
    assert.equal(snapshot.inviteParams, null);
    assert.equal(snapshot.inviteSequence, 0);

    const observed: Array<{ sequence: number; lobbyCode?: string }> = [];
    const unsubscribe = service.subscribe((next) => {
      observed.push({
        sequence: next.inviteSequence,
        lobbyCode: next.inviteParams?.lobbyCode,
      });
    });

    const firstInvite = { lobbyCode: 'USE1-WARM', region: 'us-east-1' };
    emitJoinRoom(firstInvite);
    firstInvite.lobbyCode = 'MUTATED';
    emitJoinRoom({ lobbyCode: 'EUW1-NEXT', region: 'eu-west-1' });

    assert.deepEqual(observed, [
      { sequence: 0, lobbyCode: undefined },
      { sequence: 1, lobbyCode: 'USE1-WARM' },
      { sequence: 2, lobbyCode: 'EUW1-NEXT' },
    ]);
    assert.deepEqual(service.getSnapshot().inviteParams, {
      lobbyCode: 'EUW1-NEXT',
      region: 'eu-west-1',
    });
    unsubscribe();
  });
});

test('an enabled v3 adapter bridges settings, data, rooms, identity, and ads', async () => {
  const previousBuild = process.env.CRAZYGAMES_BUILD;
  const previousAds = process.env.CRAZYGAMES_ADS_ENABLED;
  const previousData = process.env.CRAZYGAMES_DATA_ENABLED;
  const hadWindow = 'window' in globalThis;
  const previousWindow = (globalThis as any).window;

  process.env.CRAZYGAMES_BUILD = 'true';
  process.env.CRAZYGAMES_ADS_ENABLED = 'true';
  process.env.CRAZYGAMES_DATA_ENABLED = 'true';

  let settingsListener = (_settings: any) => {};
  let joinListener = (_params: Record<string, string>) => {};
  let authListener = (_user: any) => {};
  const calls: string[] = [];
  const roomUpdates: any[] = [];
  const data = new Map<string, string>();
  const portalUser = {
    __dangerousUserId: 'display-only-id',
    username: 'Portal.Player',
    profilePictureUrl: 'https://example.test/avatar.png',
  };

  const sdk = {
    environment: 'local',
    init: async () => { calls.push('init'); },
    ad: {
      hasAdblock: async () => false,
      requestAd: (_type: string, callbacks: any) => {
        calls.push('adRequested');
        callbacks.adStarted();
        callbacks.adFinished();
      },
    },
    banner: {
      requestBanner: async () => { calls.push('banner'); },
      requestResponsiveBanner: async () => {},
      clearBanner: () => {},
      clearAllBanners: () => {},
    },
    data: {
      clear: () => data.clear(),
      getItem: (key: string) => data.get(key) ?? null,
      removeItem: (key: string) => { data.delete(key); },
      setItem: (key: string, value: string) => { data.set(key, value); },
    },
    game: {
      settings: { disableChat: false, muteAudio: false },
      isInstantMultiplayer: true,
      inviteParams: { lobbyCode: 'USE1-START' },
      addSettingsChangeListener: (listener: typeof settingsListener) => { settingsListener = listener; },
      removeSettingsChangeListener: () => {},
      addJoinRoomListener: (listener: typeof joinListener) => { joinListener = listener; },
      removeJoinRoomListener: () => {},
      gameplayStart: () => { calls.push('gameplayStart'); },
      gameplayStop: () => { calls.push('gameplayStop'); },
      loadingStart: () => { calls.push('loadingStart'); },
      loadingStop: () => { calls.push('loadingStop'); },
      happytime: () => { calls.push('happytime'); },
      reportGameCompletedPercentage: () => {},
      setGameContext: () => {},
      clearGameContext: () => {},
      updateRoom: (update: any) => { roomUpdates.push(update); },
      leftRoom: () => { calls.push('leftRoom'); },
      inviteLink: (params: Record<string, string>) => `https://example.test/${params.lobbyCode}`,
      getInviteParam: () => null,
    },
    user: {
      isUserAccountAvailable: true,
      systemInfo: { locale: 'en-US' },
      getUser: async () => portalUser,
      getUserToken: async () => 'signed.jwt',
      listFriends: async () => ({ friends: [], page: 1, size: 50, hasMore: false, total: 0 }),
      showAuthPrompt: async () => portalUser,
      showAccountLinkPrompt: async () => ({ response: 'yes' as const }),
      addAuthListener: (listener: typeof authListener) => { authListener = listener; },
      removeAuthListener: () => {},
    },
  };
  (globalThis as any).window = { CrazyGames: { SDK: sdk } };

  try {
    const enabledModule = await import(`../../services/crazyGames.ts?mock=${Date.now()}`);
    const service = enabledModule.crazyGames;
    const initialized = await service.init();
    await Promise.resolve();

    assert.equal(initialized.available, true);
    assert.equal(initialized.environment, 'local');
    assert.equal(initialized.isInstantMultiplayer, true);
    assert.equal(service.getSnapshot().portalUser?.username, 'Portal.Player');
    assert.equal(service.getSnapshot().inviteSequence, 1);

    settingsListener({ disableChat: true, muteAudio: true });
    assert.deepEqual(service.getSnapshot().settings, { disableChat: true, muteAudio: true });
    joinListener({ lobbyCode: 'USE1-LIVE' });
    assert.equal(service.getSnapshot().inviteParams?.lobbyCode, 'USE1-LIVE');
    assert.equal(service.getSnapshot().inviteSequence, 2);

    service.getDataModule()?.setItem('preference', 'on');
    assert.equal(service.getDataModule()?.getItem('preference'), 'on');
    service.updateRoom({ roomId: 'use1:room', isJoinable: true });
    service.leftRoom();
    assert.deepEqual(roomUpdates, [{ roomId: 'use1:room', isJoinable: true }]);
    assert.equal(service.inviteLink({ lobbyCode: 'USE1-LIVE' }), 'https://example.test/USE1-LIVE');

    service.loadingStart();
    service.loadingStop();
    service.gameplayStart();
    assert.deepEqual(await service.requestAd('midgame'), { status: 'finished' });
    assert.deepEqual(
      calls.filter((call) => ['loadingStart', 'loadingStop', 'gameplayStart', 'gameplayStop', 'adRequested'].includes(call)),
      ['loadingStart', 'loadingStop', 'gameplayStart', 'adRequested', 'gameplayStop'],
    );

    authListener({ ...portalUser, username: 'Renamed.Player' });
    assert.equal(service.getSnapshot().portalUser?.username, 'Renamed.Player');
    assert.equal(await service.getUserToken(), 'signed.jwt');
    assert.equal(await service.requestBanner({ id: 'banner', width: 728, height: 90 }), true);
  } finally {
    if (hadWindow) {
      (globalThis as any).window = previousWindow;
    } else {
      delete (globalThis as any).window;
    }
    process.env.CRAZYGAMES_BUILD = previousBuild;
    process.env.CRAZYGAMES_ADS_ENABLED = previousAds;
    process.env.CRAZYGAMES_DATA_ENABLED = previousData;
  }
});
