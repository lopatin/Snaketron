import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildCrazyGamesRoomUpdate,
  enterCrazyGamesInviteTarget,
  resolveCrazyGamesInvite,
} from '../../utils/crazyGamesMultiplayer.ts';

test('a waiting lobby is reported as one stable joinable CrazyGames room', () => {
  assert.deepEqual(
    buildCrazyGamesRoomUpdate(
      { code: 'use1-abc123', state: 'waiting' },
      2,
    ),
    {
      roomId: 'lobby:USE1-ABC123',
      isJoinable: true,
      inviteParams: { lobbyCode: 'USE1-ABC123' },
    },
  );

  assert.equal(
    buildCrazyGamesRoomUpdate({ code: 'USE1-ABC123', state: 'queued' }, 2)?.isJoinable,
    false,
  );
  assert.equal(
    buildCrazyGamesRoomUpdate({ code: 'USE1-ABC123', state: 'waiting' }, 4)?.isJoinable,
    false,
  );
});

test('cold friend invitations resolve directly to the invited lobby', () => {
  assert.deepEqual(
    resolveCrazyGamesInvite(
      { lobbyCode: 'use1-cold', region: 'untrusted-and-unneeded' },
      null,
    ),
    {
      lobbyCode: 'USE1-COLD',
      route: '/lobby/USE1-COLD',
      leaveCurrentLobby: false,
    },
  );
});

test('warm friend invitations leave a different room before joining the target', () => {
  assert.deepEqual(
    resolveCrazyGamesInvite({ lobbyCode: 'EUW1-FRIEND' }, 'USE1-CURRENT'),
    {
      lobbyCode: 'EUW1-FRIEND',
      route: '/lobby/EUW1-FRIEND',
      leaveCurrentLobby: true,
    },
  );
});

test('the full warm invitation flow leaves first and then navigates', async () => {
  const target = resolveCrazyGamesInvite(
    { lobbyCode: 'EUW1-FRIEND' },
    'USE1-CURRENT',
  );
  assert.ok(target);
  const events: string[] = [];

  const navigated = await enterCrazyGamesInviteTarget(target, {
    leaveLobby: async () => {
      events.push('leave:start');
      await Promise.resolve();
      events.push('leave:done');
    },
    navigate: (route) => events.push(`navigate:${route}`),
    isInviteCurrent: () => true,
  });

  assert.equal(navigated, true);
  assert.deepEqual(events, [
    'leave:start',
    'leave:done',
    'navigate:/lobby/EUW1-FRIEND',
  ]);
});

test('the full cold flow navigates without leaving and stale warm invites are suppressed', async () => {
  const coldTarget = resolveCrazyGamesInvite({ lobbyCode: 'USE1-COLD' }, null);
  const warmTarget = resolveCrazyGamesInvite(
    { lobbyCode: 'EUW1-STALE' },
    'USE1-CURRENT',
  );
  assert.ok(coldTarget);
  assert.ok(warmTarget);

  const events: string[] = [];
  assert.equal(
    await enterCrazyGamesInviteTarget(coldTarget, {
      leaveLobby: async () => events.push('unexpected-leave'),
      navigate: (route) => events.push(`navigate:${route}`),
      isInviteCurrent: () => true,
    }),
    true,
  );
  assert.equal(
    await enterCrazyGamesInviteTarget(warmTarget, {
      leaveLobby: async () => events.push('leave:stale'),
      navigate: (route) => events.push(`unexpected:${route}`),
      isInviteCurrent: () => false,
    }),
    false,
  );
  assert.deepEqual(events, ['navigate:/lobby/USE1-COLD', 'leave:stale']);
});

test('same-room, malformed, and missing invitations are safe no-ops', () => {
  assert.equal(
    resolveCrazyGamesInvite({ roomCode: 'use1-same' }, 'USE1-SAME'),
    null,
  );
  assert.equal(resolveCrazyGamesInvite({ lobbyCode: '../bad' }, null), null);
  assert.equal(resolveCrazyGamesInvite({}, null), null);
  assert.equal(
    buildCrazyGamesRoomUpdate({ code: '../bad', state: 'waiting' }, 1),
    null,
  );
});
