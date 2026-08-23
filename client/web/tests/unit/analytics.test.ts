import assert from 'node:assert/strict';
import test from 'node:test';
import {
  adBreakEvent,
  buildEventId,
  buildMatchResultEvent,
  deathCauseSlug,
  gameTypeSlug,
  matchProgression,
  queueIntentEvents,
  queueSlug,
  sanitizeEventPart,
} from '../../services/analytics/events.ts';
import {
  addressVerdictFromConsent,
  readAnalyticsOverrideFromUrl,
  resolveAnalyticsDecision,
  type ExclusionInputs,
} from '../../services/analytics/exclusion.ts';
import {
  resolveAnalyticsBuildConfig,
  resolveEmbeddedAnalyticsSupport,
} from '../../services/analytics/config.ts';
import type { GameType } from '../../types/generated/GameType.ts';

const duel: GameType = { TeamMatch: { per_team: 1 } };
const teamOfTwo: GameType = { TeamMatch: { per_team: 2 } };
const ffa: GameType = { FreeForAll: { max_players: 6 } };
const customDuel = {
  Custom: { settings: { game_mode: 'Duel' } },
} as unknown as GameType;

const counted: ExclusionInputs = {
  configured: true,
  supportedDistribution: true,
  localOverride: null,
  addressVerdict: 'counted',
  operatorAccount: false,
};

// --- the exclusion gate ---------------------------------------------------

test('an ordinary player on a configured web build is reported', () => {
  assert.deepEqual(resolveAnalyticsDecision(counted), { report: true });
  assert.deepEqual(
    resolveAnalyticsDecision({ ...counted, addressVerdict: 'unknown' }),
    { report: true },
    'a server that could not be reached must not silence every player',
  );
});

/**
 * The embedded packages report by default now — both portals permit
 * third-party game analytics, and portal traffic is most of the players. The
 * switch exists for a policy change, so it has to be the thing that turns it
 * off, not the default.
 */
test('an embedded package reports unless the switch is thrown', () => {
  assert.deepEqual(
    resolveEmbeddedAnalyticsSupport({ distribution: 'crazygames', disableEmbedded: false }),
    true,
  );
  assert.deepEqual(
    resolveEmbeddedAnalyticsSupport({ distribution: 'itch', disableEmbedded: false }),
    true,
  );
  assert.deepEqual(
    resolveEmbeddedAnalyticsSupport({ distribution: 'crazygames', disableEmbedded: true }),
    false,
  );
  assert.deepEqual(
    resolveEmbeddedAnalyticsSupport({ distribution: 'itch', disableEmbedded: true }),
    false,
  );
  // The website build is never affected by a switch aimed at release packages.
  assert.deepEqual(
    resolveEmbeddedAnalyticsSupport({ distribution: 'web', disableEmbedded: true }),
    true,
  );
});

test('a bundle with no keys, or a disabled distribution, never reports', () => {
  assert.deepEqual(resolveAnalyticsDecision({ ...counted, configured: false }), {
    report: false,
    reason: 'notConfigured',
  });
  assert.deepEqual(
    resolveAnalyticsDecision({ ...counted, supportedDistribution: false }),
    { report: false, reason: 'unsupportedDistribution' },
  );
});

test('each self-exclusion signal is independently sufficient', () => {
  assert.deepEqual(resolveAnalyticsDecision({ ...counted, localOverride: 'off' }), {
    report: false,
    reason: 'localOverride',
  });
  assert.deepEqual(resolveAnalyticsDecision({ ...counted, addressVerdict: 'excluded' }), {
    report: false,
    reason: 'excludedAddress',
  });
  assert.deepEqual(resolveAnalyticsDecision({ ...counted, operatorAccount: true }), {
    report: false,
    reason: 'operatorAccount',
  });
});

/**
 * The precedence that matters: `?analytics=on` undoes a previous opt-out in
 * this browser, but cannot re-enable a session the deployment or the account
 * excludes. Otherwise the operator's own switch would be a way to defeat the
 * exclusion they set up in the first place.
 */
test('an explicit opt-in cannot override an address or account exclusion', () => {
  assert.deepEqual(resolveAnalyticsDecision({ ...counted, localOverride: 'on' }), {
    report: true,
  });
  assert.deepEqual(
    resolveAnalyticsDecision({ ...counted, localOverride: 'on', addressVerdict: 'excluded' }),
    { report: false, reason: 'excludedAddress' },
  );
  assert.deepEqual(
    resolveAnalyticsDecision({ ...counted, localOverride: 'on', operatorAccount: true }),
    { report: false, reason: 'operatorAccount' },
  );
});

test('the analytics switch is read from either half of the URL', () => {
  assert.equal(readAnalyticsOverrideFromUrl('?analytics=off', ''), 'off');
  assert.equal(readAnalyticsOverrideFromUrl('', '#/play/7?analytics=off'), 'off');
  assert.equal(readAnalyticsOverrideFromUrl('?analytics=ON', ''), 'on');
  assert.equal(readAnalyticsOverrideFromUrl('?analytics=0', ''), 'off');
  assert.equal(readAnalyticsOverrideFromUrl('?analytics=1', ''), 'on');
  assert.equal(readAnalyticsOverrideFromUrl('?region=eu', '#/'), null);
  assert.equal(readAnalyticsOverrideFromUrl('?analytics=maybe', ''), null);
  assert.equal(
    readAnalyticsOverrideFromUrl('?analytics=off', '#/x?analytics=on'),
    'off',
    'the query string wins over the hash',
  );
});

/**
 * A newer server may learn a second reason to exclude an operator. A stale
 * bundle that does not recognize the reason must still honor the exclusion.
 */
test('any server exclusion is honored, including an unrecognized reason', () => {
  assert.equal(addressVerdictFromConsent({ excluded: false, reason: null }), 'counted');
  assert.equal(
    addressVerdictFromConsent({ excluded: true, reason: 'excludedAddress' }),
    'excluded',
  );
  assert.equal(
    addressVerdictFromConsent({ excluded: true, reason: 'somethingNewer' as never }),
    'excluded',
  );
});

test('both GameAnalytics keys are required for a bundle to be configured', () => {
  assert.equal(resolveAnalyticsBuildConfig('key', '', '1.0.0'), null);
  assert.equal(resolveAnalyticsBuildConfig('', 'secret', '1.0.0'), null);
  assert.equal(resolveAnalyticsBuildConfig('  ', '  ', '1.0.0'), null);
  assert.deepEqual(resolveAnalyticsBuildConfig('key', 'secret', '1.2.3'), {
    gameKey: 'key',
    secretKey: 'secret',
    build: '1.2.3',
  });
  assert.deepEqual(
    resolveAnalyticsBuildConfig('key', 'secret', undefined),
    { gameKey: 'key', secretKey: 'secret', build: '0.0.0' },
    'an unlabelled build still reports, under a placeholder version',
  );
});

/**
 * Fill rate is only meaningful if the misses are counted, so every resolution
 * maps to an event — a break that silently failed must not look identical to
 * one that never ran.
 */
test('every ad break outcome maps to a GameAnalytics ad event', () => {
  assert.deepEqual(adBreakEvent('completed'), { action: 'show', noAdReason: null });
  assert.deepEqual(adBreakEvent('unavailable'), {
    action: 'failedShow',
    noAdReason: 'noFill',
  });
  assert.deepEqual(adBreakEvent('error'), {
    action: 'failedShow',
    noAdReason: 'internalError',
  });
  // An ad blocker and a missed deadline are both "could not show, no reason
  // given". Inventing a closer reason would misreport fill rate.
  assert.deepEqual(adBreakEvent('blocked'), { action: 'failedShow', noAdReason: 'unknown' });
  assert.deepEqual(adBreakEvent('timed_out'), { action: 'failedShow', noAdReason: 'unknown' });
});

// --- event taxonomy -------------------------------------------------------

/**
 * GameAnalytics rejects an event whose parts leave its alphabet, and reports
 * the rejection as an SDK error rather than failing loudly — so the sanitizer
 * is the difference between a working dashboard and a silently empty one.
 */
test('event parts are forced into the alphabet GameAnalytics accepts', () => {
  assert.equal(sanitizeEventPart('team-2v2'), 'team-2v2');
  assert.equal(sanitizeEventPart('Solo Run (ranked)!?'), 'Solo Run (ranked)!?');
  assert.equal(sanitizeEventPart('mode:with:colons'), 'mode-with-colons');
  assert.equal(sanitizeEventPart('emoji 🐍 mode'), 'emoji -- mode');
  assert.equal(sanitizeEventPart(''), null);
  assert.equal(sanitizeEventPart('   '), null);
  assert.equal(sanitizeEventPart('🐍'), '--', 'replacement keeps a usable part');
  assert.equal(sanitizeEventPart('a'.repeat(80)), 'a'.repeat(64));
});

test('a design event id refuses more than five parts or an unusable one', () => {
  assert.equal(buildEventId(['match', 'death', 'wall']), 'match:death:wall');
  assert.equal(buildEventId(['a', 'b', 'c', 'd', 'e']), 'a:b:c:d:e');
  assert.equal(buildEventId(['a', 'b', 'c', 'd', 'e', 'f']), null);
  assert.equal(buildEventId([]), null);
  assert.equal(buildEventId(['match', '']), null);
});

test('game types map to stable, comparable slugs', () => {
  assert.equal(gameTypeSlug('Solo'), 'solo');
  assert.equal(gameTypeSlug(duel), 'duel');
  assert.equal(gameTypeSlug(teamOfTwo), 'team-2v2');
  assert.equal(gameTypeSlug(ffa), 'ffa');
  assert.equal(gameTypeSlug(customDuel), 'custom-duel');
});

/**
 * A custom game carries a nominal `queue_mode` it never actually queued in.
 * Reporting that verbatim would inflate quickmatch volume with private games.
 */
test('custom games are their own queue, whatever queue mode they carry', () => {
  assert.equal(queueSlug(duel, 'Quickmatch'), 'quickmatch');
  assert.equal(queueSlug(duel, 'Competitive'), 'competitive');
  assert.equal(queueSlug(customDuel, 'Competitive'), 'custom');
  assert.equal(queueSlug(customDuel, 'Quickmatch'), 'custom');
});

test('death causes collapse to bounded, non-identifying slugs', () => {
  assert.equal(deathCauseSlug('Wall'), 'wall');
  assert.equal(deathCauseSlug('OutOfBounds'), 'out-of-bounds');
  assert.equal(deathCauseSlug('EnemyBase'), 'enemy-base');
  assert.equal(deathCauseSlug('SelfCollision'), 'self');
  assert.equal(deathCauseSlug('Banked'), 'banked');
  assert.equal(deathCauseSlug('Unknown'), 'unknown');
  // The killer's per-match snake id is deliberately dropped: it would make the
  // dimension unbounded without answering "how do players die".
  assert.equal(deathCauseSlug({ SnakeBody: { killer_snake_id: 3 } }), 'enemy-body');
  assert.equal(deathCauseSlug({ HeadToHead: { other_snake_id: 5 } }), 'head-to-head');
});

test('a match progression is the mode and the queue it came from', () => {
  assert.deepEqual(matchProgression(teamOfTwo, 'Competitive'), {
    progression01: 'team-2v2',
    progression02: 'competitive',
  });
  assert.deepEqual(matchProgression('Solo', 'Quickmatch'), {
    progression01: 'solo',
    progression02: 'quickmatch',
  });
});

test('a finished match reports the local outcome, score, and duration', () => {
  const state = { game_type: ffa, queue_mode: 'Competitive' } as const;

  assert.deepEqual(buildMatchResultEvent(state, { score: 42, isWinner: true }, 95_400), {
    progression01: 'ffa',
    progression02: 'competitive',
    outcome: 'complete',
    score: 42,
    durationSeconds: 95,
  });
  assert.deepEqual(buildMatchResultEvent(state, { score: 7, isWinner: false }, 1_000), {
    progression01: 'ffa',
    progression02: 'competitive',
    outcome: 'fail',
    score: 7,
    durationSeconds: 1,
  });
});

/** A spectator has no result, and a negative clock is not a duration. */
test('a match with no local player reports nothing, and values stay sane', () => {
  const state = { game_type: ffa, queue_mode: 'Quickmatch' } as const;
  assert.equal(buildMatchResultEvent(state, null, 1_000), null);

  const clamped = buildMatchResultEvent(state, { score: -5, isWinner: false }, -2_000);
  assert.equal(clamped?.score, 0);
  assert.equal(clamped?.durationSeconds, 0);

  const rounded = buildMatchResultEvent(state, { score: 3.6, isWinner: false }, 2_500);
  assert.equal(rounded?.score, 4, 'GameAnalytics stores an integer score');
  assert.equal(rounded?.durationSeconds, 3);
});

/**
 * Multi-mode queueing is one socket message but several statements of intent.
 * Per-mode demand is the whole question, so it must not collapse to one event.
 */
test('a queue request reports one event per selected mode', () => {
  assert.deepEqual(
    queueIntentEvents({ QueueForMatch: { game_type: duel, queue_mode: 'Competitive' } }),
    [{ queue: 'competitive', mode: 'duel' }],
  );
  assert.deepEqual(
    queueIntentEvents({
      QueueForMatchMulti: { game_types: [duel, ffa, teamOfTwo], queue_mode: 'Quickmatch' },
    }),
    [
      { queue: 'quickmatch', mode: 'duel' },
      { queue: 'quickmatch', mode: 'ffa' },
      { queue: 'quickmatch', mode: 'team-2v2' },
    ],
  );
  assert.deepEqual(queueIntentEvents('LeaveQueue'), []);
  assert.deepEqual(queueIntentEvents({ JoinGame: 7 }), []);
  assert.deepEqual(queueIntentEvents(null), []);
});
