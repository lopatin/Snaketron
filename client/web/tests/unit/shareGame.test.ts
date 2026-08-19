import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildGameShareUrl,
  buildShareContent,
  buildShareTargets,
  canUseNativeShare,
} from '../../utils/shareGame.ts';

test('share links address the permanent public match page', () => {
  assert.equal(
    buildGameShareUrl({ gameId: 4242, origin: 'https://snaketron.io' }),
    'https://snaketron.io/g/4242',
  );
  assert.equal(
    buildGameShareUrl({ gameId: '4242', origin: 'https://snaketron.io/' }),
    'https://snaketron.io/g/4242',
  );
});

test('a local origin is kept so a dev link points at the server that played the match', () => {
  assert.equal(
    buildGameShareUrl({ gameId: 7, origin: 'http://localhost:3000' }),
    'http://localhost:3000/g/7',
  );
});

test('embedded builds link to the canonical site, never their portal host', () => {
  // itch and CrazyGames serve the bundle from a deep path on someone else's
  // static host; a link to that origin would strand whoever opened it.
  assert.equal(
    buildGameShareUrl({
      gameId: 12,
      origin: 'https://html-classic.itch.zone/html/9999/index.html',
      isEmbeddedBuild: true,
    }),
    'https://snaketron.io/g/12',
  );
});

test('an unusable origin falls back rather than emitting a half-formed link', () => {
  assert.equal(buildGameShareUrl({ gameId: 3, origin: 'snaketron.io' }), 'https://snaketron.io/g/3');
  assert.equal(buildGameShareUrl({ gameId: 3, origin: null }), 'https://snaketron.io/g/3');
});

test('only a real decimal game id is shareable', () => {
  for (const gameId of [null, undefined, '', '   ', 'abc', '-1', '0', '1.5', '4294967296']) {
    assert.equal(
      buildGameShareUrl({ gameId, origin: 'https://snaketron.io' }),
      null,
      `expected ${String(gameId)} to be unshareable`,
    );
  }
  assert.equal(
    buildGameShareUrl({ gameId: 4294967295, origin: 'https://snaketron.io' }),
    'https://snaketron.io/g/4294967295',
  );
});

test('share text falls back to something truthful before a result exists', () => {
  const withHeadline = buildShareContent('https://snaketron.io/g/1', '  Ada won a Duel, 12–7.  ');
  assert.equal(withHeadline.text, 'Ada won a Duel, 12–7.');

  const midMatch = buildShareContent('https://snaketron.io/g/1', null);
  assert.equal(midMatch.text, 'Watch this Snaketron match.');
});

test('every share target carries the encoded url', () => {
  const content = buildShareContent('https://snaketron.io/g/1', 'Ada won a Duel, 12–7.');
  const targets = buildShareTargets(content);

  assert.deepEqual(
    targets.map((target) => target.id),
    ['x', 'reddit', 'facebook', 'whatsapp'],
  );
  for (const target of targets) {
    assert.ok(
      target.href.includes(encodeURIComponent(content.url)),
      `${target.id} does not carry the share url`,
    );
    assert.ok(target.href.startsWith('https://'), `${target.id} is not https`);
    // A raw URL in a query string would break on the first `&` or `#`.
    assert.ok(
      !target.href.includes('https://snaketron.io/g/1'),
      `${target.id} embedded the url unencoded`,
    );
  }
});

test('native share is treated as a capability, not an assumption', () => {
  assert.equal(canUseNativeShare({} as Navigator), false);
  assert.equal(canUseNativeShare(undefined), false);
  assert.equal(canUseNativeShare({ share: () => Promise.resolve() } as unknown as Navigator), true);
});
