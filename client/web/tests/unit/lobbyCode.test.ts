import assert from 'node:assert/strict';
import test from 'node:test';

import {
  getLobbyCodeValidationError,
  normalizeLobbyCodeInput,
} from '../../utils/lobbyCode.ts';

test('normalizes complete region-prefixed lobby codes', () => {
  assert.equal(normalizeLobbyCodeInput('  use1-a3b2c4d5  '), 'USE1-A3B2C4D5');
  assert.equal(getLobbyCodeValidationError('USE1-A3B2C4D5'), null);
});

test('extracts lobby codes from current and legacy invite links', () => {
  assert.equal(
    normalizeLobbyCodeInput('https://snaketron.io/lobby/USE1-A3B2C4D5?from=copy#invite'),
    'USE1-A3B2C4D5',
  );
  assert.equal(
    normalizeLobbyCodeInput('/join/us-6bkvcpvp/'),
    'US-6BKVCPVP',
  );
});

test('decodes copied URL path segments', () => {
  assert.equal(
    normalizeLobbyCodeInput('https://snaketron.io/lobby/USE1%2DA3B2C4D5'),
    'USE1-A3B2C4D5',
  );
});

test('accepts older unprefixed codes and rejects malformed input', () => {
  assert.equal(getLobbyCodeValidationError('A3B2C4D5'), null);
  assert.equal(
    getLobbyCodeValidationError('USE1_A3B2C4D5'),
    'Lobby codes use only letters, numbers, and hyphens.',
  );
  assert.equal(
    getLobbyCodeValidationError('   '),
    'Enter a lobby code or invite link.',
  );
});
