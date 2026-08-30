import assert from 'node:assert/strict';
import test from 'node:test';

import {
  BUX_UNIT,
  coerceBalance,
  formatBux,
  purchaseMessage,
  shouldShowBuxChip,
} from '../../utils/walletChip.ts';

test('a balance is read as a number whatever the wire calls it', () => {
  // The declared type says bigint; JSON.parse says number. Both arrive.
  assert.equal(coerceBalance(1250), 1250);
  assert.equal(coerceBalance(1250n), 1250);
  assert.equal(coerceBalance(0), 0);

  // A chargeback may take an account negative, which is a real state and not
  // one to hide behind a clamp.
  assert.equal(coerceBalance(-40), -40);

  // Anything that is not a balance is an absent balance, not a zero: zero is
  // itself a balance and would read as one.
  for (const value of [undefined, null, '1250', {}, NaN, Infinity]) {
    assert.equal(coerceBalance(value), null, `${String(value)} is not a balance`);
  }
});

test('the chip appears only for a signed-in player whose balance arrived', () => {
  assert.equal(shouldShowBuxChip(true, 500, true), true);
  assert.equal(shouldShowBuxChip(true, 0, true), true, 'nothing is a balance too');

  assert.equal(shouldShowBuxChip(false, 500, true), false, 'signed out has no wallet');
  assert.equal(
    shouldShowBuxChip(true, null, true),
    false,
    'a failed fetch must not paint a hopeful zero — zero is a real balance',
  );
});

test('a build that cannot sell Snakebux does not mention Snakebux', () => {
  assert.equal(
    shouldShowBuxChip(true, 500, false),
    false,
    'a balance nobody can add to is an invitation to a shop that is not open',
  );
  assert.equal(
    shouldShowBuxChip(true, 500, null),
    false,
    'unknown is not yes: no chip until the shop has actually answered',
  );

  // …and the moment Xsolla is connected the shop stops being empty, so the
  // same balance appears with no other change anywhere.
  assert.equal(shouldShowBuxChip(true, 500, true), true);
});

test('a balance is grouped, because the header has no room to spare', () => {
  assert.equal(formatBux(0), '0');
  assert.equal(formatBux(999), '999');
  assert.match(formatBux(12500), /12.500/, 'five figures get a separator');
});

test('a buyer is told what actually happened', () => {
  assert.match(purchaseMessage('purchased', null), /Bought/);
  assert.match(purchaseMessage('alreadyOwned', null), /already own/);

  // The case that was reaching players as "Request failed": the route answers
  // 402 with a body carrying no `error` or `message` key at all.
  assert.match(purchaseMessage('insufficientFunds', null), /Not enough/);
  assert.ok(purchaseMessage('insufficientFunds', null).includes(BUX_UNIT));

  // A moved price says what it moved to, so the re-prompt is actionable.
  assert.match(purchaseMessage('priceChanged', 750), /750/);
  assert.match(purchaseMessage('priceChanged', null), /price changed/i);

  // An outcome from a newer server is still a sentence.
  assert.ok(purchaseMessage('somethingNew', null).length > 0);
  assert.ok(purchaseMessage(undefined, null).length > 0);
});
