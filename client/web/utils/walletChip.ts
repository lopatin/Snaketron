/**
 * The arithmetic and the wording behind the header's Snake Bux chip.
 *
 * Pure, and in `utils/` rather than in the component, because that is the only
 * place this repo's test runner can reach: `npm run test:unit` runs
 * `node --test tests/unit/*.test.ts` against plain modules, and there is no
 * React rendering harness. Logic worth checking has to live somewhere it can
 * be checked.
 */

/** What a balance is called, once, so the chip and the copy agree. */
export const BUX_UNIT = 'BUX';

/**
 * Read a balance off the wire.
 *
 * `Wallet.balanceBux` is declared `bigint` — ts-rs stamps that on a Rust `i64`
 * — and is a `number` at runtime, because the transport is `JSON.parse`, which
 * has never produced a `bigint` in its life. So the declared type is a lie the
 * compiler believes: `typeof balance === 'bigint'` is false, and arithmetic
 * against a number literal type-errors while working perfectly.
 *
 * Coerced once here, at the boundary, so nothing downstream ever sees it.
 *
 * A balance is signed on purpose — the ledger allows a chargeback to take an
 * account negative — so this does not clamp. A negative balance is a real
 * state and the player should be able to see it.
 */
export const coerceBalance = (value: unknown): number | null => {
  if (typeof value === 'bigint') {
    return Number(value);
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  return null;
};

/**
 * A balance, grouped for reading.
 *
 * Thousands separators because a five-figure balance is otherwise a wall of
 * digits in an 11px chip, and the header has three separate media queries
 * devoted to stopping this cluster overflowing on a phone.
 */
export const formatBux = (balance: number): string =>
  new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(balance);

/**
 * Whether the chip belongs on screen at all.
 *
 * Only for a signed-in player with a balance we actually have. A signed-out
 * visitor has no wallet — `GET /api/wallet` is auth-gated and 401s — and a
 * chip showing a hopeful zero while the request fails would be worse than no
 * chip, because zero is a real balance and looks like one.
 */
export const shouldShowBuxChip = (
  signedIn: boolean,
  balance: number | null,
): balance is number => signedIn && balance !== null;

/**
 * What to tell a buyer, from the outcome the server actually returned.
 *
 * The purchase route answers 402 and 409 with a `PurchaseResult` body and no
 * `error` or `message` key, so the generic error reader falls through to
 * "Request failed" — which is what a player short of Bux was being told. A
 * balance chip beside that message is the moment it stops being tolerable.
 */
export const purchaseMessage = (
  outcome: string | undefined,
  actualPriceBux: number | null | undefined,
): string => {
  switch (outcome) {
    case 'purchased':
      return 'Bought.';
    case 'alreadyOwned':
      return 'You already own this one.';
    case 'insufficientFunds':
      return `Not enough ${BUX_UNIT} for this one.`;
    case 'priceChanged':
      return actualPriceBux === null || actualPriceBux === undefined
        ? 'The price changed. Try again.'
        : `The price changed to ${formatBux(actualPriceBux)} ${BUX_UNIT}. Try again.`;
    default:
      return 'That did not go through.';
  }
};
