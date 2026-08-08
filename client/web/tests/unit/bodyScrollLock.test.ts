import assert from 'node:assert/strict';
import test from 'node:test';

// The util touches only `document.body.style.overflow`, so a minimal stand-in
// is enough and keeps these tests free of a full DOM.
(globalThis as unknown as { document: { body: { style: { overflow: string } } } }).document = {
  body: { style: { overflow: '' } },
};
const bodyOverflow = (): string => (
  (globalThis as unknown as { document: { body: { style: { overflow: string } } } })
    .document.body.style.overflow
);

const { lockBodyScroll } = await import('../../utils/bodyScrollLock.ts');

test('overlapping modals restore the real page value, never a nested lock', () => {
  (globalThis as unknown as { document: { body: { style: { overflow: string } } } })
    .document.body.style.overflow = 'auto';

  // The briefing/help screen locks first, then a removal dialog mounts over it
  // in the same commit — the exact overlap the merge made reachable.
  const releaseFirst = lockBodyScroll();
  assert.equal(bodyOverflow(), 'hidden');
  const releaseSecond = lockBodyScroll();
  assert.equal(bodyOverflow(), 'hidden');

  // The outer surface closing must not unlock while the inner one is still up.
  releaseFirst();
  assert.equal(bodyOverflow(), 'hidden');

  releaseSecond();
  assert.equal(bodyOverflow(), 'auto', 'the page value the first locker saw');
});

test('a release is idempotent, so a double cleanup cannot unlock a live modal', () => {
  (globalThis as unknown as { document: { body: { style: { overflow: string } } } })
    .document.body.style.overflow = '';

  const releaseOuter = lockBodyScroll();
  const releaseInner = lockBodyScroll();

  releaseInner();
  releaseInner();
  assert.equal(bodyOverflow(), 'hidden', 'the outer lock still holds');

  releaseOuter();
  assert.equal(bodyOverflow(), '');
});
