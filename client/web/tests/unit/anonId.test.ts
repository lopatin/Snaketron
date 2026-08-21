import assert from 'node:assert/strict';
import test from 'node:test';
import {
  ANON_ID_STORAGE_KEY,
  getOrCreateAnonId,
  isValidAnonId,
  resetAnonIdCacheForTests,
} from '../../utils/anonId.ts';

/** Minimal localStorage double; `throwOnAccess` models private-browsing modes. */
const installStorage = (
  initial: Record<string, string> = {},
  throwOnAccess = false
) => {
  const store = new Map(Object.entries(initial));
  (globalThis as { window?: unknown }).window = {
    localStorage: {
      getItem(key: string) {
        if (throwOnAccess) throw new Error('storage blocked');
        return store.get(key) ?? null;
      },
      setItem(key: string, value: string) {
        if (throwOnAccess) throw new Error('storage blocked');
        store.set(key, value);
      },
    },
  };
  return store;
};

test('mints and persists a v4 uuid on first use', () => {
  resetAnonIdCacheForTests();
  const store = installStorage();
  const id = getOrCreateAnonId();
  assert.ok(isValidAnonId(id), `expected a uuid, got ${id}`);
  assert.equal(store.get(ANON_ID_STORAGE_KEY), id);
  assert.equal(id[14], '4', 'must be uuid version 4');
  assert.ok(['8', '9', 'a', 'b'].includes(id[19]), 'must use the RFC 4122 variant');
});

test('reuses the persisted id across calls', () => {
  resetAnonIdCacheForTests();
  installStorage({ [ANON_ID_STORAGE_KEY]: '3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607' });
  assert.equal(getOrCreateAnonId(), '3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607');
});

// A tampered or truncated value must not propagate into analytics events.
test('replaces a malformed stored id rather than trusting it', () => {
  resetAnonIdCacheForTests();
  const store = installStorage({ [ANON_ID_STORAGE_KEY]: 'not-a-uuid' });
  const id = getOrCreateAnonId();
  assert.ok(isValidAnonId(id));
  assert.equal(store.get(ANON_ID_STORAGE_KEY), id);
});

// Storage throwing must degrade analytics, never break the page.
test('still returns a usable id when storage is unavailable', () => {
  resetAnonIdCacheForTests();
  installStorage({}, true);
  const id = getOrCreateAnonId();
  assert.ok(isValidAnonId(id));
  assert.equal(getOrCreateAnonId(), id, 'must stay stable for the page lifetime');
});

test('rejects non-uuid shapes', () => {
  assert.equal(isValidAnonId(''), false);
  assert.equal(isValidAnonId(null), false);
  assert.equal(isValidAnonId('3F1A2B4C-5D6E-4F70-8A91-B2C3D4E5F607'), false);
  assert.equal(isValidAnonId('3f1a2b4c5d6e4f708a91b2c3d4e5f607'), false);
});
