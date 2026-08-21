import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

import {
  attestRendererBundle,
  classifyRendererResponse,
  parsePinnedRendererBundle,
} from './renderer-bundle-attestation.mjs';

const manifest = {
  schema_version: 1,
  root: 'client/web/dist',
  assets: {
    'client_bg.wasm': { sha256: 'c'.repeat(64), size_bytes: 30 },
    'index.html': { sha256: 'a'.repeat(64), size_bytes: 10 },
    'main.js': { sha256: 'b'.repeat(64), size_bytes: 20 },
  },
};
const rawManifest = JSON.stringify(manifest);
const manifestSha = createHash('sha256').update(rawManifest).digest('hex');

test('parses only the exact behavior-pinned manifest', () => {
  assert.deepEqual(parsePinnedRendererBundle(rawManifest, manifestSha), manifest);
  assert.throws(
    () => parsePinnedRendererBundle(`${rawManifest} `, manifestSha),
    /does not match its SHA-256/,
  );
});

test('maps the history-fallback document and rejects cross-origin executable assets', () => {
  assert.deepEqual(
    classifyRendererResponse(
      'https://client.test/qa/skins',
      'document',
      'text/html',
      'https://client.test',
    ),
    { path: 'index.html', kind: 'html' },
  );
  assert.match(
    classifyRendererResponse(
      'https://untrusted.test/main.js',
      'script',
      'application/javascript',
      'https://client.test',
    ).error,
    /cross-origin/,
  );
});

test('attests exact served HTML, JavaScript, and WASM bytes', () => {
  const attestation = attestRendererBundle(manifest, manifestSha, [
    { path: 'index.html', kind: 'html', sha256: 'a'.repeat(64), size_bytes: 10 },
    { path: 'main.js', kind: 'js', sha256: 'b'.repeat(64), size_bytes: 20 },
    { path: 'client_bg.wasm', kind: 'wasm', sha256: 'c'.repeat(64), size_bytes: 30 },
  ]);
  assert.deepEqual(attestation.errors, []);
  assert.equal(attestation.bundle_manifest_sha256, manifestSha);
  assert.deepEqual(
    attestation.assets.map(({ path }) => path),
    ['client_bg.wasm', 'index.html', 'main.js'],
  );
});

test('fails closed for served JavaScript or WASM drift', () => {
  for (const path of ['main.js', 'client_bg.wasm']) {
    const attestation = attestRendererBundle(manifest, manifestSha, [
      { path: 'index.html', kind: 'html', sha256: 'a'.repeat(64), size_bytes: 10 },
      {
        path,
        kind: path.endsWith('.wasm') ? 'wasm' : 'js',
        sha256: '0'.repeat(64),
        size_bytes: manifest.assets[path].size_bytes,
      },
    ]);
    assert.ok(attestation.errors.includes(`served bytes differ for ${path}`));
  }
});
