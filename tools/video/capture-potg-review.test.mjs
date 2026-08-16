import assert from 'node:assert/strict';
import test from 'node:test';
import { captureArguments, parseArgs } from './capture-potg-review.mjs';

test('review capture requires explicit manifest and output locations', () => {
  const options = parseArgs([
    '--manifest', 'review/top-20-review.json',
    '--out', '/tmp/potg-review',
    '--capture-vfps', '120',
    '--url', 'http://127.0.0.1:3000',
    '--virtual-time',
    '--limit', '2',
  ]);
  assert.equal(options.captureVfps, 120);
  assert.equal(options.limit, 2);
  assert.equal(options.virtualTime, true);
  const args = captureArguments(options, '/tmp/clip.json', '/tmp/render');
  assert.deepEqual(args.slice(-6), [
    '--capture-vfps', '120', '--viewer-timing', '--url',
    'http://127.0.0.1:3000', '--virtual-time',
  ]);
  assert.ok(args.includes('--viewer-timing'));
  assert.ok(args.includes('/tmp/clip.json'));
  assert.ok(args.includes('/tmp/render'));
});
