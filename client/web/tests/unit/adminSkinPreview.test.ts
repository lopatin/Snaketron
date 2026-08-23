import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_SKIN_PERIOD_MS,
  adminPublishDisabled,
  adminPreviewTargetKey,
  adminRejectActionLabel,
  advanceSkinTimeline,
  initialSkinPreviewPlaying,
  shortContentRef,
  skinAnimationPeriodMs,
  skinDocumentUsesImages,
  skinPreviewAssetError,
} from '../../utils/adminSkinPreview.ts';

test('admin preview reads the live SkinDoc period from both schema versions', () => {
  assert.equal(skinAnimationPeriodMs({ schema_version: 2, period_ms: 4_800 }), 4_800);
  assert.equal(
    skinAnimationPeriodMs({ schema_version: 1, animation: { period_ms: 1_250 } }),
    1_250,
  );
  assert.equal(skinAnimationPeriodMs({ schema_version: 2 }), DEFAULT_SKIN_PERIOD_MS);
  assert.equal(skinAnimationPeriodMs({ period_ms: Number.NaN }), DEFAULT_SKIN_PERIOD_MS);
});

test('admin preview plays one loop, supports scrubbing, and starts still', () => {
  assert.equal(advanceSkinTimeline(900, 250, 1_000), 150);
  assert.equal(advanceSkinTimeline(400, 0, 1_000), 400);
  assert.equal(initialSkinPreviewPlaying(), false);
});

test('exact target labels keep both ends of the immutable hash visible', () => {
  const contentRef = `sha256:${'12345678'}${'a'.repeat(48)}${'87654321'}`;
  assert.equal(shortContentRef(contentRef), 'sha256:12345678…87654321');
});

test('reject copy distinguishes a private draft from an edit to a published skin', () => {
  assert.equal(adminRejectActionLabel(null), 'Reject draft');
  assert.equal(adminRejectActionLabel(7), 'Reject pending edit');
});

test('publish stays fail-closed until the exact preview is ready', () => {
  assert.equal(adminPublishDisabled(false, undefined), true);
  assert.equal(adminPublishDisabled(false, 'loading'), true);
  assert.equal(adminPublishDisabled(false, 'error'), true);
  assert.equal(adminPublishDisabled(false, 'ready'), false);
  assert.equal(adminPublishDisabled(true, 'ready'), true);
  assert.notEqual(
    adminPreviewTargetKey(9, 2, 'sha256:same'),
    adminPreviewTargetKey(9, 3, 'sha256:same'),
    'a moving pending target cannot inherit the previous preview readiness',
  );
});

test('image previews require loaded pixels to reach the canvas before approval', () => {
  assert.equal(skinDocumentUsesImages({ layers: [{ source: { type: 'solid' } }] }), false);
  assert.equal(
    skinDocumentUsesImages({ layers: [{ source: { type: 'image', texture: 'coat' } }] }),
    true,
  );

  const ready = {
    requested: 1,
    pending: 0,
    ready: 1,
    failed: 0,
    drawnImages: 1,
    drawCalls: 3,
  };
  assert.equal(skinPreviewAssetError(ready, true, 2), null);
  assert.match(
    skinPreviewAssetError({ ...ready, failed: 1 }, true, 2) ?? '',
    /could not/i,
  );
  assert.match(
    skinPreviewAssetError({ ...ready, drawCalls: 2 }, true, 2) ?? '',
    /never reached/i,
  );
  assert.equal(skinPreviewAssetError({ ...ready, drawCalls: 0 }, false, 0), null);
});
