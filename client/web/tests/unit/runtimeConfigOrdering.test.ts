import assert from 'node:assert/strict';
import test from 'node:test';
import { shouldApplyRuntimeConfigResponse } from '../../utils/runtimeConfigOrdering.ts';

test('the latest runtime config response may reaffirm the current version', () => {
  assert.equal(shouldApplyRuntimeConfigResponse({
    requestSequence: 3,
    latestRequestSequence: 3,
    responseVersion: 7,
    appliedVersion: 7,
  }), true);
});

test('an obsolete same-version response cannot supersede a newer refresh attempt', () => {
  assert.equal(shouldApplyRuntimeConfigResponse({
    requestSequence: 2,
    latestRequestSequence: 3,
    responseVersion: 7,
    appliedVersion: 7,
  }), false);
});

test('an out-of-order response carrying a newer durable version is still applied', () => {
  assert.equal(shouldApplyRuntimeConfigResponse({
    requestSequence: 2,
    latestRequestSequence: 3,
    responseVersion: 8,
    appliedVersion: 7,
  }), true);
});
