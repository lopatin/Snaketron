import assert from 'node:assert/strict';
import test from 'node:test';

import {
  exactPublicationRequest,
  exactSkinUpdate,
} from '../../utils/skinApiContracts.ts';

test('skin writes and review requests bind exact immutable revision authority', () => {
  assert.deepEqual(
    exactSkinUpdate({
      name: 'Exact edit',
      document: { schema_version: 2 },
      expectedHeadRevision: 7,
    }),
    {
      name: 'Exact edit',
      document: { schema_version: 2 },
      expectedHeadRevision: 7,
    },
  );
  assert.deepEqual(exactPublicationRequest(8, `sha256:${'a'.repeat(64)}`), {
    revision: 8,
    contentRef: `sha256:${'a'.repeat(64)}`,
  });
});
