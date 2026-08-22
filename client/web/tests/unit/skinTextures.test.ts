import assert from 'node:assert/strict';
import test from 'node:test';

import {
  namedTextures,
  reconcileTextures,
  type BuiltinTexture,
} from '../../utils/skinTextures.ts';

const CATALOGUE: BuiltinTexture[] = [
  { id: 'jaguar.v1', label: 'Jaguar', kind: 'coat', contentRef: 'tex:jaguar' },
  { id: 'pitlane.v1', label: 'Pitlane', kind: 'coat', contentRef: 'tex:pitlane' },
  { id: 'chevron.v1', label: 'Chevron', kind: 'sheet', contentRef: 'tex:chevron' },
  { id: 'scales.v1', label: 'Scales', kind: 'coat', contentRef: 'tex:scales' },
  { id: 'sparks.v1', label: 'Sparks', kind: 'overlay', contentRef: 'tex:sparks' },
];

const wearing = (texture: string) => ({
  layers: [{ name: 'Body', source: { kind: 'image', texture } }],
});

test('a layer naming a texture gets it declared', () => {
  const doc = reconcileTextures(wearing('jaguar.v1'), CATALOGUE);
  assert.deepEqual(doc.textures, [
    { name: 'jaguar.v1', ref: 'tex:jaguar', kind: 'coat' },
  ]);
});

test('nested layers are searched too', () => {
  const doc = {
    layers: [
      { name: 'Group', layers: [{ source: { kind: 'image', texture: 'chevron.v1' } }] },
    ],
  };
  assert.deepEqual([...namedTextures(doc)], ['chevron.v1']);
  assert.equal((reconcileTextures(doc, CATALOGUE).textures as unknown[]).length, 1);
});

/**
 * The bug this module was extracted for. Declarations used to only ever be
 * added, so browsing textures — one selected at a time — walked the document
 * into "at most 4 textures per skin" with a single texture in use.
 */
test('browsing textures never declares more than the one in use', () => {
  let doc: Record<string, unknown> = wearing('jaguar.v1');
  for (const id of ['pitlane.v1', 'chevron.v1', 'scales.v1', 'sparks.v1', 'jaguar.v1']) {
    doc = reconcileTextures(
      { ...doc, layers: [{ name: 'Body', source: { kind: 'image', texture: id } }] },
      CATALOGUE,
    );
    assert.equal(
      (doc.textures as unknown[]).length,
      1,
      `declared more than one texture after picking ${id}`,
    );
  }
  assert.deepEqual(doc.textures, [
    { name: 'jaguar.v1', ref: 'tex:jaguar', kind: 'coat' },
  ]);
});

test('several layers wearing several textures all stay declared', () => {
  const doc = reconcileTextures(
    {
      layers: [
        { source: { kind: 'image', texture: 'jaguar.v1' } },
        { source: { kind: 'image', texture: 'chevron.v1' } },
        { source: { kind: 'image', texture: 'jaguar.v1' } },
      ],
    },
    CATALOGUE,
  );
  assert.deepEqual(
    (doc.textures as Array<{ name: string }>).map((each) => each.name),
    ['jaguar.v1', 'chevron.v1'],
  );
});

/**
 * Generated and uploaded art is in no catalogue and its ref cannot be
 * recovered from its name, so a surviving declaration must be kept verbatim
 * rather than rebuilt.
 */
test('art outside the catalogue survives as long as a layer names it', () => {
  const made = { name: 'gen-7f2a', ref: 'tex:sha256-7f2a', kind: 'coat' };
  const held = reconcileTextures(
    { ...wearing('gen-7f2a'), textures: [made] },
    CATALOGUE,
  );
  assert.deepEqual(held.textures, [made]);
  assert.equal((held.textures as unknown[])[0], made, 'kept by identity');

  // ...and is dropped once nothing points at it, since nothing else can.
  const moved = reconcileTextures(
    { ...held, layers: [{ source: { kind: 'image', texture: 'jaguar.v1' } }] },
    CATALOGUE,
  );
  assert.deepEqual(moved.textures, [
    { name: 'jaguar.v1', ref: 'tex:jaguar', kind: 'coat' },
  ]);
});

test('a layer that names nothing leaves the list empty', () => {
  const doc = reconcileTextures(
    { layers: [{ source: { kind: 'solid', color: '#fff' } }], textures: [] },
    CATALOGUE,
  );
  assert.deepEqual(doc.textures, []);
});

test('a settled document is returned unchanged, so React sees no edit', () => {
  const doc = reconcileTextures(wearing('jaguar.v1'), CATALOGUE);
  assert.equal(reconcileTextures(doc, CATALOGUE), doc);
});

test('a name in no catalogue and with no declaration is simply not declared', () => {
  // The layer keeps its reference and the validator is what complains; this
  // must not invent a ref it cannot know. A document that never held a
  // `textures` key does not gain an empty one either.
  const doc = reconcileTextures(wearing('missing.v1'), CATALOGUE);
  assert.equal(doc.textures, undefined);
});
