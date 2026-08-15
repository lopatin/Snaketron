import test from 'node:test';
import assert from 'node:assert/strict';

import { RANK_BANDS } from '../../utils/rank.ts';
import { RANK_ICON_DATA } from '../../components/rankIconData.ts';
import {
  RANK_ICON_DIVISION_DATA,
  type RankIconDivisionTier,
} from '../../components/rankIconDivisionData.ts';

/**
 * The ladder (RANK_BANDS) and the generated badge art are produced by
 * separate pipelines — TypeScript by hand, icon data by the Python
 * generators — so nothing but these assertions stops them from drifting
 * apart. Re-slicing the bands is exactly the change that silently leaves a
 * rank with no artwork, or artwork for a rank that no longer exists.
 */

test('every ladder band has division artwork', () => {
  for (const band of RANK_BANDS) {
    const tier = band.tier as RankIconDivisionTier;
    const byDivision = RANK_ICON_DIVISION_DATA[tier];
    assert.ok(byDivision, `no division art for tier ${band.tier}`);

    const art = byDivision[band.division];
    assert.ok(art, `no art for ${band.tier} division ${band.division}`);
    assert.ok(
      art.shapes.length > 0,
      `${band.tier} ${band.division} has no shapes`,
    );
  }
});

test('every division icon corresponds to a real ladder band', () => {
  const bandKeys = new Set(RANK_BANDS.map(b => `${b.tier}:${b.division}`));
  for (const [tier, byDivision] of Object.entries(RANK_ICON_DIVISION_DATA)) {
    for (const division of Object.keys(byDivision)) {
      assert.ok(
        bandKeys.has(`${tier}:${division}`),
        `${tier} division ${division} has art but no ladder band`,
      );
    }
  }
});

test('the ladder runs divisions 1-3 in every tier', () => {
  const byTier = new Map<string, number[]>();
  for (const band of RANK_BANDS) {
    byTier.set(band.tier, [...(byTier.get(band.tier) ?? []), band.division]);
  }
  for (const [tier, divisions] of byTier) {
    assert.deepEqual(
      [...divisions].sort(),
      [1, 2, 3],
      `${tier} should span divisions 1-3`,
    );
  }
});

test('gradient references resolve within their own icon', () => {
  const definitions = [
    ...Object.entries(RANK_ICON_DATA).map(([k, v]) => [k, v] as const),
    ...Object.entries(RANK_ICON_DIVISION_DATA).flatMap(([tier, byDivision]) =>
      Object.entries(byDivision).map(([d, v]) => [`${tier}-${d}`, v] as const),
    ),
  ];

  for (const [name, definition] of definitions) {
    const ids = new Set(definition.gradients.map(g => g.id));
    for (const shape of definition.shapes) {
      if (!shape.fill.startsWith('url(#')) continue;
      const ref = shape.fill.slice(5, -1);
      assert.ok(ids.has(ref), `${name}: fill references missing gradient ${ref}`);
    }
    // Duplicate ids would make the browser resolve the wrong gradient.
    assert.equal(
      ids.size,
      definition.gradients.length,
      `${name}: duplicate gradient ids`,
    );
  }
});
