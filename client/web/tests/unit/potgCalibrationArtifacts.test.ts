import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const artifact = (name: string): any => JSON.parse(readFileSync(new URL(
  `../../../../docs/qa/play-of-the-game-calibration/${name}`,
  import.meta.url,
), 'utf8'));

test('checked-in PotG calibration clears every launch gate within the tuning budget', () => {
  const summary = artifact('corpus-summary.json');
  const tuning = artifact('tuning-rounds.json');
  const review = artifact('top-20-review.json');

  assert.equal(summary.automatic_gate.passed, true);
  assert.ok(summary.production_rate_bps >= 7_000);
  assert.ok(Math.max(...Object.values<number>(summary.category_share_bps)) <= 6_000);
  assert.ok(tuning.rounds.length <= 3);
  assert.equal(tuning.rounds.at(-1).passed, true);

  assert.equal(review.entries.length, 20);
  assert.equal(review.minimum_deserved_reviews, 16);
  assert.equal(summary.human_review.completed_reviews, review.entries.length);
  assert.equal(summary.human_review.status, 'passed');

  const deserved = review.entries.filter((entry: any) => {
    const fields = entry.review;
    const everyRubricItemPassed = [
      fields.causal_clarity,
      fields.visible_skill,
      fields.fair_credit,
      fields.clip_integrity,
      fields.proud_to_show,
    ].every((value) => value === true);
    assert.equal(fields.verdict === 'deserved', everyRubricItemPassed);
    assert.equal(typeof fields.reviewer, 'string');
    assert.ok(fields.reviewer.length > 0);
    assert.equal(typeof fields.notes, 'string');
    assert.ok(fields.notes.length > 0);
    if (fields.verdict === 'rejected') {
      assert.ok(fields.rejection_codes.length > 0);
    }
    return fields.verdict === 'deserved';
  });

  assert.equal(deserved.length, 16);
  assert.ok(deserved.length >= review.minimum_deserved_reviews);
  assert.equal(summary.human_review.deserved_reviews, deserved.length);
  assert.equal(new Set(review.entries.map((entry: any) => entry.review.reviewer)).size, 3);
});
