import assert from 'node:assert/strict';
import test from 'node:test';
import {
  TUTORIAL_PROTOTYPE_QUERY_PARAM,
  TUTORIAL_PROTOTYPES,
  parseTutorialPrototype,
} from '../../utils/tutorialPrototype.ts';

test('the prototype registry exposes the three review concepts with switch copy', () => {
  assert.equal(TUTORIAL_PROTOTYPE_QUERY_PARAM, 'tutorial-prototype');
  assert.deepEqual(
    TUTORIAL_PROTOTYPES.map(({ id }) => id),
    ['lens', 'manual', 'coach'],
  );
  assert.equal(new Set(TUTORIAL_PROTOTYPES.map(({ id }) => id)).size, 3);

  for (const prototype of TUTORIAL_PROTOTYPES) {
    assert.ok(prototype.label.length > 0, `${prototype.id} needs a switch label`);
    assert.ok(prototype.description.length > 0, `${prototype.id} needs a description`);
  }
});

test('the parser accepts each exact prototype id in a location search string', () => {
  for (const { id } of TUTORIAL_PROTOTYPES) {
    assert.equal(parseTutorialPrototype(`?tutorial-prototype=${id}`), id);
    assert.equal(parseTutorialPrototype(`region=us&tutorial-prototype=${id}&step=2`), id);
  }
});

test('the parser fails closed for absent, invalid, or ambiguous prototype values', () => {
  for (const search of [
    '',
    '?step=2',
    '?tutorial-prototype=',
    '?tutorial-prototype=unknown',
    '?tutorial-prototype=Lens',
    '?tutorial-prototype=%20lens%20',
    '?tutorial-prototype=lens&tutorial-prototype=coach',
  ]) {
    assert.equal(parseTutorialPrototype(search), null, search);
  }
});
