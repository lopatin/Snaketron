import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createRosterSnakeDrawPlan,
  getRosterSnakeLabelColor,
  truncateRosterSnakeName,
} from '../../utils/rosterSnakeCanvas.ts';

const baseInput = {
  width: 124,
  height: 19,
  name: 'Troncat33',
  fill: '#70bfe3',
  outline: '#5299bb',
  labelColor: '#172033',
};

const luminance = (hex: string): number => {
  const channels = [1, 3, 5].map((offset) => {
    const value = Number.parseInt(hex.slice(offset, offset + 2), 16) / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
};

const contrast = (first: string, second: string): number => {
  const firstLuminance = luminance(first);
  const secondLuminance = luminance(second);
  return (Math.max(firstLuminance, secondLuminance) + 0.05)
    / (Math.min(firstLuminance, secondLuminance) + 0.05);
};

test('right-facing roster geometry mirrors the arena stroke and places its head inward', () => {
  const plan = createRosterSnakeDrawPlan({ ...baseInput, facing: 'right' });

  assert.equal(plan.body.tailX, 9.5);
  assert.equal(plan.body.headX, 114.5);
  assert.equal(plan.body.centerY, 9.5);
  assert.equal(plan.body.outerWidth, 19);
  assert.equal(plan.body.innerWidth, 15);
  assert.equal(plan.head.centerX, plan.body.headX);
  assert.equal(plan.head.faceRadius, 5.7);
  assert.equal(plan.fill, '#70bfe3');
  assert.equal(plan.outline, '#5299bb');
  assert.ok(plan.label.x > plan.width / 2, 'the label should lean toward the head');
  assert.ok(plan.label.maxWidth > 70);
});

test('left and right plans are exact horizontal mirrors', () => {
  const right = createRosterSnakeDrawPlan({ ...baseInput, facing: 'right' });
  const left = createRosterSnakeDrawPlan({ ...baseInput, facing: 'left' });

  assert.equal(left.head.centerX, right.width - right.head.centerX);
  assert.equal(left.body.tailX, right.width - right.body.tailX);
  assert.ok(Math.abs(left.label.x - (right.width - right.label.x)) < 1e-9);
  assert.ok(Math.abs(left.highlight.startX - (right.width - right.highlight.startX)) < 1e-9);
  assert.ok(Math.abs(left.highlight.endX - (right.width - right.highlight.endX)) < 1e-9);
});

test('mobile geometry keeps the same proportions at the existing 17px roster height', () => {
  const plan = createRosterSnakeDrawPlan({
    ...baseInput,
    width: 86,
    height: 17,
    facing: 'left',
  });

  assert.equal(plan.body.outerWidth, 17);
  assert.equal(plan.body.innerWidth, 14);
  assert.equal(plan.head.outerRadius, 8.5);
  assert.equal(plan.label.font.startsWith('900 7.5px '), true);
  assert.ok(plan.label.maxWidth > 45);
});

test('name fitting preserves short labels and truncates long labels without splitting unicode', () => {
  const measure = (candidate: string) => Array.from(candidate).length * 5;

  assert.equal(truncateRosterSnakeName('You', 30, measure), 'You');
  assert.equal(truncateRosterSnakeName('Troncat33', 30, measure), 'Tronc…');
  assert.equal(truncateRosterSnakeName('A🐍BCDE', 25, measure), 'A🐍BC…');
  assert.equal(truncateRosterSnakeName('Anything', 4, measure), '');
});

test('label contrast chooses the same exact ink for every team skin', () => {
  for (const fill of ['#70bfe3', '#3c8dde', '#ff6b6b', '#e34e5b']) {
    const label = getRosterSnakeLabelColor(fill);
    assert.equal(label, '#0f172a');
    assert.ok(contrast(label, fill) >= 4.5, `${label} must clear AA on ${fill}`);
  }
  assert.equal(getRosterSnakeLabelColor('#172033'), '#ffffff');
});

test('draw plans are deterministic and sanitize invalid dimensions', () => {
  const first = createRosterSnakeDrawPlan({ ...baseInput, facing: 'right' });
  const second = createRosterSnakeDrawPlan({ ...baseInput, facing: 'right' });
  assert.deepEqual(first, second);

  const sanitized = createRosterSnakeDrawPlan({
    ...baseInput,
    width: Number.NaN,
    height: -1,
    facing: 'left',
  });
  assert.equal(sanitized.width, 1);
  assert.equal(sanitized.height, 1);
  assert.ok(sanitized.body.innerWidth > 0);
});
