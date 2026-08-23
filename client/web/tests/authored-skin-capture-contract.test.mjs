import assert from "node:assert/strict";
import test from "node:test";

import {
  LIVE_CAPTURE_CELL_SIZES,
  REQUIRED_CAPTURE_POSES,
  validateAuthoredSkinCaptureFixtures,
} from "./authored-skin-capture-contract.mjs";

const fixtures = () => ({
  poses: REQUIRED_CAPTURE_POSES.map((name) => ({
    name,
    cellsWide: 20,
    cellsHigh: 8,
  })),
  roles: ["own", "enemy"],
  cellSizes: [...LIVE_CAPTURE_CELL_SIZES],
});

test("capture contract uses real poses and every live arena scale", () => {
  assert.deepEqual(REQUIRED_CAPTURE_POSES, [
    "single_cell",
    "starting_length",
    "single_corner",
    "wide_u_turn",
    "longer_than_head_gradient",
    "zigzag",
  ]);
  assert.ok(!REQUIRED_CAPTURE_POSES.includes("short_straight"));
  assert.deepEqual(LIVE_CAPTURE_CELL_SIZES, [5, 10, 15]);
  assert.deepEqual(validateAuthoredSkinCaptureFixtures(fixtures()), {
    poses: [...REQUIRED_CAPTURE_POSES],
    liveCellSizes: [5, 10, 15],
  });
});

test("capture fails closed when a required renderer pose is absent", () => {
  const incomplete = fixtures();
  incomplete.poses = incomplete.poses.filter(
    ({ name }) => name !== "starting_length",
  );
  assert.throws(
    () => validateAuthoredSkinCaptureFixtures(incomplete),
    /required renderer poses are absent: starting_length/,
  );
});

test("capture fails closed when live renderer scales drift", () => {
  const incomplete = fixtures();
  incomplete.cellSizes = [5, 15];
  assert.throws(
    () => validateAuthoredSkinCaptureFixtures(incomplete),
    /renderer live cell sizes must be 5, 10, 15; got 5, 15/,
  );
});
