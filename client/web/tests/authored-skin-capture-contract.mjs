// Factory evidence must exercise real renderer fixtures at the actual live
// scale range. Keep these names outside the executable capture script so they
// can be regression-tested without credentials, a server, or Chromium.

export const REQUIRED_CAPTURE_POSES = Object.freeze([
  "single_cell",
  "starting_length",
  "single_corner",
  "wide_u_turn",
  "longer_than_head_gradient",
  "zigzag",
]);

export const LIVE_CAPTURE_CELL_SIZES = Object.freeze([5, 10, 15]);

export function validateAuthoredSkinCaptureFixtures(fixtures) {
  if (!fixtures || !Array.isArray(fixtures.poses))
    throw new Error("renderer fixture payload has no poses");
  if (!Array.isArray(fixtures.roles) || fixtures.roles.length === 0)
    throw new Error("renderer fixture payload has no roles");

  const poseNames = new Set(fixtures.poses.map((pose) => pose?.name));
  const missingPoses = REQUIRED_CAPTURE_POSES.filter(
    (name) => !poseNames.has(name),
  );
  if (missingPoses.length > 0)
    throw new Error(
      `required renderer poses are absent: ${missingPoses.join(", ")}`,
    );

  if (!Array.isArray(fixtures.cellSizes))
    throw new Error("renderer fixture payload has no live cell sizes");
  const actualSizes = fixtures.cellSizes.map(Number);
  if (
    actualSizes.length !== LIVE_CAPTURE_CELL_SIZES.length ||
    actualSizes.some((size, index) => size !== LIVE_CAPTURE_CELL_SIZES[index])
  ) {
    throw new Error(
      `renderer live cell sizes must be ${LIVE_CAPTURE_CELL_SIZES.join(", ")}; ` +
        `got ${actualSizes.join(", ")}`,
    );
  }

  return {
    poses: [...REQUIRED_CAPTURE_POSES],
    liveCellSizes: [...LIVE_CAPTURE_CELL_SIZES],
  };
}
