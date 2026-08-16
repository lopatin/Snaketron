// The fixture matrix every skin baseline is captured over.
//
// Shared by the capture path and the comparison path so a baseline can never be
// recorded over one matrix and checked against another — which would fail as a
// pixel difference and be debugged as one.

/** The skins that ship. Mirrors `server/src/skin_catalog.rs`. */
export const SHIPPED_SKINS = [
  'classic@1',
  'ember@1',
  'aurora@1',
  'tidewave@1',
  'voltage@1',
  'lantern@1',
  'gambit@1',
  'harlequin@1',
  'pitlane@1',
  'zebra@1',
  'zebra-print@1',
  'tiger@1',
  'tiger-print@1',
  'jaguar@1',
  'jaguar-print@1',
];

/**
 * Two variants per pose.
 *
 * Calm-and-friendly plus boosting-and-hostile covers both palettes and both
 * contour configurations without doubling the matrix again: the Boost band is
 * the one piece of a skin that is competitive information, so it is never
 * captured only in the state where it is absent.
 */
export function baselineSpecs(skin, poseNames) {
  return poseNames.flatMap((pose) => [
    { skin, pose, role: 'own', cellSize: 15, boost: false, reducedMotion: true },
    {
      skin,
      pose,
      role: 'enemy',
      cellSize: 15,
      boost: true,
      reducedMotion: true,
    },
  ]);
}

/** A stable, filesystem-safe name for a skin's sheet. */
export function sheetName(skin) {
  return `${skin.replace(/[^a-z0-9]+/gi, '-')}.png`;
}
