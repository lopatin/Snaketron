export const TUTORIAL_PROTOTYPE_QUERY_PARAM = 'tutorial-prototype' as const;

/**
 * Presentation-only tutorial concepts available through the opt-in review URL.
 * These labels are intentionally short enough for a compact in-modal switch.
 */
export const TUTORIAL_PROTOTYPES = [
  {
    id: 'lens',
    label: 'Arena lens',
    description: 'A focused arena crop with one clear instruction at a time.',
  },
  {
    id: 'manual',
    label: 'Field manual',
    description: 'One screen where each animated lesson is revealed in sequence.',
  },
  {
    id: 'coach',
    label: 'Coach rail',
    description: 'One large arena with a compact lesson rail alongside it.',
  },
] as const;

export type TutorialPrototypeId = (typeof TUTORIAL_PROTOTYPES)[number]['id'];
export type TutorialPrototype = (typeof TUTORIAL_PROTOTYPES)[number];

const TUTORIAL_PROTOTYPE_IDS = new Set<string>(
  TUTORIAL_PROTOTYPES.map(({ id }) => id),
);

/**
 * Read the prototype opt-in from a `location.search`-style string.
 * Unknown, empty, or repeated values fail closed to the production experience.
 */
export const parseTutorialPrototype = (search: string): TutorialPrototypeId | null => {
  const values = new URLSearchParams(search).getAll(TUTORIAL_PROTOTYPE_QUERY_PARAM);
  if (values.length !== 1 || !TUTORIAL_PROTOTYPE_IDS.has(values[0])) {
    return null;
  }
  return values[0] as TutorialPrototypeId;
};
