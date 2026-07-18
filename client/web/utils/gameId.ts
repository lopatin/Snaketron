export const RUST_U32_MAX = 0xFFFF_FFFF;
export const INVALID_GAME_ID_REASON =
  `This game link is invalid. Game IDs must be whole numbers from 0 to ${RUST_U32_MAX}.`;

export function parseU32GameId(value: unknown): number | null {
  let candidate: number;

  if (typeof value === 'number') {
    candidate = value;
  } else if (typeof value === 'string') {
    const normalized = value.trim();
    if (!/^\d+$/.test(normalized)) {
      return null;
    }
    candidate = Number(normalized);
  } else {
    return null;
  }

  if (
    !Number.isSafeInteger(candidate) ||
    candidate < 0 ||
    candidate > RUST_U32_MAX
  ) {
    return null;
  }

  return candidate;
}
