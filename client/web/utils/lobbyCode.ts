const LOBBY_CODE_PATTERN = /^[A-Z0-9]+(?:-[A-Z0-9]+)*$/;
const MAX_LOBBY_CODE_LENGTH = 64;

/**
 * Extract a lobby code from either a raw code or a copied `/lobby/:code`
 * (`/join/:code` is accepted for compatibility with older invite links).
 */
export function normalizeLobbyCodeInput(input: string): string {
  const trimmedInput = input.trim();
  if (!trimmedInput) {
    return '';
  }

  const pathMatch = trimmedInput.match(/(?:^|\/)(?:lobby|join)\/([^/?#]+)/i);
  let candidate = pathMatch?.[1] ?? trimmedInput.split(/[?#]/, 1)[0];

  try {
    candidate = decodeURIComponent(candidate);
  } catch {
    // Keep the original text so validation can explain that it is not a code.
  }

  return candidate.replace(/\s+/g, '').toUpperCase();
}

export function getLobbyCodeValidationError(input: string): string | null {
  const code = normalizeLobbyCodeInput(input);

  if (!code) {
    return 'Enter a lobby code or invite link.';
  }
  if (code.length > MAX_LOBBY_CODE_LENGTH) {
    return 'That lobby code is too long.';
  }
  if (!LOBBY_CODE_PATTERN.test(code)) {
    return 'Lobby codes use only letters, numbers, and hyphens.';
  }

  return null;
}
