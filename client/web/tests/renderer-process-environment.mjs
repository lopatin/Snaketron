// Environment policy for the untrusted Chromium renderer process.
//
// BrowserType.launch inherits the Node environment by default.  Capture Node
// intentionally owns the private-skin service token, so inheritance would
// give that token (and the renderer attestation payload) to Chromium.  Build
// the browser environment from a fixed runtime-only allowlist instead.

export const browserRuntimeEnvironmentNames = Object.freeze([
  "APPDATA",
  "HOME",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "LOCALAPPDATA",
  "PATH",
  "PATHEXT",
  "SystemRoot",
  "TEMP",
  "TMP",
  "TMPDIR",
  "TZ",
  "USERPROFILE",
  "WINDIR",
  "XDG_RUNTIME_DIR",
]);

export function browserRuntimeEnvironment(source = process.env) {
  return Object.fromEntries(
    browserRuntimeEnvironmentNames
      .filter((name) => source[name] !== undefined)
      .map((name) => [name, source[name]]),
  );
}

export async function fetchPrivateSkin(request, apiUrl, contentRef, token) {
  const response = await request.get(
    `${apiUrl.replace(/\/$/, "")}/api/skins/by-ref/${encodeURIComponent(contentRef)}`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  if (!response.ok()) {
    throw new Error(
      `private skin fetch failed: HTTP ${response.status()} ${await response.text()}`,
    );
  }
  return response.text();
}
