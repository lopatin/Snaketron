import { createHash } from 'node:crypto';

const rendererSuffixes = new Set(['.css', '.html', '.js', '.wasm']);

const sha256 = (value) => createHash('sha256').update(value).digest('hex');

export function parsePinnedRendererBundle(rawManifest, expectedDigest) {
  if (!rawManifest || !expectedDigest) {
    throw new Error('pinned renderer bundle manifest and SHA-256 are required');
  }
  if (sha256(rawManifest) !== expectedDigest) {
    throw new Error('pinned renderer bundle manifest does not match its SHA-256');
  }
  const manifest = JSON.parse(rawManifest);
  if (
    manifest?.schema_version !== 1 ||
    manifest?.root !== 'client/web/dist' ||
    !manifest.assets ||
    typeof manifest.assets !== 'object' ||
    Array.isArray(manifest.assets)
  ) {
    throw new Error('pinned renderer bundle manifest is malformed');
  }
  return manifest;
}

export function classifyRendererResponse(responseUrl, resourceType, contentType, webUrl) {
  const served = new URL(responseUrl);
  const expectedOrigin = new URL(webUrl).origin;
  const decodedPath = decodeURIComponent(served.pathname).replace(/^\/+/, '');
  const suffixIndex = decodedPath.lastIndexOf('.');
  const suffix = suffixIndex === -1 ? '' : decodedPath.slice(suffixIndex).toLowerCase();
  const relevant =
    resourceType === 'document' ||
    resourceType === 'script' ||
    resourceType === 'stylesheet' ||
    rendererSuffixes.has(suffix) ||
    /(?:javascript|wasm|css|html)/i.test(contentType || '');
  if (!relevant) return null;
  if (served.origin !== expectedOrigin) {
    return { error: `renderer loaded cross-origin executable asset ${served.href}` };
  }
  if (decodedPath.includes('\\') || decodedPath.split('/').includes('..')) {
    return { error: `renderer loaded an invalid asset path ${served.pathname}` };
  }
  return {
    path: resourceType === 'document' ? 'index.html' : decodedPath,
    kind:
      resourceType === 'document'
        ? 'html'
        : suffix === '.wasm' || /wasm/i.test(contentType || '')
          ? 'wasm'
          : suffix.replace(/^\./, '') || resourceType,
  };
}

export function attestRendererBundle(manifest, manifestDigest, observations) {
  const errors = [];
  const assets = new Map();
  for (const observation of observations) {
    if (observation.error) {
      errors.push(observation.error);
      continue;
    }
    const expected = manifest.assets[observation.path];
    if (!expected) {
      errors.push(`unexpected served renderer asset ${observation.path}`);
      continue;
    }
    if (
      expected.sha256 !== observation.sha256 ||
      expected.size_bytes !== observation.size_bytes
    ) {
      errors.push(`served bytes differ for ${observation.path}`);
      continue;
    }
    const previous = assets.get(observation.path);
    if (previous && (previous.sha256 !== observation.sha256 || previous.size_bytes !== observation.size_bytes)) {
      errors.push(`renderer asset ${observation.path} changed within one capture`);
      continue;
    }
    assets.set(observation.path, observation);
  }
  if (!assets.has('index.html')) errors.push('served index.html was not attested');
  if (![...assets.keys()].some((path) => path.endsWith('.js'))) {
    errors.push('no served JavaScript asset was attested');
  }
  if (![...assets.keys()].some((path) => path.endsWith('.wasm'))) {
    errors.push('no served WASM asset was attested');
  }
  return {
    schema_version: 1,
    bundle_manifest_sha256: manifestDigest,
    assets: [...assets.values()]
      .map(({ path, kind, sha256: digest, size_bytes: size }) => ({
        path,
        kind,
        sha256: digest,
        size_bytes: size,
      }))
      .sort((left, right) => left.path.localeCompare(right.path)),
    errors,
  };
}

export function digestRendererBytes(value) {
  return sha256(value);
}
