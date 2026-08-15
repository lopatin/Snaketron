#!/usr/bin/env node

import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const CAPTURE_SCRIPT = resolve(SCRIPT_DIR, 'capture.mjs');

function usage(message) {
  if (message) process.stderr.write(`${message}\n\n`);
  process.stderr.write(
    'Usage: node tools/video/capture-potg-review.mjs --manifest TOP-20.json --out DIR ' +
    '[--url URL] [--capture-vfps 60] [--headless-shell PATH] [--virtual-time] [--limit N]\n' +
    'PotG review captures always use the authored 12.5 s viewer timing.\n',
  );
  process.exit(message ? 2 : 0);
}

export function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--help' || token === '-h') usage();
    if (token === '--virtual-time') {
      values.virtualTime = true;
      continue;
    }
    if (!token.startsWith('--') || index + 1 >= argv.length) usage(`Invalid argument: ${token}`);
    values[token.slice(2)] = argv[index + 1];
    index += 1;
  }
  if (!values.manifest) usage('--manifest is required');
  if (!values.out) usage('--out is required (keep rendered masters outside git)');
  const captureVfps = Number(values['capture-vfps'] ?? 60);
  const limit = values.limit === undefined ? null : Number(values.limit);
  if (!Number.isInteger(captureVfps) || captureVfps < 1 || captureVfps > 240) {
    usage('--capture-vfps must be an integer from 1 to 240');
  }
  if (limit !== null && (!Number.isInteger(limit) || limit < 1)) {
    usage('--limit must be a positive integer');
  }
  return {
    manifest: resolve(values.manifest),
    out: resolve(values.out),
    url: values.url,
    captureVfps,
    headlessShell: values['headless-shell'],
    virtualTime: Boolean(values.virtualTime),
    limit,
  };
}

export function captureArguments(options, clipFile, outputDirectory) {
  const args = [
    CAPTURE_SCRIPT,
    '--scenario', clipFile,
    '--out', outputDirectory,
    '--capture-vfps', String(options.captureVfps),
    '--viewer-timing',
  ];
  if (options.url) args.push('--url', options.url);
  if (options.headlessShell) args.push('--headless-shell', options.headlessShell);
  if (options.virtualTime) args.push('--virtual-time');
  return args;
}

function runCapture(args) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'inherit'] });
    let stdout = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => { stdout += chunk; });
    child.once('error', rejectPromise);
    child.once('exit', (code, signal) => {
      if (code !== 0) {
        rejectPromise(new Error(`capture exited ${signal ?? code}`));
        return;
      }
      try {
        resolvePromise(JSON.parse(stdout));
      } catch (error) {
        rejectPromise(new Error(`capture returned invalid JSON: ${error.message}`));
      }
    });
  });
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const manifest = JSON.parse(await readFile(options.manifest, 'utf8'));
  if (!Array.isArray(manifest.entries) || manifest.entries.length === 0) {
    throw new Error('review manifest has no entries');
  }
  const entries = options.limit === null
    ? manifest.entries
    : manifest.entries.slice(0, options.limit);
  await mkdir(options.out, { recursive: true });
  const results = [];
  for (const [index, entry] of entries.entries()) {
    if (!Number.isInteger(entry.rank) || typeof entry.clip_file !== 'string') {
      throw new Error(`review entry ${index} lacks rank/clip_file`);
    }
    const clipFile = resolve(dirname(options.manifest), entry.clip_file);
    const outputDirectory = resolve(
      options.out,
      `${String(entry.rank).padStart(2, '0')}-game-${entry.game_id}`,
    );
    process.stderr.write(
      `capturing review ${index + 1}/${entries.length}: game ${entry.game_id}\n`,
    );
    const capture = await runCapture(captureArguments(options, clipFile, outputDirectory));
    results.push({
      rank: entry.rank,
      game_id: entry.game_id,
      clip_file: clipFile,
      output_directory: outputDirectory,
      ...capture,
    });
  }
  const report = {
    schema_version: 1,
    corpus_id: manifest.corpus_id,
    source_manifest: options.manifest,
    capture_vfps: options.captureVfps,
    rendered: results,
  };
  const reportFile = resolve(options.out, 'capture-review-manifest.json');
  await writeFile(reportFile, `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify({ ...report, report_file: reportFile }, null, 2)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`PotG review capture failed: ${error.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
