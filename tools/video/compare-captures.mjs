#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';

function usage(message) {
  if (message) process.stderr.write(`${message}\n\n`);
  process.stderr.write(
    'Usage: node tools/video/compare-captures.mjs [--mode exact|perceptual] ' +
    '[--threshold 0.999] <master-a.mkv> <master-b.mkv>\n',
  );
  process.exit(message ? 2 : 0);
}

function parseArgs(argv) {
  let mode = 'exact';
  let threshold = 0.999;
  const files = [];
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--help' || token === '-h') usage();
    if (token === '--perceptual') {
      mode = 'perceptual';
      continue;
    }
    if (token === '--mode' || token === '--threshold') {
      if (index + 1 >= argv.length) usage(`${token} requires a value`);
      const value = argv[index + 1];
      index += 1;
      if (token === '--mode') mode = value;
      else threshold = Number(value);
      continue;
    }
    if (token.startsWith('--')) usage(`Unknown option: ${token}`);
    files.push(token);
  }
  if (!['exact', 'perceptual'].includes(mode)) usage('--mode must be exact or perceptual');
  if (!Number.isFinite(threshold) || threshold <= 0 || threshold > 1) {
    usage('--threshold must be greater than 0 and at most 1');
  }
  if (files.length !== 2) usage('Exactly two masters are required');
  return { mode, threshold, left: files[0], right: files[1] };
}

const digest = async (path) => {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
};

const run = (command, args) => new Promise((resolvePromise, rejectPromise) => {
  const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  child.once('error', rejectPromise);
  child.once('exit', (code, signal) => {
    if (code === 0) resolvePromise({ stdout, stderr });
    else rejectPromise(new Error(`${command} failed (${signal ?? code}): ${stderr.trim()}`));
  });
});

async function probe(path) {
  const { stdout } = await run(process.env.FFPROBE ?? 'ffprobe', [
    '-v', 'error', '-count_frames', '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height,pix_fmt,avg_frame_rate,nb_read_frames',
    '-of', 'json', path,
  ]);
  const stream = JSON.parse(stdout).streams?.[0];
  if (!stream) throw new Error(`No video stream found in ${path}`);
  return stream;
}

function assertComparable(left, right) {
  for (const field of ['width', 'height', 'avg_frame_rate', 'nb_read_frames']) {
    if (String(left[field]) !== String(right[field])) {
      throw new Error(
        `Masters are not frame-aligned: ${field} is ${left[field]} vs ${right[field]}`,
      );
    }
  }
}

async function perceptualCompare(left, right, threshold) {
  const [leftProbe, rightProbe] = await Promise.all([probe(left), probe(right)]);
  assertComparable(leftProbe, rightProbe);
  const scratch = await mkdtemp(join(tmpdir(), 'snaketron-ssim-'));
  const stats = join(scratch, 'frames.log');
  try {
    await run(process.env.FFMPEG ?? 'ffmpeg', [
      '-hide_banner', '-loglevel', 'error',
      '-i', left, '-i', right,
      '-filter_complex',
      `[0:v]setpts=PTS-STARTPTS[a];[1:v]setpts=PTS-STARTPTS[b];[a][b]ssim=stats_file=${stats}`,
      '-an', '-f', 'null', '-',
    ]);
    const lines = (await readFile(stats, 'utf8')).trim().split(/\r?\n/).filter(Boolean);
    const values = lines.map((line) => {
      const match = line.match(/\bAll:([^ ]+)/);
      if (!match) throw new Error(`Cannot parse SSIM frame: ${line}`);
      return match[1].toLowerCase().includes('nan') ? 1 : Number(match[1]);
    });
    if (values.length !== Number(leftProbe.nb_read_frames)) {
      throw new Error(
        `SSIM inspected ${values.length} frames; expected ${leftProbe.nb_read_frames}`,
      );
    }
    const minimum = Math.min(...values);
    const mean = values.reduce((total, value) => total + value, 0) / values.length;
    const result = {
      mode: 'perceptual',
      threshold,
      passed: minimum >= threshold,
      frames: values.length,
      minimum_frame_ssim: minimum,
      mean_frame_ssim: mean,
      geometry: `${leftProbe.width}x${leftProbe.height}`,
      frame_rate: leftProbe.avg_frame_rate,
    };
    if (!result.passed) {
      process.stderr.write(`${JSON.stringify(result, null, 2)}\n`);
      process.exitCode = 1;
      return;
    }
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

const options = parseArgs(process.argv.slice(2));
if (options.mode === 'exact') {
  const [leftHash, rightHash] = await Promise.all([
    digest(options.left),
    digest(options.right),
  ]);
  if (leftHash !== rightHash) {
    process.stderr.write(
      `capture mismatch\n${options.left}: ${leftHash}\n${options.right}: ${rightHash}\n`,
    );
    process.exitCode = 1;
  } else {
    process.stdout.write(`bit-identical ${leftHash}\n`);
  }
} else {
  await perceptualCompare(options.left, options.right, options.threshold);
}
