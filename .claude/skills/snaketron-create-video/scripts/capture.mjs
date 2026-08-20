#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));

function findCaptureCli() {
  if (process.env.SNAKETRON_CAPTURE_CLI) {
    return resolve(process.env.SNAKETRON_CAPTURE_CLI);
  }
  let cursor = scriptDir;
  for (;;) {
    const candidate = join(cursor, "tools", "video", "capture.mjs");
    if (existsSync(candidate) && resolve(candidate) !== resolve(fileURLToPath(import.meta.url))) {
      return candidate;
    }
    const parent = dirname(cursor);
    if (parent === cursor) break;
    cursor = parent;
  }
  return null;
}

const captureCli = findCaptureCli();
if (!captureCli) {
  console.error(
    "capture: could not find tools/video/capture.mjs; run this skill inside a SnakeTron checkout or set SNAKETRON_CAPTURE_CLI",
  );
  process.exit(2);
}

const child = spawn(process.execPath, [captureCli, ...process.argv.slice(2)], {
  cwd: process.cwd(),
  env: process.env,
  stdio: "inherit",
});

for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => child.kill(signal));
}

child.on("error", (error) => {
  console.error(`capture: ${error.message}`);
  process.exitCode = 1;
});
child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
  } else {
    process.exitCode = code ?? 1;
  }
});
