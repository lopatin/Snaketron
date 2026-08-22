#!/usr/bin/env node
import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize, resolve, sep } from 'node:path';

const [rootArgument = 'client/web/dist', host = '127.0.0.1', portArgument = '3000'] =
  process.argv.slice(2);
const root = resolve(rootArgument);
const port = Number(portArgument);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('port must be 1..65535');
for (const required of ['index.html']) {
  if (!existsSync(join(root, required))) throw new Error(`cached renderer bundle is missing ${required}`);
}

const mediaTypes = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.wasm': 'application/wasm',
  '.webp': 'image/webp',
};

createServer((request, response) => {
  const pathname = decodeURIComponent(new URL(request.url, `http://${host}:${port}`).pathname);
  const relative = normalize(pathname).replace(/^[/\\]+/, '');
  let path = resolve(root, relative);
  if (path !== root && !path.startsWith(`${root}${sep}`)) {
    response.writeHead(400).end('invalid path');
    return;
  }
  if (!existsSync(path) || !statSync(path).isFile()) path = join(root, 'index.html');
  response.writeHead(200, {
    'Content-Type': mediaTypes[extname(path).toLowerCase()] || 'application/octet-stream',
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
  });
  createReadStream(path).pipe(response);
}).listen(port, host, () => {
  process.stdout.write(`Serving exact cached renderer bundle ${root} at http://${host}:${port}\n`);
});
