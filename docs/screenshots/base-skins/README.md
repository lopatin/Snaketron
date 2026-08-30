# Base-skin screenshots

Regenerate with `client/web/tests/capture-base-skins.mjs`:

```bash
cd client && wasm-pack build --target web --out-dir pkg
cd web && npm ci
REACT_APP_API_URL=http://localhost:8791 npm start   # in one shell
node tests/capture-base-skins.mjs                   # in another
```

The dev server needs something answering `/api/skins?kind=snake|base`,
`/api/skins/browse`, `/api/regions`, `/api/regions/user-counts`, `/api/health`
and `/api/news`. A twenty-line node stub is enough; the full stack is not.

**Build the wasm package first, and rebuild it after any change to
`client/src/`.** A worktree's `pkg/` is stale until you do, and a stale one is
exactly what a broken feature looks like: the first base painted its picture and
every other row showed a plain tint, because the catalogue in the loaded wasm
was one entry long.

`header-no-bux.png` / `header-with-bux.png` come from an A/B over the same
signed-in home page where the only difference is what `/api/wallet/packs`
answers: intercept that route with Playwright, return `[]` for one shot and one
pack for the other, and count `[class*="home-bux"]` (0 versus 2).
