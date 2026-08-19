# PRD: First-class skins

| Field | Value |
| --- | --- |
| Status | Draft for review |
| Product | Snaketron cosmetics: user-owned skins, textures, the Skins page, the Skin Builder, generation, and the Boost Bux economy |
| Scope | Skin/texture entities in DynamoDB + S3; skins/textures/generation/wallet APIs; equip persistence; browse and editor UI; admin review; Xsolla-funded Boost Bux |
| Depends on | `specs/skins-prd.md` (boundary, registry, refs), `specs/skin-shading-prd.md` (layer model, atlas, DSL) |
| Owners | Product / Client / Server / Ops |
| Last updated | 2026-08-19 |

## 1. Executive decision

Skins graduate from a compiled-in catalogue to a first-class entity: created by players, stored in DynamoDB, rendered from content-addressed documents, browsed on a Skins page, edited in a Skin Builder, optionally AI-generated, and sold for Boost Bux. This is `specs/skins-prd.md` section 11 — the three preserved phases (selection, composition, unlocks) — built for real, plus the two things that PRD did not anticipate: a first-class **Texture** entity feeding the shading engine's image layers, and a generation pipeline that productizes the existing local LaMa tooling.

The proposed design is:

- A **Skin** is a stable numeric id plus an append-only chain of immutable, content-addressed **revisions**. A revision is a validated `SkinDoc` (schema v2, section 5.3) whose bytes hash to a `sha256:<hex>` ref — the ref format `MAX_SKIN_REF_LENGTH = 96` was already sized for (`server/src/skin_catalog.rs:46-47`). Ownership, equipping, and browse operate on skin ids; match snapshots, spectators, and replays operate on revision refs, so what a viewer renders is exact by construction and a creator's later edit can never change a finished game.
- **SkinDoc schema v2** exposes the shading engine's existing layer model — layers, sources including `image`, `pattern`, and a new `text` source, spans, fits, groups — as validated document vocabulary. Today that vocabulary exists only for first-party Rust skins (`client/src/skin/checker.rs:1-9` states a SkinDoc "has no vocabulary for a pattern"); v2 closes the gap so a user document can express what the catalogue's textured skins already do. The compiled document model and its validator move wholly into the `skin-schema` crate, which the server takes as a dependency for the first time — one validator, native on the server, wasm in the Builder, exactly as `skin-schema/src/lib.rs:3-6` was designed for.
- A **Texture** is a first-class entity: one logical image, stored in S3 as a small ladder of resolution variants, with metadata — dimensions, repeat length, seam-check results, the last generation prompt — in DynamoDB. Skin documents reference textures by content hash; the client fetches variant bytes through an authenticated-write, public-read API route with long cache headers, following the replay store's S3-behind-the-API precedent (`server/src/replay_store.rs`, `server/src/http_server.rs:360-365`).
- The **equip channel that already exists ships for real.** `User.selected_skin`, the catalogue check at match prep (`server/src/matchmaking.rs:1262-1285`), and the fingerprint-excluded `GameState.skins` map are all live today — but nothing ever writes `selected_skin` (verified: the only DynamoDB reference is the read at `server/src/db/dynamodb.rs:3027`; creation paths write `None`). Phase M0 adds the write path and a player-facing Skins page over the built-in catalogue, making today's localStorage-only selection actually visible to other players — user value shipped before any new entity exists.
- **Base skins become a second equip slot.** A base skin is a skin of kind `base` — today's viewer-attributed `BaseTheme` promoted to its own equippable entity. Attribution rules from `specs/skins-prd.md` section 5.6 are unchanged: bases are viewer-attributed and never leave the client, so the base slot needs no wire change at all; snake-skin-embedded base themes remain as the fallback beneath an explicitly equipped base skin.
- **Generation is an async job pipeline**, not a synchronous endpoint: prompt templates per texture kind (lengthwise animated sprite, static overlay, dual-axis tileable print), image generation via the OpenAI Images API with OpenRouter as the alternate provider, then the existing seam machinery — LaMa `[T, X, T]` inpaint-wrap and roll-and-repair with the measured seam gates from `client/design/tools/sprite_sheet.py` — run in a containerized worker built from those exact scripts. The pipeline rejects rather than ships marginal art, exactly as the local tools do today; a rejected generation retries with feedback, bounded, under hard per-job and global cost ceilings.
- **Publishing is a review gate, not a toggle.** Publication and review are independent dimensions of a skin (section 5.1): default private; unpublished-to-the-world skins are equippable only by their creator; publishing a revision requires admin approval through a new `/admin` section built on the existing runtime-config admin pattern (versioned, audited — `server/src/api/admin.rs`). Unpublishing removes a skin from browse and purchase but never unequips anyone — once granted, ownership is permanent. Moderation gets the one cosmetic exception: an admin **disable** (distinct from unpublish) makes a skin resolve to classic everywhere within minutes, because "yours forever" cannot mean "abusive content renders forever." The disable path, minimal reporting, and the text-redaction rule ship in the *first* phase that lets user pixels reach another player's screen, not with the later admin UI.
- **Boost Bux** is a server-authoritative virtual currency: a signed balance on the user item plus an idempotent, source-namespaced ledger, following the completion-effect transaction pattern (`server/src/db/dynamodb.rs:2413-2460`). Xsolla (the user brief's "Zsolla") sells Boost Bux on the web site as merchant of record; the game server's payment surface is a server-minted checkout token plus a signature-verified webhook that credits — and, on refund or chargeback, reverses — the ledger. Purchases are distribution-gated the way ads already are (`server/src/ads.rs::ClientDistribution`) because CrazyGames prohibits exposing IAP without portal approval (`CRAZYGAMES.md:75`).

The honest headline, carried over from the shading PRD: for textured skins the machine can verify structure, size, seams, and budgets, but not what the pixels *depict* — and once a body is a texture, even the hue-window and label-contrast rails describe the document's declared colors, not the pixels (`specs/skin-shading-prd.md` section 13). First-class skins therefore make human review a load-bearing part of the design — publish review for public visibility, report-and-disable for what slips through — rather than pretending the validator covers content.

## 2. Product problem

Skins exist, but no player can touch them. The catalogue is 19 compiled-in entries registered in two hardcoded lists (`client/src/skin/registry.rs`, `server/src/skin_catalog.rs:23-43`); the only selector is the dev-only `/qa/skins` route; the selection a player makes there lives in localStorage and is invisible to every other player because the server-side field it should sync to has no write path. There is no browse surface, no creation surface, no ownership, no economy. The entire cosmetic system — two PRDs, a layer compositor, a validator, an authoring skill — currently ships zero player-visible product beyond the default look.

We want:

- skins as durable, ownable entities: created by anyone, owned forever, stored server-side, rendered identically for every viewer including spectators and replays;
- a Skins page where anyone can browse published skins, equip what they own, and buy what they don't;
- a Skin Builder where anyone can edit their own skins — a real layer editor over the document model, not a form of hand-picked fields;
- textures as first-party citizens: uploadable, generatable from a prompt, resolution-laddered, seam-checked;
- AI generation for both textures and whole skins, productizing the pipeline that today runs as local Python;
- a currency (Boost Bux) and a way to buy it (Xsolla), so published skins can be priced.

The mechanic must not:

- weaken any invariant the previous PRDs locked: renderer-owned geometry/occlusion/classification, validator-enforced boost telegraph, fallback-to-classic, sync-fingerprint blindness, no user code ever (`specs/skins-prd.md` sections 5.1, 7; `specs/skin-shading-prd.md` section 9.2);
- let user content reach other players' screens without a kill switch (disable) and a report path existing for it — in the same phase, not a later one;
- let a creator mutate what someone else already equipped or what a replay shows (content addressing forecloses this);
- add per-frame cost: runtime-registered skins must ride the same compiled-at-registration path as bundled documents;
- make the server trust the client for anything that costs money: balances, grants, and ownership are server-authoritative with idempotent, namespaced ledger writes;
- ever add cookie- or session-based authentication while CORS remains `Any` (`server/src/http_server.rs:278-281`). Auth stays Authorization-header Bearer only; this is the invariant that makes wide-open CORS safe for the wallet, purchase, and admin mutations this PRD adds, and it is now load-bearing enough to state.

## 3. Goals and non-goals

### 3.1 Goals

1. Close the equip gap: a write path for `selected_skin` (and the new `selected_base`), a Skins page over the built-in catalogue, and skin identity actually reaching other players — shippable before any new entity exists (M0).
2. Skin and Texture as DynamoDB entities with immutable content-addressed skin revisions and S3-backed texture variant ladders.
3. SkinDoc schema v2: the layer model (layers, groups, sources including `image`/`pattern`/`text`, spans, fits) as validated document vocabulary, with the document model and validator consolidated in `skin-schema` and the server depending on it.
4. Full CRUD + browse APIs for skins and textures; equip API; content-addressed public fetch of revisions and texture bytes for rendering.
5. Runtime skin registration in the client: unknown `sha256:` refs resolve to classic until their document and textures arrive, then render exactly — spectators and replays included.
6. The Skin Builder: schema-driven property editor (generically generated from the Rust types — never hand-synced), drag-reorderable layers, groups, the texture picker with upload/generate/variants, live always-animating previews for the poses that matter.
7. Texture and skin generation as async jobs: prompt templates per texture kind, optional reference images, OpenAI Images + OpenRouter providers, containerized LaMa seam repair with the existing measured gates, under hard cost ceilings.
8. Publication/review lifecycle with admin approval, browse of published skins by anyone, reporting, and the moderation disable — disable and reporting landing in the first user-content phase.
9. Boost Bux wallet with a signed, source-namespaced idempotent ledger; Xsolla checkout on the web site including refund/chargeback reversal; skin purchase flow granting permanent ownership.
10. Every *structural* safety rail from the prior PRDs enforced on user documents server-side: schema validation, boost-band pin, overhang caps, animation bounds, op-budget static analysis, size caps — with hue windows and label contrast machine-enforced for color-derived layers and explicitly review-enforced for image-bearing documents, per `specs/skin-shading-prd.md` section 13.

### 3.2 Non-goals

- Creator payouts, revenue share, or cash-out. v1 pricing moves Boost Bux from buyer to house; whether creators earn is an open decision (section 20) explicitly deferred.
- Trading, gifting, or a secondary market.
- User-provided code of any kind. The document schema is the permanent user ceiling; the expression DSL's totality is a sandbox boundary and does not relax (`specs/skin-shading-prd.md` section 9.2).
- Skinning arena gameplay objects (food, NOS canisters, grid) — the boundary rulings of `specs/skins-prd.md` section 7 stand unamended except where section 5.2 below explicitly extends them (base as a separate entity kind).
- Rating, commenting, or social features on the Skins page.
- In-game (mid-match) skin switching. Equip changes apply at the next match, exactly as the current once-per-mount read does (`client/web/components/GameArena.tsx:261-263`).
- CrazyGames in-portal purchases. The CG build browses and equips owned skins; purchase surfaces are compile-time absent until portal approval exists (`CRAZYGAMES.md:75`).
- Automated content moderation beyond provider-side filters, the structural validator, and text normalization. Review is human in v1.

## 4. Repository baseline

Verified facts this design builds on, with locations:

- **The equip channel is plumbed but dormant.** `User.selected_skin` exists (`server/src/db/models.rs:51`, attribute `selectedSkin`); `apply_player_skin` reads it at match prep, resolves it through the catalogue, and writes `GameState.skins: HashMap<u32, String>` (`server/src/matchmaking.rs:1262-1285`, `common/src/game_state.rs:2790-2803`); the map is fingerprint-excluded beside `usernames` (`common/src/fingerprint.rs:20`). But no endpoint, WS message, or DB method ever writes `selected_skin` — creation paths initialize `None` (`server/src/db/dynamodb.rs:2780,2845`) and the sole other reference is the read at `dynamodb.rs:3027`. The client's choice lives in localStorage `snaketron:skin:v1` (`client/web/utils/skinPreference.ts:13`) and reaches only the local renderer (`GameArena.tsx:263,961`); remote players always see classic today. The `SkinsQA.tsx:21-23` claim that the selector exercises the full channel is aspirational.
- **Refs are forward-provisioned.** `MAX_SKIN_REF_LENGTH = 96` is documented as sized for `sha256:<64 hex>` (`skin_catalog.rs:46-47`); `skin_catalog.rs:9-12` states player skins "will be content-addressed and validated by the shared `skin-schema` crate"; the client-side ref hygiene regex caps at 96 (`skinPreference.ts:13-46`). `resolve_skin_ref` currently rejects everything outside the 19-entry const CATALOG, coercing to `classic@1` (`skin_catalog.rs:53-67`).
- **The registry is compile-time.** `SkinRegistry` holds static structs plus four `include_str!` documents compiled in a `OnceLock` (`client/src/skin/registry.rs:22-57`), handed out as a process-wide `&'static SkinRegistry` (`registry.rs:106-109`); the sprite family's `engine()` path requires `&'static CompositeSkin` (`client/src/skin/sprite.rs:330`), which is why the live-tuning path `Box::leak`s rebuilt skins (`sprite.rs:169-192`) — the lifetime friction runtime registration must resolve. Server CATALOG parity is pinned by a client-crate test (`registry.rs:113-152`). The server crate does not depend on `skin-schema` (`server/Cargo.toml`).
- **Runtime document compilation already works.** `ParamSkin::from_json` compiles arbitrary JSON at runtime (`client/src/skin/doc.rs:176-184`); atlases fetch lazily by URL at first paint with structural fallback to the layer beneath (`client/src/skin/atlas.rs:18-24,293-319`); the validator is designed to compile to wasm (`skin-schema/src/lib.rs:3-6`) with a native CLI (`skin-schema/src/bin/validate-skin.rs`).
- **SkinDoc v1 has no image/pattern/text vocabulary.** Patterns, textures, animated head gradients, and sprite sheets are Rust-only (`checker.rs:1-9`, `animal.rs:3-8`, `ember.rs:1-13`, `sprite.rs`); all Rust skins and `ParamSkin` lower onto the same `CompositeSkin` layer engine (`client/src/skin/composite.rs`, `layer.rs`: `Region`, `Source::{Solid,…,Tiled,Image}`, `Fit::{Clip,Stretch,Tile,Cutout}`, spans, anchors). The sandboxed expression DSL exists (`skin-schema/src/expr.rs`) but no v1 document field carries an expression.
- **Texture conventions are measured and enforced.** Coats: one cell = 64 texels, opaque (`animal.rs:195-243`), friend/foe via `SideCue::Contour` (`animal.rs:17,418-419`; per-role tinting is not built — `specs/skin-shading-prd.md` section 20.3). Sheets: one cell = 16×16 texels hard ceiling, y is time, `DEFAULT_SPRITE_ROWS = 20`, 128-color palette, height must divide by rows (IHDR test `sprite.rs:1313-1352`); pictures must have zero frame translation (build error, `sprite_sheet.py:599-624`). Arena cells walk 15→5 CSS px on a non-DPR-scaled canvas (`client/src/skin/mod.rs:174`); no per-resolution variants exist today — one PNG per skin.
- **Seam repair exists as local Python.** LaMa via `simple-lama-inpainting`: `[T, X, T]` inpaint-wrap for crops (`build_coat_textures.py:363-393`, `GAP_FRACTION = 0.05`, one pass only), roll-and-repair for generated tiles (`sprite_sheet.py:447-493`) with double seam gates, multi-scale structural checks, bisection-found slice widths, and deliberate rejection of structurally disagreeing sources (`give_up`, `sprite_sheet.py:712-739`). Generation itself has no code: the four sprite sources were made manually with ChatGPT out-of-band (`client/web/THIRD_PARTY_ASSETS.md:41-58`).
- **Persistence patterns.** Single-table `snaketron-main` (PK `pk`/SK `sk`, GSI1/GSI2, PAY_PER_REQUEST, TTL; `dynamodb.rs:640-749`) with item families `USER#…`, `GAME#…`, counter-allocated i32 ids (`dynamodb.rs:850-879`). Tables and GSIs are created/patched by the server at startup (`ensure_tables_exist`, `dynamodb.rs:326-373`) — there is no migration tool; CLAUDE.md's Postgres guidance is stale for user data. Content precedents: versioned JSON blobs with optimistic concurrency (runtime config `dynamodb.rs:4040-4070`, CG preferences `dynamodb.rs:2172-2201`), large content in S3 with verified metadata in Dynamo (replays, `replay_store.rs:27-73`).
- **S3 is server-fronted and private.** The replay bucket has public-access-block and SSE; bytes reach clients only through `/api/games/:game_id/replay` with an 8 MB cap, byte-range support, and `public, max-age=300` (`http_server.rs:360-365,475-478`). No CDN, no public bucket, no presigned URLs, no multipart upload anywhere (the largest client→server body is the 10 MB JSON client-trace, `http_server.rs:796-840`).
- **Auth and admin.** axum; HS256 JWT; `auth_middleware` loads the user from DynamoDB every request and derives authorization from the DB, not claims (`server/src/api/middleware.rs:57-140`). Admin is an env allowlist (`SNAKETRON_ADMIN_USER_IDS`, `middleware.rs:33-49`) — no DB role; guests can never be admins. The admin surface precedent is runtime config: strict deserialization, `expectedVersion` optimistic concurrency → 409, audit rows with actor (`admin.rs:118-123,269-288`); client `/admin` is `AdminRoute`-gated, section-tabbed (`AdminPage.tsx`), absent from embedded builds. Auth is Authorization-header-only; no cookie is read or set anywhere in the server.
- **Guests and providers.** Guests are full user rows with durable ids; upgrade preserves the id (`dynamodb.rs:2849+`); CrazyGames `save_preferences` rejects guests (`crazygames.rs:614-616`) — the precedent that server-persisted cosmetic state gates on non-guest. Email/password and CrazyGames identities are deliberately never merged (`CRAZYGAMES.md:40-45`).
- **No economy exists.** Zero currency, inventory, entitlement, or payment-provider code anywhere; `User.xp` with atomic `add_user_xp` (`dynamodb.rs:3089`) and the idempotent completion-effect transaction (`dynamodb.rs:2413-2460`) are the closest primitives.
- **UI system.** React Router v7 with lazy dev-only QA routes (`App.tsx:47-67`); Tailwind v4 tokens + ~6,200 lines of BEM-ish CSS (`index.css`); graphite/paper/blue/red tokens (`index.css:86-99`); `HomeHeader` nav hardcodes `activePage?: 'play' | 'leaderboards'` (`HomeHeader.tsx:19-20,99-111`). Preview machinery is complete: `renderSkinFixture` (`client/src/render.rs:918`), `skinCatalog()`, `skinColorsForRole`, `whenSkinAssetsSettle()` (`client/web/wasm/index.ts:65-83`), and the `/qa/skins` `FixtureTile` pattern (fixed-sample plus live rAF). The strongest editor precedent is `SkinTuningSidebar.tsx` (draft/settle inputs, instant WASM preview rebuild). `gamePresentation.ts:278-289` builds `SnakeSkinInputs` without `skin_ref` although the field exists — results-surface swatches show positional palette, not the player's skin.
- **Perf.** The per-snake op ceiling is 200 (`client/src/skin/perf.rs:284`); the nine skins measured in `perf.rs`'s comment span 60–135 (`perf.rs:274-276`), and the ten textured skins are census-bounded at ≤ 200 but not individually recorded.

## 5. Entities and object model

### 5.1 Skin

```
Skin {
  skin_id: i32              // counter-allocated, stable forever
  kind: "snake" | "base"
  creator_user_id: i32
  name: String              // display name, 1..=40 chars, validated
  publication: "private" | "published" | "unpublished" | "disabled"
  pending_revision: Option<u32>     // revision awaiting review, if any
  price_bux: u32            // 0 = free; meaningful only when published
  head_revision: u32
  published_revision: Option<u32>
  head_content_ref / published_content_ref   // denormalized so match prep is one read
  created_at / updated_at / published_at
}

SkinRevision {
  skin_id, revision: u32       // 1-based, append-only
  content_ref: String          // "sha256:<64 hex>" of the canonical doc bytes
  doc: SkinDoc                 // schema v2, stored as canonical JSON
  texture_refs: Vec<String>    // content hashes of every referenced texture
  validated_schema: u32        // schema version the write-time validator passed
  exposed_at: Option<ts>       // first time this revision entered a match snapshot
  review_approved: bool        // set by admin approval; gates text visibility (5.3)
  created_at
}
```

Rules:

- Revisions are immutable and content-addressed. The `content_ref` is the sha256 of the canonicalized document bytes (sorted keys, no insignificant whitespace — canonicalization is specified in `skin-schema` and tested, because two serializers must never hash the same document differently).
- **Publication and review are independent dimensions.** `publication` is what the world sees; `pending_revision` is what an admin has been asked to look at. A publish request sets `pending_revision`; approval sets `published_revision` to it, marks the revision `review_approved`, sets `publication = published`, and clears the pending slot; rejection clears the pending slot with a reason and changes nothing else — a published skin stays published on its previously approved revision, a private skin stays private. This is what makes "the previously published revision stays live until the new one is approved" true by construction, and makes rejecting an *edit* of a published skin a no-op for the public rather than a silent unpublish.
- Every save through the Builder creates a new head revision. Editing therefore never changes what browsers, owners, or replays see until an admin approves the new revision.
- **Ownership is a grant, not a pointer.** An ownership row (section 6) names `skin_id`, not a revision, and many users may hold one; the skin's `creator_user_id` is a distinct concept and is the only non-admin who may edit. What a grant-holder equips resolves at match prep to the skin's `published_revision` — or, when the equipping user is the creator, the head revision (this is what "unpublished skins can be equipped by you" means: creators see their work-in-progress live).
- **Publication semantics.** `private` → never published; visible only to the creator (and admins). `published` → browseable, purchasable, equippable by any grant-holder. `unpublished` → withdrawn from browse/purchase; existing grant-holders keep equipping the last approved revision (the brief's rule, verbatim: unpublishing doesn't unequip). `disabled` → moderation kill switch: resolves to `classic@1` at match prep for everyone including the creator, disappears from browse, and by-ref resolution refuses its revisions (section 6.3); ownership rows persist so a reversal restores everyone. Only admins disable.
- Revision retention: unexposed, never-published revisions beyond the newest 20 per skin may be pruned; exposed or published revisions are kept forever (replays depend on them). Stated as a deliberate cost decision.
- Built-in catalogue skins are presented as rows of a reserved system owner so the Skins page is one list, but they resolve through the compiled registry as today; their refs stay `name@N` contract epochs per `specs/skins-prd.md` section 5.3.

### 5.2 Base skins as an entity kind

Today the base is dressing on a snake skin (`SkinDoc.base: Option<BaseTheme>`), viewer-attributed. That stays. Kind `base` adds a second, independent equip slot whose document is just the `BaseTheme` vocabulary (plus, later, whatever base-specific layers a future schema adds). Resolution order for the viewer's base dressing: explicitly equipped base skin → equipped snake skin's embedded `base` theme → classic. Because bases are viewer-attributed (`specs/skins-prd.md` section 5.6), the base slot never enters `GameState`, the wire, or any other player's screen — it is resolved locally from the viewer's own equip record. This is the one place this PRD extends a section 7 ruling, and it extends rather than contradicts: attribution, hue locks, and renderer-owned geometry are unchanged.

### 5.3 SkinDoc schema v2

Schema v2 is the existing layer engine, documented. It adds to v1:

- `layers: Vec<LayerNode>` where a node is a layer or a `group` (children plus shared span/transform/opacity applied at compile time — groups are authoring structure, flattened at registration so the static-topology rule and op accounting see only layers);
- sources: `solid`, `linear`/`radial` gradient, `pattern` (a tiled sub-stack), `image` (a texture reference by content hash + region + `fit` + repeat), and **`text`** — a string rendered along the body, one character per cell, in a bundled first-party font raster (never a user font), with per-role ink validated like labels. Text is capped at 24 characters from a safelisted set (`[A-Za-z0-9 .!?'-]`); one glyph blit per cell keeps the op budget linear and the conformance recorder honest. Validation normalizes before the denylist check — leet-mapping digits to letters and stripping separators — because `N1GG4` and `s.l.u.r` must not pass a literal match. And because text is the one source whose content a machine *can* read, it is gated structurally: **by-ref document serving blanks text sources of revisions that have not passed review** (`review_approved`), so free-form strings never reach an opponent's screen un-reviewed. Creators see their own text via the authenticated skin route and local registration; everyone else sees it only after approval;
- spans (`from`/`natural`/`min`/`priority`, `between`), fits (`clip`/`stretch`/`tile`/`cutout`), anchors, and fades — the grammar of `specs/skin-shading-prd.md` sections 6.1 and 8.3, unchanged;
- expressions where the layer engine already accepts them (e.g. tiled-source alpha), under the existing total DSL — v2 exposes what exists, it does not extend the DSL;
- `textures: { "<name>": "sha256:<hex>" }` — the document's texture manifest; image sources refer to names. Documents embed no pixels, ever.

Validator scope, stated honestly: everything **structural** that v1 enforces continues server-side and Builder-side from the same crate — boost-band pin `#fff200`/6 px, overhang ≤ 3 px, animation bounds (`skin-schema/src/lib.rs:335-728`) — plus per-v2: texture refs must resolve to seam-passed textures of a compatible kind; opaque image body fills require `SideCue::Contour` (per-role tinting is not built — `specs/skin-shading-prd.md` section 20.3 — so a texture cannot carry the friend/foe read; the contour restores *that read*, nothing more); text safelist/denylist; the op budget (section 5.5); document size ≤ 32 KB canonical bytes. Hue windows and label contrast are machine-enforced exactly where the machine can see color — palette-derived and declared-color layers — and are **review-enforced for image-bearing documents**, mirroring `specs/skin-shading-prd.md` section 13: once the body is a texture, the declared colors are a claim, not an observation, and the admin contact sheet is the oracle. The ~2 KB cap floated in `specs/skins-prd.md` section 11 predates the layer model and is superseded here.

The v1→v2 relationship is additive: every valid v1 document is a valid v2 document. Old clients receiving a v2-only ref fall back to classic by the standing rule; this is acceptable because refs are resolved per-viewer and degrade independently.

### 5.4 Texture

```
Texture {
  texture_id: i32
  owner_user_id: i32
  content_ref: String        // sha256 of the canonical PNG
  kind: "coat" | "sheet" | "overlay"
  width_px, height_px        // of the canonical variant
  canonical_tpc: u32         // 64 for coats/overlays, 16 for sheets
  repeat_cells: Option<f32>  // coats/sheets: body-axis repeat length
  rows: Option<u32>          // sheets: frame count (y is time)
  seams: SeamReport          // measured gate results per axis, per variant
  last_prompt: Option<String>       // saved for regeneration iteration
  generation: Option<GenerationMeta> // provider, model, job id, cost
  variants: [ { texels_per_cell, s3_key, width, height, bytes, sha256 } ]
  created_at
}
```

- **The ladder is per kind.** Coats and overlays: canonical at 64 texels/cell (supersampled at every real cell size), with 32 and 16 rungs produced automatically by the wrap-aware resize-first path the build tools already use (resize before measurement is load-bearing; `.claude/skills/author-skin` step 0). Sheets: canonical at 16 texels/cell (the hard pixel-art ceiling, `sprite_sheet.py:71`) with one optional rung at 8 for the smallest arena cells. Any rung may be **overridden** by an explicit upload — the brief's point that a few-pixels-per-cell rendering sometimes needs hand-simplified art rather than a downscale. The renderer picks the variant nearest `cell_size × devicePixelRatio`.
- **Variants are content-addressed individually.** Each rung's bytes hash to their own sha256 and serve at an immutable URL keyed by that hash (section 6.3); the texture's short-cached manifest lists current rungs. An override therefore mints a new variant hash and re-points the manifest — it never mutates bytes behind an immutable URL.
- Caps: PNG only; canonical ≤ 2048×2048 and ≤ 2 MB per variant; sheets ≤ 20 rows; dimensions must satisfy the kind's divisibility rules (height divisible by rows; whole-cell widths) — the same checks `sprite.rs:1313-1352` runs against shipped art, executed by the worker at upload/generation time. Decode hardening is explicit: magic-byte sniff, IHDR dimension bounds checked *before* any decode, and a decoded-size ceiling, because a 4 MB body may declare gigapixel dimensions.
- Textures are immutable once created; "editing" a texture is creating a new one, and the Builder's picker makes that cheap. A texture is private to its owner until a revision referencing it is **exposed** (equipped into a match) or published — at that point the referenced bytes are public by necessity and permanently so (only admin disable removes them from resolution). Stated plainly so nobody assumes texture privacy survives a match played in a draft.

### 5.5 Budgets

The per-snake op ceiling stays 200 (`client/src/skin/perf.rs:284`). For user documents the ceiling is enforced by static analysis at validation time: compile the document (natively, in `skin-schema`) and bound ops as `Σ_layers (3 + 2·R_max) + clip + head + text_cells`, with `R_max` the run count of the worst committed fixture (the zigzag). The bound is conservative relative to the committed fixture corpus — the same corpus that gates first-party skins — not to every conceivable live body; the layer cap is what bounds the pathological turn-every-cell case. A document that cannot prove ≤ 200 is rejected with the count in the error. Layer count ≤ 12 after group flattening; animation stays the 32-step ring with the Nyquist warning (`specs/skin-shading-prd.md` section 9.1).

## 6. Persistence

### 6.1 DynamoDB

New item families in `snaketron-main`, created/patched by `ensure_tables_exist` exactly like existing families:

| pk | sk | Item | Notes |
| --- | --- | --- | --- |
| `SKIN#{skin_id}` | `META` | Skin | GSI1 `SKIN_PUBLISHED#{kind}` / `{published_at RFC3339}` — present only while `publication = published`; the browse index, newest first. GSI2 `SKIN_OWNER#{user_id}` / `{created_at}` — "my creations". |
| `SKIN#{skin_id}` | `REV#{revision:06}` | SkinRevision | Canonical doc JSON inline (≤ 32 KB fits comfortably under the 400 KB item cap). GSI1 `SKINREF#{content_ref}` / `-` — resolve a raw `sha256:` ref without knowing the skin id (spectator/replay path); PK carries the hash so the index shards naturally instead of forming one hot partition. |
| `SKIN#{skin_id}` | `REVIEW#{ts}` | Review-queue entry + decisions | Audit trail, actor recorded. Open requests additionally carry GSI1 `SKIN_REVIEW_QUEUE` / `{requested_at}` — a *sparse* entry (only while open, cleared on decision) so `GET /api/admin/skins?status=in_review` and the queue-age metric are an index query, not a scan; the open-queue partition stays small by construction because entries leave it on decision. |
| `USER#{user_id}` | `SKINOWN#{skin_id}` | Ownership grant | `{acquired_at, price_paid_bux, source: purchase\|own_creation\|grant}`. Rows, not a list attribute: ownership is unbounded and the user META item must not grow toward the item cap. This satisfies the brief's "part of your user record" as an item-collection under the user's pk. |
| `USER#{user_id}` | `TXN#{source}#{external_or_uuid}` | Bux ledger entry | `source ∈ XSOLLA \| PURCHASE \| REFUND \| ADMIN`; `{delta: i64, request_hash, external_id, created_at}`. The namespaced sort key means a client-minted PURCHASE key can never occupy an XSOLLA credit's slot; client keys must be well-formed UUIDs; a replay whose `request_hash` differs from the stored one is a 409, not a silent success. GSI2 `WALLET#{user_id}` / `{created_at}` serves the recent-ledger read in time order. |
| `TEXTURE#{texture_id}` | `META` | Texture | GSI2 `TEXTURE_OWNER#{user_id}` / `{created_at}`. GSI1 `TEXREF#{content_ref}` / `-` for hash resolution (sharded like SKINREF). |
| `GENJOB#{job_id}` | `META` | Generation job | `{owner, kind, prompt, reference_texture_ids, reference_image_s3_keys, state: queued\|generating\|repairing\|validating\|done\|failed, result, error, cost}`; TTL 7 days. |

User META additions: `selected_skin` gains its writer at last; new `selected_base: Option<i32>`, `boost_bux: i64` (atomic ADD only, never read-modify-write; **signed**, because refunds and chargebacks can drive it below zero — section 11). `selected_skin` stays a String with a defined representation: `name@N` for built-ins (what M0 writes) or `skin:<i32>` for first-class skins; the parse rule is the prefix.

Match-prep resolution (`apply_player_skin` grows into this): equipped value → built-ins short-circuit to their `name@N` ref; `skin:<id>` → read Skin META (one read — the content refs are denormalized there) → `disabled` → classic; else pick `published_content_ref` (or `head_content_ref` for the creator) and write it into `GameState.skins`. First exposure of a revision additionally sets `exposed_at` with an idempotent conditional write — one read plus at most one write per revision ever. The map itself, its fingerprint exclusion, and the ≤ 96-char ref invariant are untouched.

### 6.2 S3

Bucket `snaketron-skin-assets` (env-configured like the replay bucket, absent-disables): keys `textures/{variant_sha256}.png` — one immutable object per variant — plus `genrefs/{job_id}/{n}.png` for generation reference images (TTL'd via lifecycle rule). Same hardening as replays: private bucket, SSE, public-access-block; the public surface is the API route, not the bucket. Local dev: created in `localstack-init.sh` alongside the replay bucket and mirrored in `scripts/init-dynamodb.sh`'s dev path (the persistence research flagged that the dev-only init script creates no buckets today; this PRD makes the two inits agree).

### 6.3 Asset serving, caching, and canvas taint

Three public read routes with three deliberate cache policies:

- **Texture bytes** — `GET /api/textures/blob/{variant_sha256}.png`: truly immutable (the URL *is* the hash), `Cache-Control: public, max-age=31536000, immutable`, `Content-Type: image/png`, `X-Content-Type-Options: nosniff`, 2 MB cap.
- **Texture manifest** — `GET /api/textures/by-ref/{content_ref}/manifest`: the current variant list (rung → sha, dimensions). `public, max-age=300, must-revalidate` — this is what makes variant overrides propagate.
- **Skin documents** — `GET /api/skins/by-ref/{content_ref}`: the revision document (text-redacted unless `review_approved`, section 5.3). `public, max-age=300, must-revalidate` with the ref as ETag. **Deliberately not immutable**, although the content is: this TTL is the moderation propagation bound. A disabled skin's refs return **410** from this route, so every viewer — including replay viewers — loses the content within five minutes; texture blobs may stay cached but are unreachable without a resolvable document. Serving policy: any revision that was ever exposed or published resolves; drafts never equipped into a match return 404 uniformly (existence of unexposed content is not disclosed).

Because the atlas loader draws these into a canvas and the API origin can differ from the page origin (`REACT_APP_API_URL`), the loader must request them with `crossOrigin = "anonymous"` (today `atlas.rs`'s `request()` sets no crossOrigin) and the routes must emit CORS headers (the server's CORS is already `Any`) — otherwise the first user texture taints the arena canvas and breaks `toDataURL`-based tooling. Called out here because it is invisible until it breaks capture and QA paths.

Disable and identical documents: two skins whose revisions hash to the same `content_ref` share the by-ref row space; the 410 rule is therefore "410 iff every skin carrying this ref is disabled," resolved via the sharded SKINREF index at request time.

## 7. APIs

All JSON camelCase, `{"error": …}` failures, ts-rs generated wire types, bodies size-capped per route — house conventions. Existence is not disclosed to unauthorized callers: any skin/texture the caller cannot see returns the same 404 as one that does not exist (sequential i32 ids are otherwise an enumeration oracle over private drafts). New routes on the production router:

**Skins**

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /api/skins?kind=&filter=published\|mine\|owned&cursor=&limit=` | filter≠published requires auth | Browse. Published: GSI1 newest-first, public. |
| `GET /api/skins/{skin_id}` | public if published; creator, grant-holder, or admin otherwise; else uniform 404 | Skin + the revision the caller may see (published revision; head for creator/admin). Creators receive un-redacted text. |
| `GET /api/skins/by-ref/{content_ref}` | anonymous | Render-path document fetch (section 6.3): exposed/published revisions only, text-redacted unless approved, 410 when disabled, 404 otherwise. |
| `POST /api/skins` | auth, non-guest | Create (kind, name, initial doc). Validates; allocates id; grants `own_creation` ownership. |
| `PUT /api/skins/{skin_id}` | creator or admin | New head revision (doc) and/or metadata. Full validation; returns new `content_ref`. **Name changes on a published skin do not take effect until the next approved revision** — metadata must not bypass the review gate (rename-to-impersonate is otherwise trivial). Price changes take effect immediately but purchases are price-conditioned (below). |
| `POST /api/skins/{skin_id}/publish-request` | creator | Sets `pending_revision` to head; enters the sparse review-queue index. |
| `POST /api/skins/{skin_id}/purchase` | auth; non-guest for priced skins | Body: `{idempotencyKey: uuid, expectedPriceBux}`. Transaction: ledger TXN put (conditional on key absence) + Bux debit (conditional balance ≥ price **and** price == expectedPriceBux, else 409 — the confirm dialog's price is what the buyer pays, never a raced update) + ownership row put (conditional absence). Free skins skip the debit and are grantable to guests. |
| `PUT /api/users/me/equipped` | auth | `{selectedSkin?: string, selectedBase?: string}` in the section 6.1 representation — the write path that closes the M0 gap. Validates ownership (or built-in), writes user META, mirrors to localStorage client-side for instant local render. |

**Textures**

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /api/textures?cursor=` | auth | Own textures (Builder picker). |
| `GET /api/textures/{texture_id}` | owner/admin; else 404 | Metadata incl. variants, seams, lastPrompt. |
| `GET /api/textures/by-ref/{content_ref}/manifest`, `GET /api/textures/blob/{variant_sha}.png` | anonymous | Section 6.3. |
| `POST /api/textures` | auth, non-guest, quota | Upload: multipart/form-data (the repo's first; 4 MB request cap) with kind + metadata part + PNG part(s). Returns a job id — dimension checks, seam measurement, and ladder generation run in the **worker**, not the request handler (synchronous pixel work in the API process is a CPU-exhaustion vector behind free registration). The handler does only magic-byte + IHDR bounds checks before accepting. |
| `POST /api/textures/{texture_id}/variants` | owner | Override one ladder rung with hand-simplified art (same async shape). Mints a new variant hash; re-points the manifest. |

**Generation**

| Route | Auth | Purpose |
| --- | --- | --- |
| `POST /api/textures/generate` | auth, non-guest, quota | multipart: `{kind, prompt, referenceTextureIds?}` + up to 3 raw reference images (PNG/JPEG, ≤ 4 MB total — the brief's "optional reference images" means arbitrary images, not only pre-validated textures; they store under `genrefs/`, TTL'd, never as Textures). → `{jobId}`. |
| `POST /api/skins/generate` | auth, non-guest, quota | Same input shape. Plans a document, generates textures it needs, assembles, validates; result is a draft skin. |
| `GET /api/generation-jobs/{job_id}` | owner | State machine + result (textureId / skinId) or structured failure (which gate rejected, provider refusal, etc.). Client polls; no WS change. |

**Wallet**

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /api/wallet` | auth | Balance + recent ledger (GSI2 time-ordered). |
| `POST /api/wallet/xsolla/checkout-token` | auth, non-guest, web distribution | Server-minted Pay Station token binding the authenticated user id and a pack SKU from runtime config — the client never asserts identity or amounts. |
| `POST /api/wallet/xsolla/webhook` | Xsolla signature (timing-safe compare), IP allowlist | Credit on payment (amount taken from the **SKU in runtime config**, never from the webhook body), reverse on refund/chargeback; both idempotent on the Xsolla transaction id in the XSOLLA/REFUND namespaces. Never callable by clients. |

**Reporting and admin**

| Route | Auth | Purpose |
| --- | --- | --- |
| `POST /api/skins/{skin_id}/report` | auth, per-user rate limit | `{reason enum, note}` → review queue. Ships in the first user-content phase (M1), not with the admin UI. |
| `GET /api/admin/skins?status=in_review\|reported\|published…` | admin | Queue via the sparse review-queue index; search. |
| `PUT /api/admin/skins/{skin_id}/status` | admin | Approve (sets `published_revision` + `review_approved`, clears pending), reject (clears pending with reason), unpublish, **disable**, re-enable. Actor + reason on the REVIEW audit row; `expectedVersion`-style concurrency like runtime config. |
| `POST /api/admin/users/{user_id}/grant` | admin | Grant skin or Bux (support tooling), ADMIN-namespace ledger-idempotent. |

Rate limits: mutation routes get per-user (not per-IP) limits since every caller is authenticated; generation and uploads are quota-gated (section 20); the anonymous by-ref fetchers ride the global limiter like replays. The auth middleware's per-request Dynamo user read predates this PRD; browse being anonymous keeps the hot path cheap, and wiring `auth_middleware` to the existing Redis `UserCache` is noted as an independent optimization, not a dependency.

## 8. Generation pipeline

### 8.1 Shape

A `texture-forge` worker container (Python — PIL/numpy/torch/LaMa, built from `client/design/tools/`) consumes GENJOB items — and, per section 7, also processes uploads. The Rust server owns the job state machine, quotas, and provider calls; the worker owns pixels: decode, dimension checks, seam measurement, LaMa repair, ladder resizing, palette quantization. The container holds no credentials beyond scoped access to the skin-assets bucket and the job items. Stages: `queued → generating → repairing → validating → done|failed`, each transition written to the job item so the Builder can show real progress.

### 8.2 Prompt assembly

The server builds provider prompts from the texture kind, following the brief's templates:

- **sheet** (animated, y-is-time): "Create a `{W}×{H}` px seamless texture of: `{prompt}`. This is an animated sprite for a snake in a Snake game. The texture is applied to the snake lengthwise, left to right, starting at the head; the y axis is time — every tick advances one row, wrapping like a kernel. Every cell of the snake is 16×16 px. We need `{rows}` rows before the animation tiles. The image must be seamless vertically" — plus "and horizontally" when the sheet repeats along the body.
- **coat/print** (static, dual-tileable): seamless in both axes, flat even lighting (`client/design/sprites/README.md:44-56`), 6–8 marks across the height (`.claude/skills/author-skin/SKILL.md:287`) — the measured source constraints encoded into the prompt rather than hoped for.
- **overlay** (e.g. a helmet): an N-cell strip at 16 px/cell on transparency, head-anchored, no tiling requirement.

Reference images (raw uploads and/or the caller's existing textures) attach as provider image inputs. Providers: OpenAI Images first, OpenRouter image models as fallback/alternative; keys via env; per-job cost recorded on the job item. Provider content filters are the first moderation layer; a provider refusal is a structured job failure, not a retry.

### 8.3 Repair, gates, and cost ceilings

Generated output runs the existing machinery verbatim: roll-and-repair for "seamless" claims (both wrap joins moved to center, thin-slice LaMa inpaint, byte-restored surroundings), the percentile+ratio double gate, the multi-scale structural check (because a one-pixel metric certifies its own repair — `sprite_sheet.py:108-116`), frame-translation rejection for pictures, palette quantization, then wrap-aware ladder resizing. A texture that fails gates after one repair pass is **regenerated with feedback** (the failed axis and measurements appended to the prompt), at most 3 attempts, then the job fails with the evidence — the pipeline inherits the local tools' philosophy that structurally disagreeing sources are re-generated, never force-repaired.

Cost is bounded before the money is spent, not alarmed after: a hard per-job provider-call budget (≤ 10 calls; exceeding it fails the job), a cap on image-bearing layers per generated skin plan (≤ 4), per-user daily quotas gated on account age (a fresh registration does not get a quota on day zero — free accounts are otherwise a Sybil multiplier on real provider spend), and a **global daily-spend circuit breaker that halts the pipeline**, in addition to the section 16 alarms.

### 8.4 Skin generation

`POST /api/skins/generate` adds a planning stage before the texture stages: a text-model call (same provider abstraction) that emits a SkinDoc v2 draft from the prompt — palette, layer stack, which layers need textures and of what kind — followed by texture jobs for each needed image (within the plan cap), assembly, and full validation. Validation failures loop back to the planner with the validator's structured errors, at most 2 replans, all inside the same per-job call budget. The output is always a private draft skin owned by the requester; generation never publishes.

## 9. The Skins page

Route `/skins`, public (browse requires no account; equip/buy/create do). `HomeHeader`'s `activePage` union gains `'skins'` and a nav link — the hardcoded union called out in the baseline. Layout per the brief:

- **Main column: snake skins.** Vertical list; each row is a wide horizontal snake preview — `renderSkinFixture` on the long straight fixture at a fixed `anim_ms` sample (frozen), swapping to a live rAF loop on hover/focus, exactly the `FixtureTile` fixed-sample/live split from `/qa/skins`. Rows carry name, creator, price (Bux glyph or "Free"), and the contextual action: Equip (owned), Buy (published, priced), Get (published, free — available to guests too; a granted free skin is a small permanent row and denying guests the one costless acquisition path would make section 11's "guests equip free skins they hold" a null set), Edit (yours). Published list paginates on GSI1 newest-first; tabs for All / Owned / Mine.
- **Right column: base skins.** Vertical list of rectangular base previews (the base fixture scene rendered through the same entry), frozen, animating on hover; same actions against the `selected_base` slot.
- Visual language: graphite-on-paper, uppercase-italic nav typography, the brutalist `border-2 border-black shadow-[8px_8px_0_#000]` card idiom — the home page and GameOverCard system, not a new one.
- Previews of user skins require their documents and textures: the page fetches by-ref documents for the visible window, registers them at runtime (section 13), and lets structural fallback paint until textures settle (`whenSkinAssetsSettle()` exists for the once-painted case). Virtualize the list; never fetch more than the viewport + one screen.
- Reduced motion: `prefers-reduced-motion` suppresses the hover animation entirely — previews stay at step 0, the standing rule.

Purchases from this page: confirm dialog showing price and balance → `POST /purchase` with a client-minted idempotency key and the displayed price as `expectedPriceBux` (a raced price change 409s and re-prompts, it never silently charges more) → optimistic equip prompt on success. Insufficient balance routes to the Boost Bux purchase page (web builds only; CG builds show owned/free actions and hide priced ones per the distribution gate).

## 10. The Skin Builder

Route `/skins/builder/{skin_id}` (and `/skins/builder/new`). Creator or admin only (admins may edit any skin — the brief's rule; admin edits create revisions attributed to the admin in the audit trail).

- **Preview strip (top).** Always animating (rAF; reduced-motion pins step 0). Snake skins: the main horizontal pose large, flanked by the poses that catch regressions — enemy-role view and the u-turn/corner fixture. Base skins: Own and Enemy variants side by side. All through `renderSkinFixture` against a runtime-registered draft compiled from the current editor state on every settled change — the `SkinTuningSidebar` draft/settle + instant-WASM-rebuild pattern, generalized.
- **Layer rack.** The document's layer/group tree; drag to reorder (the repo's first drag-and-drop — keep it dependency-light or use the maintained dnd-kit; decision at implementation); group/ungroup; per-layer enable, opacity, and the source-specific property panel.
- **Schema-driven properties.** The property UI is generated from the document schema, never hand-built: `skin-schema` derives a machine-readable schema (JSON Schema via schemars, exported to `client/web/types/generated/` beside the ts-rs types, CI-diffed like `gen-types.sh` output). The Builder walks the schema: numbers→sliders with bounds from validator constants, enums→segmented controls, colors→pickers, optionals→toggles, arrays→lists. A schema node kind the walker doesn't know is a **build error**, not a silently missing control — that is the "never keep it in sync by hand" requirement made structural. Validator messages surface inline on the offending control (the validator returns structured paths).
- **Texture picker.** For image sources: a widget listing the user's textures with the current selection, plus Upload (multipart, async job) and Generate (prompt box pre-filled with the texture's `lastPrompt` — saved precisely so regeneration iteration is one edit away; job progress inline from the job poller; reference-image attach). Variant management: the ladder rungs shown per texture with per-rung override upload for the hand-simplified-low-res case.
- **Text element.** A layer whose source is `text`: the input enforces the 24-char safelist live and previews the normalization the validator applies; the Builder states plainly that text is hidden from other players until the revision passes review (section 5.3).
- **Save** = `PUT /api/skins/{id}` → new head revision; the Builder shows validation results from the server as the authority even though the wasm validator pre-checks locally (same crate, same result, but the server's word is the one that counts). Publish request button appears on valid drafts.

## 11. Boost Bux and Xsolla

- Balance lives on the user META (`boost_bux: i64`), mutated only by atomic ADDs guarded by namespaced ledger TXN rows — no read-modify-write anywhere. Every credit and debit has an idempotency identity in its own namespace: `XSOLLA#<txn>` for credits, `REFUND#<txn>` for reversals, `PURCHASE#<uuid>` for skin purchases, `ADMIN#<action>` for grants (section 6.1) — a client can never occupy a webhook's ledger slot.
- **Xsolla** (assumed: the brief's "Zsolla") is merchant of record. Checkout: the client asks the authenticated `checkout-token` endpoint for a Pay Station token; the server binds the user id and a pack SKU from runtime config, so identity and amounts are never client-asserted. Settlement: the webhook (timing-safe signature verification, IP allowlist, replay-safe via the ledger) credits the SKU's configured Bux amount — the webhook body's amount field is cross-checked, never trusted. No card data, no PII beyond the Xsolla account linkage, ever touches Snaketron.
- **Refunds and chargebacks are first-class.** Xsolla delivers reversal webhooks; each writes a REFUND ledger row and atomically subtracts the original credit. The balance is signed and may go negative — a negative balance blocks purchases and generation until repaid, surfaced in the wallet UI. Ownership already granted from since-reversed Bux is **not** traced and revoked in v1 (Bux are fungible; per-item clawback is a support action via the admin grant tooling when warranted); repeat-chargeback account policy is an open decision (section 20).
- Skin pricing: creator picks a tier from a fixed set — Free, 100, 250, 500, 1000 BB (runtime-config, admin-tunable). v1 debits the buyer and grants ownership; the Bux sink is the house. Creator revenue share is deliberately deferred (section 20) — it changes the legal shape of the currency and deserves its own decision.
- Purchases and checkout are **web-distribution only** at launch: the CrazyGames build compiles them out via the `ClientDistribution` gating ads already use, honoring `CRAZYGAMES.md:75`. Owned and free skins work everywhere.
- Guests: can browse, acquire free published skins (a costless grant on a durable row), and equip built-ins and skins they hold — but creating, uploading, generating, and *priced* purchases require a registered account, consistent with the CG-preferences precedent of gating persisted state on non-guest, and because a tab-scoped guest token orphaning a paid skin is a support incident by design. Guest→registered upgrade preserves the user id, so anything a guest was granted survives promotion.
- Provider silos: an email account and a CrazyGames account are never merged (`CRAZYGAMES.md:40-45`), so ownership does not roam across providers. Stated as accepted v1 behavior, surfaced in UI copy at purchase time on CG-linked accounts.
- **Privacy and deletion.** This PRD creates data classes the CrazyGames privacy inventory (`CRAZYGAMES_PRIVACY.md`) does not cover: stored generation prompts (free text — users will paste anything), a financial ledger, ownership rows, and publicly displayed creator attribution. Account deletion semantics: creator attribution on published skins is anonymized ("a deleted creator"), published/exposed revisions are retained (replays and other players' ownership depend on them — the same reasoning as match history), prompts and unexposed drafts are deleted, and ledger rows are retained under their financial record-keeping basis. Updating the privacy inventory and notice copy is a named exit criterion for M1 (creations/equips) and M5 (wallet).

## 12. Publishing, moderation, admin

- Flow: creator hits Publish → `pending_revision` set, sparse queue entry → admin queue in the new `/admin` Skins section (section-tab beside runtime config; same `AdminRoute`/env-allowlist admin model — a DB role is out of scope) → approve (publishes that revision, browseable instantly) or reject with a reason the creator sees in the Builder. Rejection clears only the pending slot: a published skin stays published on its prior approved revision; nothing is silently withdrawn.
- Re-publishing after edits re-enters review; the previously published revision stays live until the new one is approved — true by construction under the two-dimension model of section 5.1. Metadata does not bypass the gate: renames of published skins wait for the next approval; prices are purchase-conditioned (section 7). No path exists by which approved public content changes without another approval.
- What review is *for*, stated plainly: the validator has already enforced everything structural before a human sees it. Review exists for what machines cannot see (`specs/skin-shading-prd.md` section 13) — what textures depict, hue/contrast honesty of image-bearing documents, what text says, trademark/IP problems, and hate/sexual content that survived provider filters. The admin view renders the full contact-sheet-style preview (all roles, poses, animation strip) so the reviewer sees what players will.
- **Disable** is the kill switch (sections 5.1, 6.3): match prep resolves to classic immediately, by-ref returns 410, warm caches age out within the five-minute document TTL, ownership is retained, the action is reversible and audited. Unpublish is a business action; disable is a moderation action; the UI keeps them visually distinct so neither is reached by accident.
- **Reporting ships with the first exposure surface, not with this section's UI.** `POST /api/skins/{id}/report` and the disable path land in M1 (section 18) because that is the phase in which a user pixel first reaches another player's screen; the admin *queue UI* for them lands here. Until M4's UI exists, reports surface to admins through the existing config-audit-style listing — minimal, but the kill switch works from day one of exposure.
- Unpublished-but-equipped exposure: a creator playing a public match wearing a private draft exposes that draft's imagery to their opponents' renderers by necessity (the ref must resolve). Review therefore cannot be the *only* gate on offensive content reaching players — the structural gates (text redaction until approval, provider filters on generated imagery) plus report-and-disable cover the window, and this residual exposure is a stated, accepted risk (section 20, risk 1).

## 13. Client runtime integration

- **Registry.** `SkinRegistry` gains a runtime layer: `register_remote(content_ref, doc_json) -> Result<(), SkinDocError>` compiling through `ParamSkin::from_json` into the same `CompositeSkin` engine. The lifetime friction is real (the registry is a process-wide `&'static`, and the sprite engine path demands `&'static CompositeSkin` — the live-tuning precedent `Box::leak`s): the registry refactors to `Arc<dyn SnakeSkin>` handles internally (interned per `specs/skins-prd.md` section 8.3 — the render loop still does an index lookup, never a map lookup), with leak-based registration acceptable as an interim only if the Arc refactor proves invasive; a session registers a bounded set (≤ 64 remote skins), so even the leak ceiling is finite.
- **Match flow.** Client receives `GameState.skins` refs as today. For each unknown `sha256:` ref: fetch `GET /api/skins/by-ref/{ref}` (five-minute revalidating cache), validate+compile in wasm, register; textures fetch their manifest then blobs lazily at first paint through the existing atlas path (now with `crossOrigin` per section 6.3). Until then that snake paints classic — the standing fallback rule, now doing real work. No WS protocol change, no version-gate bump: the map and ref format are unchanged.
- **Own render.** `local_skin_ref` continues to feed the viewer's own snake and (fallback) base dressing; a new `local_base_ref` parameter carries the equipped base skin's resolved theme. The creator's own drafts — including unapproved text — register from the authenticated skin route or the Builder's local state, never from the redacting anonymous route. Equip state loads from `GET /api/auth/me` (extended with equipped + owned summaries) with localStorage as the offline echo, replacing localStorage-as-authority.
- **Results surfaces.** `gamePresentation.ts` starts populating the `skin_ref` field it already has (`utils/snakeSkin.ts:23`), so GameOverCard swatches and roster portraits finally show actual skins — the dormant plumbing lights up as a side effect of M0.
- **Server catalogue.** `resolve_skin_ref` grows from const-allowlist to: built-in list ∪ Dynamo-resolved refs, with the same coerce-to-classic default. The server takes `skin-schema` as a dependency and validates at write time (PUT), stamping `validated_schema` on the revision; match prep checks only publication status and existence — no re-validation in the hot path.

## 14. Sync, wire, and compatibility

- `GameState.skins` stays a fingerprint-excluded `HashMap<u32, String>`; refs stay ≤ 96 chars (`sha256:` = 7 chars + 64 hex = 71). The chaos suite must stay green with user refs present, absent, and mutated mid-game — same clause as the original identity channel, re-asserted for the new ref population.
- Old clients: unknown ref shape → classic, deterministic, logged. New clients on old servers: no new server behavior is required to *render* (by-ref fetch would 404 → classic); equip API absence degrades to localStorage-local behavior, i.e. today.
- Replays and spectators render exactly what players saw, by construction: the ref in the snapshot is the content hash of the exact bytes, and revisions are immutable. Disable is the one deliberate divergence: by-ref resolution refuses disabled refs (410, section 6.3), so moderation reaches replays within the document cache TTL — a replay of a disabled skin renders classic, and that is the point.

## 15. Edge cases and required behavior

| Case | Required behavior |
| --- | --- |
| Ref unknown, invalid, unfetchable, or fetch still in flight | Classic, per the standing rule; retry fetch with backoff; never a broken frame. |
| Document fetched but texture still loading | Structural fallback layers paint (atlas contract); pixels pop in when settled. |
| Creator equips draft, then saves a new revision mid-match | Match keeps the ref it resolved at prep; the next match picks up the new head. |
| Grant-holder of an unpublished (not disabled) skin | Keeps equipping the last approved revision forever; it simply isn't browseable or purchasable. |
| Published skin's edit is rejected in review | Pending slot cleared; the published revision never flickered. |
| Skin disabled while equipped / while in a replay | Match prep → classic immediately; by-ref → 410; warm caches age out ≤ 5 min. Re-enable restores. |
| Purchase raced twice (double-click, retry) | PURCHASE-namespace idempotency: one debit, one grant; an exact replay returns the first result; a same-key different-payload request (mismatched `request_hash`) is 409. |
| Purchase raced against a price change | `expectedPriceBux` condition fails → 409 → UI re-prompts with the new price; the buyer is never charged a price they did not see. |
| Purchase with insufficient Bux | Conditional debit fails the transaction atomically; no partial grant. |
| Xsolla webhook replayed | XSOLLA-namespace ledger condition rejects the duplicate; 200 returned (Xsolla retries on non-200). |
| Xsolla refund/chargeback after Bux were spent | REFUND row subtracts the credit; balance may go negative; purchases and generation block until repaid; ownership is not auto-revoked. |
| Guest taps Get on a free published skin | Granted (costless, durable row); priced Buy prompts registration. |
| Guest upgrades to account | Same user id — ownership, Bux, equips all survive (existing upgrade semantics). |
| Texture referenced by an exposed or published revision, owner deletes it | Deletion refused; content-addressed bytes are cheap to keep and replays depend on them. |
| Generation provider refuses prompt | Job fails with the provider's category; no retry; quota consumed (deliberate — refusal spam is the abuse case). |
| Generation passes provider but fails seam gates 3× | Job fails with measured evidence; quota consumed; prompt preserved for iteration. |
| Generation job would exceed its provider-call budget | Job fails at the ceiling; the circuit breaker halts the whole pipeline if the global daily spend trips. |
| Two skins' revisions hash to identical content | Refs collide harmlessly for rendering (same bytes ⇒ same pixels); by-ref disable is "410 iff every carrying skin is disabled" (section 6.3). |
| Admin edits someone's skin | New revision, audit row names the admin; publication state unchanged until review. |
| Skins page opened logged-out | Full browse; every action button routes to auth. |
| `/skins` in CrazyGames build | Browse + equip owned/free; priced actions hidden; no checkout surface compiled in. |
| Op-budget or size-cap rejection in Builder | Inline structured error on the offending layer; save refused server-side regardless of client state. |
| Text layer with disallowed characters or a normalized denylist hit | Rejected at validation with the offending span; and un-reviewed text is redacted from anonymous document serving regardless (section 5.3). |
| Request for a private draft by a non-creator | Uniform 404 — existence is not disclosed. |

## 16. Observability

Bounded-cardinality, no user-id labels except in the admin audit trail (which is data, not metrics):

- equip writes, by slot and success/failure; match-prep resolutions by outcome (built-in, user ref, disabled→classic, missing→classic);
- by-ref fetches: hit/miss/410/latency; manifest and blob fetches by variant;
- validation failures by constraint (the existing skins counters, extended with v2 constraints: op budget, texture compat, text safelist/denylist);
- generation funnel: jobs by kind × terminal state; provider errors; seam-gate failures by gate; per-job cost, daily spend, and **circuit-breaker state** (tripping it is an incident, not a log line);
- economy: webhook credits and reversals, purchase attempts/success, price-condition 409s, insufficient-balance count, idempotency rejections, negative-balance account count (a spike in any of these is an attack signature);
- review queue depth and age of oldest item via the sparse queue index (the moderation SLA is a product promise; this is its gauge); reports filed;
- runtime registry: remote registrations per session, compile failures, the ≤ 64 ceiling being hit.

## 17. Test plan

- **skin-schema (native, CI):** v2 vocabulary round-trips; canonicalization stability (hash-equality across serializer variations); every new validator rule (texture compat, SideCue requirement for opaque image fills, text safelist + normalization + denylist, op-budget bound, size caps) with rejection-message tests; v1 docs remain valid v2.
- **Server (integration, per house rules — servers self-maintain, no DB pokes from the harness):** skin lifecycle across the publication × review matrix (publish request, approve, reject-of-published-edit leaves publication intact, unpublish, disable, re-enable) with per-state visibility/equip assertions; uniform-404 assertions for private content; ownership and purchase transactions incl. double-submit, price-race (`expectedPriceBux` 409), and cross-payload key-reuse (409) tests; wallet credit + reversal idempotency against a mock webhook incl. negative-balance blocking; checkout-token binding; match-prep resolution matrix (built-in / published / creator-draft / disabled / missing) incl. the `exposed_at` write; by-ref 404/410/redaction semantics; upload handler rejects oversized IHDR before decode; generation state machine incl. call-budget exhaustion and circuit-breaker halt against mock providers and a mock forge worker.
- **Client (native):** runtime registration through `ParamSkin::from_json` for v2 docs; conformance suite over a corpus of representative user documents (boost distinctness, overhang honesty, op-count invariance, reduced-motion) — the suite already iterates the registry, so registered fixtures ride for free; classic goldens untouched (this PRD changes no first-party pixels — assert it).
- **Client (browser, drain harness):** Skins page browse/equip against MockWebSocket + fetch stubs; Builder edit→preview→save loop; a remote-skin match fixture proving fetch→register→exact-render and the taint-free canvas (`toDataURL` still works after a cross-origin texture — the section 6.3 regression trap, pinned); text-redaction visibility (owner sees text, opponent fixture does not, until approval).
- **Forge worker (Python, its own CI job):** the seam gates against committed known-good and known-bad fixtures from `.sprite-state/` evidence; ladder resize wrap-preservation; decode-bomb rejection.
- **Sync:** chaos suite green with `sha256:` refs in the map; fingerprint tests re-asserted.

## 18. Migration plan

Ordered, each phase gated by the one before; every phase ships player-visible or dev-visible value on its own:

1. **M0 — light up the dormant channel.** `PUT /api/users/me/equipped` + `selected_skin` write path + `/skins` page over built-ins only (all free) + `gamePresentation.ts` skin_ref population. Gate: two browsers see each other's built-in skins in a real match; chaos suite green. No new entities.
2. **M1 — entities, resolution, and the kill switch.** Dynamo skin/texture/ownership families, S3 bucket, by-ref routes with the section 6.3 cache/410 semantics, runtime client registration, server `skin-schema` dependency, `resolve_skin_ref` growth — **plus disable and `POST /report`**, because this is the phase in which a user-authored pixel can first reach another player's screen, and the section 2 must-not requires the kill switch to exist in the same phase, not two later. Ship behind a flag with seeded first-party test entities. Gate: a hand-seeded user skin renders exactly for a spectator and in a replay; disabling it goes classic everywhere within the cache TTL.
3. **M2 — SkinDoc v2 + Builder.** Schema v2 in `skin-schema` (including text redaction in by-ref serving), schemars export + CI diff, the Builder (rack, schema-driven properties, texture upload picker, text element, previews), draft save/equip. Gate: author a textured skin end-to-end in the Builder without touching code, pass conformance corpus; an opponent fixture never sees un-reviewed text.
4. **M3 — generation.** Forge worker container (also taking over upload pixel work), job machine, texture generation with gates and cost ceilings, then skin generation. Gate: prompt→playable draft skin with all gates enforced, budgets exhausted gracefully, circuit breaker demonstrated.
5. **M4 — publishing.** Review flow and queue UI, admin Skins section, browse of published user skins, the report queue surfaced properly. Gate: full lifecycle exercised including reject-of-published-edit and disable-reaches-replays.
6. **M5 — economy.** Wallet, ledger, checkout token, Xsolla webhook incl. reversals, priced skins, purchase flow, CG distribution gating, privacy-inventory update. Gate: end-to-end purchase and refund in Xsolla sandbox; idempotency and price-race tests green.

M0 is deliberately first and standalone: it is the smallest change that makes skins *real* to players, it derisks the wire assumptions for everything after, and it is the phase the current codebase has already 90% built.

## 19. Acceptance criteria

First-class skins v1 is complete when:

1. A player can select any built-in skin and every other participant — player, spectator, replay viewer — sees it (M0 closed the dormant channel).
2. Skins and textures exist as DynamoDB entities with immutable content-addressed revisions; equips resolve per section 5.1 rules; ownership rows are permanent; publication and review are independent dimensions with the reject-keeps-published property tested.
3. SkinDoc v2 expresses layers/groups/sources including image, pattern, and text; the consolidated validator runs identically native (server) and wasm (Builder); every v1 document remains valid; un-reviewed text never renders for non-creators.
4. The Skins page browses published skins publicly with frozen-until-hover previews in the site's visual system; equip, buy, get, and edit actions behave per publication state and ownership.
5. The Builder edits any owned skin with a schema-generated property UI (schema-node exhaustiveness is a build error), drag-reorderable layers, groups, the texture picker with upload/generate/ladder-override, and always-animating previews of the specified poses.
6. Texture and skin generation run as quota-gated async jobs through provider APIs and the containerized LaMa gates, accept raw reference images, and are bounded by per-job call budgets and the global circuit breaker; failures carry evidence.
7. Publish review gates all public visibility including renames; re-publication of edits re-enters review without disturbing the live revision; disable removes content everywhere — live, spectate, and replay — within the document cache TTL; reporting exists from M1; all admin actions are audited.
8. Boost Bux is credited only via the verified webhook against runtime-config SKUs, reversed on refund/chargeback with signed balances and negative-balance blocking, and debited only via namespaced idempotent purchase transactions conditioned on the displayed price; no client-trusted balance mutation exists; CG builds compile out purchase surfaces.
9. Every prior-PRD invariant is re-verified in CI with user documents present: boost-band pin, overhang caps, op budget, classic goldens byte-identical, chaos suite and fingerprint tests green with `sha256:` refs — and the hue/contrast rails are enforced structurally where machine-visible and listed on the admin review checklist where not.
10. The section 16 counters exist; generation spend has a circuit breaker, and review-queue age has an alert.

## 20. Risks and open decisions

Risks:

1. **Content moderation is human-bottlenecked.** Review capacity is the launch constraint. The residual exposure window — a private draft's *imagery* reaching opponents before any human has seen it (text is structurally gated, images are not) — is accepted and bounded by provider filters on generated art, the M1 report/disable path, and the five-minute revocation TTL. The review guideline doc (owned by Product, referenced by the admin UI) must exist before M4 exits.
2. **IP and generated content.** Users will prompt copyrighted characters; provider filters catch some, review catches more, neither is airtight. Same guideline doc; same disable path.
3. **No pixel oracle for user textures.** Native tests never see texture pixels (`atlas.rs` native stubs); the machine checks structure and seams, not depiction — and for image-bearing documents, not hue/contrast either. Carried from `specs/skin-shading-prd.md` section 13 and mitigated by review, not solved.
4. **Cost control on generation.** Image APIs bill per call and the pipeline may call several times per job. The ceilings are launch-blocking: per-job call budget, per-plan image-layer cap, age-gated quotas, and the auto-halting circuit breaker — alarms alone page a human after the money is gone.
5. **Registry lifetime refactor.** The `Arc<dyn SnakeSkin>` change touches the render hot path; the interned-index discipline (`specs/skins-prd.md` section 8.3) must be re-proven with the perf smoke, and the ≤ 64 remote-skin ceiling bounds memory (atlases held for process lifetime add up).
6. **First multipart, first webhook, first public-write surface.** Three new attack surfaces in one PRD. Each has a named mitigation (magic-byte + IHDR-before-decode + worker-isolated pixel work; timing-safe signature + IP allowlist + namespaced ledger idempotency + SKU-sourced amounts; per-user rate limits + validation-first writes + uniform 404s) — and each deserves focused security review before its phase ships.
7. **Xsolla/CG policy coupling.** If CrazyGames later approves IAP, the distribution gate design should let the portal build adopt purchases without re-architecture; if Xsolla terms complicate virtual-currency handling in some regions, pack availability is runtime config and can be geo-shaped.
8. **Economy integrity.** Bux dupes via webhook replay, purchase races, or ledger-slot poisoning are the catastrophic bug class; the namespaced ledger-conditional pattern must be the *only* mutation path, enforced by review and by the race tests in section 17. Chargebacks are handled, not prevented — the negative-balance state is the containment.
9. **Privacy debt.** New durable personal data (prompts, ledger, attribution) lands before automated deletion tooling exists; the section 11 deletion semantics and inventory updates are named exit criteria, not aspirations.

Open decisions (each needs an owner before its phase starts, none blocks M0–M2):

- **Creator revenue share** (M5): house-sink v1 is proposed; sharing Bux with creators changes moderation incentives and the currency's legal posture. Recommendation: defer, revisit with usage data.
- **Price tiers and pack pricing** (M5): proposed tiers in section 11 are placeholders; Product owns final numbers via runtime config.
- **Generation quotas and pricing** (M3): free daily quota for aged registered accounts vs. Bux-metered generation. Recommendation: small free daily quota + Bux beyond, so the currency has a sink besides skins.
- **Review SLA and staffing** (M4): who reviews, target queue age, escalation path.
- **Repeat-chargeback policy** (M5): threshold at which reversal-abusing accounts lose purchase privileges or are suspended; interacts with the negative-balance state.
- **Xsolla vs. alternatives** (M5): the brief names Xsolla ("Zsolla"); if merchant-of-record terms disappoint, Stripe + tax handling is the fallback with materially more compliance surface. Decision needed before M5 design review.

## 20. Addendum: the forge does not belong in the game server

Section 6 sketched a `texture-forge` worker consuming the job queue and doing
the pixel work — decode, seam measurement, **LaMa repair**, ladder resizing.
Investigating what it would take to wire that up produced a different answer,
recorded here so the question is settled with evidence rather than re-litigated.

**The server must not shell out to `forge.py`.** There is no precedent for it:
the only `process::Command` anywhere outside a test harness is in
`server/tests/executor_process_chaos_tests.rs`, which spawns the server binary
in order to kill it. And the runtime image cannot run it anyway —
`server/Dockerfile`'s runtime stage is `ubuntu:24.04` with ca-certificates,
libssl3 and curl. `forge.py` needs Python, numpy and Pillow, and its repair
path imports `simple_lama_inpainting`, which brings PyTorch, opencv, and a
learned model. That is hundreds of megabytes and a model load *inside the
process running authoritative game partitions*, added to every region's image
to support a feature that is off wherever no image-provider key is configured.

**A full Rust port is the wrong framing too**, because the two halves are not
equally portable. The *measurement* half — seam ratio, correlation length, the
multi-scale structural check — is array arithmetic, and the repo already treats
those numbers as a shared contract: `forge.py`'s ladders are asserted against
`server/src/texture.rs` rather than trusted, and `SeamReport::ACCEPTABLE_RATIO`
is already the Rust copy of the Python threshold. The *repair* half is a neural
model, is not portable, and — decisively — **is not the gate**. `forge.py` says
so itself: a structurally disagreeing source is regenerated, never
force-repaired, because inpainting a join between two pieces of texture that
were never meant to meet produces mush, and mush passes a one-pixel metric.

**So the shape is**: port measurement to Rust, drop repair from the server path
entirely, and let a failed seam become a `SeamsRejected` fed back through
`retry_prompt` — which the generation module's own documentation already frames
as the local tooling's philosophy. `forge.py` stays what it is: the offline
tool for hand-authored art, where a human is present to judge a repair. The
cost is honest and priced: without repair, a borderline generation that the
model would have rescued now spends another provider call, and
`Budget::max_attempts_per_texture` already bounds that.

If the repair ever proves load-bearing for acceptance rates, the correct shape
is a **separate service** — its own image, called over HTTP with a timeout and
a size cap, exactly as `generation_providers.rs` already calls image models.
Never a subprocess inside the serving container.

### 20.1 What this investigation fixed on the way

Three bugs in the existing pipeline, none reachable today because nothing
drains the queue, all of them landmines for whoever builds the worker. Each is
now fixed with a test:

- **The daily spend total read only the first page.** It is the circuit breaker
  on a bill, and it stopped halting exactly when the spend was highest.
- **A job's lifetime was written only at creation**, while an update replaces
  the whole item — so the first progress write made the record permanent.
- **The ledger ignored the spend a job had already persisted**, so a job
  re-claimed after a crash got a fresh full budget, turning a per-job ceiling
  into a per-attempt-of-the-job one.

### 20.2 What a worker still needs before it can be written

Recorded so the next attempt starts from the real list rather than rediscovering
it: **a claim lease**. `claim_generation_job` removes the queue index entry and
sets `generating` with no expiry, so a worker that dies mid-job strands that job
invisibly forever and the client polls a state that will never change. A lease
plus a reaper — or a claim condition that also accepts a stale `generating` —
is a prerequisite, not a refinement.
