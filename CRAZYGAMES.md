# CrazyGames release and account-integration runbook

Snaketron uses CrazyGames as an additive identity provider. A verified
CrazyGames identity maps to the same internal Snaketron user on every device;
XP, MMR, rankings, high scores, match history, and preferences remain
server-authoritative. The ordinary website and itch builds retain their
existing guest, email, and password behavior.

The release target is the CrazyGames **in-game account (full)** integration,
even if the first portal release is still classified as a Basic Launch. The
CrazyGames package does not use the Data Module for authoritative progress.

## Account and progress contract

| CrazyGames state | Snaketron behavior |
| --- | --- |
| Signed in, first visit | Verify a fresh CrazyGames JWT on the server, create a durable Snaketron user, and log in automatically. |
| Signed in, returning visit or another device | Verify a fresh JWT, resolve the immutable CrazyGames `userId`, restore the same internal user and all server progress, then log in automatically. |
| Signed out | Allow immediate guest play. Do not show the CrazyGames authentication prompt automatically. Keep guest preferences in tab-scoped browser storage. |
| Guest signs in during play | Detect the CrazyGames auth event, leave transient lobby/match state safely, reload, verify a fresh JWT, and ask with CrazyGames' standard account-link prompt only if the server confirms that this eligible guest could be promoted into a new CrazyGames identity. Promote atomically only after `yes`; `no` starts the new account without guest data. |
| Known CrazyGames identity signs in over a browser guest | Restore the known CrazyGames account without an unnecessary link prompt. Do not merge unrelated guest progression into it. |
| CrazyGames profile is renamed | Update the active account/lobby display name and avatar from newer verified claims while retaining the same internal user and progress. Historical leaderboard rows remain immutable snapshots. |
| Multiple CrazyGames accounts share a device | Resolve the current portal JWT on every launch; never select an account from a stale browser token. |

Only the server-verified JWT `userId` identifies a CrazyGames account. The
client-only `__dangerousUserId`, username, and avatar must never authorize an
account. CrazyGames JWTs are short-lived exchange credentials and must not be
persisted or logged.

For verified CrazyGames users, Snaketron stores tutorial, lobby, and input
preferences on its own backend alongside server-owned progression. For signed-
out guests those preferences remain local. Local settings are imported only
when the same tab proves ownership of an eligible Snaketron guest session and
the player accepts CrazyGames' standard account-link prompt. Declining does not
attach the guest or its settings. A new provider account never inherits
unscoped settings from a shared browser.
The CrazyGames Data Module is not a dependency and is disabled in the standard
package.

An existing Snaketron email/password account and a CrazyGames identity are
intentionally separate accounts. This release does not guess that they belong
to the same person or automatically merge/link their progression. Players use
their portal-linked account inside CrazyGames and their password account on the
ordinary website; any future cross-provider linking needs an explicit,
authenticated account-linking design.

## Isolation from other builds

These are release invariants:

- `CRAZYGAMES_BUILD` is compile-time isolation for the CrazyGames UI and SDK.
- Website and itch clients never initialize the CrazyGames SDK or call the
  CrazyGames token-exchange endpoint.
- `/login`, `/register`, `/guest`, password-account upgrades, and their token
  storage remain unchanged outside a CrazyGames build.
- CrazyGames identities do not enter the password username index and do not
  expose email/password or other external login controls in the portal build.
- An unavailable CrazyGames public-key service can fail the CrazyGames exchange
  without affecting health checks, normal authentication, matchmaking, or itch.
- The expected portal `gameId` is explicit deployment configuration and not a
  secret; release workflows pin the reviewed value rather than accepting an
  ambient runtime default.

## Other SDK behavior retained

The account work does not remove the previously integrated portal behavior:

| Area | CrazyGames behavior |
| --- | --- |
| SDK/bootstrap | Initialize the HTML5 v3 SDK before React; treat `local`, `crazygames`, and `disabled` environments explicitly. |
| Loading/gameplay | Report loading around WASM/match loading and gameplay start/stop around controllable play, menus, countdowns, disconnects, and completion. |
| Rooms/friends | Mirror the real server lobby through `updateRoom`/`leftRoom`, invite parameters, invite links, live joins, and Instant Multiplayer. Keep a party together across rounds and report queued/in-progress rooms as non-joinable. |
| Chat/audio | Honor live `disableChat` and `muteAudio` settings; keep server profanity filtering in place. |
| Ads | Keep ads disabled for Basic Launch. A later ad-enabled package must use only CrazyGames SDK ads and pass filled, unfilled, error, and ad-block tests. |
| Optional APIs | Do not expose IAP or native leaderboard UI until CrazyGames grants and configures those capabilities. |

## Backend deployment configuration

The ECS task receives these variables only when account exchange is explicitly
enabled:

```text
SNAKETRON_CRAZYGAMES_AUTH_ENABLED=true
SNAKETRON_CRAZYGAMES_GAME_ID=60112
```

CDK exposes them through two matching contexts. Enabling authentication without
a valid game ID fails synthesis; omitting both leaves the task definition free
of CrazyGames account configuration.

The Snaketron game ID was verified from the CrazyGames QA wrapper metadata as
`60112`. The development and production workflows pin that exact string. Do
not substitute a developer ID, user ID, game slug, or the example ID from
CrazyGames documentation.

Development synthesis/deployment adds:

```bash
CRAZYGAMES_GAME_ID=60112

--context crazyGamesAuthEnabled=true \
--context crazyGamesGameId="$CRAZYGAMES_GAME_ID"
```

Production `cdk diff` and `cdk deploy` must add the same two contexts to their
existing `environment=production` and immutable `imageTag` contexts. The
reviewed non-secret ID is pinned as `CRAZYGAMES_GAME_ID: '60112'` in both
workflow files; do not replace it with an ambient repository/environment value
or secret. Both production regions must receive that same pinned value.

After deployment, inspect the active ECS task definition and confirm that the
Snaketron container has exactly these two names, that authentication is `true`,
and that the game ID exactly matches Preview. Then exercise the exchange route
with a real Preview token; a malformed or wrong-game token must be rejected.

The backend contract is:

| Route | Authentication | Purpose |
| --- | --- | --- |
| `POST /api/auth/crazygames/exchange` | CrazyGames JWT in the request; optional existing Snaketron bearer token is considered only as an eligible guest-promotion candidate | Verify the provider identity; perform a read-only `check`, an explicitly approved `allow`, or a non-linking `decline`; atomically create/restore/promote the internal user; return a normal Snaketron session and preferences |
| `PUT /api/auth/crazygames/preferences` | Linked Snaketron bearer token | Validate and automatically persist the linked user's CrazyGames-build preferences |

The exchange request's `guestPromotion` field defaults to `decline`; an absent
decision can never claim a guest. `check` returns `409` with the stable code
`guestLinkConsentRequired` only when the verified identity is new and the
attached bearer resolves to an eligible guest, and it writes nothing. The
client then uses CrazyGames' standard prompt and retries with `allow` or
`decline`. Initial guest preferences are considered only with `allow`.

The routes remain installed while the feature is disabled and return a bounded
`503` response. Verification uses only RS256, the fixed official public-key
URL, 2-second connect/5-second total fetch timeouts, a 32 KiB response limit, a
15-minute in-memory cache, single-flight fetches, one rotation check after an
invalid signature, a 30-second forced-refresh cooldown, and a 5-second failed-
fetch backoff. The cooldown prevents forged JWTs from causing one outbound key
request each while limiting real rotation delay. Do not add the key URL or
cache settings as user-controlled deployment inputs.

The current DynamoDB item contract is:

```text
IDENTITY#CRAZYGAMES#<sha256 userId> / META
USER#<internal user id> / META
USER#<internal user id> / PREFERENCES#CRAZYGAMES
```

The user metadata identifies `authProvider=crazygames` and holds verified
profile metadata; the preferences record carries a schema/version and update
time. Identity creation and guest promotion must remain transaction-owned so
concurrent first launches cannot create duplicate accounts.

## Build the portal package

From the outer repository root, after backend and frontend tests pass:

```bash
./scripts/build-crazygames.sh
```

The default release flags are:

```text
ITCH_BUILD=false
CRAZYGAMES_BUILD=true
CRAZYGAMES_ADS_ENABLED=false
CRAZYGAMES_DATA_ENABLED=false
```

`CRAZYGAMES_DATA_ENABLED=true` is an explicit exceptional build. Do not use it
unless a later feature has a separately documented Data Module contract and the
matching portal toggle is enabled. Account identity, XP, MMR, results, and
preferences must never depend on that flag.

The build produces three reviewable artifacts:

```text
dist-crazygames/snaketron-crazygames.zip
dist-crazygames/snaketron-crazygames.zip.sha256
dist-crazygames/snaketron-crazygames-build.txt
```

The script fails unless all of these package checks pass:

- the ZIP is structurally valid and has `index.html` at its root;
- ZIP paths contain no traversal, absolute entries, `.DS_Store`, or `__MACOSX`;
- `index.html` uses a relative base and no root-relative bundled asset URLs;
- every local asset referenced by `index.html` exists;
- the CrazyGames HTML5 v3 SDK loads before the local Snaketron bundle;
- Google Tag Manager/Google Analytics are absent from the embedded package;
- the bundle contains at most 1,500 files and no more than exactly 250,000,000
  extracted bytes (the portal's decimal-byte ceiling);
- because this is currently one eager package, the script conservatively treats
  the entire extracted package as the initial bundle and limits both that total
  and the ZIP to 50,000,000 bytes;
- the compiled bundle contains the `gameplayStart` lifecycle marker as well as
  the CrazyGames token exchange, standard guest-link consent, and preference-
  save markers;
- the production API endpoint is compiled into the bundle (regional WebSocket
  endpoints are returned by the production region-discovery API); and
- a SHA-256 checksum, outer/submodule source commits, source-tree cleanliness,
  clean-build enforcement flag, build flags, endpoint values, file count,
  exact byte sizes, and build-tool versions are recorded beside the ZIP.

Before upload, independently verify the recorded checksum:

```bash
cd dist-crazygames
shasum -a 256 -c snaketron-crazygames.zip.sha256
unzip -tq snaketron-crazygames.zip
```

Drag **`dist-crazygames/snaketron-crazygames.zip`** into the CrazyGames
Developer Portal Preview upload area. Keep the checksum and build report as the
submission record; do not upload those two sidecar files. A report that records
either source tree as dirty is suitable for local testing only—rebuild from the
exact committed source used by the backend deployment before submission.
Set `RELEASE_REQUIRE_CLEAN=true` to make either dirty state a build-stopping
error; the default remains `false` so local review packages can still be made.

## Developer Portal settings

Use these values for the account-integrated package:

- Progress Save: **Yes, linked to a game account on the game's backend, which
  is associated with the CrazyGames User**
- CrazyGames Data Module: **disabled**
- External/email login in the CrazyGames build: **disabled**
- CrazyGames account login: user-initiated secondary action; never an automatic
  prompt or gameplay gate
- Ads for the initial Basic Launch: **disabled**
- Lobby sizes: **1, 2, 4**
- Multiplayer: **enabled / Online with Friends**
- Chat: **enabled**, with the SDK `disableChat` preference honored live
- Orientation: **landscape**, responsive HTML5 iframe

The backend-linked Progress Save choice and the Data Module choice are mutually
exclusive descriptions of Snaketron's authoritative progress path. Enabling a
Data Module preference experiment later does not change the fact that account
progress is linked through Snaketron's backend.

## Rollout order

1. Configure the verified portal game ID `60112` and both CDK contexts in the
   development certification workflow.
2. Deploy the backend exchange and persistence support before uploading a
   client that depends on it.
3. Verify forged, expired, wrong-algorithm, and wrong-game JWT rejection; key
   fetch/cache/rotation; concurrent first login; and same-user restoration on
   two devices.
4. Verify guest play, guest-to-new-account promotion, known-account restoration,
   portal profile rename, in-session login/reload, and two accounts sharing one
   browser in CrazyGames Preview.
5. Verify XP, both MMR values, rankings, high scores, match history, and
   preferences survive reload and a second device.
6. Run website and itch authentication regressions and confirm neither build
   calls the CrazyGames exchange route.
7. Keep the same pinned game ID contexts in both production CDK diff and
   deploy, deploy the certified immutable server image, and verify the active
   task definitions in both regions.
8. Build the ZIP from that same reviewed source commit, verify its checksum,
   upload it to Preview, and rerun the account matrix against production.
9. Confirm the in-game `#/privacy` notice and monitored deletion contact,
   select backend-linked Progress Save, keep Data Module off, and submit after
   the manual checks below are complete.

Do not disable backend exchange after account-linked users have launched except
for an emergency. That makes returning players appear to lose progress. A
client rollback should use the prior known-good account-linked package; identity
mappings and progression records should be retained.

## Manual release checks

- Load Preview at every advertised iframe size and verify no horizontal
  scrollbar, clipped controls, custom fullscreen prompt, or broken back action.
- Play Solo, 1v1, 2v2, and FFA; verify keyboard, touch, countdown, reconnect,
  game-over, menu, and Play Again behavior.
- Test `disableChat` and `muteAudio`, copied invite links, friends joining,
  full/queued lobbies, Instant Multiplayer, and continuing as one party across
  rounds.
- Confirm signed-out play has no automatic login popup. Trigger login manually
  and confirm the account resolves before matchmaking, invites, or WebSocket
  identity-sensitive actions resume. For a new identity with an eligible guest,
  exercise both answers to the standard account-link prompt: `yes` preserves
  guest progress and settings, while `no` leaves them unattached. Confirm a
  known CrazyGames identity does not receive an unnecessary link prompt.
- In two clean browser profiles signed into the same CrazyGames account, confirm
  the same Snaketron identity, progression, and preferences. Then switch one
  browser to another CrazyGames account and confirm no old identity or lobby
  state leaks.
- Inspect the browser console/network log for SDK, CORS, mixed-content,
  WebSocket, missing-asset, and third-party-tracker failures.
- Confirm the portal displays the correct username/avatar and the privacy notice
  is reachable from the game without blocking guest play.

## Privacy and operations

The data inventory, player-facing notice, retention/deletion checklist, and
incident guidance live in [CRAZYGAMES_PRIVACY.md](./CRAZYGAMES_PRIVACY.md). The
CrazyGames-only footer links to the packaged `#/privacy` page, which lists the
monitored contact `alerts@snaketron.io`; neither is shown in ordinary web/itch
builds or blocks guest play.

## Authoritative CrazyGames references

- <https://docs.crazygames.com/requirements/account-integration/>
- <https://docs.crazygames.com/sdk/user/>
- <https://docs.crazygames.com/sdk/user-linking/user-linking-html5-v3/>
- <https://docs.crazygames.com/sdk/data/>
- <https://docs.crazygames.com/sdk/game/>
- <https://docs.crazygames.com/requirements/multiplayer/>
- <https://docs.crazygames.com/requirements/technical/>
- <https://docs.crazygames.com/requirements/ads/>
