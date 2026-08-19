# GameAnalytics integration and operator self-exclusion

Snaketron reports gameplay to [GameAnalytics](https://gameanalytics.com) from
the ordinary web build only. This file is the release-owner record for how it
is configured, what it sends, and — the part that needs setting up once — how
an operator keeps their own play out of their own numbers.

## Configuring keys

The game key and secret come from the GameAnalytics dashboard
(**Game Settings → Game Keys**) and are compiled into the bundle by webpack's
`DefinePlugin`:

```bash
GAMEANALYTICS_GAME_KEY=<game key> \
GAMEANALYTICS_SECRET_KEY=<secret key> \
GAMEANALYTICS_BUILD=web-1.4.0 \
npm run build:prod
```

The game key is exactly 32 alphanumeric characters and the secret exactly 40.
The SDK refuses to initialize on any other shape — it logs and returns, with no
failed request to notice — so the build validates both shapes and fails loudly
rather than producing a release that looks correct and reports nothing. Both
keys are also required together. The
secret is not a credential in the usual sense — GameAnalytics' HTML5 SDK signs
payloads with it in the browser, so it is readable by anyone who opens the
bundle. Treat it as a public per-game identifier, not a secret to protect, and
rotate it in the dashboard if it is ever abused.

`GAMEANALYTICS_BUILD` is the version label attached to every event, so a
regression can be traced to the release that introduced it. It defaults to
`0.0.0`.

**A checkout with no keys never loads the SDK.** That is deliberate and is what
keeps developer machines, CI, and forks out of the live game's numbers with no
switch to remember. To exercise the integration locally, set the keys — ideally
against a separate GameAnalytics game, not the production one.

## Distribution scope

GameAnalytics ships in the `web` distribution only.

`CRAZYGAMES.md` records "Google Tag Manager/Google Analytics are absent from the
embedded package" as a packaging invariant, and the itch and CrazyGames ZIPs are
reviewed release artifacts. A third-party analytics SDK is the same class of
payload, so it follows the same rule, enforced in two places:

- the keys are blanked for any embedded build, so the integration is inert; and
- `gameanalytics` is aliased to an empty module, so webpack does not emit the
  ~93 KB vendor chunk into the ZIP at all.

An embedded package therefore contains no GameAnalytics SDK code, no
`api.gameanalytics.com` endpoint, and no keys. A ~2 KB inert wrapper and a
98-byte empty chunk remain; neither can reach a network.

If CrazyGames or itch later approve game-analytics SDKs, removing the
`isEmbeddedBuild` guards in `client/web/webpack.config.js` and
`ANALYTICS_SUPPORTED_DISTRIBUTION` in `client/web/services/analytics/config.ts`
is the whole change — but update the packaging invariant first.

## Keeping your own play out of the numbers

GameAnalytics counts an event the moment it arrives and offers no way to retract
it, so exclusion has to be decided *before* anything is sent. Three independent
signals do that, and **any one of them is enough**:

| Signal | Covers | Setup |
| --- | --- | --- |
| `?analytics=off` | Any browser, anywhere — phone, VPN, someone else's network | Visit the game once with the parameter |
| Excluded address list | Every browser and device on your network, signed out, with no per-device setup | Set `SNAKETRON_ANALYTICS_EXCLUDED_IPS` on the server |
| Administrator account | You, wherever you sign in | Already covered by `SNAKETRON_ADMIN_USER_IDS` |

An excluded browser never downloads the SDK chunk, never opens a GameAnalytics
session, and never contacts GameAnalytics at all.

### The URL switch

```text
https://snaketron.io/?analytics=off
```

The choice is stored in `localStorage` and applies to every later visit in that
browser, including deep links. `?analytics=on` clears it.

It cannot re-enable a session the deployment's address list or your
administrator account excludes — otherwise the switch would be a way to defeat
the exclusion you set up in the first place.

### The excluded-address list

Set on the server task (it is read per request, so no rebuild is needed):

```text
SNAKETRON_ANALYTICS_EXCLUDED_IPS=203.0.113.4,198.51.100.0/24,2001:db8::/32
```

Comma-separated IPv4/IPv6 literals and CIDR blocks. Unset excludes nobody. A
malformed entry is skipped without discarding the rest of the list, so one typo
cannot silently stop excluding the other addresses. IPv4-mapped IPv6 callers
match their plain IPv4 configuration, so you only list the address you know.

To find the address the server sees you as:

```bash
curl -s https://api.ipify.org
```

The client asks `GET /api/analytics/consent`, which resolves the caller from
`X-Forwarded-For` (leftmost entry, which is the real client behind the load
balancer) or `X-Real-IP`. That header is client-settable, which is harmless
here in a way it would not be for authorization: forging it can only remove the
forger's own events, never anyone else's.

The verdict is cached in `localStorage`. That matters — without it, every API
hiccup would quietly fold your own play back into the numbers. A player who has
never been excluded caches `counted` and is unaffected.

**Local development is already covered**, because a dev build has no keys.

### The administrator account

Any user in `SNAKETRON_ADMIN_USER_IDS` is excluded as soon as `/api/auth/me`
resolves. Because that can happen after the session has already opened, signing
in both stops the live SDK and writes the local opt-out, so the *next* load in
that browser is excluded before the SDK is ever fetched.

### Verifying

1. Open the site with the browser devtools network tab filtered to
   `gameanalytics`.
2. A counted session requests the `gameanalytics.<hash>.js` chunk and then
   `POST`s to `api.gameanalytics.com`.
3. An excluded session requests neither. `GET /api/analytics/consent` returns
   `{"excluded":true,"reason":"excludedAddress"}` when the address list matched.

## What is reported

### Player identity

The durable internal Snaketron user id is sent, so one player on a laptop and a
phone is one player in GameAnalytics and analytics can be joined against our own
tables. It reaches GameAnalytics two ways:

| Field | Value |
| --- | --- |
| `user_id` (primary) | The Snaketron user id when this browser already holds a session; otherwise GameAnalytics' own device id |
| `user_id_ext` | Always the Snaketron user id, from the moment it is known |

Two fields, because GameAnalytics only accepts a custom `user_id` *before*
`initialize` — and that is earlier than `/api/auth/me` can answer. The id is
therefore read synchronously at boot out of the `sub` claim of the session
token this browser already stores (`services/sessionIdentity.ts`). Its signature
is not verified: nothing is authorized by the value, and a browser tampering
with its own token can only mislabel its own events. Expired tokens are
rejected, because the session they describe is about to be replaced.

A brand-new visitor has no token yet, so their *first* session is keyed on the
device id and picks the user id up from the next load onward. `user_id_ext` is
populated in both cases — including when it duplicates `user_id` — so a query
against it never has to coalesce two fields.

Guests are reported under their own durable user id like anyone else; the
account dimension distinguishes them.

Usernames, emails, and CrazyGames identifiers are never sent.

**Operational consequence:** the user id is personal data held by a third party.
A deletion request under the procedure in `CRAZYGAMES_PRIVACY.md` now also means
deleting that id from GameAnalytics (their GDPR/data-deletion tooling), not only
from Snaketron's own records.

**Session dimensions**

| Dimension | Values |
| --- | --- |
| Custom 01 | `guest`, `registered` |
| Custom 02 | `keyboard`, `touch` |

**Progression events** — one per match played, opened on the same
"the player is actually controllable" signal the CrazyGames SDK uses, so a
match that was only spectated or abandoned in the lobby never opens one.

```text
Start    <mode>:<queue>
Complete <mode>:<queue>   score = the local player's score   (won)
Fail     <mode>:<queue>   score = the local player's score   (lost)
```

`<mode>` is `solo`, `duel`, `team-2v2`, `ffa`, or `custom-*`. `<queue>` is
`quickmatch`, `competitive`, or `custom` — custom games report their own queue
so private games do not inflate quickmatch volume.

**Design events**

```text
queue:request:<queue>:<mode>    one per mode selected, on intent
match:death:<cause>:<mode>      cause ∈ wall, out-of-bounds, enemy-base,
                                self, enemy-body, head-to-head, banked
match:duration:<mode>           value = match length in seconds
```

Death causes drop the killer's snake id: it is per-match and would make the
dimension unbounded without answering "how do players die".

**Error events** — only genuine client failures that are invisible in gameplay
funnels, currently a failed WASM initialization.

## Code map

| File | Role |
| --- | --- |
| `server/src/api/analytics.rs` | `GET /api/analytics/consent`; address matching |
| `client/web/services/sessionIdentity.ts` | Reads the user id from the session token, before init |
| `client/web/services/analytics/config.ts` | Build-time keys and distribution scope |
| `client/web/services/analytics/exclusion.ts` | The three-signal gate (pure decision) |
| `client/web/services/analytics/events.ts` | Game state → event taxonomy (pure) |
| `client/web/services/analytics/gameAnalytics.ts` | SDK transport, lazy load, pre-consent queue |
| `client/web/components/AnalyticsBridge.tsx` | React → session dimensions, operator exclusion |

Two properties are load-bearing and covered by tests in
`client/web/tests/unit/analytics*.test.ts`:

- **It cannot break the game.** Every SDK call is wrapped; a failure degrades to
  "analytics off" rather than retrying.
- **It cannot report an excluded session.** The SDK is imported only after the
  gate resolves, so an excluded browser has no SDK present to mis-wire.
