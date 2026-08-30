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
GAME_ANALYTICS_BUILD=web-1.4.0 npm run build:prod
```

`GAME_ANALYTICS_GAME_KEY` and `GAME_ANALYTICS_SECRET_KEY` are exported from the
release owner's shell profile, so an interactive build picks them up with no
further setup.

## Continuous integration and deployment

The keys must be stored on **both** repositories, because two different
repositories build a bundle from this checkout:

| Name | Kind | Why |
| --- | --- | --- |
| `GAME_ANALYTICS_GAME_KEY` | Repository **variable** | It ships inside the client bundle, so it is a public identifier, not a credential |
| `GAME_ANALYTICS_SECRET_KEY` | Repository **secret** | Also reaches the bundle, but keeping it out of a public repository's files and out of build logs costs nothing |

| Repository | Builds | Needs the keys because |
| --- | --- | --- |
| `lopatin/SnakeTron` | CI only, never deployed | The guard build below has to exercise a real key |
| `lopatin/snaketron-io` | **The bundle players actually load** | `release-production.yml` → `deploy-client.yml` is the only path to snaketron.io |

Storing them on the game repository alone is the failure this section exists to
prevent, and it is not hypothetical: the production client shipped for a full
release cycle with both keys compiled to the empty string, because CI had them
and the deploying repository did not. Nothing failed — that is the whole
problem, and it is what `GAME_ANALYTICS_REQUIRED` below now catches.

Neither is a credential in the usual sense: GameAnalytics' HTML5 SDK signs
payloads in the browser, so anyone can read both out of the served JavaScript.
Treat them as a rotatable per-game identifier pair. Rotating means updating the
value in the GameAnalytics dashboard and here — nothing else stores them.

`wasm-build` in `.github/workflows/github-action-test-simple-game.yml` consumes
both. It builds a production bundle that is never deployed, purely so two
failures cannot reach a release unnoticed: a broken webpack config, and a key
that no longer has the exact shape the SDK demands. A malformed key makes the
SDK decline to initialize with no failed request to notice, so this guard is
the only thing standing between a bad rotation and a silently dead release. A
second step asserts that `GAME_ANALYTICS_DISABLE_EMBEDDED=true` really does
strip the SDK from an embedded package, because that escape hatch is otherwise
only visible in the shape of a built artifact.

**A deployment workflow needs nothing beyond passing the two values to the
client build:**

```yaml
env:
  GAME_ANALYTICS_GAME_KEY: ${{ vars.GAME_ANALYTICS_GAME_KEY }}
  GAME_ANALYTICS_SECRET_KEY: ${{ secrets.GAME_ANALYTICS_SECRET_KEY }}
  GAME_ANALYTICS_BUILD: web-<short sha>   # 32 characters maximum
  GAME_ANALYTICS_REQUIRED: 'true'         # fail rather than ship inert
```

A **reusable** workflow needs one thing more: a repository secret is invisible
across a `workflow_call` boundary unless it is declared under
`on.workflow_call.secrets` *and* passed by the caller. Repository variables
need neither. Missing that declaration looks exactly like a missing key.

**No AWS or ECS configuration is required for the keys.** They are compile-time
client configuration baked into the bundle, and the server never talks to
GameAnalytics — see the section above on why server-side events stay in the
first-party pipeline. Nothing belongs in Secrets Manager or a task definition.

The one analytics setting that *is* server runtime configuration is the
operator exclusion list, which the ECS task reads per request:

```text
SNAKETRON_ANALYTICS_EXCLUDED_IPS=<your public IP or CIDR>
```

Unset excludes nobody, so production works without it; setting it is what keeps
the release owner's own play out of the numbers on every device at home,
including signed-out ones.

**`GAME_ANALYTICS_REQUIRED=true` makes a keyless build fail.** A checkout with
no keys is a supported state everywhere else — that is what keeps developer
machines and forks out of the numbers — but for a build that is going to be
distributed it is indistinguishable at runtime from a working release that
reports nothing. Both `deploy-client.yml` and `scripts/build-itch.sh` set it.

The game key is exactly 32 alphanumeric characters and the secret exactly 40.
The SDK refuses to initialize on any other shape — it logs and returns, with no
failed request to notice — so the build validates both shapes and fails loudly
rather than producing a release that looks correct and reports nothing. Both
keys are also required together. The
secret is not a credential in the usual sense — GameAnalytics' HTML5 SDK signs
payloads with it in the browser, so it is readable by anyone who opens the
bundle. Treat it as a public per-game identifier, not a secret to protect, and
rotate it in the dashboard if it is ever abused.

`GAME_ANALYTICS_BUILD` is the version label attached to every event, so a
regression can be traced to the release that introduced it. It defaults to
`0.0.0`, and is capped at **32 characters** by the SDK — which logs and keeps
its default beyond that rather than failing, so a full 40-character commit SHA
cannot be used as a label. The build validates the length. Deployments tag
releases `web-<short sha>`; the itch package uses `itch-<short sha>`.

**A checkout with no keys never loads the SDK.** That is deliberate and is what
keeps developer machines, CI, and forks out of the live game's numbers with no
switch to remember. To exercise the integration locally, set the keys — ideally
against a separate GameAnalytics game, not the production one.

## Distribution scope

GameAnalytics ships in **every** distribution by default — website, itch, and
CrazyGames. Both portals permit third-party game analytics; CrazyGames goes
further and publishes [ByteBrew](https://docs.crazygames.com/resources/partners/)
as an official analytics partner, and itch.io offers its own Google Analytics
integration. Portal traffic is the larger audience, so excluding it would hide
most of the players.

`CRAZYGAMES.md` still keeps Google Tag Manager and Google Analytics out of the
embedded packages. That invariant is about a general-purpose tag manager that
can inject arbitrary third-party script — a different risk from a game
analytics SDK — and it stands.

To take analytics back out of the reviewed release packages, for a portal whose
policy changes or a submission that must carry no third-party SDK:

```bash
GAME_ANALYTICS_DISABLE_EMBEDDED=true CRAZYGAMES_BUILD=true npm run build:prod
```

That blanks the keys *and* aliases `gameanalytics` to an empty module, because
dropping the keys alone is not enough: the dynamic import stays statically
reachable, so webpack would still emit the ~93 KB vendor chunk into the ZIP.
With the switch on, the package contains no SDK code, no
`api.gameanalytics.com` endpoint, and no keys — only a ~2 KB inert wrapper and
a 98-byte empty chunk, neither of which can reach a network.

The switch never affects the ordinary website build.

## Keeping your own play out of the numbers

GameAnalytics counts an event the moment it arrives and offers no way to retract
it, so exclusion has to be decided *before* anything is sent. Three independent
signals do that, and **any one of them is enough**:

| Signal | Covers | Setup |
| --- | --- | --- |
| `?analytics=off` | Any browser, anywhere — phone, VPN, someone else's network | Visit the game once with the parameter |
| Excluded address list | Every browser and device on your network, signed out, with no per-device setup | Set `SNAKETRON_ANALYTICS_EXCLUDED_IPS` on the server |
| Administrator account | You, wherever you sign in | Already covered once your account is an administrator |

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

Any administrator is excluded as soon as `/api/auth/me` resolves — whether the
grant is the durable `isAdmin` flag on the account or an entry in
`SNAKETRON_ADMIN_USER_IDS`. Because that can happen after the session has
already opened, signing
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
from Snaketron's own records. The player-facing notice at `/privacy` states
this, and is linked from the footer of every build.

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

**Ad events** — one per pre-match break, for every outcome. Fill rate is only
meaningful when the misses are counted, and a break that silently failed is
otherwise indistinguishable from one that never ran.

| Snaketron resolution | GameAnalytics |
| --- | --- |
| `completed` | Show |
| `unavailable` | FailedShow, `no_fill` |
| `error` | FailedShow, `internal_error` |
| `blocked` (ad blocker), `timed_out` | FailedShow, `unknown` |

Reported as an interstitial at placement `pre_match`, with the provider id as
the ad SDK name.

**Error events** — genuine client failures: a failed WASM initialization,
uncaught exceptions, and unhandled promise rejections. A crash is invisible in
every gameplay funnel, because the session simply stops producing events;
these are the only place the client can say why.

Deduplicated and capped at 10 distinct messages per page session. The SDK does
not rate-limit error events, so a render or reconnect loop raising the same
error every frame would otherwise spend the player's bandwidth and bury the
dashboard under one repeated line.

Death causes drop the killer's snake id: it is per-match and would make the
dimension unbounded without answering "how do players die".

**Error events** — only genuine client failures that are invisible in gameplay
funnels, currently a failed WASM initialization.

## What is deliberately not reported

**Resource events** are implemented for the Snakebux **sink**: buying a skin
sends `Sink / bux / <price> / skin / <reference>`. Only the `purchased`
outcome reports — a free skin and an already-owned one move no currency, and
counting them would flatten the sink/source comparison the economy report
exists to make.

The **source** side is not wired, and neither are **business events**, for one
shared reason worth understanding before either is added. See below.

## The open gap: reporting a completed payment

Snakebux are bought through Xsolla. Checkout opens Pay Station in a new tab,
the payment settles by **webhook to our server**, and the client learns about
it only by refetching its wallet — `WalletModal` says as much: there is no
moment in the browser that *is* the purchase completing.

That leaves three ways to report the business event, none free:

1. **From the client, on seeing the balance rise.** Fits GameAnalytics' model,
   attributes revenue to the real session — but the client never sees a
   purchase the player abandoned the tab on, and the trigger is a balance
   change, which a refund or an admin grant also produces.
2. **From the server webhook.** Authoritative, which revenue has to be. But
   every GameAnalytics event needs a `session_id`, and a server-synthesized one
   distorts DAU, session counts, and session length — the reasoning in the
   section above applies here too, just at lower volume.
3. **Server decides, client reports.** The wallet already returns recent
   ledger entries carrying `source: "xsolla"`, the provider's transaction id,
   and the delta. The client reports each once, inside its own real session,
   using the server's numbers.

Option 3 is the right shape and the recommendation. What it still needs is
**exactly-once reporting**: two devices opening the game after one purchase
would both see the same ledger entry and both report it, and doubled revenue
is worse than late revenue. Local dedupe narrows that window but does not
close it; a `reportedToAnalytics` marker on the ledger entry does, at the cost
of a write path and a schema field.

Until that is settled, GameAnalytics' monetization dashboards stay empty while
the ledger remains the source of truth for actual revenue — which is the safe
way round.

**Boost fuel and XP** remain unreported. Neither is a currency the player
chooses to spend against alternatives, which is what the sink/source metrics
compare; Snakebux is.

**Server-side events do not go to GameAnalytics, by design.** Snaketron already
has a first-party server analytics pipeline (`server/src/analytics/`) that
records session, connection, lobby, queue, game, and per-player result events
into Iceberg tables — strictly more detail than GameAnalytics would hold, and
queryable with SQL.

Bridging those into GameAnalytics would actively damage the metrics
GameAnalytics is good at. Its model is client-session-centric: every event
needs a `session_id`, and server-synthesized sessions would inflate session
counts, depress average session length, and distort DAU and retention — the
exact figures the integration exists to provide. The two systems are kept to
what each does best: GameAnalytics for player-behaviour dashboards, the
first-party lake for authoritative gameplay records.

If a server-side GameAnalytics stream is ever wanted anyway, point it at a
*separate* GameAnalytics game key so it cannot contaminate the player-facing
one, and expect to synthesize session annotations for every event.

## Code map

| File | Role |
| --- | --- |
| `server/src/api/analytics_consent.rs` | `GET /api/analytics/consent`; address matching (routed in `server/src/http_server.rs`) |
| `client/web/components/PrivacyPolicy.tsx` | The player-facing notice, incl. the analytics disclosure |
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
