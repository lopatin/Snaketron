# CrazyGames privacy and data-operations record

This file is the release-owner record for Snaketron's CrazyGames account
integration. It supplies the source-of-truth copy and operational data map for
the packaged CrazyGames-only `#/privacy` page.

CrazyGames requires a Terms and Conditions and/or Privacy Policy notice when a
game collects personal data beyond SDK events. The notice should be visible to
new players without turning sign-in into a blocking popup:
<https://docs.crazygames.com/requirements/technical/#user-consent>

## Player-facing notice copy

The CrazyGames footer gives new players a concise notice and links to the
packaged `#/privacy` page containing the following substance:

> Snaketron uses your CrazyGames account ID, username, and profile picture to
> sign you in automatically and keep your game progress available across
> devices. We store your Snaketron profile, match results, XP, ratings, scores,
> and game preferences on Snaketron's servers. Your CrazyGames sign-in token is
> used only to verify your account and is not saved. Guests can play without
> signing in; guest preferences are stored only in the current browser tab.
> If an eligible guest later signs in to a new CrazyGames-linked account,
> CrazyGames asks permission before Snaketron attaches that guest's progress
> and settings. Declining leaves the guest data unattached. See our Privacy
> Policy for retention, deletion requests, and contact information.

Do not claim that CrazyGames itself stores Snaketron's XP, MMR, history, or
preferences. Those records are stored by Snaketron and linked to a verified
CrazyGames identity.

## Data inventory

| Data | Source | Purpose | Storage/handling |
| --- | --- | --- | --- |
| Immutable CrazyGames `userId` | Server-verified CrazyGames JWT | Stable account mapping and automatic login | Stored in the CrazyGames identity mapping; never accepted from `getUser()` or `__dangerousUserId` |
| CrazyGames username and profile-picture URL | Server-verified JWT | Portal-consistent player identity | Stored as mutable profile metadata and refreshed only from newer verified claims |
| CrazyGames JWT, `iat`, and `exp` | CrazyGames User module | One-time server authentication and freshness checks | JWT is held only long enough to exchange; never persist or log it. Minimal timestamps may be stored for profile update ordering |
| Internal Snaketron user ID | Snaketron backend | Own all gameplay records independent of provider details | Durable server record linked one-to-one with the verified CrazyGames identity |
| XP, MMR, rankings, high scores, results, and match ownership | Snaketron gameplay services | Progression, matchmaking, history, and competitive features | Durable, server-authoritative Snaketron records |
| Tutorial, lobby, and input preferences | Player actions | Restore experience across devices | Snaketron backend for linked users; tab-scoped browser storage for signed-out guests |
| Guest session and transient lobby/reconnect state | Snaketron client/server | Guest play and session continuity | Isolated from linked-account selection and cleared when identity changes require it |

Snaketron does not need a CrazyGames player's email address or password. Do not
add either to this flow. Do not put user IDs, usernames, JWTs, or profile URLs
in metric dimensions. Authentication logs should contain bounded result classes
such as `created`, `returning`, `guest_claimed`, `invalid_token`, `wrong_game`,
`mapping_conflict`, `key_refresh`, and `provider_unavailable`, not credentials or
raw provider identifiers.

## Retention and deletion operations

Before submission, the public policy must state a defensible retention period
or criterion for account mappings, profile metadata, progression, match
history, preferences, security logs, and backups. “Indefinitely” should not be
used accidentally simply because no cleanup job exists.

The public notice must provide one monitored deletion/contact route. A release
owner must record each request, authenticate it without requesting a password
Snaketron never collected, and identify every item keyed by either the internal
Snaketron user ID or the CrazyGames identity mapping.

Until an automated deletion flow exists, use this controlled procedure:

1. Authenticate the requester through an approved support process and capture
   the immutable CrazyGames identity only through a newly verified token or
   CrazyGames-supported account evidence.
2. Resolve the identity mapping to the internal Snaketron user ID without
   copying raw identifiers into tickets, chat, or metrics.
3. Enumerate the mapping, user profile, preferences, progression, rankings,
   high scores, match/history ownership, active sessions, and applicable
   backups before mutation.
4. Apply the public retention policy and any record-preservation obligation.
   Delete or anonymize all eligible records consistently; never delete only the
   identity mapping and leave an unreachable personal profile.
5. Revoke active Snaketron sessions, verify the mapping can no longer restore
   the account, and record completion without retaining the deleted raw ID.

This procedure is intentionally descriptive rather than a copy-paste database
command: deleting only part of the DynamoDB record graph can corrupt rankings,
history, or ownership. Add and test a transaction-aware deletion tool before
processing deletion directly in production.

## Security and incident checks

- Accept only the configured JWT algorithm and validate signature, expiry,
  issued-at time, claim types, and exact `gameId` on the server.
- Cache CrazyGames' public key only with bounded freshness and retry a
  single-flight refresh on signature failure, subject to the 30-second
  forced-refresh cooldown and 5-second failed-fetch backoff, so key rotation
  does not require a deployment and forged tokens cannot trigger unbounded
  outbound requests.
- Never log request bodies or authorization headers on the exchange route.
- Rate-limit exchange attempts independently from gameplay APIs.
- Alert on sustained invalid-token, wrong-game, mapping-conflict, public-key
  refresh, and provider-unavailable rates without adding personal identifiers.
- If a mapping collision or cross-account exposure is suspected, stop new
  exchanges, preserve audit evidence, keep existing progression records, and
  do not “repair” it by merging users automatically.

## Release values required before submission

Record these in the private release record, not in this repository:

- CrazyGames portal game ID `60112`, verified against production/QA wrapper
  metadata;
- packaged `#/privacy` route and its CrazyGames-only footer notice;
- monitored `alerts@snaketron.io` privacy/deletion contact and responsible owner;
- approved retention periods and backup behavior; and
- the uploaded ZIP's source commit and SHA-256 from the generated build report.
