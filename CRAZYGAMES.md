# CrazyGames integration

Snaketron's CrazyGames target is a deliberately fail-open HTML5 build. The
ordinary web build never loads or calls the CrazyGames SDK, and an unavailable
or disabled SDK never blocks the game.

## Build and preview

From the outer repository root:

```bash
./scripts/build-crazygames.sh
```

The upload artifact is `dist-crazygames/snaketron-crazygames.zip`. It contains
`index.html` at the zip root, relative asset URLs, the v3 SDK script before the
game bundle, and no third-party analytics script. The build script enforces the
CrazyGames 1,500-file and 250 MB uncompressed limits.

Portal settings for the pilot:

- Launch phase: **Basic Launch**
- Progress Save: **CrazyGames Data Module**
- Ads: disabled (the default `CRAZYGAMES_ADS_ENABLED=false` build flag)
- Lobby sizes: **1, 2, 4**
- Multiplayer: enabled / Online with Friends
- Chat: enabled, with the SDK `disableChat` switch honored dynamically
- Orientation: **landscape** (responsive HTML5 iframe)

For a later Full Launch ad build, set `CRAZYGAMES_ADS_ENABLED=true`. Do not turn
that flag on for Basic Launch.

## Implemented pilot surface

| Area | Basic Launch behavior |
| --- | --- |
| SDK/bootstrap | Initializes the HTML5 v3 SDK before React and treats `local`, `crazygames`, and `disabled` environments explicitly. |
| Loading/gameplay | Reports loading around WASM/match loading and gameplay start/stop around actual controllable play, menus, countdowns, disconnects, and match completion. |
| Game context | Sends non-sensitive match, mode, queue, and lobby context for actionable player feedback, then clears it on exit. |
| Account UX | Hides Snaketron email/external login in the CrazyGames build. Both CrazyGames guests and signed-in CrazyGames players enter as Snaketron guests; signed-in players' CrazyGames name/avatar are displayed. The CrazyGames auth prompt is only opened by an explicit user click. |
| Data | Uses the CrazyGames Data module for tutorial and gameplay/lobby preferences, with LocalStorage fallback. Tokens, reconnect state, MMR, XP, and server-authoritative state are never copied into SDK storage. |
| Rooms/friends | Mirrors the real server lobby into `updateRoom`, using the globally unique region-prefixed lobby code as its stable room ID, joinability, and invite parameters. It calls `leftRoom`, consumes initial and live room joins, creates an immediate private lobby for Instant Multiplayer, and uses the SDK invite link for copy-link UX. CrazyGames' native friends UI can join/invite from this room state. |
| Round continuity | Existing parties stay together after a match and one Play Again action queues the entire lobby. Per-member acknowledged match handoffs survive missed notifications and fast round cleanup, and play-to-play routes render immediately even in throttled portal tabs. A queued or in-progress room is reported non-joinable. |
| Chat/UGC | Dynamically hides and blocks client chat when `disableChat` is true. Lobby and game chat are also profanity-filtered on the server before publish/history storage. |
| Audio | Dynamically enforces `muteAudio` over current and newly-created HTML media and mutes while an ad is actually playing. |
| Ads | A complete midgame/rewarded/banner adapter is present. UI is blocked from request through finish/error and ad failures fail open. The only midgame seam is the natural post-match break. Basic Launch makes every ad request a no-op and exposes no ineffective rewarded-ad button. |
| Engagement | `happytime()` is reserved for a competitive win. Completion percentage is intentionally not reported because a replayable PvP match is not meaningful persistent game completion. |
| Unsupported/invite-only SDKs | IAP and native leaderboards are explicitly capability-disabled for the pilot; no misleading UI is exposed. |

## Full Launch follow-up

These items are intentionally not presented as complete in the pilot:

1. Verify CrazyGames JWTs server-side with the published RS256 key, key backend
   accounts by the immutable CrazyGames `userId`, auto-create/restore those
   accounts, and handle auth-listener account changes. Never trust
   `__dangerousUserId` from the browser.
2. Define a server-authoritative rewarded-ad benefit before adding any rewarded
   button. Keep the continue-without-ad action equally prominent.
3. Add spectator/late-join support if CrazyGames QA requires joining rooms while
   a round is in progress. The pilot accurately reports those rooms as closed.
4. Add native leaderboard and IAP adapters only after CrazyGames grants access;
   reconcile purchases and rewards on the server.
5. Revisit player progression persistence when CrazyGames identity is linked;
   MMR, XP, entitlements, and match results must remain server-authoritative.

## Manual QA checklist

- Load through the Developer Portal Preview at every advertised iframe size;
  verify no horizontal scrollbars, clipped controls, custom fullscreen prompt,
  or broken back navigation.
- Play Solo, 1v1, 2v2, and FFA; verify keyboard, touch, countdown, reconnect,
  game-over, menu, and play-again flows.
- Use `?disableChat=true` and confirm no chat UI/messages are available; remove
  it and confirm censored lobby/game chat still works.
- Use `?muteAudio=true` and confirm platform mute cannot be overridden.
- Test signed-out and signed-in CrazyGames states, the user-initiated login
  prompt, avatar/name rendering, and multi-tab/account-switch refresh behavior.
- Test copied invite links, the CrazyGames friends drawer, joining while already
  in another lobby, a full/queued lobby, and Instant Multiplayer launch.
- Keep Basic Launch ads disabled and verify post-match does not freeze. In the
  Full Launch sandbox, test filled, unfilled, error, and adblock paths.
- Check the browser console/network log for SDK, CORS, mixed-content, WebSocket,
  missing asset, and third-party tracker errors.

## Source requirements

- <https://docs.crazygames.com/sdk/intro/>
- <https://docs.crazygames.com/sdk/game/>
- <https://docs.crazygames.com/sdk/video-ads/>
- <https://docs.crazygames.com/sdk/banners/>
- <https://docs.crazygames.com/sdk/user/>
- <https://docs.crazygames.com/sdk/data/>
- <https://docs.crazygames.com/requirements/gameplay/>
- <https://docs.crazygames.com/requirements/account-integration/>
- <https://docs.crazygames.com/requirements/multiplayer/>
- <https://docs.crazygames.com/requirements/ads/>
- <https://docs.crazygames.com/requirements/technical/>
