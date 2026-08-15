# Advertising design

Snaketron treats advertising as a server-owned lobby policy with a replaceable
browser SDK boundary. A deployment declares which providers and placements are
available for each distribution; the live runtime record decides whether an
eligible lobby may enter a pre-match break. Browser code never independently
decides that an ad should run.

## Authority and provider routing

```mermaid
flowchart LR
    env["Deployment config<br/>capability ceiling"]
    runtime["Versioned runtime config<br/>live authorization"]
    session["Authenticated session<br/>web · CrazyGames · itch"]
    policy["Server ad policy"]
    wire["Session-specific<br/>AdConfiguration"]
    barrier["Redis lobby<br/>ad-break barrier"]
    cooldown["DynamoDB per-player<br/>cooldown claim"]
    factory["Browser provider factory"]
    cg["CrazyGames adapter"]
    future["Website H5 / future adapter"]
    none["Null adapter"]
    queue["Matchmaking admission"]

    env --> policy
    runtime --> policy
    session --> policy
    policy --> wire
    wire --> factory
    factory --> cg
    factory --> future
    factory --> none
    policy --> cooldown
    cooldown --> barrier
    cg --> barrier
    future --> barrier
    none --> barrier
    barrier --> queue
```

The deployment and runtime layers are both fail-closed. Runtime controls cannot
enable a provider or placement that the deployment did not make available, and
an unknown/missing SDK resolves through the null adapter without delaying the
lobby indefinitely.

## Lobby state and acknowledgement flow

```mermaid
sequenceDiagram
    participant H as Lobby host
    participant S as Server
    participant D as DynamoDB
    participant R as Redis barrier
    participant A as Ad-capable client
    participant N as No-ad / blocked client

    H->>S: Queue lobby
    S->>S: Read current runtime policy
    S->>S: Check every member's games played
    alt Ads disabled, newcomer present, or no capable target
        S->>S: Admit lobby directly
    else Lobby is eligible
        S->>D: Atomic cooldown claim for every targeted player
        alt Any target is inside the interval
            D-->>S: Claim rejected
            S->>S: Admit lobby directly
        else Claim succeeds
            S->>R: Begin roster-fenced ad break
            R-->>A: Ad-break snapshot
            R-->>N: Same barrier snapshot
            A->>A: Run distribution provider
            N->>N: Show neutral fallback
            A->>R: ACK completed / blocked / unavailable / error
            N->>R: ACK unavailable / blocked
            R->>S: All members resolved, or deadline reached
            S->>S: Atomically admit the same lobby generation
        end
    end
```

Every terminal provider result counts as resolved. Ad blocking is honored as a
normal result; the UI never asks a player to disable or uninstall anything.
Reconnects replay the authoritative barrier snapshot and an identity-bound
outbox resends the same acknowledgement until the server confirms it.

## Banner layouts

The generic layout supports one bottom placement on mobile and desktop plus two
desktop rails when the provider permits three concurrent banners. Provider
capabilities may reduce the selected set; for example, the CrazyGames adapter
caps itself at two concurrent banners. These captures use neutral sponsor-space
placeholders rather than simulated ad creative. The desktop rails are fixed
viewport overlays: they never create columns, narrow the app, or move its
controls. Only the bottom placement reserves layout space.

### Desktop: reserved bottom bar and floating rails

![Desktop banner layout](screenshots/ads/banner-layout-desktop.png)

### Mobile: bottom bar only

![Mobile banner layout](screenshots/ads/banner-layout-mobile.png)

## Pre-match fallback

Blocked, unavailable, no-fill, and integration-error outcomes use the same
neutral waiting surface. Matchmaking proceeds when every member has resolved or
the server deadline closes the barrier.

![Pre-match ad fallback](screenshots/ads/pre-match-fallback.png)

## Runtime administration

The controls from PR #66 are intentionally modeled as pre-match policy. The
minimum interval is enforced durably per targeted player, and every member of
the lobby must meet the minimum-games threshold.

![Pre-match advertising administration](screenshots/admin-page/configuration.jpg)
