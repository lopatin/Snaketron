# Base skins

**Status:** implemented.
**Amends:** `specs/skins-prd.md` sections 1, 3.1, 5.6, 7 (rulings 10 and 11 and
their table row), 12 step 7, 13, 17 risk 8; `specs/first-class-skins-prd.md`
sections 1, 3.2, 5.2. Those documents rule base dressing viewer-attributed and
explicitly reject attributing it to the base's owner. This document overturns
that for a new kind of base and explains what replaces the reasoning behind it.

## 1. What a base skin is

A **base skin** is two pictures and a text colour.

- a **home** picture, painted on the end of the arena the viewer is defending;
- an **away** picture, painted on the end they are attacking;
- one **text colour**, which the players' names on that endzone are written in.

Each picture is an illustrated banner — a scene, a character, a vista, with a
subject in it — and its palette is whatever the subject wants. **One banner
spans the whole endzone**: it is not tiled, not repeated, and there is exactly
one copy of it on screen.

That is the whole authored surface. There is no geometry, no layout, no font,
no rotation handling and no side classification in it — all of that stays in
the renderer, exactly as section 7 ruling 10 already required.

It is a different entity from a `BaseTheme`, which is the six hex colours a
*snake* skin carries and which still exists unchanged. A base skin is painted
**on top of** the theme, inside the same rectangle. The theme is therefore also
the fallback: an endzone whose picture has not decoded, or whose picture 404s,
is the endzone exactly as it looked before base skins existed.

## 2. What changed, and why

### 2.1 Base skins travel

`specs/skins-prd.md` section 5.6 rules that "other players do not see your base
theme — base dressing is how *your* game looks, like a controller theme, not a
flex." A base skin is the opposite: it is chosen by the team that owns an
endzone, published in `GameState.team_bases`, and seen by everybody in the
match, including spectators.

That is a product decision, taken deliberately. Section 17 risk 8 anticipated
it — "product decisions a playtest could overturn — revisiting them changes
plumbing, not architecture" — and it is the plumbing that changed, not the
architecture: one new cosmetic map beside `skins`, resolved once at match
preparation and never again.

The colour themes are **not** affected. An account still holding a
`base:<snake ref>` from before this change keeps seeing exactly what it saw:
that value is viewer-local dressing and does not travel. Only a
`base:<base skin id>` reaches other players. The picker no longer offers the
themes, because thirty-six rows under one heading meaning two different things
is not a picker.

### 2.2 The two named reasons for rejecting owner-attribution

Section 5.6 rejected owner-attribution for two reasons. They are answered
separately, because only one of them was really about attribution.

**"A 2v2 team has two players with potentially different skins and no
deterministic owner."** This is dissolved by defining the owner (section 4).

**"A per-owner mix would break the friend-cool/foe-warm read the hue locks
exist to protect."** This one stands, and it is the serious one: running into
the *enemy* base kills you (`DeathCause::EnemyBase`) and running into your own
does not, so an endzone that reads as the wrong side is a competitive-integrity
bug, not a style complaint. Section 7 ruling 11 says so in as many words.

A picture cannot be held to a hue window the way six declared colours can —
the same argument `specs/skin-shading-prd.md` already makes about textured
snake bodies. Four answers were tried, in this order:

1. **Wash the viewer's own side colour over the picture** at a fixed alpha the
   art cannot defeat. Enforces the rule absolutely. Rejected: at any strength
   that reads as a side, every base looks the same, which removes the reason to
   pick one.
2. **Replace hue and chroma** with the side colour using a `color` composite
   pass, preserving the picture's luminance. Also absolute, and prettier — but
   it makes every base monochrome blue or red, with the same consequence.
3. **Author the distinction, and measure it.** Ship a home kit and an away kit
   and hold the shipped pixels to "away is warmer than home" at build time.
   This shipped first, and it worked. It also produced seventeen home kits that
   were seventeen shades of blue and seventeen away kits that were seventeen
   shades of red — the palette rule swallowed the theme, which is the whole
   thing a player is choosing. Rejected on sight of the result.
4. **Take the answer off the art entirely.** What ships.

### 2.3 Where the side read actually lives

Nothing about a base skin's art says which end is which, and nothing checks
that it does. Four renderer-owned cues carry it instead, none of which a skin
can reach:

- **the goal wall**, painted from the *viewer's* own `BaseTheme`, hue-locked
  cool/warm, drawn after every snake so it is on top of the whole frame;
- **the snakes**, whose friendly-cool/enemy-warm palettes are unchanged and
  still validated by the conformance suite;
- **the names**, written across each endzone, which say whose end it is
  literally;
- **where you spawn**, which is the first thing a match tells you.

The pair of pictures survives anyway, for a different reason: both teams can
equip the same skin, and each viewer sees the home picture at their own end and
the away one at the other, so a mirror match does not have two identical ends.
`the_two_ends_are_tellable_apart` decodes the committed PNGs in `cargo test` and
requires only that the pair is at least 0.10 apart in OKLab. It says nothing
about direction.

This is a deliberate loosening of section 7 ruling 11's *mechanism* while
keeping its *guarantee*: the ruling requires the side classification to be the
renderer's and never a skin's, and moving the answer entirely onto renderer-owned
cues honours that more literally than asking art to declare its own side did.

## 3. Delivery

Pictures are committed at `client/web/public/images/bases/<id>.{home,away}.v1.png`
and referenced by versioned relative URL, resolved against the page's
`<base href>` so embedded builds under a non-root path work. They go through
the existing `skin::atlas` image store, which brings the whole contract with it:

- **Lazy.** A picture is requested by the first frame that actually paints it,
  so a client downloads the home kit of one base and the away kit of another
  and never the two it will not show.
- **Never blocking.** Painting is synchronous, decoding is not. Until a picture
  decodes, the endzone shows the tint underneath.
- **Never failing.** A 404 is the same as a picture that has not arrived yet:
  the tint stays. Nothing about a cosmetic can fail a match.
- **Native builds never decode**, so every Rust test exercises the fallback,
  which is the path that has to stay correct.

A banner is laid along the endzone's **long** axis and cover-fitted across its
depth. The strip runs down the screen on a landscape display and across it on a
portrait one, so the renderer turns the banner a quarter turn for the first
case rather than squashing it — the art rides the arena's own rotation, the way
paint on a pitch does, while the players' names stay upright because text is the
one thing that has to be read rather than seen.

An endzone is a 4:1 strip and the image API's widest output is 3:2, so
cover-fitting keeps the banner's full length and crops away roughly its top and
bottom third. The generator composes for that: everything that matters lives in
a wide band across the middle, and the top and bottom of the frame are
atmosphere that can be lost.

### 3.1 What tiling cost, and why it went

The first two rounds tiled a square four times down each endzone, which brought
a wrap requirement with it: the art had to join to itself in **both** axes,
because the arena rotates. `build_base_textures.py` closed the join by offsetting
the tile half a turn and healing the resulting cross in the gradient domain —
measuring the colour step across the join, smoothing it along the join, and
spreading the correction outwards to zero, rather than cross-fading, which
ghosts every motif near the seam.

All of that worked. It was removed anyway, because you could see it working:
four identical dragons in a column read as wallpaper, not as a banner. The
wrap requirement was also quietly deforming the art — "keep the subject clear
of all four edges" pushes every composition into a corner, which is why the
first banners looked arranged rather than designed.

The art improved the moment it stopped having to join to itself. That is the
whole reason the seam machinery is gone rather than merely unused.

### 3.2 The two gates on a banner

Both are enforced twice — in `build_base_textures.py --check`, which is what an
author runs, and in `client/src/skin/base_skin.rs`, which is what CI runs over
the committed PNGs. Only the second one is automatic, which is why it exists.

**Value**, which is a gameplay constraint: mean WCAG luminance in 0.10–0.55,
with the brightest 2% reaching 0.30. Snakes are drawn on top of a banner and
players score inside it. Two numbers rather than one because a mean alone
passed a dragon cave that was black corner to corner.

**Chroma**, which is not: mean OKLCH chroma at least 0.070. Told to stay
mid-tone, a generator reaches for grey, and a whole batch came back — in the
words of the person who asked for them — "washed out blue" and "washed out
green", every one of which cleared the value band comfortably. Saturation is
free; nothing about a snake staying readable requires the background to be drab.

Grid dots stop at an endzone that is actually showing a picture. Over a flat
tint they read as the arena's own texture; over a picture they read as dirt on
it, and there are about two thousand of them.

## 4. Whose base is it

Resolved **once**, on the server, in `matchmaking::resolve_team_base`, at match
preparation, and written into `GameState.team_bases`. Never re-resolved: lobby
leadership migrates on every read, so "the leader's base" is only well defined
at a point in time, and a promotion two minutes into a match must not repaint an
endzone.

Every member of a team is ranked by:

1. **lobby leader first** — `member.user_id == lobby.requesting_user_id`, which
   is the effective leader because `QueueForMatch` is leader-gated and the queue
   entry is frozen at admission and fenced by its exact JSON;
2. then **ascending user id**, which is arbitrary but is the same arbitrary
   answer every time.

The team's base is the first base skin in that order. Concretely:

| Roster shape | Answer |
| --- | --- |
| 2v2 from one party | The party leader's, if they have one |
| Leader has none, teammate does | The teammate's — "preference if their skin is not default" |
| Nobody has one | No entry; the endzone is painted as it always was |
| Two solo lobbies on one team | Two leaders; lowest user id wins |
| Three-player party split across both teams | One team has no leader at all; lowest user id wins |
| Duel (`per_team: 1`) | That player's, trivially |
| Bots and guests | No account equipment, so they contribute nothing |
| Solo / FFA | No `team_zone_config`, so no endzones and nothing to resolve |
| Spectator | Sees both teams' bases, like every other viewer |
| Mid-match join, reconnect, recovery | Reads `team_bases` out of the snapshot like everything else |
| Rematch | Re-runs match preparation, so it re-resolves |

A legacy `base:<snake ref>` theme resolves to *no* contribution. Promoting one
would change what a player's existing choice means — silently showing opponents
a look they picked for their own screen.

Every failure degrades to "no base": an unreadable account, an unknown id, an id
from a newer build. `apply_player_skin`'s contract — cosmetics never block a
join — is unchanged, and the base slot is read from the same `User` row the
snake slot already fetched, so this costs no extra database round trips.

## 5. Wire and sync

`GameState.team_bases: HashMap<TeamId, String>`, `#[serde(default)]`, beside
`skins` and for the same reasons: it is cosmetic, it is keyed outside `Snake`,
and it is on the documented exclusion list in `common/src/fingerprint.rs`.
`a_teams_base_skin_never_changes_the_fingerprint` pins that.

**No protocol bump.** The field defaults, `GameState` does not deny unknown
fields, and no client has to understand it for a match to work — an older one
paints the endzone the old way. A hard cutover would disconnect every player
mid-match and invalidate every stored highlight clip, because
`GAMEPLAY_REPLAY_VERSION` gates playback on an exact match, to deliver a
cosmetic.

## 6. Deferred

- **Player-authored base skins.** The slot resolves through the built-in
  allowlist only. Authored bases would need the moderation reach first-class
  skins already have — and more of it, since a base is the *background of the
  arena* for opponents rather than one snake's body, which is a strictly larger
  exposure surface.
- **ScenarioCanvas / trailer capture** does not set `team_bases`; scenarios
  render the classic endzone.
- **Snake occlusion over art.** A snake in an endzone paints a one-to-three
  pixel field-coloured silhouette before its skin paints, which was invisible
  over a pale tint and is a thin bright outline over a picture. Left alone: it
  is pre-existing, and a field-coloured silhouette arguably helps a snake read
  over busy art.
