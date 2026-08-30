//! Pluggable snake skins.
//!
//! A skin owns the pixels of one snake's body plus two pieces of optional world
//! dressing (the team base and the goal celebration). Everything else —
//! geometry, occlusion, layering, and the viewer-relative friend/foe decision —
//! stays in the generic renderer, so a skin can restyle the game without being
//! able to misreport it.
//!
//! The load-bearing rule is that [`SnakeSkin::colors`] is a pure function of
//! [`SkinIdentity`]. Every viewer-dependent question ("is this snake mine?",
//! "am I spectating?") is answered by the renderer before a skin is consulted,
//! so no skin can make an opponent look like a teammate.
//!
//! See `specs/skins-prd.md` for the boundary rulings this module implements.

pub mod atlas;
pub mod base_skin;
pub mod checker;
pub mod classic;
pub mod composite;
pub mod corpse;
pub mod doc;
pub mod docv2;
pub mod ember;
pub mod fixtures;
pub mod geometry;
#[cfg(test)]
pub mod goldens;
pub mod layer;
pub mod paint;
pub mod perf;
pub mod registry;
pub mod space;

#[cfg(test)]
mod conformance;

pub use classic::ClassicSkin;
pub use paint::PaintCtx;
pub use registry::skin_registry;

use wasm_bindgen::prelude::*;

/// A snake's role from the viewer's point of view.
///
/// Computed only by the renderer. The variants are already fully resolved —
/// there is no "…but only if the viewer is spectating" left in them — which is
/// what lets a skin be a plain lookup table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnakeRole {
    /// The viewer's own snake.
    Own,
    /// A snake on the viewer's team.
    Teammate,
    /// A snake the viewer is playing against.
    Enemy,
    /// A team seen by someone with no snake in the match. Team 0 is the
    /// canonical blue side, everything else reads as the red side.
    SpectatedTeam(u8),
    /// A free-for-all snake, in a paint slot the renderer has already resolved
    /// (which is why slot 0 — the blue slot — can only ever reach a spectator).
    FreeForAll { palette_slot: u8 },
}

/// Everything a skin is allowed to know about who it is painting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkinIdentity {
    pub role: SnakeRole,
    /// Distinguishes players within one team so a 2v2 roster can map each
    /// player to a snake. Always 0 outside team games.
    pub shade_slot: u8,
}

impl SkinIdentity {
    /// Resolve a snake's role from the raw arena facts.
    ///
    /// The branch order here is deliberately identical to the palette logic it
    /// replaced, including combinations the engine cannot actually produce (a
    /// snake with no team inside a team game, say). The exhaustive palette
    /// table in the golden trace covers those too, and matching it exactly is
    /// cheaper than reasoning about which ones are reachable.
    pub fn resolve(
        snake_index: usize,
        snake_team: Option<u8>,
        team_member_slot: usize,
        snake_count: usize,
        is_team_game: bool,
        local_snake_id: Option<usize>,
        local_team: Option<u8>,
    ) -> Self {
        if is_team_game {
            let shade_slot = (team_member_slot % 2) as u8;
            let role = match (local_team, snake_team) {
                (Some(ours), Some(theirs)) if ours == theirs => SnakeRole::Teammate,
                (Some(_), Some(_)) => SnakeRole::Enemy,
                (None, Some(team)) => SnakeRole::SpectatedTeam(team),
                _ if Some(snake_index) == local_snake_id => SnakeRole::Own,
                _ => SnakeRole::Enemy,
            };
            return Self { role, shade_slot };
        }

        if Some(snake_index) == local_snake_id {
            return Self {
                role: SnakeRole::Own,
                shade_slot: 0,
            };
        }

        // A two-snake game is a duel: the other snake is the opponent, and a
        // spectator sees both as opponents rather than picking a side.
        if snake_count == 2 {
            return Self {
                role: SnakeRole::Enemy,
                shade_slot: 0,
            };
        }

        // Free-for-all. Slot 0 is the friendly blue, so it is reserved for
        // spectators; a playing viewer's opponents land on the gold slot
        // instead of wearing the colour that means "you".
        let palette_slot = match snake_index % 4 {
            0 if local_snake_id.is_none() => 0,
            1 => 1,
            2 => 2,
            _ => 3,
        };
        Self {
            role: SnakeRole::FreeForAll { palette_slot },
            shade_slot: 0,
        }
    }
}

/// One snake, posed and ready to paint.
///
/// Deliberately carries no `GameState`: the roster's synthetic two-point body
/// and the tutorial's staged scenes go through exactly the same entry point as
/// a live arena snake.
#[derive(Clone, Copy, Debug)]
pub struct SnakePose<'a> {
    /// Compressed body, head first, in whole screen cells with rotation
    /// already applied.
    pub cells: &'a [(f64, f64)],
    pub cell_size: f64,
    pub boost_active: bool,
    /// A number that is stable for one snake and differs between snakes.
    ///
    /// The expression language has always offered `seed` for exactly this —
    /// "stable per snake, so two snakes wearing one skin can differ" — and
    /// until documents could reach the language, every caller passed zero and
    /// nothing noticed. Exposing expressions to authors makes that dishonest:
    /// an author who writes `seed` and gets a constant has been sold a
    /// capability that does not exist.
    ///
    /// Only cosmetics may read it, and only through a skin. It is presentation
    /// state exactly as `anim_ms` is: never in game state, events, snapshots,
    /// or the sync fingerprint. Static surfaces (roster glyphs, golden traces,
    /// contact sheets) pin it to zero so their output stays reproducible.
    pub seed: f64,
    /// Cosmetic animation clock in milliseconds.
    ///
    /// Presentation time only — it never enters game state, events, snapshots,
    /// or the sync fingerprint, and no gameplay code may read it. Static
    /// surfaces (roster glyphs, golden traces, contact sheets) pin it.
    pub anim_ms: f64,
    /// Mirrors the viewer's `prefers-reduced-motion` setting. An animated skin
    /// must paint a still frame when this is set.
    pub reduced_motion: bool,
    /// Multiplier on every contour weight a skin document quotes.
    ///
    /// A skin quotes its contours in pixels at 1x, and the arena canvas is not
    /// devicePixelRatio-scaled — so on a high-DPI display, or under the
    /// trailer capture at deviceScaleFactor 4, the arena draws a cell several
    /// times larger while a 2 px contour stays 2 px and reads as a scratch.
    /// The arena sets this from its cell size; every other surface leaves it
    /// at 1.
    ///
    /// It cannot be inferred from `cell_size` alone: the roster glyph sizes
    /// its cell to fill a row and legitimately reaches ~28 px at 1x, which is
    /// larger than anything the arena draws. Cell size does not distinguish a
    /// big glyph from a zoomed one; only the caller knows which it is.
    pub detail_scale: f64,
}

/// The cell the arena caps at, in CSS pixels (`GameArena.tsx` walks down from
/// here). Contour weights are quoted at this size.
pub const ARENA_MAX_CELL_PX: f64 = 15.0;

/// Contour multiplier for an arena cell. 1.0 at or below the cap, so 1x
/// rendering is untouched.
pub fn arena_detail_scale(cell_size: f64) -> f64 {
    (cell_size / ARENA_MAX_CELL_PX).max(1.0)
}

impl<'a> SnakePose<'a> {
    /// A pose for a surface that does not animate: roster glyphs, golden
    /// traces, contact sheets. All of them are 1x, so contours are unscaled.
    pub fn still(cells: &'a [(f64, f64)], cell_size: f64, boost_active: bool) -> Self {
        Self {
            cells,
            cell_size,
            boost_active,
            // A still surface has to be reproducible, and a per-snake seed is
            // the one input that would make two otherwise identical glyphs
            // differ. Pinned here rather than left to each caller.
            seed: 0.0,
            anim_ms: 0.0,
            reduced_motion: true,
            detail_scale: 1.0,
        }
    }
}

/// The colours generic chrome needs, in a form it can use without knowing
/// anything about how the skin paints.
///
/// Every field is a flat hex string even for gradient or animated skins,
/// because CSS swatches, contrast maths, and label ink all need one
/// representative colour they can reason about.
/// Borrowed from the skin, which resolved these at registration. Nothing here
/// is allocated per frame — the arena asks every snake for its colours on every
/// animation frame, so this has to be a lookup, not a build.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SkinColors<'a> {
    /// The representative body colour, and the contrast source for labels.
    pub fill: &'a str,
    pub outline: &'a str,
    /// Ink for the carried-food readout and the roster name.
    pub label: &'a str,
    /// One flat colour for DOM micro-surfaces that cannot render a skin.
    pub swatch: &'a str,
}

/// Which of a skin's reported colours carries the friend/foe reading.
///
/// Team games are played through colour, and the guarantee that survives every
/// skin is that a viewer can tell a teammate from an opponent. What *changes*
/// between skins is which part of the snake says so.
///
/// A painted skin says it with the body: classic's blue and red fills are the
/// whole cue. A skin whose body is a photographic coat cannot — a tiger is
/// orange whoever is wearing it, and tinting it cool for a teammate would make
/// it a striped blue snake rather than a tiger. Those skins keep the body
/// truthful and move the cue to the contour, which they widen to pay for it.
///
/// Declaring this is what keeps the rule enforceable rather than abandoned:
/// `skin_conformance_team_colours_stay_on_their_own_side` checks whichever
/// channel the skin nominates, and checks that the two sides are actually far
/// apart in it. A skin cannot opt out of carrying the cue — only choose where.
///
/// Nothing outside the test build reads it, and that is the intended shape: it
/// is a *claim a skin makes about itself* so the suite can hold it to the right
/// rule. Painting never consults it — the colours it describes are already in
/// [`SkinColors`], where the renderer and the DOM both find them.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SideCue {
    /// The body fill reads as the side. The default, and what every painted
    /// skin does.
    Body,
    /// The contour reads as the side, because the body is a coat that belongs
    /// to an animal rather than to a team.
    Contour,
}

/// What the renderer needs to know about a skin's shape to lay out around it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SkinMetrics {
    /// Fixed contour/signal paint beyond the body cells, per side, in live
    /// pixels. Raster bleed aprons are reported separately because they scale with
    /// the live cell size; consumers use [`Self::visible_overhang_px`] to
    /// combine both components.
    pub overhang_px: f64,
    /// Transverse image bleed per side around the unchanged 16×16 logical
    /// body cell. Unlike `overhang_px`, this scales with a live cell until the
    /// declared authored-pixel cap is reached.
    pub raster_overhang_px: u32,
    /// Where the head's core ends, as a fraction of one cell. Label anchoring
    /// uses it to stay clear of the head.
    pub head_core_radius_ratio: f64,
    /// Whether the head core is dark enough for the roster's white ready-check
    /// to read against it.
    pub head_core_is_dark: bool,
}

impl SkinMetrics {
    /// Furthest live paint distance beyond the body, per side.
    pub fn visible_overhang_px(self, cell_size: f64) -> f64 {
        self.overhang_px
            .max(space::raster_overhang_cells(cell_size, self.raster_overhang_px) * cell_size)
    }
}

/// Viewer-attributed dressing for the team bases.
///
/// Colours only: the renderer keeps zone geometry, rotation, wall thickness,
/// and text layout, and decides which side is friendly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseTheme<'a> {
    pub friendly_zone: &'a str,
    pub enemy_zone: &'a str,
    pub friendly_wall: &'a str,
    pub enemy_wall: &'a str,
    pub friendly_text: &'a str,
    pub enemy_text: &'a str,
}

impl<'a> BaseTheme<'a> {
    /// Resolve the theme into the renderer's left/right screen order.
    ///
    /// Which side is friendly is the renderer's call, never the skin's: the
    /// canonical arena puts team 0 on the left, so a team-1 viewer sees their
    /// own colours on the right. A spectator gets the canonical arrangement.
    pub fn sides(&self, local_team: Option<u8>) -> BaseSides<'a> {
        let flipped = local_team == Some(1);
        BaseSides {
            left_zone: if flipped {
                self.enemy_zone
            } else {
                self.friendly_zone
            },
            right_zone: if flipped {
                self.friendly_zone
            } else {
                self.enemy_zone
            },
            left_wall: if flipped {
                self.enemy_wall
            } else {
                self.friendly_wall
            },
            right_wall: if flipped {
                self.friendly_wall
            } else {
                self.enemy_wall
            },
            left_text: if flipped {
                self.enemy_text
            } else {
                self.friendly_text
            },
            right_text: if flipped {
                self.friendly_text
            } else {
                self.enemy_text
            },
        }
    }
}

/// A [`BaseTheme`] arranged for the screen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseSides<'a> {
    pub left_zone: &'a str,
    pub right_zone: &'a str,
    pub left_wall: &'a str,
    pub right_wall: &'a str,
    pub left_text: &'a str,
    pub right_text: &'a str,
}

/// Scorer-attributed dressing for the goal celebration.
///
/// `effect` names a first-party renderer; a skin chooses which one plays and
/// what colour it is, never what code runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CelebrationTheme<'a> {
    pub effect: &'a str,
    pub friendly_accent: &'a str,
    pub enemy_accent: &'a str,
    pub readout_friendly: &'a str,
    pub readout_enemy: &'a str,
}

/// The contract every skin implements.
pub trait SnakeSkin: Send + Sync {
    /// A stable identifier, e.g. `classic@1`.
    fn id(&self) -> &str;

    /// Human-readable name for catalogues and QA surfaces.
    fn name(&self) -> &str;

    fn colors(&self, identity: &SkinIdentity) -> SkinColors<'_>;

    fn metrics(&self, boost_active: bool) -> SkinMetrics;

    /// Lazy image readiness scoped to this skin.
    ///
    /// Procedural skins keep the empty default. Composite skins override this
    /// with their own atlas so review/capture surfaces can prove one exact
    /// paint drew decoded pixels without observing unrelated skins.
    fn asset_status(&self) -> atlas::AssetStatus {
        atlas::AssetStatus::default()
    }

    /// Where this skin's friend/foe reading lives. See [`SideCue`] — including
    /// why only the conformance suite calls it.
    #[allow(dead_code)]
    fn side_cue(&self) -> SideCue {
        SideCue::Body
    }

    /// Paint one living snake.
    ///
    /// There is no mask parameter: erasing whatever sits behind the snake is
    /// the renderer's job for the logical body and its fixed contour, so a
    /// skin cannot paint the arena's background colour and a future non-white
    /// arena does not have to touch every skin. A raster bleed apron is not
    /// erased first: its RGBA pixels composite over the arena and earlier
    /// snakes in normal draw order.
    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), JsValue>;

    /// Paint one dead snake. The default is the shared gray corpse, and no
    /// skin overrides it today (ruling 5).
    fn paint_dead(&self, ctx: &mut PaintCtx, pose: &SnakePose) -> Result<(), JsValue> {
        corpse::paint_dead(ctx, pose.cells, pose.cell_size)
    }

    /// Base dressing. Colours only — the renderer keeps zone geometry, wall
    /// thickness, text layout, and the decision about which side is friendly.
    ///
    /// This is attributed to the *viewer*: your base theme is how your game
    /// looks, like a controller skin, not something opponents see.
    fn base_theme(&self) -> Option<BaseTheme<'_>> {
        None
    }

    /// Celebration dressing, attributed to whoever *scored* — this is the
    /// surface everyone watching sees, so it is the one worth showing off.
    /// The effect id names a first-party renderer; a skin picks which one
    /// plays and what colour it is, never what code runs.
    fn celebration_theme(&self) -> Option<CelebrationTheme<'_>> {
        None
    }
}

/// Paint one living snake the way every surface does: occlusion first, then the
/// skin.
///
/// `mask_color` is the colour of whatever the snake is covering — the arena
/// passes its white field so the grid dots vanish under the body; the roster,
/// which has nothing behind it, passes `None`.
///
/// A single-cell snake gets no occlusion pass at all. Its disc and outline
/// already cover the dot beneath it, and this is exactly how it has always
/// looked.
pub fn paint_alive_with_occlusion(
    ctx: &mut PaintCtx,
    skin: &dyn SnakeSkin,
    pose: &SnakePose,
    identity: &SkinIdentity,
    mask_color: Option<&str>,
) -> Result<(), JsValue> {
    if pose.cells.is_empty() {
        return Ok(());
    }

    if let Some(mask) = mask_color
        && pose.cells.len() > 1
    {
        let overhang = skin.metrics(pose.boost_active).overhang_px;
        let cell_size = pose.cell_size;
        ctx.set_fill(mask);

        for segment in geometry::segments(pose.cells) {
            let (min, max) = segment.span_edges(cell_size);
            let axis = segment.axis_edge(cell_size);
            if segment.vertical {
                ctx.fill_rect(
                    axis - overhang,
                    min - overhang,
                    cell_size + overhang * 2.0,
                    (max - min) + cell_size + overhang * 2.0,
                );
            } else {
                ctx.fill_rect(
                    min - overhang,
                    axis - overhang,
                    (max - min) + cell_size + overhang * 2.0,
                    cell_size + overhang * 2.0,
                );
            }
        }

        for cell in pose.cells {
            ctx.fill_rect(
                cell.0 * cell_size - overhang,
                cell.1 * cell_size - overhang,
                cell_size + overhang * 2.0,
                cell_size + overhang * 2.0,
            );
        }
    }

    skin.paint_alive(ctx, pose, identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RasterTestSkin;

    impl SnakeSkin for RasterTestSkin {
        fn id(&self) -> &str {
            "raster-test@1"
        }

        fn name(&self) -> &str {
            "Raster test"
        }

        fn colors(&self, _identity: &SkinIdentity) -> SkinColors<'_> {
            SkinColors {
                fill: "#f00",
                outline: "#f00",
                label: "#fff",
                swatch: "#f00",
            }
        }

        fn metrics(&self, _boost_active: bool) -> SkinMetrics {
            SkinMetrics {
                overhang_px: 1.0,
                raster_overhang_px: 4,
                head_core_radius_ratio: 0.38,
                head_core_is_dark: true,
            }
        }

        fn paint_alive(
            &self,
            _ctx: &mut PaintCtx,
            _pose: &SnakePose,
            _identity: &SkinIdentity,
        ) -> Result<(), JsValue> {
            Ok(())
        }
    }

    fn test_identity() -> SkinIdentity {
        SkinIdentity {
            role: SnakeRole::Own,
            shade_slot: 0,
        }
    }

    /// The one rule that keeps team games readable: a skin can restyle a role
    /// but never choose one.
    #[test]
    fn roles_resolve_without_consulting_any_skin() {
        let teammate = SkinIdentity::resolve(2, Some(0), 1, 4, true, Some(0), Some(0));
        assert_eq!(teammate.role, SnakeRole::Teammate);
        assert_eq!(teammate.shade_slot, 1);

        let enemy = SkinIdentity::resolve(1, Some(1), 0, 4, true, Some(0), Some(0));
        assert_eq!(enemy.role, SnakeRole::Enemy);

        let spectated = SkinIdentity::resolve(0, Some(1), 0, 4, true, None, None);
        assert_eq!(spectated.role, SnakeRole::SpectatedTeam(1));
    }

    #[test]
    fn raster_apron_does_not_expand_the_opaque_occlusion_mask() {
        use crate::skin::paint::{OpRecorder, PaintOp};

        let cells = [(0.0, 0.0), (4.0, 0.0)];
        let mut recorder = OpRecorder::new();
        paint_alive_with_occlusion(
            &mut PaintCtx::recording(&mut recorder),
            &RasterTestSkin,
            &SnakePose::still(&cells, 5.0, false),
            &test_identity(),
            Some("#fff"),
        )
        .unwrap();

        let rectangles = recorder
            .ops()
            .iter()
            .filter_map(|op| match op {
                PaintOp::FillRect(x, y, width, height) => Some((*x, *y, *width, *height)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rectangles,
            vec![
                (-1.0, -1.0, 27.0, 7.0),
                (-1.0, -1.0, 7.0, 7.0),
                (19.0, -1.0, 7.0, 7.0),
            ],
            "the logical body and fixed one-pixel contour remain masked"
        );
        assert!(
            !recorder.ops().iter().any(|op| matches!(
                op,
                PaintOp::BeginPath
                    | PaintOp::Arc(..)
                    | PaintOp::Ellipse(..)
                    | PaintOp::Rect(..)
                    | PaintOp::Fill
            )),
            "the RGBA raster apron must not receive an opaque shape fill before image paint"
        );
    }

    /// A playing viewer's free-for-all opponents must never land on the blue
    /// slot, because blue means "you" everywhere else in the game.
    #[test]
    fn free_for_all_blue_slot_is_reserved_for_spectators() {
        let spectating = SkinIdentity::resolve(4, None, 0, 4, false, None, None);
        assert_eq!(
            spectating.role,
            SnakeRole::FreeForAll { palette_slot: 0 },
            "index 4 is slot 0 for a spectator"
        );

        let playing = SkinIdentity::resolve(4, None, 0, 4, false, Some(1), None);
        assert_eq!(
            playing.role,
            SnakeRole::FreeForAll { palette_slot: 3 },
            "the same snake is gold when the viewer is playing"
        );
    }

    /// Both snakes in a spectated duel are opponents; nobody gets the home
    /// colour when there is no home.
    #[test]
    fn spectated_duel_gives_neither_snake_the_friendly_colour() {
        for index in 0..2 {
            let identity = SkinIdentity::resolve(index, None, 0, 2, false, None, None);
            assert_eq!(identity.role, SnakeRole::Enemy);
        }
    }
}

#[cfg(test)]
mod detail_scale_tests {
    use super::*;

    /// Contour weight is a fixed share of the body at any zoom, and 1x is
    /// untouched. Skins quote their contours in 1x pixels, and the arena canvas
    /// is not devicePixelRatio-scaled, so without this a 2 px rim stays 2 px
    /// around a 60 px body and reads as a scratch.
    #[test]
    fn arena_scales_above_its_cap_and_nowhere_below_it() {
        for cell in [4.0, 9.0, 15.0] {
            assert_eq!(
                arena_detail_scale(cell),
                1.0,
                "1x rendering must be byte-identical at cell {cell}"
            );
        }
        assert_eq!(arena_detail_scale(2.0 * ARENA_MAX_CELL_PX), 2.0);
        assert_eq!(arena_detail_scale(4.0 * ARENA_MAX_CELL_PX), 4.0);
    }

    /// The scale cannot be inferred from cell size: the roster glyph fills a
    /// row and legitimately reaches ~28 px at 1x, larger than anything the
    /// arena draws. Every non-arena surface must therefore opt out explicitly,
    /// which `still()` does — the golden traces are the regression barrier.
    #[test]
    fn still_poses_never_scale_however_large_their_cell() {
        for cell in [17.0, 28.0, 64.0] {
            let cells = [(0.0, 0.0), (3.0, 0.0)];
            assert_eq!(
                SnakePose::still(&cells, cell, false).detail_scale,
                1.0,
                "a 1x surface must not scale merely because its cell is large"
            );
        }
    }
}
