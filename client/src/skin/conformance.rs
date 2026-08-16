//! Rules every skin has to satisfy, no matter who wrote it.
//!
//! Run the whole suite by name, never filtered to one skin:
//!
//! ```text
//! cargo test -p client skin_conformance
//! ```
//!
//! A filter that matches nothing exits successfully, so filtering by a skin's
//! name is a good way to believe a broken skin passed.

use crate::skin::doc::ParamSkin;
use crate::skin::fixtures::{ANIM_SAMPLES, CELL_SIZES, POSES};
use crate::skin::paint::{OpRecorder, PaintCtx};
use crate::skin::registry::skin_registry;
use crate::skin::{ClassicSkin, SideCue, SkinIdentity, SnakePose, SnakeRole, SnakeSkin};

/// Every role a skin can be asked to paint, with both shade slots.
fn identities() -> Vec<SkinIdentity> {
    let mut identities = Vec::new();
    for shade_slot in 0..2u8 {
        for role in [
            SnakeRole::Own,
            SnakeRole::Teammate,
            SnakeRole::Enemy,
            SnakeRole::SpectatedTeam(0),
            SnakeRole::SpectatedTeam(1),
        ] {
            identities.push(SkinIdentity { role, shade_slot });
        }
    }
    for palette_slot in 0..4u8 {
        identities.push(SkinIdentity {
            role: SnakeRole::FreeForAll { palette_slot },
            shade_slot: 0,
        });
    }
    identities
}

fn record(skin: &dyn SnakeSkin, pose: &SnakePose, identity: &SkinIdentity) -> OpRecorder {
    let mut recorder = OpRecorder::new();
    skin.paint_alive(&mut PaintCtx::recording(&mut recorder), pose, identity)
        .expect("a recording painter cannot fail");
    recorder
}

/// The catalogue plus any document skins the suite compiles itself, so a new
/// skin is covered the moment it is registered.
fn skins_under_test() -> Vec<Box<dyn SnakeSkin>> {
    let mut skins: Vec<Box<dyn SnakeSkin>> = vec![Box::new(ClassicSkin)];
    for skin in skin_registry().entries() {
        if skin.id() != ClassicSkin.id() {
            // Registry entries are borrowed for the process lifetime; wrap the
            // classic reference rather than cloning trait objects.
            skins.push(Box::new(RegistryRef(skin)));
        }
    }
    skins.push(Box::new(
        ParamSkin::from_json(include_str!("../../../skin-schema/skins/classic.skin.json"))
            .expect("the shipped classic document compiles"),
    ));
    skins
}

/// Adapter so a borrowed catalogue entry can join the boxed list.
struct RegistryRef(&'static dyn SnakeSkin);

impl SnakeSkin for RegistryRef {
    fn id(&self) -> &str {
        self.0.id()
    }
    fn name(&self) -> &str {
        self.0.name()
    }
    fn colors(&self, identity: &SkinIdentity) -> crate::skin::SkinColors<'_> {
        self.0.colors(identity)
    }
    fn metrics(&self, boost_active: bool) -> crate::skin::SkinMetrics {
        self.0.metrics(boost_active)
    }
    fn side_cue(&self) -> SideCue {
        self.0.side_cue()
    }
    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), wasm_bindgen::JsValue> {
        self.0.paint_alive(ctx, pose, identity)
    }
    // Forwarding every method matters: a defaulted one here would silently
    // test the default instead of the skin, and the suite would pass while
    // proving nothing about the skin it named.
    fn paint_dead(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
    ) -> Result<(), wasm_bindgen::JsValue> {
        self.0.paint_dead(ctx, pose)
    }
    fn base_theme(&self) -> Option<crate::skin::BaseTheme<'_>> {
        self.0.base_theme()
    }
    fn celebration_theme(&self) -> Option<crate::skin::CelebrationTheme<'_>> {
        self.0.celebration_theme()
    }
}

/// Boost is competitive information: an opponent has to be able to tell you are
/// boosting. A skin may restyle the signal; it may not remove it.
#[test]
fn skin_conformance_boost_is_always_visibly_different() {
    for skin in skins_under_test() {
        for identity in identities() {
            for pose in POSES {
                let calm = record(
                    skin.as_ref(),
                    &SnakePose::still(pose.cells, 10.0, false),
                    &identity,
                );
                let boosting = record(
                    skin.as_ref(),
                    &SnakePose::still(pose.cells, 10.0, true),
                    &identity,
                );
                assert_ne!(
                    calm.to_golden(),
                    boosting.to_golden(),
                    "{} paints a boosting snake identically to a calm one \
                     ({}, {identity:?}); opponents read that band",
                    skin.id(),
                    pose.name
                );
            }
        }
    }
}

/// How far past its own body a skin painted, per side, in pixels.
///
/// Extracted so the honesty check and the tests-for-the-test below run the
/// *same* measurement. A liar checked by a hand-rolled copy of this logic would
/// only prove that the copy works.
fn overhang_overrun(
    skin: &dyn SnakeSkin,
    cells: &[(f64, f64)],
    cell_size: f64,
    boost_active: bool,
    identity: &SkinIdentity,
) -> Option<[(f64, &'static str); 4]> {
    let recorder = record(
        skin,
        &SnakePose::still(cells, cell_size, boost_active),
        identity,
    );
    let (x0, y0, x1, y1) = recorder.painted_extent()?;

    let body_x0 = cells.iter().map(|cell| cell.0).fold(f64::MAX, f64::min) * cell_size;
    let body_y0 = cells.iter().map(|cell| cell.1).fold(f64::MAX, f64::min) * cell_size;
    let body_x1 = cells.iter().map(|cell| cell.0).fold(f64::MIN, f64::max) * cell_size + cell_size;
    let body_y1 = cells.iter().map(|cell| cell.1).fold(f64::MIN, f64::max) * cell_size + cell_size;

    Some([
        (body_x0 - x0, "left"),
        (body_y0 - y0, "top"),
        (x1 - body_x1, "right"),
        (y1 - body_y1, "bottom"),
    ])
}

/// The renderer sizes the occlusion mask and the roster row from
/// `metrics().overhang_px`. A skin that paints wider than it admits leaves grid
/// dots showing through its own outline and overflows the roster.
#[test]
fn skin_conformance_painted_extent_stays_inside_reported_overhang() {
    for skin in skins_under_test() {
        for boost_active in [false, true] {
            let overhang = skin.metrics(boost_active).overhang_px;
            for identity in identities() {
                for pose in POSES {
                    for &cell_size in CELL_SIZES {
                        let Some(sides) = overhang_overrun(
                            skin.as_ref(),
                            pose.cells,
                            cell_size,
                            boost_active,
                            &identity,
                        ) else {
                            continue;
                        };

                        // A hair of tolerance for float accumulation, not for
                        // sloppy reporting.
                        let slack = 1e-9;
                        for (painted, side) in sides {
                            assert!(
                                painted <= overhang + slack,
                                "{} paints {painted}px past its body on the {side} \
                                 but reports only {overhang}px of overhang \
                                 ({}, cell {cell_size}, boost {boost_active})",
                                skin.id(),
                                pose.name
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Generic chrome puts these straight into CSS and contrast maths, so they have
/// to be plain, opaque hex — even for a skin that paints gradients.
#[test]
fn skin_conformance_reported_colours_are_flat_hex() {
    for skin in skins_under_test() {
        for identity in identities() {
            let colors = skin.colors(&identity);
            for (field, value) in [
                ("fill", colors.fill),
                ("outline", colors.outline),
                ("label", colors.label),
                ("swatch", colors.swatch),
            ] {
                assert!(
                    skin_schema::color::Rgb::parse(value).is_some(),
                    "{}'s {field} is `{value}`, which is not a 6-digit hex colour",
                    skin.id()
                );
            }
        }
    }
}

/// The rule that makes animation safe: a skin may change what its ops *say*
/// between frames, never which ops it issues. That keeps the cost of an
/// animated skin identical to a still one, and keeps every frame checkable
/// against the same structural expectations.
#[test]
fn skin_conformance_animation_only_varies_paint_arguments() {
    for skin in skins_under_test() {
        for identity in identities() {
            for pose in POSES {
                let reference = record(
                    skin.as_ref(),
                    &SnakePose {
                        cells: pose.cells,
                        cell_size: 10.0,
                        boost_active: false,
                        anim_ms: 0.0,
                        reduced_motion: false,
                    },
                    &identity,
                );

                for &anim_ms in ANIM_SAMPLES {
                    let sampled = record(
                        skin.as_ref(),
                        &SnakePose {
                            cells: pose.cells,
                            cell_size: 10.0,
                            boost_active: false,
                            anim_ms,
                            reduced_motion: false,
                        },
                        &identity,
                    );
                    assert_eq!(
                        reference.shapes(),
                        sampled.shapes(),
                        "{} changes its op sequence at {anim_ms}ms ({}); \
                         animation may vary arguments only",
                        skin.id(),
                        pose.name
                    );
                }
            }
        }
    }
}

/// Someone who has asked the OS for less motion should get a still picture,
/// not a slower one.
#[test]
fn skin_conformance_reduced_motion_is_actually_still() {
    for skin in skins_under_test() {
        for identity in identities() {
            for pose in POSES {
                let mut frames = ANIM_SAMPLES.iter().map(|&anim_ms| {
                    record(
                        skin.as_ref(),
                        &SnakePose {
                            cells: pose.cells,
                            cell_size: 10.0,
                            boost_active: false,
                            anim_ms,
                            reduced_motion: true,
                        },
                        &identity,
                    )
                    .to_golden()
                });
                let first = frames.next().expect("there is at least one sample");
                for frame in frames {
                    assert_eq!(
                        first,
                        frame,
                        "{} still moves under reduced motion ({})",
                        skin.id(),
                        pose.name
                    );
                }
            }
        }
    }
}

/// Team games are read through colour: blue is a friend, red is not. That
/// reading is enforced for document skins by the schema validator, and here for
/// every skin — including first-party Rust ones, which the validator never
/// sees. A skin that gets this wrong is a competitive bug, not a bold choice.
///
/// **Which colour carries the reading is the skin's to choose**, and it says so
/// through [`SideCue`]. A painted skin says it with the body; a skin whose body
/// is an animal's coat says it with the contour, because a tiger is orange
/// whoever wears it. What no skin may do is fail to say it at all — so the
/// nominated channel is held to the hue windows *and* required to be visibly
/// different between the two sides, which is the property a hue window on its
/// own does not give.
#[test]
fn skin_conformance_team_colours_stay_on_their_own_side() {
    use skin_schema::color::{ENEMY_HUES, FRIENDLY_HUES, NEUTRAL_CHROMA, Rgb, perceptual_distance};

    /// Two sides that differ by less than this are not telling anyone apart.
    /// Comfortably above the ~0.08 nobody would miss.
    const MIN_SIDE_DISTANCE: f64 = 0.15;

    let friendly_roles = [
        SnakeRole::Own,
        SnakeRole::Teammate,
        SnakeRole::SpectatedTeam(0),
    ];
    let hostile_roles = [SnakeRole::Enemy, SnakeRole::SpectatedTeam(1)];

    for skin in skins_under_test() {
        let cue = skin.side_cue();
        let read = |colors: &crate::skin::SkinColors<'_>| -> String {
            match cue {
                SideCue::Body => colors.fill.to_string(),
                SideCue::Contour => colors.outline.to_string(),
            }
        };

        for shade_slot in 0..2u8 {
            for (roles, window, side) in [
                (friendly_roles.as_slice(), FRIENDLY_HUES, "friendly"),
                (hostile_roles.as_slice(), ENEMY_HUES, "hostile"),
            ] {
                for role in roles {
                    let colors = skin.colors(&SkinIdentity {
                        role: *role,
                        shade_slot,
                    });
                    let value = read(&colors);
                    let parsed = Rgb::parse(&value).expect("a flat hex colour");
                    let (hue, chroma) = parsed.oklch_hue_chroma();
                    if chroma <= NEUTRAL_CHROMA {
                        // A near-gray carries no side information; legibility
                        // is checked elsewhere. A skin that nominated this
                        // channel and then made it gray is caught by the
                        // distance check below rather than slipping through.
                        continue;
                    }
                    assert!(
                        window.contains(hue),
                        "{} paints {role:?}'s {cue:?} as {value} ({hue:.0}deg), \
                         which does not read as {side}",
                        skin.id(),
                    );
                }
            }

            // ...and the two sides have to be *distinguishable* in that
            // channel, not merely each inside a window. This is the check that
            // catches a skin nominating a channel it does not actually vary.
            let friendly = skin.colors(&SkinIdentity {
                role: SnakeRole::Teammate,
                shade_slot,
            });
            let hostile = skin.colors(&SkinIdentity {
                role: SnakeRole::Enemy,
                shade_slot,
            });
            let (friendly, hostile) = (read(&friendly), read(&hostile));
            let distance = perceptual_distance(
                Rgb::parse(&friendly).expect("a flat hex colour"),
                Rgb::parse(&hostile).expect("a flat hex colour"),
            );
            assert!(
                distance >= MIN_SIDE_DISTANCE,
                "{}: a teammate's {cue:?} `{friendly}` and an opponent's \
                 `{hostile}` are only {distance:.3} apart, so this skin's own \
                 side cue does not tell them apart",
                skin.id(),
            );
        }
    }
}

/// The document layer's honesty test.
///
/// The classic look written as a document has to paint exactly what the
/// hand-written classic skin paints. If this ever fails, the interpreter has
/// drifted from the reference — and since the reference is what the golden
/// traces pin, the interpreter is the one that is wrong.
///
/// Colours are compared after normalising hex shorthand, because `#333` and
/// `#333333` are the same colour to a canvas and it would be silly to make the
/// document spell it the short way.
#[test]
fn skin_conformance_document_classic_matches_reference_classic() {
    let document =
        ParamSkin::from_json(include_str!("../../../skin-schema/skins/classic.skin.json"))
            .expect("the shipped classic document compiles");

    for identity in identities() {
        for pose in POSES {
            for &cell_size in CELL_SIZES {
                for boost_active in [false, true] {
                    let posed = SnakePose::still(pose.cells, cell_size, boost_active);
                    let reference = normalise(&record(&ClassicSkin, &posed, &identity).to_golden());
                    let interpreted = normalise(&record(&document, &posed, &identity).to_golden());
                    assert_eq!(
                        reference, interpreted,
                        "the classic document paints differently from the \
                         classic skin ({}, cell {cell_size}, boost \
                         {boost_active}, {identity:?})",
                        pose.name
                    );
                }
            }
        }
    }

    // ...and it has to report the same numbers, not just paint the same pixels.
    for boost_active in [false, true] {
        let reference = ClassicSkin.metrics(boost_active);
        let interpreted = document.metrics(boost_active);
        assert_eq!(reference.overhang_px, interpreted.overhang_px);
        assert_eq!(
            reference.head_core_radius_ratio,
            interpreted.head_core_radius_ratio
        );
        assert_eq!(reference.head_core_is_dark, interpreted.head_core_is_dark);
    }

    for identity in identities() {
        let reference = ClassicSkin.colors(&identity);
        let interpreted = document.colors(&identity);
        assert_eq!(
            expand_hex(reference.fill),
            expand_hex(interpreted.fill),
            "{identity:?}"
        );
        assert_eq!(
            expand_hex(reference.outline),
            expand_hex(interpreted.outline)
        );
        assert_eq!(expand_hex(reference.label), expand_hex(interpreted.label));
        assert_eq!(expand_hex(reference.swatch), expand_hex(interpreted.swatch));
    }
}

/// `#333` and `#333333` name the same colour; treat them as equal.
fn expand_hex(value: &str) -> String {
    let lowered = value.to_ascii_lowercase();
    let Some(body) = lowered.strip_prefix('#') else {
        return lowered;
    };
    if body.len() == 3 && body.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut expanded = String::with_capacity(7);
        expanded.push('#');
        for c in body.chars() {
            expanded.push(c);
            expanded.push(c);
        }
        return expanded;
    }
    lowered
}

/// Normalise every colour token in a recorded trace.
fn normalise(trace: &str) -> String {
    trace
        .lines()
        .map(|line| {
            line.split(' ')
                .map(|token| {
                    if token.starts_with('#') {
                        expand_hex(token)
                    } else {
                        token.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A skin that lies about its overhang must be caught, or the check above is
/// decorative. This is the test for the test.
#[test]
fn skin_conformance_catches_a_skin_that_underreports_its_overhang() {
    struct Liar;
    impl SnakeSkin for Liar {
        fn id(&self) -> &str {
            "liar@1"
        }
        fn name(&self) -> &str {
            "Liar"
        }
        fn colors(&self, identity: &SkinIdentity) -> crate::skin::SkinColors<'_> {
            ClassicSkin.colors(identity)
        }
        fn metrics(&self, _boost_active: bool) -> crate::skin::SkinMetrics {
            crate::skin::SkinMetrics {
                overhang_px: 0.0,
                head_core_radius_ratio: 0.38,
                head_core_is_dark: true,
            }
        }
        fn paint_alive(
            &self,
            ctx: &mut PaintCtx,
            pose: &SnakePose,
            identity: &SkinIdentity,
        ) -> Result<(), wasm_bindgen::JsValue> {
            // Paints the classic contour while claiming to paint nothing
            // outside the body.
            ClassicSkin.paint_alive(ctx, pose, identity)
        }
    }

    let identity = SkinIdentity {
        role: SnakeRole::Own,
        shade_slot: 0,
    };
    let sides = overhang_overrun(&Liar, POSES[2].cells, 10.0, false, &identity)
        .expect("the liar paints something");
    let worst = sides
        .iter()
        .map(|(painted, _)| *painted)
        .fold(f64::MIN, f64::max);

    assert!(
        worst > Liar.metrics(false).overhang_px,
        "the extent check would not notice an under-reported overhang"
    );
}

/// The other way to lie about overhang, and the one that only exists once the
/// compositor can transform a layer: paint entirely inside your budget, then
/// move the result outside it.
///
/// `specs/skin-shading-prd.md` section 16 requires this skin to land in the
/// same commit as the first transform op, because a recorder that measured
/// pre-transform coordinates would report this skin as honest. It is the proof
/// that the CTM tracking in `skin::paint` is load-bearing rather than
/// decorative — comment out the transform replay there and only this test
/// fails.
#[test]
fn skin_conformance_catches_a_skin_that_paints_honestly_then_moves_it() {
    struct TransformLiar;
    impl SnakeSkin for TransformLiar {
        fn id(&self) -> &str {
            "transform-liar@1"
        }
        fn name(&self) -> &str {
            "Transform liar"
        }
        fn colors(&self, identity: &SkinIdentity) -> crate::skin::SkinColors<'_> {
            ClassicSkin.colors(identity)
        }
        fn metrics(&self, _boost_active: bool) -> crate::skin::SkinMetrics {
            crate::skin::SkinMetrics {
                overhang_px: 0.0,
                head_core_radius_ratio: 0.38,
                head_core_is_dark: true,
            }
        }
        fn paint_alive(
            &self,
            ctx: &mut PaintCtx,
            pose: &SnakePose,
            _identity: &SkinIdentity,
        ) -> Result<(), wasm_bindgen::JsValue> {
            // The body's leftmost cell, so the breach shows up on the side the
            // assertion reads rather than being swallowed by the body's width.
            let left = pose
                .cells
                .iter()
                .map(|cell| cell.0)
                .fold(f64::MAX, f64::min);
            let top = pose
                .cells
                .iter()
                .map(|cell| cell.1)
                .fold(f64::MAX, f64::min);
            ctx.save();
            // Every coordinate below is inside the body. The transform is
            // what carries them out of it.
            ctx.translate(-BREACH_PX, 0.0)?;
            ctx.set_fill("#000000");
            ctx.fill_rect(
                left * pose.cell_size,
                top * pose.cell_size,
                pose.cell_size,
                pose.cell_size,
            );
            ctx.restore();
            Ok(())
        }
    }

    /// Larger than any tolerance, small enough to be a plausible mistake.
    const BREACH_PX: f64 = 7.0;

    let identity = SkinIdentity {
        role: SnakeRole::Own,
        shade_slot: 0,
    };
    let sides = overhang_overrun(&TransformLiar, POSES[2].cells, 10.0, false, &identity)
        .expect("the liar paints something");
    let left = sides
        .iter()
        .find(|(_, side)| *side == "left")
        .expect("the measurement reports a left side")
        .0;

    assert!(
        (left - BREACH_PX).abs() < 1e-9,
        "a translated breach measured as {left}px instead of {BREACH_PX}px, so \
         the recorder is not replaying the transform and the overhang check is \
         a no-op for any skin that moves a layer"
    );
    assert!(
        left > TransformLiar.metrics(false).overhang_px,
        "the extent check would not notice overhang smuggled through a transform"
    );
}
