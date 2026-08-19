//! What a skin document contains, as data an editor can walk.
//!
//! The Skin Builder builds its controls from this rather than from a
//! hand-written form, so a field added to [`SkinDoc`] cannot quietly go missing
//! from the editor. That guarantee is the compiler's, not discipline's: the
//! `exhaustiveness` module below destructures every struct in the document, so
//! adding a field stops this file compiling until it is described.
//!
//! Numeric bounds come from the validator's own constants, so a slider cannot
//! offer a value the server will then refuse.

use crate::{
    AnimationSpec, AnimationTrack, BaseTheme, CelebrationTheme, ColorPair, HeadGradient, HeadStyle,
    KNOWN_EFFECTS, LabelStyle, OutlineStyle, RolePalette, SkinDoc, TrackTarget, WaveSpec,
};
use serde::{Deserialize, Serialize};

/// Never called, and that is the point.
///
/// Each function destructures a whole struct, so adding a field to any of them
/// stops this file compiling until the new field is described. This is the
/// "static types enforce it" half of the Builder's promise that its controls
/// cannot drift from the object model; the tests at the bottom are the runtime
/// half.
#[allow(dead_code)]
mod exhaustiveness {
    use super::*;

    fn document(value: SkinDoc) {
        let SkinDoc {
            // Not editable: the version belongs to the format, and the id is
            // assigned by storage.
            schema_version: _,
            id: _,
            name: _,
            palette: _,
            head: _,
            outline: _,
            labels: _,
            animation: _,
            base: _,
            celebration: _,
        } = value;
    }

    fn palette(value: RolePalette) {
        let RolePalette {
            friendly: _,
            enemy: _,
            free_for_all: _,
        } = value;
    }

    fn pair(value: ColorPair) {
        let ColorPair {
            fill: _,
            outline: _,
        } = value;
    }

    fn head(value: HeadStyle) {
        let HeadStyle {
            core_ratio: _,
            core_color: _,
            gradient: _,
        } = value;
    }

    fn gradient(value: HeadGradient) {
        let HeadGradient {
            length_cells: _,
            max_opacity: _,
            color: _,
        } = value;
    }

    fn outline(value: OutlineStyle) {
        let OutlineStyle {
            extra_px: _,
            // Deliberately undescribed. The boost band is the one thing a skin
            // may not restyle, because it is how opponents know you are
            // boosting; if that ever changes, this is where the decision has to
            // be made on purpose rather than by adding a control.
            boost_band: _,
        } = value;
    }

    fn labels(value: LabelStyle) {
        let LabelStyle { ink: _, swatch: _ } = value;
    }

    fn animation(value: AnimationSpec) {
        let AnimationSpec {
            period_ms: _,
            tracks: _,
            wave: _,
        } = value;
    }

    fn track(value: AnimationTrack) {
        let AnimationTrack {
            target: _,
            amplitude: _,
            phase: _,
        } = value;
    }

    fn wave(value: WaveSpec) {
        let WaveSpec {
            cells_per_crest: _,
            amplitude: _,
            crests_per_cycle: _,
        } = value;
    }

    fn base(value: BaseTheme) {
        let BaseTheme {
            friendly_zone: _,
            enemy_zone: _,
            friendly_wall: _,
            enemy_wall: _,
            friendly_text: _,
            enemy_text: _,
        } = value;
    }

    fn celebration(value: CelebrationTheme) {
        let CelebrationTheme {
            effect: _,
            friendly_accent: _,
            enemy_accent: _,
            readout_friendly: _,
            readout_enemy: _,
        } = value;
    }

    /// Every target a track may drive.
    ///
    /// Matched exhaustively so the editor's choice list is the language's own
    /// list rather than a copy that could fall behind it.
    pub fn track_targets() -> Vec<&'static str> {
        [
            TrackTarget::BodyLightness,
            TrackTarget::OutlineLightness,
            TrackTarget::GradientOpacity,
        ]
        .into_iter()
        .map(|target| match target {
            TrackTarget::BodyLightness => "body_lightness",
            TrackTarget::OutlineLightness => "outline_lightness",
            TrackTarget::GradientOpacity => "gradient_opacity",
        })
        .collect()
    }
}

/// One editable thing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldNode {
    /// Dotted path into the document, e.g. `head.gradient.max_opacity`.
    pub path: String,
    /// What to call it in the editor.
    pub label: String,
    pub kind: FieldKind,
    /// Why it exists, shown where an editor has room for help text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
}

/// The control an editor should render.
///
/// Deliberately a small closed set: an editor that has to handle an open-ended
/// kind cannot be exhaustive, and exhaustiveness is the point.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "control", rename_all = "camelCase")]
pub enum FieldKind {
    /// A six-digit hex colour.
    Color,
    Number {
        min: f64,
        max: f64,
        step: f64,
    },
    Text {
        max_length: usize,
    },
    /// One of a fixed set.
    Choice {
        options: Vec<String>,
    },
    /// A section containing other fields.
    Group {
        children: Vec<FieldNode>,
    },
    /// A section that may be absent entirely.
    Optional {
        children: Vec<FieldNode>,
    },
    /// A repeated section.
    List {
        item_label: String,
        children: Vec<FieldNode>,
    },
}

fn node(path: &str, label: &str, kind: FieldKind) -> FieldNode {
    FieldNode {
        path: path.to_string(),
        label: label.to_string(),
        kind,
        help: None,
    }
}

fn explained(path: &str, label: &str, kind: FieldKind, help: &str) -> FieldNode {
    FieldNode {
        path: path.to_string(),
        label: label.to_string(),
        kind,
        help: Some(help.to_string()),
    }
}

/// The whole document, as an editor should present it.
pub fn describe_skin_doc() -> Vec<FieldNode> {
    vec![
        explained(
            "name",
            "Name",
            FieldKind::Text { max_length: 40 },
            "What players see in the picker.",
        ),
        node(
            "palette",
            "Colours",
            FieldKind::Group {
                children: describe_palette(),
            },
        ),
        node(
            "head",
            "Head",
            FieldKind::Group {
                children: describe_head(),
            },
        ),
        node(
            "outline",
            "Outline",
            FieldKind::Group {
                children: describe_outline(),
            },
        ),
        node(
            "labels",
            "Labels",
            FieldKind::Group {
                children: describe_labels(),
            },
        ),
        explained(
            "animation",
            "Animation",
            FieldKind::Optional {
                children: describe_animation(),
            },
            "Varies colour over time. What the snake paints never changes.",
        ),
        explained(
            "base",
            "Base dressing",
            FieldKind::Optional {
                children: describe_base(),
            },
            "Themes the arena you are looking at. Other players see their own.",
        ),
        explained(
            "celebration",
            "Goal celebration",
            FieldKind::Optional {
                children: describe_celebration(),
            },
            "Plays for everyone when you score.",
        ),
    ]
}

fn describe_color_pair(prefix: &str, label: &str) -> FieldNode {
    node(
        prefix,
        label,
        FieldKind::Group {
            children: vec![
                node(&format!("{prefix}.fill"), "Fill", FieldKind::Color),
                node(&format!("{prefix}.outline"), "Outline", FieldKind::Color),
            ],
        },
    )
}

fn describe_palette() -> Vec<FieldNode> {
    let mut children = Vec::new();
    for (index, shade) in ["light", "dark"].iter().enumerate() {
        children.push(describe_color_pair(
            &format!("palette.friendly.{index}"),
            &format!("Friendly ({shade})"),
        ));
    }
    for (index, shade) in ["light", "dark"].iter().enumerate() {
        children.push(describe_color_pair(
            &format!("palette.enemy.{index}"),
            &format!("Enemy ({shade})"),
        ));
    }
    for index in 0..4 {
        children.push(describe_color_pair(
            &format!("palette.free_for_all.{index}"),
            &format!("Free-for-all slot {}", index + 1),
        ));
    }
    children
}

fn describe_head() -> Vec<FieldNode> {
    vec![
        explained(
            "head.core_ratio",
            "Core size",
            FieldKind::Number {
                min: crate::MIN_HEAD_CORE_RATIO,
                max: crate::MAX_HEAD_CORE_RATIO,
                step: 0.01,
            },
            "How much of the head the dark centre takes.",
        ),
        node("head.core_color", "Core colour", FieldKind::Color),
        node(
            "head.gradient",
            "Head glow",
            FieldKind::Group {
                children: vec![
                    node(
                        "head.gradient.length_cells",
                        "Length",
                        FieldKind::Number {
                            min: 0.0,
                            max: 20.0,
                            step: 0.5,
                        },
                    ),
                    node(
                        "head.gradient.max_opacity",
                        "Strength",
                        FieldKind::Number {
                            min: 0.0,
                            max: 1.0,
                            step: 0.01,
                        },
                    ),
                    node("head.gradient.color", "Colour", FieldKind::Color),
                ],
            },
        ),
    ]
}

fn describe_outline() -> Vec<FieldNode> {
    vec![explained(
        "outline.extra_px",
        "Width",
        FieldKind::Number {
            min: crate::MIN_OUTLINE_EXTRA_PX,
            max: crate::MAX_OUTLINE_EXTRA_PX,
            step: 0.5,
        },
        "The boost band is fixed and has no control: it is how opponents know \
         you are boosting.",
    )]
}

fn describe_labels() -> Vec<FieldNode> {
    vec![
        explained(
            "labels.ink",
            "Label ink",
            FieldKind::Optional {
                children: vec![node("labels.ink", "Colour", FieldKind::Color)],
            },
            "Carried-food digits. Left alone, it is derived from your fill.",
        ),
        node(
            "labels.swatch",
            "Swatch",
            FieldKind::Optional {
                children: vec![node("labels.swatch", "Colour", FieldKind::Color)],
            },
        ),
    ]
}

fn describe_animation() -> Vec<FieldNode> {
    vec![
        explained(
            "animation.period_ms",
            "Cycle length",
            FieldKind::Number {
                min: crate::MIN_ANIMATION_PERIOD_MS,
                max: crate::MAX_ANIMATION_PERIOD_MS,
                step: 10.0,
            },
            "How long one full cycle takes.",
        ),
        node(
            "animation.tracks",
            "Tracks",
            FieldKind::List {
                item_label: "Track".to_string(),
                children: vec![
                    node(
                        "target",
                        "What it moves",
                        FieldKind::Choice {
                            options: exhaustiveness::track_targets()
                                .into_iter()
                                .map(str::to_string)
                                .collect(),
                        },
                    ),
                    node(
                        "amplitude",
                        "How far",
                        FieldKind::Number {
                            min: 0.0,
                            max: crate::MAX_ANIMATION_AMPLITUDE,
                            step: 0.01,
                        },
                    ),
                    node(
                        "phase",
                        "Offset",
                        FieldKind::Number {
                            min: 0.0,
                            max: 1.0,
                            step: 0.01,
                        },
                    ),
                ],
            },
        ),
        node(
            "animation.wave",
            "Travelling wave",
            FieldKind::Optional {
                children: vec![
                    node(
                        "animation.wave.cells_per_crest",
                        "Cells per crest",
                        FieldKind::Number {
                            min: crate::MIN_WAVE_CELLS_PER_CREST,
                            max: crate::MAX_WAVE_CELLS_PER_CREST,
                            step: 1.0,
                        },
                    ),
                    node(
                        "animation.wave.amplitude",
                        "How far",
                        FieldKind::Number {
                            min: 0.0,
                            max: crate::MAX_ANIMATION_AMPLITUDE,
                            step: 0.01,
                        },
                    ),
                    node(
                        "animation.wave.crests_per_cycle",
                        "Crests per cycle",
                        FieldKind::Number {
                            min: -16.0,
                            max: 16.0,
                            step: 0.5,
                        },
                    ),
                ],
            },
        ),
    ]
}

fn describe_base() -> Vec<FieldNode> {
    vec![
        node("base.friendly_zone", "Your zone", FieldKind::Color),
        node("base.enemy_zone", "Their zone", FieldKind::Color),
        node("base.friendly_wall", "Your goal wall", FieldKind::Color),
        node("base.enemy_wall", "Their goal wall", FieldKind::Color),
        node("base.friendly_text", "Your endzone text", FieldKind::Color),
        node("base.enemy_text", "Their endzone text", FieldKind::Color),
    ]
}

fn describe_celebration() -> Vec<FieldNode> {
    vec![
        explained(
            "celebration.effect",
            "Effect",
            FieldKind::Choice {
                options: KNOWN_EFFECTS.iter().map(|id| (*id).to_string()).collect(),
            },
            "Which first-party effect plays. A skin picks one; it never ships code.",
        ),
        node(
            "celebration.friendly_accent",
            "Your accent",
            FieldKind::Color,
        ),
        node("celebration.enemy_accent", "Their accent", FieldKind::Color),
        node(
            "celebration.readout_friendly",
            "Your readout",
            FieldKind::Color,
        ),
        node(
            "celebration.readout_enemy",
            "Their readout",
            FieldKind::Color,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn walk(nodes: &[FieldNode], visit: &mut impl FnMut(&FieldNode)) {
        for node in nodes {
            visit(node);
            match &node.kind {
                FieldKind::Group { children }
                | FieldKind::Optional { children }
                | FieldKind::List { children, .. } => walk(children, visit),
                FieldKind::Color
                | FieldKind::Number { .. }
                | FieldKind::Text { .. }
                | FieldKind::Choice { .. } => {}
            }
        }
    }

    fn all_paths() -> Vec<String> {
        let mut paths = Vec::new();
        walk(&describe_skin_doc(), &mut |node| {
            paths.push(node.path.clone())
        });
        paths
    }

    /// Every part of the document an author can set has a control.
    ///
    /// The compile-time guarantee is the `exhaustiveness` module; this is the
    /// runtime half — that the descriptor actually reaches those fields rather
    /// than merely compiling alongside them.
    #[test]
    fn the_descriptor_covers_every_section_of_the_document() {
        let paths = all_paths();
        for expected in [
            "name",
            "palette",
            "palette.friendly.0.fill",
            "palette.enemy.1.outline",
            "palette.free_for_all.3.fill",
            "head.core_ratio",
            "head.core_color",
            "head.gradient.max_opacity",
            "outline.extra_px",
            "labels.ink",
            "animation.period_ms",
            "animation.tracks",
            "animation.wave.cells_per_crest",
            "base.friendly_zone",
            "celebration.effect",
        ] {
            assert!(
                paths.iter().any(|path| path == expected),
                "the editor would have no control for {expected}"
            );
        }
    }

    /// A slider must not offer a value the server will refuse, so the bounds
    /// are read from the validator's own constants rather than guessed.
    #[test]
    fn numeric_bounds_match_what_the_validator_enforces() {
        let mut bounds = std::collections::HashMap::new();
        walk(&describe_skin_doc(), &mut |node| {
            if let FieldKind::Number { min, max, .. } = node.kind {
                bounds.insert(node.path.clone(), (min, max));
            }
        });

        assert_eq!(
            bounds.get("head.core_ratio"),
            Some(&(crate::MIN_HEAD_CORE_RATIO, crate::MAX_HEAD_CORE_RATIO))
        );
        assert_eq!(
            bounds.get("outline.extra_px"),
            Some(&(crate::MIN_OUTLINE_EXTRA_PX, crate::MAX_OUTLINE_EXTRA_PX))
        );
        assert_eq!(
            bounds.get("animation.period_ms"),
            Some(&(
                crate::MIN_ANIMATION_PERIOD_MS,
                crate::MAX_ANIMATION_PERIOD_MS
            ))
        );
        assert_eq!(
            bounds.get("animation.wave.cells_per_crest"),
            Some(&(
                crate::MIN_WAVE_CELLS_PER_CREST,
                crate::MAX_WAVE_CELLS_PER_CREST
            ))
        );
    }

    /// The boost band is deliberately absent: it is the one thing a skin may
    /// not restyle, because it is how opponents know you are boosting.
    #[test]
    fn the_boost_band_has_no_control() {
        assert!(
            !all_paths().iter().any(|path| path.contains("boost_band")),
            "the boost telegraph must not be editable"
        );
    }

    /// Neither is the id or the schema version: one belongs to storage, the
    /// other to the format.
    #[test]
    fn identity_and_version_have_no_controls() {
        let paths = all_paths();
        assert!(!paths.iter().any(|path| path == "id"));
        assert!(!paths.iter().any(|path| path == "schema_version"));
    }

    /// A celebration may only name an effect the client can actually draw.
    #[test]
    fn the_effect_choice_offers_only_known_effects() {
        let mut options = None;
        walk(&describe_skin_doc(), &mut |node| {
            if node.path == "celebration.effect"
                && let FieldKind::Choice { options: choices } = &node.kind
            {
                options = Some(choices.clone());
            }
        });
        assert_eq!(
            options.expect("the effect field is a choice"),
            KNOWN_EFFECTS
                .iter()
                .map(|id| (*id).to_string())
                .collect::<Vec<_>>()
        );
    }

    /// Every animation target the language has is offered, so a target added
    /// to `TrackTarget` cannot be one the editor silently cannot select.
    #[test]
    fn every_animation_target_is_offered() {
        let mut options = None;
        walk(&describe_skin_doc(), &mut |node| {
            if node.path == "target"
                && let FieldKind::Choice { options: choices } = &node.kind
            {
                options = Some(choices.clone());
            }
        });
        let options = options.expect("the track target is a choice");
        assert_eq!(options.len(), 3);
        for target in ["body_lightness", "outline_lightness", "gradient_opacity"] {
            assert!(options.iter().any(|option| option == target));
        }
    }

    #[test]
    fn the_descriptor_serializes_for_the_editor() {
        let json = serde_json::to_string(&describe_skin_doc()).expect("serializes");
        assert!(json.contains("\"control\":\"color\""));
        assert!(json.contains("\"control\":\"number\""));
        assert!(json.contains("\"control\":\"optional\""));
    }
}
