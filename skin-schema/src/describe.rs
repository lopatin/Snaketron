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
            accent: _,
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
    pub fn track_targets() -> Vec<(&'static str, &'static str)> {
        [
            TrackTarget::BodyLightness,
            TrackTarget::OutlineLightness,
            TrackTarget::GradientOpacity,
        ]
        .into_iter()
        .map(|target| match target {
            TrackTarget::BodyLightness => ("body_lightness", "Body brightness"),
            TrackTarget::OutlineLightness => ("outline_lightness", "Outline brightness"),
            TrackTarget::GradientOpacity => ("gradient_opacity", "Head glow strength"),
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
    /// What to insert when an optional section is switched on.
    ///
    /// Supplied here rather than guessed by the editor, because "on" has to
    /// mean a section that *validates*: switching Animation on and being handed
    /// an error is a worse first move than not offering the switch at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
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
        // Renamed explicitly: `rename_all` on an enum renames its *variants*,
        // not the fields inside them, so without this the editor receives
        // `max_length` while reading `maxLength` and silently caps nothing.
        #[serde(rename = "maxLength")]
        max_length: usize,
    },
    /// One of a fixed set. Each option carries the value that goes in the
    /// document and the words an author reads — `gradient_opacity` is a wire
    /// name, not a label.
    Choice {
        options: Vec<ChoiceOption>,
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
        #[serde(rename = "itemLabel")]
        item_label: String,
        /// What one new item starts as. Adding a track that does nothing until
        /// three sliders are moved is a worse "add" than none.
        #[serde(rename = "itemDefault")]
        item_default: serde_json::Value,
        children: Vec<FieldNode>,
    },
}

/// One selectable value and the words for it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChoiceOption {
    pub value: String,
    pub label: String,
}

fn choice(value: &str, label: &str) -> ChoiceOption {
    ChoiceOption {
        value: value.to_string(),
        label: label.to_string(),
    }
}

fn node(path: &str, label: &str, kind: FieldKind) -> FieldNode {
    FieldNode {
        path: path.to_string(),
        label: label.to_string(),
        kind,
        help: None,
        default: None,
    }
}

fn explained(path: &str, label: &str, kind: FieldKind, help: &str) -> FieldNode {
    FieldNode {
        path: path.to_string(),
        label: label.to_string(),
        kind,
        help: Some(help.to_string()),
        default: None,
    }
}

/// An optional section whose default is given before its children, for the
/// cases where listing the children last reads better.
fn optional_with_children(
    path: &str,
    label: &str,
    default: serde_json::Value,
    children: Vec<FieldNode>,
) -> FieldNode {
    FieldNode {
        path: path.to_string(),
        label: label.to_string(),
        kind: FieldKind::Optional { children },
        help: None,
        default: Some(default),
    }
}

/// An optional section, and the validating value that switching it on inserts.
fn optional(
    path: &str,
    label: &str,
    children: Vec<FieldNode>,
    help: &str,
    default: serde_json::Value,
) -> FieldNode {
    FieldNode {
        path: path.to_string(),
        label: label.to_string(),
        kind: FieldKind::Optional { children },
        help: Some(help.to_string()),
        default: Some(default),
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
        optional(
            "animation",
            "Animation",
            describe_animation(),
            "Varies colour over time. What the snake paints never changes.",
            // One gentle track, because an animation with nothing in it is
            // refused outright — "switched on" has to mean something moves.
            serde_json::json!({
                "period_ms": 2600.0,
                "tracks": [
                    { "target": "gradient_opacity", "amplitude": 0.06, "phase": 0.0 }
                ]
            }),
        ),
        optional(
            "base",
            "Base dressing",
            describe_base(),
            "Themes the arena you are looking at. Other players see their own.",
            // Classic's own dressing: valid by construction, and a familiar
            // starting point to nudge away from.
            serde_json::json!({
                "friendly_zone": "#e6f4fa",
                "enemy_zone": "#ffe6e6",
                "friendly_wall": "#7aa8c1",
                "enemy_wall": "#c18888",
                "friendly_text": "#c0d8e4",
                "enemy_text": "#e4c0c0",
            }),
        ),
        optional(
            "celebration",
            "Goal celebration",
            describe_celebration(),
            "Plays for everyone when you score.",
            serde_json::json!({
                "effect": "goal-impact-wave",
                "friendly_accent": "#5299bb",
                "enemy_accent": "#d45454",
                "readout_friendly": "#2b6f8c",
                "readout_enemy": "#a83232",
            }),
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
                optional(
                    &format!("{prefix}.accent"),
                    "Accent",
                    vec![node(
                        &format!("{prefix}.accent"),
                        "Colour",
                        FieldKind::Color,
                    )],
                    "A third colour for this skin's signature element. Left \
                     alone, it is your fill.",
                    serde_json::json!("#ffffff"),
                ),
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
        optional(
            "labels.ink",
            "Label ink",
            vec![node("labels.ink", "Colour", FieldKind::Color)],
            "Carried-food digits. Left alone, it is derived from your fill.",
            serde_json::json!("#1f2937"),
        ),
        optional(
            "labels.swatch",
            "Swatch",
            vec![node("labels.swatch", "Colour", FieldKind::Color)],
            "One flat colour for menus and scoreboards.",
            serde_json::json!("#1f2937"),
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
                // A new track that moves nothing is not an addition.
                item_default: serde_json::json!({
                    "target": "body_lightness",
                    "amplitude": 0.07,
                    "phase": 0.0
                }),
                children: vec![
                    node(
                        "target",
                        "What it moves",
                        FieldKind::Choice {
                            options: exhaustiveness::track_targets()
                                .into_iter()
                                .map(|(value, label)| choice(value, label))
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
        optional_with_children(
            "animation.wave",
            "Travelling wave",
            serde_json::json!({
                "cells_per_crest": 8.0,
                "amplitude": 0.08,
                "crests_per_cycle": 1.0
            }),
            vec![
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
                options: KNOWN_EFFECTS
                    .iter()
                    .map(|id| choice(id, "Goal impact wave"))
                    .collect(),
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
        let options = options.expect("the effect field is a choice");
        assert_eq!(
            options
                .iter()
                .map(|option| option.value.as_str())
                .collect::<Vec<_>>(),
            KNOWN_EFFECTS.to_vec()
        );
        assert!(
            options.iter().all(|option| option.label != option.value),
            "an author reads labels, not wire names"
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
            assert!(options.iter().any(|option| option.value == target));
        }
        assert!(
            options.iter().all(|option| !option.label.contains('_')),
            "wire names must not reach an author"
        );
    }

    /// Switching a section on must produce a section that validates. Without a
    /// default the editor would have to invent one, and an invented animation
    /// with no period is exactly the error a new author should never meet.
    #[test]
    fn every_optional_section_carries_a_validating_default() {
        let mut missing = Vec::new();
        walk(&describe_skin_doc(), &mut |node| {
            if matches!(node.kind, FieldKind::Optional { .. }) && node.default.is_none() {
                missing.push(node.path.clone());
            }
        });
        assert!(
            missing.is_empty(),
            "optional sections with no default: {missing:?}"
        );
    }

    /// And those defaults have to be real, not merely present.
    ///
    /// Scoped honestly: a default section is checked for *structural*
    /// completeness — every field the validator requires, in range — by
    /// applying it to the document it was drawn from. Whether a given animation
    /// also clears label contrast depends on the palette underneath it, which
    /// is a question only the live editor can answer, and does.
    #[test]
    fn every_default_section_is_structurally_complete() {
        let base: crate::SkinDoc = serde_json::from_str(include_str!("../skins/aurora.skin.json"))
            .expect("the shipped document parses");

        let mut defaults = std::collections::HashMap::new();
        walk(&describe_skin_doc(), &mut |node| {
            if let (FieldKind::Optional { .. }, Some(default)) = (&node.kind, &node.default) {
                defaults.insert(node.path.clone(), default.clone());
            }
        });

        // Each top-level section, applied on its own, still deserializes into a
        // document and still validates.
        for path in ["animation", "base", "celebration"] {
            let mut value = serde_json::to_value(&base).expect("serializes");
            value[path] = defaults
                .get(path)
                .unwrap_or_else(|| panic!("{path} has a default"))
                .clone();
            let doc: crate::SkinDoc = serde_json::from_value(value)
                .unwrap_or_else(|error| panic!("the {path} default is not a {path}: {error}"));
            crate::validate(&doc)
                .unwrap_or_else(|errors| panic!("the {path} default is invalid: {errors:?}"));
        }

        // The wave nests inside the animation rather than beside it.
        let mut value = serde_json::to_value(&base).expect("serializes");
        value["animation"] = defaults["animation"].clone();
        value["animation"]["wave"] = defaults["animation.wave"].clone();
        let doc: crate::SkinDoc =
            serde_json::from_value(value).expect("the wave default is a wave");
        crate::validate(&doc).expect("the wave default is valid");
    }

    #[test]
    fn the_descriptor_serializes_for_the_editor() {
        let json = serde_json::to_string(&describe_skin_doc()).expect("serializes");
        assert!(json.contains("\"control\":\"color\""));
        assert!(json.contains("\"control\":\"number\""));
        assert!(json.contains("\"control\":\"optional\""));
    }

    /// Field names inside a variant are the editor's contract, and serde does
    /// *not* camel-case them for free: `rename_all` on an enum renames the
    /// variants only. Getting this wrong is silent on both sides — the editor
    /// reads `undefined` and renders a control with no bound — so the exact
    /// keys are pinned here.
    #[test]
    fn variant_fields_reach_the_editor_in_the_case_it_reads() {
        let json = serde_json::to_string(&describe_skin_doc()).expect("serializes");
        assert!(
            json.contains("\"maxLength\""),
            "the text cap must be camelCase"
        );
        assert!(
            json.contains("\"itemLabel\""),
            "the list label must be camelCase"
        );
        assert!(
            json.contains("\"itemDefault\""),
            "the list item default must be camelCase"
        );
        assert!(
            !json.contains("\"max_length\"")
                && !json.contains("\"item_label\"")
                && !json.contains("\"item_default\""),
            "no snake_case variant field may survive into the editor's schema"
        );
    }
}
