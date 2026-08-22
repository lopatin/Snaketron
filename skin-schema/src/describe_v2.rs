//! What a v2 layer document contains, as data a layers panel can render.
//!
//! Same promise as [`crate::describe`] and the same enforcement: the editor's
//! controls are generated from this, and the [`exhaustiveness`] module below
//! matches every enum and destructures every struct in the v2 vocabulary, so a
//! variant added without a control stops this file compiling.
//!
//! The shape differs from v1's because the *editor* differs. v1 was one form
//! over one document, so one tree of fields was the right description. v2 is a
//! stack and an inspector, so this hands back three things: the fields that
//! belong to the document itself, the fields that describe whichever layer is
//! selected, and the palette of layers "add" can insert. Trying to express a
//! reorderable stack as a nested field tree would have produced a description
//! the panel then ignored, which is the failure mode the whole approach exists
//! to avoid.

use crate::v2::{
    ClipV2, CornerV2, DiscPaintName, EvalSite, MAX_GRADIENT_STOPS, MAX_TEXT_CONTENT_LEN, RegionV2,
    SlotName, TEXT_CHARSET,
};
use serde::{Deserialize, Serialize};

/// Never called, and that is the point.
///
/// Every v2 enum is matched and every struct destructured, so adding a source,
/// a layer kind, a region or a field stops the build until the panel knows how
/// to show it.
#[allow(dead_code)]
mod exhaustiveness {
    use super::*;
    use crate::v2::{
        AnchorV2, ColorRef, ColorTarget, DiscPaintV2, FadeV2, FitV2, GradientAxis, HeadCoreV2,
        LayerBodyV2, LayerV2, SkinDocV2, SourceV2, SpanV2, StopV2, TextureKindV2, TextureRefV2,
        TransformV2,
    };

    fn document(value: SkinDocV2) {
        let SkinDocV2 {
            schema_version: _,
            id: _,
            name: _,
            palette: _,
            labels: _,
            base: _,
            celebration: _,
            literals: _,
            textures: _,
            period_ms: _,
            head_core: _,
            layers: _,
        } = value;
    }

    fn layer(value: LayerV2) {
        let LayerV2 {
            name: _,
            boost_only: _,
            omit_on_single_cell: _,
            opacity: _,
            transform: _,
            body,
        } = value;
        match body {
            LayerBodyV2::Group { layers: _ } => {}
            LayerBodyV2::Ribbon {
                region: _,
                color: _,
                extra_px: _,
                joints: _,
                tail_cap: _,
            } => {}
            LayerBodyV2::Span {
                region: _,
                clip: _,
                span: _,
                corner: _,
                source: _,
            } => {}
            LayerBodyV2::HeadDisc {
                paint: _,
                radius_ratio: _,
            } => {}
            LayerBodyV2::HeadRamp {
                color: _,
                length_cells: _,
            } => {}
        }
    }

    fn source(value: SourceV2) {
        match value {
            SourceV2::Solid { color: _ } => {}
            SourceV2::Gradient { axis: _, stops: _ } => {}
            SourceV2::Band {
                color: _,
                period_cells: _,
                duty: _,
                phase_cells: _,
                half_width: _,
                t_center: _,
                alpha: _,
            } => {}
            SourceV2::Image {
                texture: _,
                fit: _,
                fade: _,
                drift_cells: _,
            } => {}
            SourceV2::Text {
                content: _,
                color: _,
                scale: _,
            } => {}
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parts(
        transform: TransformV2,
        span: SpanV2,
        stop: StopV2,
        fade: FadeV2,
        core: HeadCoreV2,
        texture: TextureRefV2,
        descriptor: crate::v2::TextureDescriptorV2,
        variant: crate::v2::TextureVariantV2,
        reference: ColorRef,
    ) {
        let TransformV2 {
            translate_s: _,
            translate_t: _,
            scale_s: _,
            scale_t: _,
            rotate_turns: _,
        } = transform;
        let SpanV2 {
            from: _,
            natural: _,
            min: _,
            priority: _,
        } = span;
        let StopV2 {
            offset: _,
            color: _,
            alpha: _,
        } = stop;
        let FadeV2 {
            lead_cells: _,
            trail_cells: _,
            steps: _,
        } = fade;
        let HeadCoreV2 { ratio: _, color: _ } = core;
        let TextureRefV2 {
            name: _,
            content_ref: _,
            kind: _,
            descriptor: _,
        } = texture;
        let crate::v2::TextureDescriptorV2 {
            kind: _,
            body_columns: _,
            frame_rows: _,
            variants: _,
        } = descriptor;
        let crate::v2::TextureVariantV2 {
            content_ref: _,
            url: _,
            width_px: _,
            height_px: _,
            bytes: _,
            texels_per_cell: _,
        } = variant;
        let ColorRef { target, lighten: _ } = reference;
        match target {
            ColorTarget::Slot { slot: _ } => {}
            ColorTarget::Literal { literal: _ } => {}
        }
    }

    fn small_enums(
        anchor: AnchorV2,
        axis: GradientAxis,
        fit: FitV2,
        paint: DiscPaintV2,
        kind: TextureKindV2,
    ) {
        match anchor {
            AnchorV2::Whole | AnchorV2::Head | AnchorV2::Tail => {}
            AnchorV2::At { at: _ } => {}
            AnchorV2::Fraction { fraction: _ } => {}
        }
        match axis {
            GradientAxis::AlongBody | GradientAxis::FromStart => {}
        }
        match fit {
            FitV2::Clip | FitV2::Stretch => {}
            FitV2::Tile {
                cells_per_repeat: _,
            } => {}
            FitV2::Cutout { cells_tall: _ } => {}
        }
        match paint {
            DiscPaintV2::Named(DiscPaintName::RampPeak) => {}
            DiscPaintV2::Ref(_) => {}
        }
        match kind {
            TextureKindV2::Coat | TextureKindV2::Sheet | TextureKindV2::Overlay => {}
        }
    }

    /// Every region, as the panel lists them.
    pub fn regions() -> Vec<(&'static str, &'static str, &'static str)> {
        [RegionV2::Contour, RegionV2::Body, RegionV2::Head]
            .into_iter()
            .map(|region| match region {
                RegionV2::Contour => (
                    "contour",
                    "Contour",
                    "Outside the snake. The only place a layer may paint wider \
                     than the body.",
                ),
                RegionV2::Body => ("body", "Body", "Inside the snake's outline."),
                RegionV2::Head => ("head", "Head", "The head cell."),
            })
            .collect()
    }

    pub fn clips() -> Vec<(&'static str, &'static str)> {
        [ClipV2::Silhouette, ClipV2::Cells]
            .into_iter()
            .map(|clip| match clip {
                ClipV2::Silhouette => ("silhouette", "The rounded outline"),
                ClipV2::Cells => ("cells", "The square cells"),
            })
            .collect()
    }

    pub fn corners() -> Vec<(&'static str, &'static str)> {
        [CornerV2::Fan, CornerV2::Bisector]
            .into_iter()
            .map(|corner| match corner {
                CornerV2::Fan => ("fan", "Fan"),
                CornerV2::Bisector => ("bisector", "Mitre"),
            })
            .collect()
    }

    pub fn slots() -> Vec<(&'static str, &'static str)> {
        [
            SlotName::Fill,
            SlotName::Outline,
            SlotName::Accent,
            SlotName::HeadCore,
        ]
        .into_iter()
        .map(|slot| match slot {
            SlotName::Fill => ("fill", "Body fill"),
            SlotName::Outline => ("outline", "Outline"),
            SlotName::Accent => ("accent", "Accent"),
            SlotName::HeadCore => ("head_core", "Head core"),
        })
        .collect()
    }
}

/// One editable thing in a v2 document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldV2 {
    /// Path relative to whatever this describes — the document for document
    /// fields, the selected layer for layer fields.
    pub path: String,
    pub label: String,
    pub kind: KindV2,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// The control a panel should render.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "control", rename_all = "camelCase")]
pub enum KindV2 {
    Color,
    /// A colour that is either a palette slot or a named literal. Rendered as
    /// a picker over the slots plus whatever literals the document declares,
    /// which is why the panel needs the document and not only this node.
    ColorRef {
        slots: Vec<ChoiceV2>,
    },
    Text {
        #[serde(rename = "maxLength")]
        max_length: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        charset: Option<String>,
    },
    Toggle,
    Number {
        min: f64,
        max: f64,
        step: f64,
    },
    /// A number that may instead be an expression.
    ///
    /// Rendered as a slider with an **fx** toggle, because a constant *is* a
    /// constant expression — the two states are one value seen two ways, not
    /// two different fields. `inputs` is what the fx editor offers and what an
    /// error will hold the author to; `site` is the cost chip beside it, so
    /// where the cost lands is visible where it is incurred.
    Expression {
        min: f64,
        max: f64,
        step: f64,
        inputs: Vec<String>,
        site: EvalSite,
        #[serde(rename = "siteLabel")]
        site_label: String,
    },
    Choice {
        options: Vec<ChoiceV2>,
    },
    Group {
        children: Vec<FieldV2>,
    },
    Optional {
        children: Vec<FieldV2>,
    },
    List {
        #[serde(rename = "itemLabel")]
        item_label: String,
        #[serde(rename = "itemDefault")]
        item_default: serde_json::Value,
        #[serde(rename = "minItems")]
        min_items: usize,
        #[serde(rename = "maxItems")]
        max_items: usize,
        children: Vec<FieldV2>,
    },
    /// A tagged union: one key selects which set of fields applies.
    ///
    /// This is the kind v1's descriptor could not express and the reason the
    /// Builder had to hard-code anything at all. A layer *is* a variant, and a
    /// source *is* a variant; without this the panel would need its own copy
    /// of the vocabulary, which is precisely the drift the descriptor exists
    /// to prevent.
    Variant {
        tag: String,
        options: Vec<VariantV2>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChoiceV2 {
    pub value: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VariantV2 {
    pub value: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    /// What switching to this option inserts. Has to *validate*, for the same
    /// reason v1's optional sections carry defaults: being handed an error for
    /// choosing a menu item is a worse experience than not offering it.
    pub default: serde_json::Value,
    pub children: Vec<FieldV2>,
}

/// Everything a layers panel needs to render itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaV2 {
    /// Fields belonging to the document rather than to any layer.
    pub document: Vec<FieldV2>,
    /// The inspector for whichever layer is selected.
    pub layer: Vec<FieldV2>,
    /// What "+ Add layer" offers.
    pub add_layer: Vec<VariantV2>,
    /// The two layers the panel shows locked, so the stacking it draws is the
    /// stacking that happens.
    pub system_layers: Vec<SystemLayer>,
    /// Budgets, so the panel's meter and the validator agree by construction.
    pub limits: Limits,
    /// The first-party art a document may name, with the kind each one is.
    ///
    /// A texture layer names a texture the *document* has to declare, and the
    /// panel has to write that declaration when an author picks from the menu
    /// — which it can only do correctly if it knows whether the art is a coat
    /// or a sheet. Sending the catalogue is what keeps that knowledge on one
    /// side of the wasm boundary instead of copied to both.
    pub builtin_textures: Vec<BuiltinTextureV2>,
}

/// What a new texture layer starts as: a coat, because a coat is the kind that
/// looks like something on any body, where a sheet on a four-cell snake is one
/// frame of an animation nobody can see yet.
///
/// Named rather than indexed, because an index into the catalogue quietly means
/// something else the moment the catalogue is reordered.
const DEFAULT_TEXTURE_ID: &str = "jaguar.v1";

/// The catalogue entry [`DEFAULT_TEXTURE_ID`] names.
fn default_texture() -> &'static crate::v2::BuiltinTexture {
    crate::v2::builtin_texture(&format!(
        "{}{DEFAULT_TEXTURE_ID}",
        crate::v2::BUILTIN_TEXTURE_PREFIX
    ))
    .expect("the default texture is in the catalogue")
}

/// One catalogue entry, as the panel needs it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinTextureV2 {
    pub id: String,
    pub label: String,
    /// `coat` or `sheet`, spelled as the document spells it.
    pub kind: String,
    /// The reference a document puts in its `textures` entry.
    pub content_ref: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemLayer {
    pub name: String,
    /// Where it sits: `top` composites over everything, `bottom` under.
    pub position: String,
    pub help: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Limits {
    pub max_layers: usize,
    pub max_ops: usize,
    pub max_textures: usize,
}

fn field(path: &str, label: &str, kind: KindV2) -> FieldV2 {
    FieldV2 {
        path: path.to_string(),
        label: label.to_string(),
        kind,
        help: None,
        default: None,
    }
}

fn explained(path: &str, label: &str, kind: KindV2, help: &str) -> FieldV2 {
    FieldV2 {
        help: Some(help.to_string()),
        ..field(path, label, kind)
    }
}

fn choice(value: &str, label: &str) -> ChoiceV2 {
    ChoiceV2 {
        value: value.to_string(),
        label: label.to_string(),
        help: None,
    }
}

fn site_label(site: EvalSite) -> String {
    match site {
        EvalSite::Palette => "per step",
        EvalSite::Snake => "per snake",
        EvalSite::Cell => "per cell",
        EvalSite::Bounded => "per step, bounded",
    }
    .to_string()
}

/// An expression-valued control, with the inputs its site actually knows.
fn expr(path: &str, label: &str, site: EvalSite, min: f64, max: f64, step: f64) -> FieldV2 {
    field(
        path,
        label,
        KindV2::Expression {
            min,
            max,
            step,
            inputs: site
                .allowed_inputs()
                .iter()
                .map(|input| input.name().to_string())
                .collect(),
            site,
            site_label: site_label(site),
        },
    )
}

fn color_ref(path: &str, label: &str) -> FieldV2 {
    field(
        path,
        label,
        KindV2::ColorRef {
            slots: exhaustiveness::slots()
                .into_iter()
                .map(|(value, label)| choice(value, label))
                .collect(),
        },
    )
}

/// The transform, shared by every layer kind that has one.
fn transform_fields(prefix: &str) -> FieldV2 {
    let at = |name: &str| format!("{prefix}.{name}");
    field(
        prefix,
        "Transform",
        KindV2::Group {
            children: vec![
                expr(
                    &at("translate_s"),
                    "Along body",
                    EvalSite::Snake,
                    -32.0,
                    32.0,
                    0.1,
                ),
                expr(
                    &at("translate_t"),
                    "Across body",
                    EvalSite::Snake,
                    -1.0,
                    1.0,
                    0.01,
                ),
                expr(
                    &at("scale_s"),
                    "Scale along",
                    EvalSite::Snake,
                    0.1,
                    8.0,
                    0.05,
                ),
                expr(
                    &at("scale_t"),
                    "Scale across",
                    EvalSite::Snake,
                    0.1,
                    8.0,
                    0.05,
                ),
                expr(
                    &at("rotate_turns"),
                    "Rotate",
                    EvalSite::Snake,
                    -1.0,
                    1.0,
                    0.01,
                ),
            ],
        },
    )
}

fn region_choice(path: &str) -> FieldV2 {
    field(
        path,
        "Region",
        KindV2::Choice {
            options: exhaustiveness::regions()
                .into_iter()
                .map(|(value, label, help)| ChoiceV2 {
                    value: value.to_string(),
                    label: label.to_string(),
                    help: Some(help.to_string()),
                })
                .collect(),
        },
    )
}

fn span_fields() -> FieldV2 {
    field(
        "span",
        "Where on the body",
        KindV2::Group {
            children: vec![
                field(
                    "span.from",
                    "Anchored to",
                    KindV2::Choice {
                        options: vec![
                            choice("whole", "The whole body"),
                            choice("head", "The head"),
                            choice("tail", "The tail"),
                        ],
                    },
                ),
                explained(
                    "span.natural",
                    "Length",
                    KindV2::Number {
                        min: 1.0,
                        max: 64.0,
                        step: 1.0,
                    },
                    "In cells. Leave empty for whatever is left.",
                ),
                explained(
                    "span.min",
                    "Minimum",
                    KindV2::Number {
                        min: 0.0,
                        max: 64.0,
                        step: 1.0,
                    },
                    "Below this the layer is left out rather than squeezed — \
                     snakes start at three cells, so this is the ordinary case.",
                ),
                field(
                    "span.priority",
                    "Priority",
                    KindV2::Number {
                        min: -100.0,
                        max: 100.0,
                        step: 1.0,
                    },
                ),
            ],
        },
    )
}

fn stop_children() -> Vec<FieldV2> {
    vec![
        expr("offset", "Position", EvalSite::Snake, 0.0, 1.0, 0.01),
        color_ref("color", "Colour"),
        expr("alpha", "Opacity", EvalSite::Snake, 0.0, 1.0, 0.01),
    ]
}

/// The five sources, each with what only it needs.
fn source_variants() -> Vec<VariantV2> {
    vec![
        VariantV2 {
            value: "solid".to_string(),
            label: "Solid".to_string(),
            help: Some("One flat colour.".to_string()),
            default: serde_json::json!({ "type": "solid", "color": { "slot": "fill" } }),
            children: vec![color_ref("source.color", "Colour")],
        },
        VariantV2 {
            value: "gradient".to_string(),
            label: "Gradient".to_string(),
            help: Some(
                "Colours blending between stops. Give a stop's position an \
                 expression that reads the clock and the blend travels — which \
                 is all a shine ever was."
                    .to_string(),
            ),
            default: serde_json::json!({
                "type": "gradient",
                "axis": "along_body",
                "stops": [
                    { "offset": 0.0, "color": { "slot": "fill" }, "alpha": 0.0 },
                    { "offset": 1.0, "color": { "slot": "accent" }, "alpha": 0.6 }
                ]
            }),
            children: vec![
                field(
                    "source.axis",
                    "Direction",
                    KindV2::Choice {
                        options: vec![
                            choice("along_body", "Along the body"),
                            choice("from_start", "Outward from the start"),
                        ],
                    },
                ),
                field(
                    "source.stops",
                    "Stops",
                    KindV2::List {
                        item_label: "Stop".to_string(),
                        item_default: serde_json::json!({
                            "offset": 0.5,
                            "color": { "slot": "accent" },
                            "alpha": 0.6
                        }),
                        min_items: crate::v2::MIN_GRADIENT_STOPS,
                        max_items: MAX_GRADIENT_STOPS,
                        children: stop_children(),
                    },
                ),
            ],
        },
        VariantV2 {
            value: "band".to_string(),
            label: "Band".to_string(),
            help: Some("A stripe repeating along the body.".to_string()),
            default: serde_json::json!({
                "type": "band",
                "color": { "slot": "accent" },
                "period_cells": 4.0,
                "duty": 0.5,
                "phase_cells": 0.0,
                "half_width": 0.2,
                "t_center": 0.0
            }),
            children: vec![
                color_ref("source.color", "Colour"),
                explained(
                    "source.period_cells",
                    "Repeat every",
                    KindV2::Number {
                        min: 0.5,
                        max: 32.0,
                        step: 0.5,
                    },
                    "In cells. Fixed rather than animatable: changing it \
                     changes how many stripes there are, and a skin has to \
                     paint the same number of things at every moment.",
                ),
                field(
                    "source.duty",
                    "Stripe share",
                    KindV2::Number {
                        min: 0.0,
                        max: 1.0,
                        step: 0.05,
                    },
                ),
                field(
                    "source.phase_cells",
                    "Offset",
                    KindV2::Number {
                        min: -32.0,
                        max: 32.0,
                        step: 0.5,
                    },
                ),
                expr(
                    "source.half_width",
                    "Half width",
                    EvalSite::Bounded,
                    0.0,
                    0.5,
                    0.01,
                ),
                expr(
                    "source.t_center",
                    "Across body",
                    EvalSite::Bounded,
                    -0.5,
                    0.5,
                    0.01,
                ),
            ],
        },
        VariantV2 {
            value: "image".to_string(),
            label: "Texture".to_string(),
            help: Some("Art you generated or uploaded.".to_string()),
            default: serde_json::json!({
                "type": "image",
                "texture": default_texture().id,
                "fit": { "type": "tile" },
                "drift_cells": 0.0
            }),
            children: vec![
                explained(
                    "source.texture",
                    "Texture",
                    // A menu of the art the client ships, not a box to type a
                    // name into. Nothing in the panel ever let an author
                    // *declare* a texture, so a free-text field could only
                    // ever name one that did not exist — the seven pieces of
                    // first-party art the renderer already paints were
                    // unreachable from the editor built to use them.
                    KindV2::Choice {
                        options: crate::v2::BUILTIN_TEXTURES
                            .iter()
                            .map(|art| ChoiceV2 {
                                value: art.id.to_string(),
                                label: art.label.to_string(),
                                help: Some(match art.kind {
                                    crate::v2::TextureKindV2::Sheet => {
                                        "Frames, played as an animation.".to_string()
                                    }
                                    _ => "One strip, worn down the body.".to_string(),
                                }),
                            })
                            .collect(),
                    },
                    "Art the client ships, and anything you have made. Picking one declares it for you.",
                ),
                field(
                    "source.fit.type",
                    "How it sits",
                    KindV2::Choice {
                        options: vec![
                            choice("tile", "Repeat along the body"),
                            choice("clip", "Draw once from the start"),
                            choice("stretch", "Stretch to fit"),
                            choice("cutout", "Show through the snake"),
                        ],
                    },
                ),
                expr(
                    "source.drift_cells",
                    "Drift",
                    EvalSite::Snake,
                    -8.0,
                    8.0,
                    0.1,
                ),
            ],
        },
        VariantV2 {
            value: "text".to_string(),
            label: "Text".to_string(),
            help: Some("Letters along the body, one per cell.".to_string()),
            default: serde_json::json!({
                "type": "text",
                "content": "SNAKE",
                "color": { "slot": "accent" },
                "scale": 0.8
            }),
            children: vec![
                field(
                    "source.content",
                    "Words",
                    KindV2::Text {
                        max_length: MAX_TEXT_CONTENT_LEN,
                        charset: Some(TEXT_CHARSET.to_string()),
                    },
                ),
                explained(
                    "source.color",
                    "Ink",
                    KindV2::ColorRef {
                        slots: exhaustiveness::slots()
                            .into_iter()
                            .map(|(value, label)| choice(value, label))
                            .collect(),
                    },
                    "Letters come in light or dark, and this picks whichever \
                     your colour is closer to — per side, so a word stays \
                     legible on both. Canvas cannot tint letters the way it \
                     tints a shape.",
                ),
                field(
                    "source.scale",
                    "Size",
                    KindV2::Number {
                        min: 0.1,
                        max: 1.0,
                        step: 0.05,
                    },
                ),
            ],
        },
    ]
}

/// Every field a layer always has, spelled out.
///
/// A variant's default has to be *complete*, not merely valid. The schema is
/// happy to fill `transform` in from serde defaults, but the panel is not: an
/// absent value reads as "no constant here", which flips an expression field
/// into its fx view showing an empty box. Valid document, baffling editor.
fn with_common_fields(mut body: serde_json::Value) -> serde_json::Value {
    let map = body.as_object_mut().expect("a layer default is an object");
    map.entry("opacity")
        .or_insert_with(|| serde_json::json!(1.0));
    map.entry("boost_only")
        .or_insert_with(|| serde_json::json!(false));
    map.entry("omit_on_single_cell")
        .or_insert_with(|| serde_json::json!(false));
    map.entry("transform").or_insert_with(|| {
        serde_json::json!({
            "translate_s": 0.0,
            "translate_t": 0.0,
            "scale_s": 1.0,
            "scale_t": 1.0,
            "rotate_turns": 0.0
        })
    });
    body
}

/// The five layer kinds an author can add.
fn layer_variants() -> Vec<VariantV2> {
    let common = |extra: Vec<FieldV2>| extra;

    let variants = vec![
        VariantV2 {
            value: "ribbon".to_string(),
            label: "Ribbon".to_string(),
            help: Some(
                "The snake's own shape, filled with one colour. This is what a \
                 body and an outline are."
                    .to_string(),
            ),
            default: serde_json::json!({
                "type": "ribbon",
                "name": "Ribbon",
                "region": "body",
                "color": { "slot": "fill" },
                "extra_px": 0.0,
                "joints": true,
                "tail_cap": true,
                "opacity": 1.0
            }),
            children: common(vec![
                region_choice("region"),
                color_ref("color", "Colour"),
                explained(
                    "extra_px",
                    "Extra width",
                    KindV2::Number {
                        min: crate::MIN_OUTLINE_EXTRA_PX,
                        max: crate::MAX_OUTLINE_EXTRA_PX,
                        step: 0.5,
                    },
                    "How far past the body it paints. Only a contour ribbon \
                     may do this at all.",
                ),
                field("joints", "Round the corners", KindV2::Toggle),
                field("tail_cap", "Round the tail", KindV2::Toggle),
            ]),
        },
        VariantV2 {
            value: "span".to_string(),
            label: "Painted stretch".to_string(),
            help: Some(
                "A stretch of the body painted with something: a colour, a \
                 gradient, a stripe, a texture, or text."
                    .to_string(),
            ),
            default: serde_json::json!({
                "type": "span",
                "name": "Stretch",
                "region": "body",
                "clip": "silhouette",
                "span": { "from": "whole", "min": 0.0, "priority": 0 },
                "corner": "fan",
                "source": { "type": "solid", "color": { "slot": "accent" } },
                "opacity": 0.5
            }),
            children: common(vec![
                region_choice("region"),
                field(
                    "clip",
                    "Kept inside",
                    KindV2::Choice {
                        options: exhaustiveness::clips()
                            .into_iter()
                            .map(|(value, label)| choice(value, label))
                            .collect(),
                    },
                ),
                span_fields(),
                field(
                    "corner",
                    "At corners",
                    KindV2::Choice {
                        options: exhaustiveness::corners()
                            .into_iter()
                            .map(|(value, label)| choice(value, label))
                            .collect(),
                    },
                ),
                field(
                    "source",
                    "Painted with",
                    KindV2::Variant {
                        tag: "type".to_string(),
                        options: source_variants(),
                    },
                ),
            ]),
        },
        VariantV2 {
            value: "head_disc".to_string(),
            label: "Head disc".to_string(),
            help: Some("A circle on the head cell.".to_string()),
            default: serde_json::json!({
                "type": "head_disc",
                "name": "Head disc",
                "paint": { "slot": "accent" },
                "radius_ratio": 0.4,
                "opacity": 1.0
            }),
            children: common(vec![
                color_ref("paint", "Colour"),
                expr(
                    "radius_ratio",
                    "Size",
                    EvalSite::Bounded,
                    crate::MIN_HEAD_CORE_RATIO,
                    0.5,
                    0.01,
                ),
            ]),
        },
        VariantV2 {
            value: "head_ramp".to_string(),
            label: "Head glow".to_string(),
            help: Some(
                "A brightening behind the head, so a glance tells you which \
                 way a snake is going. Its opacity is the whole curve — write \
                 one that reads `s` and you have reshaped the falloff."
                    .to_string(),
            ),
            default: serde_json::json!({
                "type": "head_ramp",
                "name": "Head glow",
                "color": "#ffffff",
                "length_cells": 10.0,
                "omit_on_single_cell": true,
                "opacity": "(1 - s / 10) * 0.3"
            }),
            children: common(vec![
                field("color", "Colour", KindV2::Color),
                field(
                    "length_cells",
                    "Reaches back",
                    KindV2::Number {
                        min: 0.0,
                        max: 64.0,
                        step: 1.0,
                    },
                ),
            ]),
        },
        VariantV2 {
            value: "group".to_string(),
            label: "Group".to_string(),
            help: Some("Holds other layers so they can be moved and faded together.".to_string()),
            default: serde_json::json!({
                "type": "group",
                "name": "Group",
                "opacity": 1.0,
                "layers": []
            }),
            children: Vec::new(),
        },
    ];

    variants
        .into_iter()
        .map(|variant| VariantV2 {
            default: with_common_fields(variant.default),
            ..variant
        })
        .collect()
}

/// The inspector: what every layer has, then what its kind adds.
fn layer_fields() -> Vec<FieldV2> {
    vec![
        field(
            "name",
            "Name",
            KindV2::Text {
                max_length: 48,
                charset: None,
            },
        ),
        explained(
            "opacity",
            "Opacity",
            KindV2::Expression {
                min: 0.0,
                max: 1.0,
                step: 0.01,
                inputs: EvalSite::Snake
                    .allowed_inputs()
                    .iter()
                    .map(|input| input.name().to_string())
                    .collect(),
                site: EvalSite::Snake,
                site_label: site_label(EvalSite::Snake),
            },
            "Switch to fx and give it `time` to make it pulse, or `boost` to \
             make it react.",
        ),
        field("boost_only", "Only while boosting", KindV2::Toggle),
        field(
            "omit_on_single_cell",
            "Skip on a one-cell snake",
            KindV2::Toggle,
        ),
        transform_fields("transform"),
        field(
            "type",
            "Kind",
            KindV2::Variant {
                tag: "type".to_string(),
                options: layer_variants(),
            },
        ),
    ]
}

/// Fields belonging to the document rather than to any layer.
fn document_fields() -> Vec<FieldV2> {
    let pair = |index: usize, group: &str, label: &str| {
        let prefix = format!("palette.{group}.{index}");
        field(
            &prefix,
            label,
            KindV2::Group {
                children: vec![
                    field(&format!("{prefix}.fill"), "Fill", KindV2::Color),
                    field(&format!("{prefix}.outline"), "Outline", KindV2::Color),
                    FieldV2 {
                        default: Some(serde_json::json!("#ffffff")),
                        help: Some(
                            "A third colour that flips with the side. Left \
                             alone, it is your fill."
                                .to_string(),
                        ),
                        ..field(
                            &format!("{prefix}.accent"),
                            "Accent",
                            KindV2::Optional {
                                children: vec![field(
                                    &format!("{prefix}.accent"),
                                    "Colour",
                                    KindV2::Color,
                                )],
                            },
                        )
                    },
                ],
            },
        )
    };

    let mut palette = Vec::new();
    for (index, shade) in ["light", "dark"].iter().enumerate() {
        palette.push(pair(index, "friendly", &format!("Friendly ({shade})")));
    }
    for (index, shade) in ["light", "dark"].iter().enumerate() {
        palette.push(pair(index, "enemy", &format!("Enemy ({shade})")));
    }
    for index in 0..4 {
        palette.push(pair(
            index,
            "free_for_all",
            &format!("Free-for-all {}", index + 1),
        ));
    }

    vec![
        field(
            "name",
            "Name",
            KindV2::Text {
                max_length: 48,
                charset: None,
            },
        ),
        explained(
            "period_ms",
            "Cycle length",
            KindV2::Number {
                min: crate::MIN_ANIMATION_PERIOD_MS,
                max: crate::MAX_ANIMATION_PERIOD_MS,
                step: 10.0,
            },
            "How long one full cycle takes. Only matters to fields whose \
             expression reads `time`.",
        ),
        field("palette", "Colours", KindV2::Group { children: palette }),
        field(
            "head_core",
            "Head core",
            KindV2::Group {
                children: vec![
                    field(
                        "head_core.ratio",
                        "Size",
                        KindV2::Number {
                            min: crate::MIN_HEAD_CORE_RATIO,
                            max: crate::MAX_HEAD_CORE_RATIO,
                            step: 0.01,
                        },
                    ),
                    explained(
                        "head_core.color",
                        "Colour",
                        KindV2::Color,
                        "Keep it dark: the roster paints a white tick on it \
                         when a player is ready.",
                    ),
                ],
            },
        ),
        field(
            "labels",
            "Labels",
            KindV2::Group {
                children: vec![
                    FieldV2 {
                        default: Some(serde_json::json!("#1f2937")),
                        ..field(
                            "labels.ink",
                            "Ink",
                            KindV2::Optional {
                                children: vec![field("labels.ink", "Colour", KindV2::Color)],
                            },
                        )
                    },
                    FieldV2 {
                        default: Some(serde_json::json!("#1f2937")),
                        ..field(
                            "labels.swatch",
                            "Menu swatch",
                            KindV2::Optional {
                                children: vec![field("labels.swatch", "Colour", KindV2::Color)],
                            },
                        )
                    },
                ],
            },
        ),
    ]
}

/// Everything the layers panel renders itself from.
pub fn describe_v2() -> SchemaV2 {
    SchemaV2 {
        document: document_fields(),
        layer: layer_fields(),
        add_layer: layer_variants(),
        system_layers: vec![
            SystemLayer {
                name: "Head core".to_string(),
                position: "top".to_string(),
                help: "Painted over everything, so art may cross the head \
                       freely. Its colour and size are yours; its place in the \
                       stack is not."
                    .to_string(),
            },
            SystemLayer {
                name: "Boost band".to_string(),
                position: "bottom".to_string(),
                help: "The widest thing your snake paints, and only while \
                       boosting. Opponents read it to know you are boosting, \
                       so no skin may restyle or bury it."
                    .to_string(),
            },
        ],
        limits: Limits {
            max_layers: crate::v2::MAX_LAYERS,
            max_ops: crate::v2::MAX_OPS_PER_SNAKE,
            max_textures: crate::v2::MAX_TEXTURE_REFS,
        },
        builtin_textures: crate::v2::BUILTIN_TEXTURES
            .iter()
            .map(|art| BuiltinTextureV2 {
                id: art.id.to_string(),
                label: art.label.to_string(),
                kind: match art.kind {
                    crate::v2::TextureKindV2::Sheet => "sheet".to_string(),
                    crate::v2::TextureKindV2::Overlay => "overlay".to_string(),
                    crate::v2::TextureKindV2::Coat => "coat".to_string(),
                },
                content_ref: format!("{}{}", crate::v2::BUILTIN_TEXTURE_PREFIX, art.id),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Declare the art the image default names.
    ///
    /// Read off the same constant the default is built from, so the two cannot
    /// drift — and only that one, because a document may hold four textures and
    /// the catalogue has seven.
    fn declare_default_texture(doc: &mut crate::v2::SkinDocV2) {
        let art = default_texture();
        doc.textures.push(crate::v2::TextureRefV2 {
            name: art.id.to_string(),
            content_ref: format!("{}{}", crate::v2::BUILTIN_TEXTURE_PREFIX, art.id),
            kind: art.kind,
            descriptor: None,
        });
    }
    use crate::v2::{LayerV2, SkinDocV2, SourceV2, validate_v2};

    fn base_document() -> SkinDocV2 {
        let v1: crate::SkinDoc =
            serde_json::from_str(include_str!("../skins/classic.skin.json")).expect("parses");
        crate::v2::upgrade(&v1)
    }

    /// Every "add" menu item has to produce a layer that validates. Being
    /// handed an error for choosing a menu item is the failure this default
    /// exists to prevent, and it is only a guarantee if it is checked.
    #[test]
    fn every_layer_the_menu_offers_is_a_layer_that_validates() {
        for variant in describe_v2().add_layer {
            let layer: LayerV2 =
                serde_json::from_value(variant.default.clone()).unwrap_or_else(|error| {
                    panic!("`{}` does not deserialize: {error}", variant.value)
                });

            // A group with no children is rejected on purpose, so it is
            // checked with one inside rather than excused.
            let layer = match &layer.body {
                crate::v2::LayerBodyV2::Group { .. } => LayerV2 {
                    body: crate::v2::LayerBodyV2::Group {
                        layers: vec![
                            serde_json::from_value(
                                describe_v2()
                                    .add_layer
                                    .iter()
                                    .find(|other| other.value == "ribbon")
                                    .expect("ribbon exists")
                                    .default
                                    .clone(),
                            )
                            .expect("a ribbon deserializes"),
                        ],
                    },
                    ..layer
                },
                _ => layer,
            };

            let mut doc = base_document();
            // A texture layer names a texture, so the document has to declare
            // one for the default to be checkable at all — and it has to be the
            // one the menu's default actually picks, which is the point of the
            // menu: a document may only name art it declares.
            declare_default_texture(&mut doc);
            doc.layers.push(layer);

            if let Err(errors) = validate_v2(&doc) {
                panic!(
                    "`{}` inserts a layer that does not validate: {errors:?}",
                    variant.value
                );
            }
        }
    }

    /// A default has to be *complete*, not merely valid.
    ///
    /// Serde will happily fill a missing `transform` in, so a layer without one
    /// validates — but the panel reads an absent value as "no constant here"
    /// and flips the field into its expression view showing an empty box. The
    /// document is fine and the editor looks broken, which is the worst of both.
    #[test]
    fn every_layer_default_spells_out_the_fields_every_layer_has() {
        for variant in describe_v2().add_layer {
            let object = variant
                .default
                .as_object()
                .unwrap_or_else(|| panic!("`{}` is not an object", variant.value));
            for key in ["opacity", "transform", "boost_only", "omit_on_single_cell"] {
                assert!(
                    object.contains_key(key),
                    "`{}` omits `{key}`, which leaves the panel showing an \
                     empty control for a value the schema quietly defaulted",
                    variant.value
                );
            }
            let transform = object["transform"]
                .as_object()
                .expect("the transform is an object");
            for key in [
                "translate_s",
                "translate_t",
                "scale_s",
                "scale_t",
                "rotate_turns",
            ] {
                assert!(
                    transform.contains_key(key),
                    "`{}` omits `transform.{key}`",
                    variant.value
                );
            }
        }
    }

    /// Every source the menu offers, likewise.
    #[test]
    fn every_source_the_menu_offers_is_a_source_that_validates() {
        for variant in source_variants() {
            let source: SourceV2 =
                serde_json::from_value(variant.default.clone()).unwrap_or_else(|error| {
                    panic!("`{}` does not deserialize: {error}", variant.value)
                });

            let mut doc = base_document();
            declare_default_texture(&mut doc);
            doc.layers.push(LayerV2 {
                name: "Test".to_string(),
                boost_only: false,
                omit_on_single_cell: false,
                opacity: crate::v2::PropExpr::constant(0.5),
                transform: crate::v2::TransformV2::default(),
                body: crate::v2::LayerBodyV2::Span {
                    region: crate::v2::RegionV2::Body,
                    clip: crate::v2::ClipV2::Silhouette,
                    span: crate::v2::SpanV2::whole(),
                    corner: crate::v2::CornerV2::Fan,
                    source,
                },
            });

            if let Err(errors) = validate_v2(&doc) {
                panic!(
                    "`{}` inserts a source that does not validate: {errors:?}",
                    variant.value
                );
            }
        }
    }

    /// An expression control must offer exactly the inputs its site knows.
    /// Offering more would autocomplete an author straight into a validation
    /// error; offering fewer would hide a capability.
    #[test]
    fn expression_controls_offer_exactly_what_their_site_allows() {
        fn walk(fields: &[FieldV2], seen: &mut usize) {
            for node in fields {
                match &node.kind {
                    KindV2::Expression { inputs, site, .. } => {
                        *seen += 1;
                        let expected: Vec<String> = site
                            .allowed_inputs()
                            .iter()
                            .map(|input| input.name().to_string())
                            .collect();
                        assert_eq!(inputs, &expected, "at {}", node.path);
                    }
                    KindV2::Group { children }
                    | KindV2::Optional { children }
                    | KindV2::List { children, .. } => walk(children, seen),
                    KindV2::Variant { options, .. } => {
                        for option in options {
                            walk(&option.children, seen);
                        }
                    }
                    _ => {}
                }
            }
        }

        let schema = describe_v2();
        let mut seen = 0;
        walk(&schema.document, &mut seen);
        walk(&schema.layer, &mut seen);
        assert!(
            seen >= 8,
            "only {seen} expression controls — too few to be right"
        );
    }

    /// The panel is generated, so the schema has to carry the vocabulary
    /// rather than the panel keeping a copy.
    #[test]
    fn the_schema_names_every_layer_kind_and_source() {
        let schema = describe_v2();
        let layer_kinds: Vec<&str> = schema
            .add_layer
            .iter()
            .map(|variant| variant.value.as_str())
            .collect();
        assert_eq!(
            layer_kinds,
            vec!["ribbon", "span", "head_disc", "head_ramp", "group"]
        );

        let sources: Vec<String> = source_variants()
            .into_iter()
            .map(|variant| variant.value)
            .collect();
        assert_eq!(sources, vec!["solid", "gradient", "band", "image", "text"]);
    }

    #[test]
    fn the_schema_serializes_for_the_editor() {
        let json = serde_json::to_value(describe_v2()).expect("serializes");
        // The renamed keys the editor reads. `rename_all` renames variants,
        // not the fields inside them, so each of these is explicit — and a
        // silent snake_case leak here would make the panel read `undefined`.
        let text = json.to_string();
        for key in [
            "\"maxLength\"",
            "\"itemLabel\"",
            "\"itemDefault\"",
            "\"siteLabel\"",
        ] {
            assert!(text.contains(key), "missing {key}");
        }
        assert!(!text.contains("max_length"), "snake_case leaked");
        assert!(!text.contains("item_label"), "snake_case leaked");
        assert!(!text.contains("site_label"), "snake_case leaked");
    }
}
