"""Typed SkinDoc v2 boundary used by the task-worker response schema.

Rust remains the semantic authority for document validation.  These models
mirror the serialized production handoff shape so an OpenAI-compatible strict
JSON grammar cannot emit values (notably bare hex strings at ``ColorRef``
sites) that Rust cannot deserialize in the first place.  The handoff is the
flat-layer subset of SkinDoc v2: Rust's authoring-only ``group`` node is
deliberately absent so local grammar engines never have to compile a recursive
JSON Schema.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, StrictFloat, StrictInt, StrictStr, ValidateAs


class _SkinModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


PropExpr: TypeAlias = StrictStr | StrictFloat | StrictInt
SlotName: TypeAlias = Literal["fill", "outline", "accent", "head_core"]
I32: TypeAlias = Annotated[StrictInt, Field(ge=-(2**31), le=2**31 - 1)]
U32: TypeAlias = Annotated[StrictInt, Field(ge=0, le=2**32 - 1)]
# SkinDoc is also deserialized by the wasm32 client, where Rust usize is u32.
Usize32: TypeAlias = U32


class SlotColorRef(_SkinModel):
    slot: SlotName
    lighten: PropExpr | None = None


class LiteralColorRef(_SkinModel):
    literal: StrictStr
    lighten: PropExpr | None = None


ColorRef: TypeAlias = SlotColorRef | LiteralColorRef


class TransformV2(_SkinModel):
    translate_s: PropExpr = 0
    translate_t: PropExpr = 0
    scale_s: PropExpr = 1
    scale_t: PropExpr = 1
    rotate_turns: PropExpr = 0


class AtAnchorValue(_SkinModel):
    at: StrictFloat | StrictInt


class AtAnchor(_SkinModel):
    at: AtAnchorValue


class FractionAnchorValue(_SkinModel):
    fraction: StrictFloat | StrictInt


class FractionAnchor(_SkinModel):
    fraction: FractionAnchorValue


AnchorV2: TypeAlias = Literal["whole", "head", "tail"] | AtAnchor | FractionAnchor


class SpanV2(_SkinModel):
    from_: AnchorV2 = Field(alias="from")
    natural: StrictFloat | StrictInt | None = None
    min: StrictFloat | StrictInt = 0
    priority: I32 = 0


class StopV2(_SkinModel):
    offset: PropExpr
    color: ColorRef
    alpha: PropExpr = 1


class ClipFit(_SkinModel):
    type: Literal["clip"]


class StretchFit(_SkinModel):
    type: Literal["stretch"]


class TileFit(_SkinModel):
    type: Literal["tile"]
    cells_per_repeat: StrictFloat | StrictInt | None = None


class CutoutFit(_SkinModel):
    type: Literal["cutout"]
    cells_tall: StrictFloat | StrictInt


FitV2: TypeAlias = Annotated[ClipFit | StretchFit | TileFit | CutoutFit, Field(discriminator="type")]


class FadeV2(_SkinModel):
    lead_cells: StrictFloat | StrictInt = 0
    trail_cells: StrictFloat | StrictInt = 0
    steps: Usize32 = 12


class SolidSource(_SkinModel):
    type: Literal["solid"]
    color: ColorRef


class GradientSource(_SkinModel):
    type: Literal["gradient"]
    axis: Literal["along_body", "from_start"]
    stops: list[StopV2]


class BandSource(_SkinModel):
    type: Literal["band"]
    color: ColorRef
    period_cells: StrictFloat | StrictInt
    duty: StrictFloat | StrictInt = 1
    phase_cells: StrictFloat | StrictInt = 0
    half_width: PropExpr
    t_center: PropExpr = 0
    alpha: PropExpr | None = None


class ImageSource(_SkinModel):
    type: Literal["image"]
    texture: StrictStr
    fit: FitV2
    fade: FadeV2 | None = None
    drift_cells: PropExpr = 0


class TextSource(_SkinModel):
    type: Literal["text"]
    content: StrictStr
    color: ColorRef
    scale: StrictFloat | StrictInt = 0.8


SourceV2: TypeAlias = Annotated[
    SolidSource | GradientSource | BandSource | ImageSource | TextSource,
    Field(discriminator="type"),
]


class _LayerCommon(_SkinModel):
    name: StrictStr
    boost_only: bool = False
    omit_on_single_cell: bool = False
    opacity: PropExpr = 1
    transform: TransformV2 = Field(default_factory=TransformV2)


class RibbonLayer(_LayerCommon):
    type: Literal["ribbon"]
    region: Literal["contour", "body", "head"]
    color: ColorRef
    extra_px: StrictFloat | StrictInt = 0
    joints: bool = True
    tail_cap: bool = False


class SpanLayer(_LayerCommon):
    type: Literal["span"]
    region: Literal["contour", "body", "head"]
    clip: Literal["silhouette", "cells"] = "silhouette"
    span: SpanV2
    corner: Literal["fan", "bisector"] = "fan"
    source: SourceV2


class HeadDiscLayer(_LayerCommon):
    type: Literal["head_disc"]
    paint: Literal["ramp_peak"] | ColorRef
    radius_ratio: PropExpr


class HeadRampLayer(_LayerCommon):
    type: Literal["head_ramp"]
    # This is deliberately a raw hex string in Rust; unlike ColorRef sites it
    # is not role-resolved because the ramp computes channel values per cell.
    color: StrictStr
    length_cells: StrictFloat | StrictInt


LayerV2: TypeAlias = Annotated[
    RibbonLayer | SpanLayer | HeadDiscLayer | HeadRampLayer,
    Field(discriminator="type"),
]


class ColorPair(_SkinModel):
    fill: StrictStr
    outline: StrictStr
    accent: StrictStr | None = None


class RolePalette(_SkinModel):
    friendly: list[ColorPair] = Field(min_length=2, max_length=2)
    enemy: list[ColorPair] = Field(min_length=2, max_length=2)
    free_for_all: list[ColorPair] = Field(min_length=4, max_length=4)


class LabelStyle(_SkinModel):
    ink: StrictStr | None = None
    swatch: StrictStr | None = None


class BaseTheme(_SkinModel):
    friendly_zone: StrictStr
    enemy_zone: StrictStr
    friendly_wall: StrictStr
    enemy_wall: StrictStr
    friendly_text: StrictStr
    enemy_text: StrictStr


class CelebrationTheme(_SkinModel):
    effect: StrictStr
    friendly_accent: StrictStr
    enemy_accent: StrictStr
    readout_friendly: StrictStr
    readout_enemy: StrictStr


TextureKind: TypeAlias = Literal["coat", "sheet", "overlay"]


class TextureVariantV2(_SkinModel):
    content_ref: StrictStr
    url: StrictStr
    width_px: U32
    height_px: U32
    bytes: U32
    texels_per_cell: U32


class TextureDescriptorV2(_SkinModel):
    kind: TextureKind
    body_columns: U32 | None = None
    frame_rows: U32 | None = None
    variants: list[TextureVariantV2]


class TextureRefV2(_SkinModel):
    name: StrictStr
    content_ref: StrictStr = Field(alias="ref")
    kind: TextureKind
    descriptor: TextureDescriptorV2 | None = None


class HeadCoreV2(_SkinModel):
    ratio: StrictFloat | StrictInt
    color: StrictStr


class SkinDocumentV2(_SkinModel):
    schema_version: Literal[2]
    id: StrictStr
    name: StrictStr
    palette: RolePalette
    labels: LabelStyle = Field(default_factory=LabelStyle)
    base: BaseTheme | None = None
    celebration: CelebrationTheme | None = None
    literals: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    textures: list[TextureRefV2] = Field(default_factory=list)
    period_ms: StrictFloat | StrictInt = 2_000
    head_core: HeadCoreV2
    layers: list[LayerV2] = Field(min_length=1)


def _as_plain_document(document: SkinDocumentV2) -> dict[str, Any]:
    # Preserve the worker's omission of serde-defaulted fields.  The boundary
    # validates; it does not silently rewrite the authored bytes.
    return document.model_dump(mode="json", by_alias=True, exclude_unset=True)


# Validate as the complete typed model (and therefore advertise that exact
# JSON schema), while retaining the dict API used throughout the factory.
SkinDocument: TypeAlias = Annotated[
    dict[str, Any],
    ValidateAs(SkinDocumentV2, _as_plain_document),
    PlainSerializer(lambda document: document, return_type=dict[str, Any]),
]
