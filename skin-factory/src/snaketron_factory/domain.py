"""Typed records at the factory/worker/provider boundaries.

The database deliberately stores JSON rather than importing these classes into
its schema.  These models are the validated boundary; the SQL rows are durable
history and can outlive a particular Python package version.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .skin_document import SkinDocument


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Purpose(StrEnum):
    PRODUCTION = "production"
    OPTIMIZER = "optimizer_rollout"
    TECHNIQUE = "technique_trial"
    # Durable coordinator for optimizer-side model calls. It is born terminal
    # and is never eligible for the skin build/publication state machine.
    CONTROL = "optimizer_control"


class Disposition(StrEnum):
    ACTIVE = "active"
    NEEDS_HUMAN = "needs_human"
    MACHINE_REJECTED = "machine_rejected"
    HUMAN_REJECTED = "human_rejected"
    PUBLISHED = "published"
    BLOCKED = "blocked"
    EXPERIMENT_COMPLETE = "experiment_complete"
    # The exact private revision/contentRef has been handed to Snaketron's
    # first-class Admin review queue. This is deliberately not a local Factory
    # human-review state and never implies publication.
    AWAITING_ADMIN_REVIEW = "awaiting_admin_review"


class Stage(StrEnum):
    CONCEPT = "concept"
    PROTOTYPE = "prototype"
    PROTOTYPE_TRIAGE = "prototype_triage"
    PROTOTYPE_REVIEW = "prototype_review"
    AUTHOR = "author"
    ASSETS = "assets"
    BUILD_GATE = "build_gate"
    REGISTER = "register"
    RENDER = "render"
    BUILD_TRIAGE = "build_triage"
    FINAL_REVIEW = "final_review"
    COMPLETE = "complete"


class ArtifactKind(StrEnum):
    CONCEPT_BRIEF = "concept_brief"
    PROTOTYPE = "prototype"
    PROTOTYPE_MANIFEST = "prototype_manifest"
    PROTOTYPE_SELECTION = "prototype_selection"
    DRAFT_MEDIA_PREPLAN = "draft_media_preplan"
    MEDIA_ENDPOINT_PROVIDER_OUTPUT = "media_endpoint_provider_output"
    MEDIA_ENDPOINT = "media_endpoint"
    MEDIA_ENDPOINT_SOURCE_RGBA = "media_endpoint_source_rgba"
    MEDIA_ENDPOINT_NATIVE_RGBA = "media_endpoint_native_rgba"
    MEDIA_ENDPOINT_VALIDATION_REPORT = "media_endpoint_validation_report"
    MEDIA_VIDEO = "media_video"
    MEDIA_FRAME_SHEET = "media_frame_sheet"
    MEDIA_EXTRACTED_FRAME = "media_extracted_frame"
    MEDIA_EXTRACTION_REPORT = "media_extraction_report"
    MEDIA_PROVENANCE = "media_provenance"
    MODIFIER_MANIFEST = "modifier_manifest"
    MEDIA_CATALOG = "media_catalog"
    IMPLEMENTATION_PLAN = "implementation_plan"
    SKIN_DOCUMENT = "skin_document"
    SOURCE_ASSET = "source_asset"
    FORGE_MANIFEST = "forge_manifest"
    TEXTURE_VARIANT = "texture_variant"
    CONTACT_SHEET = "contact_sheet"
    ANIMATION_CAPTURE = "animation_capture"
    RENDER_EVIDENCE = "render_evidence"
    WORKER_TRACE = "worker_trace"
    PROVIDER_RESPONSE = "provider_response"
    OPTIMIZER_CANDIDATE = "optimizer_candidate"
    TECHNIQUE_RECIPE = "technique_recipe"


class GateVerdict(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    CANDIDATE = "candidate"
    UNCERTAIN = "uncertain"
    MACHINE_REJECTED = "machine_rejected"


class OperationStatus(StrEnum):
    INTENT = "intent"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_TERMINAL = "failed_terminal"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    RESOLVED = "resolved"


class ProviderFailureKind(StrEnum):
    REFUSAL = "refusal"
    AUTHENTICATION = "authentication"
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"
    QUOTA = "quota"
    INVALID_OUTPUT = "invalid_output"
    UNKNOWN_OUTCOME = "unknown_outcome"


class PrototypeManifest(StrictModel):
    brief: str
    palette_intent: str
    motion_intent: str
    implementation_hint: Literal["layers", "texture", "sprite_sheet", "hybrid"]
    hint_rationale: str
    prompt: str
    provider_config: str = Field(alias="model_config", serialization_alias="model_config")
    image_sha256: str
    source_image_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    geometry_projection: Literal["prototype-body-mask-v1"]
    design_guidelines_sha256: str | None = None
    prototype_geometry_sha256: str | None = None
    prototype_guide_sha256: str | None = None


class AssetPlan(StrictModel):
    kind: Literal["coat", "overlay", "sheet"]
    # Defensive parser bounds keep an untrusted worker response small enough
    # to inspect. Tighter renderer limits come from the pinned capability
    # manifest before any provider spend or image allocation.
    natural_length_cells: int = Field(ge=1, le=128)
    frames: int = Field(ge=1, le=120)
    desired_fps: float | None = Field(default=None, ge=1, le=120)
    texels_per_cell: int = Field(default=16, ge=4, le=128)
    raster_overhang_px: int = Field(default=0, ge=0, le=4)
    anchor: Literal["whole", "head", "tail"] = "whole"
    fit: Literal["tile", "clip", "stretch", "cutout"] = "clip"
    tile_phase_origin: Literal["head", "tail"] | None = None
    fade: Literal["none", "leading", "trailing", "both"] = "none"
    transverse_edge_policy: Literal["fail_closed_transparent_effect", "not_applicable_opaque_fill"] = (
        "not_applicable_opaque_fill"
    )
    prompt: str = Field(min_length=1)

    @model_validator(mode="after")
    def sheet_has_shape(self) -> AssetPlan:
        if self.kind == "sheet" and self.frames < 2:
            raise ValueError("sheet assets require at least two independent frame rows")
        if self.kind == "sheet" and self.desired_fps is None:
            raise ValueError("sheet assets require a desired_fps used to derive frame rows")
        if self.kind != "sheet" and self.frames != 1:
            raise ValueError("static coat and overlay assets require exactly one frame")
        if self.kind != "sheet" and self.desired_fps is not None:
            raise ValueError("static coat and overlay assets cannot declare desired_fps")
        required_tpc = 16 if self.kind == "sheet" else 64
        if self.texels_per_cell != required_tpc:
            raise ValueError(f"{self.kind} requires {required_tpc} texels_per_cell in the current forge ladder")
        if self.fit == "tile" and self.tile_phase_origin is None:
            self.tile_phase_origin = "head"
        if self.fit != "tile" and self.tile_phase_origin is not None:
            raise ValueError("tile_phase_origin is valid only for tile fit")
        if (self.texels_per_cell * self.raster_overhang_px) % 16:
            raise ValueError("raster_overhang_px must scale exactly from the fixed 16-texel body grid")
        return self


class InputAuthorityEvidence(StrictModel):
    mode: Literal["approved_prototype", "draft_submission"]
    artifact_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    authority_record_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    human_approval_decision_id: str | None = Field(default=None, min_length=1, max_length=160)
    selection_rationale: str | None = Field(default=None, min_length=1, max_length=500)
    maximum_driver_action: Literal["register_private_revision", "request_admin_review"]

    @model_validator(mode="after")
    def mode_has_exact_authority(self) -> InputAuthorityEvidence:
        if self.mode == "approved_prototype":
            if not self.human_approval_decision_id or self.selection_rationale is not None:
                raise ValueError("approved_prototype requires a human decision and no selection rationale")
            if self.maximum_driver_action != "register_private_revision":
                raise ValueError("approved_prototype maximum action is register_private_revision")
        else:
            if self.human_approval_decision_id is not None or not self.selection_rationale:
                raise ValueError("draft_submission requires rationale and cannot claim a human decision")
            if self.maximum_driver_action != "request_admin_review":
                raise ValueError("draft_submission maximum action is request_admin_review")
        return self


class ExtractionEvidence(StrictModel):
    source_arena: Literal["reserved_empty"]
    alpha_contract: Literal["transparent_rgba", "exact_mask_matte"]
    background_removal: Literal["required"]
    matte_policy: Literal["fail_closed"]
    cropped_object_retained: Literal[True]


class VideoEvidence(StrictModel):
    start_frame_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    end_frame_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    source_video_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    extracted_sheet_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    common_period_ms: float = Field(ge=120, le=60_000)
    desired_fps: float = Field(ge=1, le=120)
    derived_frame_rows: int = Field(ge=2, le=120)
    effective_frame_row_cap: int = Field(ge=2, le=120)
    frame_extraction: Literal["deterministic_uniform_full_period"]
    alpha_matte_verification: Literal["fail_closed"]
    loop_closure: Literal["true_final_to_zero"]
    retained_inputs_and_output: Literal[True]


class ModifierPlan(StrictModel):
    asset_index: int = Field(ge=0, le=7)
    logical_key: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    component_key: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,31}$")
    texture_name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]*$")
    image_layer_name: str = Field(min_length=1, max_length=120)
    fallback_layer_name: str = Field(min_length=1, max_length=120)
    span_limit_mode: Literal["whole", "head_cells", "tail_fraction"] = "whole"
    span_limit_value: float | None = Field(default=None, gt=0, le=6)
    source_mode: Literal["direct_generate", "extracted_rgba", "reused_object", "video_frames"]
    source_object_sha256: str | None = Field(default=None, pattern=r"^sha256:[a-f0-9]{64}$")
    modifier_manifest_sha256: str | None = Field(default=None, pattern=r"^sha256:[a-f0-9]{64}$")
    provenance_sha256: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    license_id: str = Field(min_length=1, max_length=120)
    authorized_lineage_ids: list[str] = Field(min_length=1)
    required_capabilities: list[str] = Field(default_factory=list)
    extraction: ExtractionEvidence | None = None
    video: VideoEvidence | None = None

    @model_validator(mode="after")
    def source_is_bound(self) -> ModifierPlan:
        if self.span_limit_mode == "whole" and self.span_limit_value is not None:
            raise ValueError("whole modifier span cannot declare a numeric limit")
        if self.span_limit_mode == "head_cells" and self.span_limit_value is None:
            raise ValueError("head modifier span requires a cell limit no greater than 6")
        if self.span_limit_mode == "tail_fraction" and (self.span_limit_value is None or self.span_limit_value > 0.5):
            raise ValueError("tail modifier span requires a fraction no greater than 0.5")
        source_bound = self.source_object_sha256 is not None
        manifest_bound = self.modifier_manifest_sha256 is not None
        fully_bound = source_bound and manifest_bound
        if self.source_mode == "direct_generate":
            if source_bound or manifest_bound or self.extraction is not None or self.video is not None:
                raise ValueError("direct generation cannot claim bound extraction/video evidence")
        elif self.source_mode == "extracted_rgba":
            if not fully_bound or self.extraction is None or self.video is not None:
                raise ValueError("extracted_rgba requires bound object/manifest and extraction evidence")
        elif self.source_mode == "reused_object":
            if not fully_bound or self.extraction is not None or self.video is not None:
                raise ValueError("reused_object requires an exact object/manifest")
        elif not fully_bound or self.extraction is None or self.video is None:
            raise ValueError("video_frames requires bound object/manifest, extraction, and video evidence")
        elif self.video.extracted_sheet_sha256 != self.source_object_sha256:
            raise ValueError("video_frames source object must be the exact extracted sheet bytes")
        return self


class DesignGuidelinesEvidence(StrictModel):
    """Bounded proof that the locked visual contract informed authoring."""

    artistic_direction: str = Field(min_length=1, max_length=240)
    concept_twist: str = Field(min_length=1, max_length=240)
    structure: Literal["pattern", "sprite"]
    body_strategy: str = Field(min_length=1, max_length=320)
    head_zone: Literal["light_field_dark_core", "dark_field_light_disc_dark_core"]
    asset_strategy: str = Field(min_length=1, max_length=320)


class ImplementationPlan(StrictModel):
    # Optional defaults preserve parsing of retained v1 plans. The pinned v2
    # output schema requires all three fields for every new worker result.
    input_authority: InputAuthorityEvidence | None = None
    path: Literal["layers", "texture", "sprite_sheet", "hybrid"]
    rationale: str
    fidelity_features: list[str] = Field(min_length=1)
    layer_plan: list[str]
    # Eight is the defensive parser ceiling advertised by the current
    # renderer contract. Live validation still applies the exact pinned
    # capability limit, which can be lower for retained deployments.
    asset_plan: list[AssetPlan] = Field(max_length=8)
    modifier_plan: list[ModifierPlan] = Field(default_factory=list, max_length=8)
    common_period_ms: float | None = Field(default=None, ge=120, le=60_000)
    animation_plan: list[str]
    required_wrap_axes: list[Literal["x", "y"]]
    risks: list[str]
    design_guidelines: DesignGuidelinesEvidence

    @model_validator(mode="after")
    def route_matches_assets(self) -> ImplementationPlan:
        needs_assets = self.path in {"texture", "sprite_sheet", "hybrid"}
        if needs_assets and not self.asset_plan:
            raise ValueError(f"{self.path} implementation requires at least one asset")
        if self.path == "layers" and self.asset_plan:
            raise ValueError("layers implementation must not request raster assets")
        if self.path == "sprite_sheet" and not any(a.kind == "sheet" for a in self.asset_plan):
            raise ValueError("sprite_sheet implementation requires a sheet asset")
        if self.modifier_plan:
            indexes = [item.asset_index for item in self.modifier_plan]
            if sorted(indexes) != list(range(len(self.asset_plan))):
                raise ValueError("modifier_plan must map one-to-one to asset_plan indexes")
        return self


class ToolRequest(StrictModel):
    kind: Literal["generate_asset", "edit_asset", "forge_asset", "render_preview"]
    arguments: dict[str, Any]


class WorkerRequest(StrictModel):
    request_id: str
    attempt_id: str
    purpose: Purpose
    skill_sha256: str
    skill_files: dict[str, str]
    capability_manifest: dict[str, Any]
    artifact_refs: dict[str, str]
    authoring_inputs: dict[str, Any]
    inline_artifacts: dict[str, InlineArtifact] = Field(default_factory=dict)
    pure_tools: list[str] = Field(default_factory=list)
    budget: dict[str, int | float]
    output_schemas: dict[str, dict[str, Any]]
    feedback: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def only_pure_tools(self) -> WorkerRequest:
        forbidden = {
            "shell",
            "network",
            "http",
            "provider",
            "storage",
            "git",
            "publish",
            "upload",
        }
        bad = sorted(forbidden.intersection(x.lower() for x in self.pure_tools))
        if bad:
            raise ValueError(f"side-effecting worker tools are forbidden: {', '.join(bad)}")
        return self


class InlineArtifact(StrictModel):
    content_hash: str
    media_type: str
    base64_data: str


class WorkerResult(StrictModel):
    implementation_plan: ImplementationPlan
    skin_document: SkinDocument
    tool_requests: list[ToolRequest] = Field(default_factory=list)
    trace: list[dict[str, Any]] = Field(default_factory=list)
    usage: dict[str, bool | int | float] = Field(default_factory=dict)
    failure: dict[str, Any] | None = None


def current_worker_result_json_schema() -> dict[str, Any]:
    """Return the strict schema for a new AUTHOR call, not retained v1 replay.

    The Pydantic models keep defaults so old retained JSON can still be parsed
    by explicitly versioned legacy paths. A live worker must spell out every
    plan, asset, authority, and modifier field so defaults cannot hide missing
    current-contract evidence.
    """

    schema = WorkerResult.model_json_schema()
    for definition_name in (
        "ImplementationPlan",
        "AssetPlan",
        "InputAuthorityEvidence",
        "ModifierPlan",
    ):
        definition = schema["$defs"][definition_name]
        definition["required"] = list(definition["properties"])
    return schema


class ProviderResult(StrictModel):
    value: Any
    request_id: str | None = None
    resolved_model: str
    sanitized_metadata: dict[str, Any] = Field(default_factory=dict)
    usage: dict[str, bool | int | float] = Field(default_factory=dict)


class ProviderError(RuntimeError):
    def __init__(
        self,
        kind: ProviderFailureKind,
        message: str,
        *,
        outcome_known: bool = True,
        request_id: str | None = None,
        resolved_model: str | None = None,
        halt_generation: bool = False,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.outcome_known = outcome_known
        self.request_id = request_id
        self.resolved_model = resolved_model
        self.halt_generation = halt_generation


class GateResult(StrictModel):
    gate: str
    gate_version: str
    blocking: bool
    verdict: GateVerdict
    reasons: list[str] = Field(default_factory=list)
    measurements: dict[str, Any] = Field(default_factory=dict)


class VisualJudgment(StrictModel):
    verdict: Literal["candidate", "uncertain", "machine_rejected"]
    reasons: list[str] = Field(min_length=1)
    fidelity: float = Field(ge=0, le=1)
    readability: float = Field(ge=0, le=1)
    role_clarity: float = Field(ge=0, le=1)
    animation_quality: float = Field(ge=0, le=1)
    craft: float = Field(ge=0, le=1)
    review_flags: list[
        Literal[
            "protected_mark",
            "public_figure_likeness",
            "unsafe_content",
            "unlicensed_reference",
            "other",
        ]
    ] = Field(default_factory=list)


class ConceptProposal(StrictModel):
    name: str = Field(min_length=1, max_length=80)
    brief: str = Field(min_length=20, max_length=2_000)
    tags: list[str] = Field(min_length=1, max_length=12)
    seed: str
    palette_intent: str
    motion_intent: str
    implementation_hint: Literal["layers", "texture", "sprite_sheet", "hybrid"]
    implementation_rationale: str
    novelty_score: float = Field(ge=0, le=1)
    direction_score: float = Field(ge=0, le=1)
    novelty_rationale: str = Field(min_length=10, max_length=1_000)


class DoctorCheck(StrictModel):
    name: str
    ok: bool
    required: bool = True
    detail: str


class DoctorReport(StrictModel):
    ok: bool
    config_path: str
    checks: list[DoctorCheck]
