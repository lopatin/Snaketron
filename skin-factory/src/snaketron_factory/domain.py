"""Typed records at the factory/worker/provider boundaries.

The database deliberately stores JSON rather than importing these classes into
its schema.  These models are the validated boundary; the SQL rows are durable
history and can outlive a particular Python package version.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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


class AssetPlan(StrictModel):
    kind: Literal["coat", "overlay", "sheet"]
    # Defensive parser bounds keep an untrusted worker response small enough
    # to inspect. Tighter renderer limits come from the pinned capability
    # manifest before any provider spend or image allocation.
    natural_length_cells: int = Field(ge=1, le=128)
    frames: int = Field(ge=1, le=120)
    texels_per_cell: int = Field(default=16, ge=4, le=128)
    anchor: Literal["whole", "head", "tail"] = "whole"
    fit: Literal["tile", "clip", "stretch", "cutout"] = "clip"
    fade: Literal["none", "leading", "trailing", "both"] = "none"
    prompt: str = Field(min_length=1)

    @model_validator(mode="after")
    def sheet_has_shape(self) -> AssetPlan:
        if self.kind == "sheet" and self.frames < 2:
            raise ValueError("sheet assets require at least two independent frame rows")
        if self.kind != "sheet" and self.frames != 1:
            raise ValueError("static coat and overlay assets require exactly one frame")
        required_tpc = 16 if self.kind == "sheet" else 64
        if self.texels_per_cell != required_tpc:
            raise ValueError(f"{self.kind} requires {required_tpc} texels_per_cell in the current forge ladder")
        return self


class ImplementationPlan(StrictModel):
    path: Literal["layers", "texture", "sprite_sheet", "hybrid"]
    rationale: str
    fidelity_features: list[str] = Field(min_length=1)
    layer_plan: list[str]
    asset_plan: list[AssetPlan] = Field(max_length=4)
    animation_plan: list[str]
    required_wrap_axes: list[Literal["x", "y"]]
    risks: list[str]

    @model_validator(mode="after")
    def route_matches_assets(self) -> ImplementationPlan:
        needs_assets = self.path in {"texture", "sprite_sheet", "hybrid"}
        if needs_assets and not self.asset_plan:
            raise ValueError(f"{self.path} implementation requires at least one asset")
        if self.path == "layers" and self.asset_plan:
            raise ValueError("layers implementation must not request raster assets")
        if self.path == "sprite_sheet" and not any(a.kind == "sheet" for a in self.asset_plan):
            raise ValueError("sprite_sheet implementation requires a sheet asset")
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
    skin_document: dict[str, Any]
    tool_requests: list[ToolRequest] = Field(default_factory=list)
    trace: list[dict[str, Any]] = Field(default_factory=list)
    usage: dict[str, int | float] = Field(default_factory=dict)
    failure: dict[str, Any] | None = None


class ProviderResult(StrictModel):
    value: Any
    request_id: str | None = None
    resolved_model: str
    sanitized_metadata: dict[str, Any] = Field(default_factory=dict)
    usage: dict[str, int | float] = Field(default_factory=dict)


class ProviderError(RuntimeError):
    def __init__(
        self,
        kind: ProviderFailureKind,
        message: str,
        *,
        outcome_known: bool = True,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.outcome_known = outcome_known
        self.request_id = request_id


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


class ConceptProposal(StrictModel):
    name: str = Field(min_length=1, max_length=80)
    brief: str = Field(min_length=20, max_length=2_000)
    tags: list[str] = Field(min_length=1, max_length=12)
    seed: str
    palette_intent: str
    motion_intent: str
    implementation_hint: Literal["layers", "texture", "sprite_sheet", "hybrid"]
    implementation_rationale: str


class DoctorCheck(StrictModel):
    name: str
    ok: bool
    required: bool = True
    detail: str


class DoctorReport(StrictModel):
    ok: bool
    config_path: str
    checks: list[DoctorCheck]
