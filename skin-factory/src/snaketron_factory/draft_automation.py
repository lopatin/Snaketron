"""Durable prompt inbox for queue-only private skin drafts.

The Factory owns execution under its existing production lease. This module
only admits immutable user concepts and marks them imported after a behavior-
pinned Attempt exists. Human authority remains entirely in Snaketron Admin.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .db import canonical_json
from .domain import ArtifactKind, Purpose, Stage

if TYPE_CHECKING:
    from .factory import Factory


class DraftInboxError(RuntimeError):
    """The inbox cannot advance without changing retained operator input."""


_TRANSITION_SECTIONS = (
    "[Cinematography]",
    "[Subject]",
    "[Action / Transition]",
    "[Context]",
    "[Style & Ambiance]",
)


class DraftMediaFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=500)
    missing_capabilities: list[str] = Field(default_factory=list)


class DraftVideoIntent(BaseModel):
    """A hash-free request; only the driver may bind exact media evidence."""

    model_config = ConfigDict(extra="forbid")

    intent_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    logical_key: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    component_key: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,31}$")
    texture_name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]*$")
    anchor: Literal["whole", "head", "tail"]
    span_limit_mode: Literal["whole", "head_cells", "tail_fraction"]
    span_limit_value: float | None = Field(default=None, gt=0, le=6)
    # The fixed 1080px Fal arena retains a 32px safety apron on each side.
    # floor((1080 - 64) / 16) = 63; reject larger plans before endpoint spend.
    body_columns: int = Field(ge=1, le=63)
    desired_fps: float = Field(ge=1, le=60)
    common_period_ms: float = Field(ge=120, le=60_000)
    raster_overhang_px: int = Field(ge=0, le=4)
    matte_rgb: tuple[Literal[127], Literal[127], Literal[127]]
    start_frame_prompt: str = Field(min_length=1, max_length=3_000)
    end_frame_prompt: str = Field(min_length=1, max_length=3_000)
    # The driver adds its immutable safety/geometry wrapper before Fal. Keep
    # the model-authored action segment bounded so the final request remains
    # within PixVerse's exact 2048-byte prompt ceiling.
    transition_prompt: str = Field(min_length=1, max_length=1_024)
    seed: int = Field(ge=0, le=2_147_483_647)
    authorized_lineage_scope: Literal["current_concept_only"]

    @model_validator(mode="after")
    def exact_span_and_prompt(self) -> DraftVideoIntent:
        expected_mode = {"whole": "whole", "head": "head_cells", "tail": "tail_fraction"}[self.anchor]
        if self.span_limit_mode != expected_mode:
            raise ValueError("video intent span mode differs from its anchor")
        if self.span_limit_mode == "whole" and self.span_limit_value is not None:
            raise ValueError("whole video intent cannot declare a span limit")
        if self.span_limit_mode == "head_cells" and self.span_limit_value is None:
            raise ValueError("head video intent requires a span limit")
        if self.span_limit_mode == "tail_fraction" and (self.span_limit_value is None or self.span_limit_value > 0.5):
            raise ValueError("tail video intent must be bounded to at most half the snake")
        section_counts = [self.transition_prompt.count(section) for section in _TRANSITION_SECTIONS]
        if any(count != 1 for count in section_counts):
            raise ValueError("transition prompt must contain each of the five literal sections exactly once")
        offsets = [self.transition_prompt.find(section) for section in _TRANSITION_SECTIONS]
        if offsets != sorted(offsets):
            raise ValueError("transition prompt must contain all five literal sections in order")
        if len(self.transition_prompt.encode("utf-8")) > 1_024:
            raise ValueError("model transition segment exceeds its 1024-byte UTF-8 budget")
        row_texels = 16 + 2 * self.raster_overhang_px
        derived_rows = max(2, math.ceil(self.common_period_ms * self.desired_fps / 1_000))
        effective_cap = min(
            120,
            2_048 // row_texels,
            16_777_216 // (self.body_columns * 16 * row_texels * 4),
        )
        if derived_rows > effective_cap:
            raise ValueError(
                f"video intent derives {derived_rows} frame rows but its exact geometry cap is {effective_cap}"
            )
        return self


class DraftMediaPreplan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    decision: Literal["procedural_only", "video_intents", "platform_gap"]
    video_intents: list[DraftVideoIntent] = Field(default_factory=list, max_length=2)
    notes: list[str] = Field(default_factory=list, max_length=12)
    failure: DraftMediaFailure | None = None

    @model_validator(mode="after")
    def decision_matches_payload(self) -> DraftMediaPreplan:
        if self.decision == "video_intents" and (not self.video_intents or self.failure is not None):
            raise ValueError("video_intents decision requires intents and no failure")
        if self.decision == "procedural_only" and (self.video_intents or self.failure is not None):
            raise ValueError("procedural_only cannot contain media intents or failure")
        if self.decision == "platform_gap" and (self.video_intents or self.failure is None):
            raise ValueError("platform_gap requires only a typed failure")
        if len({item.intent_id for item in self.video_intents}) != len(self.video_intents):
            raise ValueError("video intent ids must be unique")
        for field in ("logical_key", "texture_name", "component_key"):
            values = [getattr(item, field) for item in self.video_intents]
            if len(set(values)) != len(values):
                raise ValueError(f"video intent {field} values must be unique")
        return self


class DraftPrompt(BaseModel):
    """One immutable concept supplied by a human or a separate ideation agent."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    queue_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{7,79}$")
    name: str = Field(min_length=1, max_length=80)
    brief: str = Field(min_length=20, max_length=2_000)
    seed: str = Field(min_length=1, max_length=160)
    tags: list[str] = Field(min_length=1, max_length=12)
    palette_intent: str = Field(min_length=1, max_length=500)
    motion_intent: str = Field(min_length=1, max_length=500)
    implementation_hint: Literal["layers", "texture", "sprite_sheet", "hybrid"]
    implementation_rationale: str = Field(min_length=1, max_length=1_000)

    @classmethod
    def from_prompt(
        cls,
        *,
        name: str,
        brief: str,
        motion_intent: str,
        palette_intent: str = "Preserve the concept's most readable game-scale palette.",
        implementation_hint: Literal["layers", "texture", "sprite_sheet", "hybrid"] = "hybrid",
        implementation_rationale: str = "Let author-skin choose the smallest faithful supported representation.",
        tags: list[str] | None = None,
        queue_id: str | None = None,
        seed: str | None = None,
    ) -> DraftPrompt:
        identity = queue_id or f"draft-{uuid.uuid4().hex}"
        return cls(
            queue_id=identity,
            name=name,
            brief=brief,
            seed=seed or identity,
            tags=tags or ["direct-draft"],
            palette_intent=palette_intent,
            motion_intent=motion_intent,
            implementation_hint=implementation_hint,
            implementation_rationale=implementation_rationale,
        )


def is_draft_attempt(attempt: dict[str, Any]) -> bool:
    try:
        behavior = json.loads(attempt["behavior_json"])
    except (KeyError, TypeError, json.JSONDecodeError):
        return False
    automation = behavior.get("draft_automation")
    return isinstance(automation, dict) and automation.get("authority_mode") == "draft_submission"


def draft_attempt_metadata(attempt: dict[str, Any]) -> dict[str, Any]:
    if not is_draft_attempt(attempt):
        raise DraftInboxError("Attempt is not a queue-authorized direct draft")
    behavior = json.loads(attempt["behavior_json"])
    metadata = behavior["draft_automation"]
    if not isinstance(metadata.get("queue_id"), str) or not isinstance(metadata.get("request_sha256"), str):
        raise DraftInboxError("draft Attempt lacks its exact queue authority")
    return metadata


class DraftInbox:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _directory(self, name: str = "") -> Path:
        directory = self.root if not name else self.root / name
        if directory.is_symlink():
            raise DraftInboxError(f"draft inbox directory cannot be a symlink: {directory}")
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
        return directory

    @staticmethod
    def _bytes(item: DraftPrompt) -> bytes:
        return (canonical_json(item.model_dump(mode="json")) + "\n").encode("utf-8")

    @staticmethod
    def request_sha256(item: DraftPrompt) -> str:
        return "sha256:" + hashlib.sha256(DraftInbox._bytes(item).rstrip(b"\n")).hexdigest()

    def enqueue(self, item: DraftPrompt) -> Path:
        inbox = self._directory()
        path = inbox / f"{item.queue_id}.json"
        if path.is_symlink():
            raise DraftInboxError(f"draft queue item cannot be a symlink: {path}")
        payload = self._bytes(item)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            if path.is_file() and path.read_bytes() == payload:
                return path
            raise DraftInboxError(f"queue id already names different bytes: {item.queue_id}") from None
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return path

    def pending(self) -> list[tuple[Path, DraftPrompt]]:
        inbox = self._directory()
        result: list[tuple[Path, DraftPrompt]] = []
        for path in sorted(inbox.glob("*.json")):
            details = path.lstat()
            if not stat.S_ISREG(details.st_mode) or details.st_mode & 0o077:
                raise DraftInboxError(f"draft queue item must be an owner-private regular file: {path}")
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                item = DraftPrompt.model_validate(payload)
            except (OSError, ValueError) as error:
                raise DraftInboxError(f"invalid draft queue item {path.name}: {error}") from error
            if path.stem != item.queue_id or re.fullmatch(r"[a-z0-9][a-z0-9_-]{7,79}", path.stem) is None:
                raise DraftInboxError(f"draft queue filename does not equal its queue_id: {path.name}")
            result.append((path, item))
        return result

    def import_next(self, factory: Factory) -> dict[str, Any] | None:
        pending = self.pending()
        if not pending:
            return None
        path, item = pending[0]
        request_sha = self.request_sha256(item)
        idempotency_key = f"direct-draft:{item.queue_id}"
        attempt = factory.database.find_attempt_by_key(idempotency_key)
        if attempt is None:
            behavior = factory.behavior_snapshot()
            behavior["draft_automation"] = {
                "schema_version": 1,
                "authority_mode": "draft_submission",
                "queue_id": item.queue_id,
                "request_sha256": request_sha,
                "maximum_driver_action": "request_admin_review",
            }
            concept = factory.database.create_concept(
                name=item.name,
                brief=item.brief,
                seed=item.seed,
                source="direct-draft-inbox",
                tags=item.tags,
            )
            attempt = factory.database.create_attempt(
                concept_id=concept["id"],
                purpose=Purpose.PRODUCTION,
                stage=Stage.PROTOTYPE,
                idempotency_key=idempotency_key,
                behavior=behavior,
                direction_sha=behavior["direction_sha"],
                skill_sha=behavior["skill_sha"],
                capability_sha=behavior["capability_sha"],
                gate_sha=behavior["gate_sha"],
                model_config_sha=behavior["model_config_sha"],
            )
        else:
            metadata = draft_attempt_metadata(attempt)
            if metadata["request_sha256"] != request_sha:
                raise DraftInboxError("retained draft Attempt differs from the current queue bytes")
        concept_brief = factory._find_lineage_artifact(attempt, ArtifactKind.CONCEPT_BRIEF)
        if concept_brief is None:
            factory._store_json_artifact(
                attempt,
                Stage.CONCEPT,
                ArtifactKind.CONCEPT_BRIEF,
                item.model_dump(mode="json"),
                metadata={"queue_id": item.queue_id, "request_sha256": request_sha},
                occurrence_key="direct-draft-input",
            )
        elif concept_brief["content_hash"] != request_sha:
            raise DraftInboxError("retained draft concept artifact differs from exact queue bytes")
        self._retire(path, item)
        return factory.database.get_attempt(attempt["id"])

    def _retire(self, source: Path, item: DraftPrompt) -> None:
        processed = self._directory("processed")
        target = processed / source.name
        if target.exists():
            if target.is_symlink() or target.read_bytes() != source.read_bytes():
                raise DraftInboxError(f"processed queue id names different bytes: {item.queue_id}")
            source.unlink()
            return
        os.replace(source, target)
        os.chmod(target, 0o600)

    def status(self) -> dict[str, Any]:
        pending = self.pending()
        processed_dir = self._directory("processed")
        return {
            "inbox": str(self.root),
            "pending": [item.queue_id for _, item in pending],
            "processed": len(list(processed_dir.glob("*.json"))),
        }
