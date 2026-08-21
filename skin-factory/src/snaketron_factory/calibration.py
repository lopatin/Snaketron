"""Independent visual-judge calibration and fail-closed routing policy."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from statistics import NormalDist
from typing import Any, Literal

from .config import FactoryConfig
from .db import Database

JudgeKind = Literal["prototype", "build"]

PROTOTYPE_JUDGE_RUBRIC = (
    "The first image is the pinned blank Snaketron geometry guide and the second is the candidate "
    "after deterministic projection through that guide's native 15px-per-cell body mask. Judge the "
    "candidate as a game-scale Snaketron direction. Its outer silhouette and system head core must "
    "match the guide: one flat, right-facing, continuous 16-by-1-cell capsule with a rounded one-cell "
    "head and rounded tail. The surrounding review canvas and empty padding are not snake geometry; "
    "never reject merely because a provider's source canvas had another aspect ratio. Internal pattern "
    "dividers or stylistic depth are allowed when they remain clipped inside one continuous silhouette; "
    "do not call them articulated body pieces unless the outer body actually gaps or separates. Detached "
    "pieces, an oversized separate head, a pointed tail, multiple snakes, or paint outside the round body "
    "are machine_rejected geometry violations. Also reject detail, text, or motion cues that collapse in "
    "the nearest-upscaled native-scale pixels. Return candidate, uncertain, or machine_rejected. Flag "
    "protected marks, public-figure likeness, unsafe content, or an apparently unlicensed reference. "
    "Any safety/IP flag routes to machine_rejected for human review; machine judgment never approves "
    "or waives it."
)
BUILD_JUDGE_RUBRIC = (
    "Compare the real Snaketron render to the human-selected prototype. Judge fidelity, "
    "game-scale readability, role clarity, animation, and craft. Independently flag protected "
    "marks, public-figure likeness, unsafe content, or an apparently unlicensed reference in "
    "the completed build. Fidelity is soft routing; any safety/IP flag is a blocking gate."
)

_ACTIONS: dict[JudgeKind, str] = {
    "prototype": "prototype_label",
    "build": "build_quality_label",
}
_POSITIVE_VERDICTS = {"candidate", "uncertain"}
_NEGATIVE_VERDICTS = {"machine_rejected", "rejected"}
_ACCEPT_TAGS = {"outcome:accept", "accept", "accepted"}
_REJECT_TAGS = {"outcome:reject", "reject", "rejected"}


def judge_evaluator_version(
    config: FactoryConfig,
    kind: JudgeKind,
    *,
    resolved_model: str | None = None,
) -> str:
    """Bind calibration to both the pinned model and exact rubric text."""

    rubric = PROTOTYPE_JUDGE_RUBRIC if kind == "prototype" else BUILD_JUDGE_RUBRIC
    digest = hashlib.sha256((rubric + "\nvisual-judgment-schema-v1").encode()).hexdigest()[:16]
    model = resolved_model or config.models.visual_judge.model or "unconfigured"
    return f"{model}+rubric:{digest}"


@dataclass(frozen=True)
class CalibrationMetrics:
    kind: JudgeKind
    evaluator_version: str
    sample_size: int
    confidence_level: float
    true_positive: int
    true_negative: int
    false_positive: int
    false_negative: int
    uncertain_count: int
    precision: float
    recall: float
    false_approve_rate: float
    false_reject_rate: float
    reversal_rate: float
    uncertainty_rate: float
    accuracy_lower: float
    accuracy_upper: float
    false_approve_upper: float
    false_reject_upper: float
    reversal_upper: float
    uncertainty_upper: float
    latest_label_at: str | None
    stale: bool

    def as_report(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RoutingStatus:
    kind: JudgeKind
    enabled: bool
    reasons: tuple[str, ...]
    calibration: dict[str, Any] | None

    def as_report(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "enabled": self.enabled,
            "reasons": list(self.reasons),
            "calibration": self.calibration,
        }


def wilson_interval(successes: int, total: int, confidence: float) -> tuple[float, float]:
    """Two-sided Wilson score interval; an empty denominator fails closed."""

    if successes < 0 or total < 0 or successes > total:
        raise ValueError("Wilson inputs must satisfy 0 <= successes <= total")
    if total == 0:
        return (0.0, 1.0)
    z = NormalDist().inv_cdf(1 - (1 - confidence) / 2)
    proportion = successes / total
    z2 = z * z
    denominator = 1 + z2 / total
    center = (proportion + z2 / (2 * total)) / denominator
    margin = z * math.sqrt((proportion * (1 - proportion) + z2 / (4 * total)) / total) / denominator
    return (max(0.0, center - margin), min(1.0, center + margin))


class JudgeCalibrationService:
    def __init__(self, database: Database, config: FactoryConfig) -> None:
        self.database = database
        self.config = config

    def refresh_all(self, *, at: datetime | None = None) -> dict[str, dict[str, Any]]:
        report: dict[str, dict[str, Any]] = {}
        for kind in ("prototype", "build"):
            typed_kind: JudgeKind = kind
            configured = judge_evaluator_version(self.config, typed_kind)
            active_version = self.active_evaluator_version(typed_kind)
            observed = self.database.judge_evaluator_versions(action=_ACTIONS[typed_kind])
            # Refresh every actual resolved-model/rubric identity independently.
            # The configured alias is also retained as a fail-closed status row
            # until a provider reports the concrete dated model behind it.
            versions = [*observed]
            for version in (active_version, configured):
                if version not in versions:
                    versions.append(version)
            refreshed = {
                version: self.refresh(typed_kind, evaluator_version=version, at=at).as_report() for version in versions
            }
            active = refreshed[active_version]
            report[kind] = {
                **active,
                "active_evaluator_version": active_version,
                "configured_evaluator_alias": configured,
                "observed_evaluator_versions": observed,
                "calibrations": refreshed,
            }
        return report

    def active_evaluator_version(self, kind: JudgeKind) -> str:
        """Resolve the evaluator identity currently routing this judge kind."""

        configured = judge_evaluator_version(self.config, kind)
        _model, separator, rubric = configured.partition("+rubric:")
        if not separator:
            return configured
        actual = self.database.latest_judge_evaluator_version(rubric_suffix=f"+rubric:{rubric}")
        return actual or configured

    def refresh(
        self,
        kind: JudgeKind,
        *,
        evaluator_version: str | None = None,
        at: datetime | None = None,
    ) -> CalibrationMetrics:
        evaluator_version = evaluator_version or judge_evaluator_version(self.config, kind)
        rows = self.database.judge_calibration_examples(action=_ACTIONS[kind], evaluator_version=evaluator_version)
        # The first decision and newest preceding evaluation appear first. A
        # later label has already seen the revealed evaluation, and the same
        # exact pixels never become a second independent sample, even when a
        # re-evaluation links them into a new Attempt/Artifact row.
        samples: list[tuple[bool, str, str]] = []
        seen: set[str] = set()
        for row in rows:
            content_hash = str(row["content_hash"])
            if content_hash in seen:
                continue
            outcome = _human_outcome(row["tags_json"])
            verdict = str(row["verdict"])
            if outcome is None or verdict not in _POSITIVE_VERDICTS | _NEGATIVE_VERDICTS:
                continue
            seen.add(content_hash)
            samples.append((outcome, verdict, str(row["label_created_at"])))

        true_positive = sum(human and verdict in _POSITIVE_VERDICTS for human, verdict, _ in samples)
        true_negative = sum(not human and verdict in _NEGATIVE_VERDICTS for human, verdict, _ in samples)
        false_positive = sum(not human and verdict in _POSITIVE_VERDICTS for human, verdict, _ in samples)
        false_negative = sum(human and verdict in _NEGATIVE_VERDICTS for human, verdict, _ in samples)
        uncertain_count = sum(verdict == "uncertain" for _, verdict, _ in samples)
        sample_size = len(samples)
        latest_label_at = max((created_at for _, _, created_at in samples), default=None)

        positive_predictions = true_positive + false_positive
        human_positives = true_positive + false_negative
        human_negatives = false_positive + true_negative
        machine_rejects = false_negative + true_negative
        correct = true_positive + true_negative
        precision = _ratio(true_positive, positive_predictions)
        recall = _ratio(true_positive, human_positives)
        false_approve_rate = _ratio(false_positive, human_negatives)
        false_reject_rate = _ratio(false_negative, human_positives)
        reversal_rate = _ratio(false_negative, machine_rejects)
        uncertainty_rate = _ratio(uncertain_count, sample_size)
        confidence = self.config.judge_calibration.confidence_level
        accuracy_lower, accuracy_upper = wilson_interval(correct, sample_size, confidence)
        false_approve_upper = wilson_interval(false_positive, human_negatives, confidence)[1]
        false_reject_upper = wilson_interval(false_negative, human_positives, confidence)[1]
        reversal_upper = wilson_interval(false_negative, machine_rejects, confidence)[1]
        uncertainty_upper = wilson_interval(uncertain_count, sample_size, confidence)[1]
        timestamp = at or datetime.now(UTC)
        stale = _is_stale(
            latest_label_at,
            timestamp,
            timedelta(days=self.config.judge_calibration.max_label_age_days),
        )
        metrics = CalibrationMetrics(
            kind=kind,
            evaluator_version=evaluator_version,
            sample_size=sample_size,
            confidence_level=confidence,
            true_positive=true_positive,
            true_negative=true_negative,
            false_positive=false_positive,
            false_negative=false_negative,
            uncertain_count=uncertain_count,
            precision=precision,
            recall=recall,
            false_approve_rate=false_approve_rate,
            false_reject_rate=false_reject_rate,
            reversal_rate=reversal_rate,
            uncertainty_rate=uncertainty_rate,
            accuracy_lower=accuracy_lower,
            accuracy_upper=accuracy_upper,
            false_approve_upper=false_approve_upper,
            false_reject_upper=false_reject_upper,
            reversal_upper=reversal_upper,
            uncertainty_upper=uncertainty_upper,
            latest_label_at=latest_label_at,
            stale=stale,
        )
        self.database.set_judge_calibration(
            kind=kind,
            evaluator_version=evaluator_version,
            sample_size=sample_size,
            true_positive=true_positive,
            true_negative=true_negative,
            false_positive=false_positive,
            false_negative=false_negative,
            lower_confidence=accuracy_lower,
            upper_confidence=accuracy_upper,
            stale=stale,
            precision=precision,
            recall=recall,
            false_approve_rate=false_approve_rate,
            false_reject_rate=false_reject_rate,
            reversal_rate=reversal_rate,
            uncertainty_rate=uncertainty_rate,
            false_approve_upper=false_approve_upper,
            false_reject_upper=false_reject_upper,
            reversal_upper=reversal_upper,
            uncertainty_upper=uncertainty_upper,
            uncertain_count=uncertain_count,
            latest_label_at=latest_label_at,
        )
        return metrics

    def routing_status(
        self,
        kind: JudgeKind,
        *,
        evaluator_version: str | None = None,
        at: datetime | None = None,
    ) -> RoutingStatus:
        quality = self.quality_status(kind, evaluator_version=evaluator_version, at=at)
        if self.config.mode == "production":
            return quality
        return RoutingStatus(
            kind,
            False,
            ("configured_shadow_mode", *quality.reasons),
            quality.calibration,
        )

    def quality_status(
        self,
        kind: JudgeKind,
        *,
        evaluator_version: str | None = None,
        at: datetime | None = None,
    ) -> RoutingStatus:
        """Assess calibration independent of the deployment's routing mode."""

        expected = evaluator_version or self.active_evaluator_version(kind)
        row = self.database.judge_calibration(kind, expected)
        if row is None:
            previous = self.database.judge_calibration(kind)
            if previous is not None:
                return RoutingStatus(
                    kind,
                    False,
                    ("evaluator_version_changed",),
                    previous,
                )
            return RoutingStatus(kind, False, ("missing_calibration",), None)
        rules = self.config.judge_calibration
        reasons: list[str] = []
        if int(row["sample_size"]) < rules.minimum_labeled_sample:
            reasons.append("insufficient_labeled_sample")
        stale = bool(row["stale"]) or _is_stale(
            row.get("latest_label_at"),
            at or datetime.now(UTC),
            timedelta(days=rules.max_label_age_days),
        )
        if stale:
            reasons.append("calibration_stale")
        bounds = (
            ("false_approve_bound", "false_approve_upper", rules.max_false_approve_upper),
            ("false_reject_bound", "false_reject_upper", rules.max_false_reject_upper),
            ("machine_reject_reversal_bound", "reversal_upper", rules.max_reversal_upper),
            ("uncertainty_bound", "uncertainty_upper", rules.max_uncertainty_upper),
        )
        for reason, column, maximum in bounds:
            if float(row.get(column, 1.0)) > maximum:
                reasons.append(reason)
        return RoutingStatus(kind, not reasons, tuple(reasons), row)

    def should_sample_reject(self, attempt_id: str, kind: JudgeKind) -> bool:
        rate = self.config.review.sampled_reject_rate
        if rate <= 0:
            return False
        if rate >= 1:
            return True
        digest = hashlib.sha256(f"sample-reject-v1:{kind}:{attempt_id}".encode()).digest()
        value = int.from_bytes(digest[:8], "big") / 2**64
        return value < rate


def _human_outcome(tags_json: str) -> bool | None:
    try:
        tags = {str(tag).strip().lower() for tag in json.loads(tags_json)}
    except (TypeError, ValueError):
        return None
    accepted = bool(tags & _ACCEPT_TAGS)
    rejected = bool(tags & _REJECT_TAGS)
    if accepted == rejected:
        return None
    return accepted


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _is_stale(value: str | None, at: datetime, maximum_age: timedelta) -> bool:
    if not value:
        return True
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return at - parsed > maximum_age
