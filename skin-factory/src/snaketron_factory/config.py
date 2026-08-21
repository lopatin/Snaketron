"""Versioned configuration with environment-only secrets."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PathsConfig(ConfigModel):
    data_dir: Path = Path("var")
    database: Path = Path("var/factory.sqlite3")
    objects: Path = Path("var/objects")
    repo_root: Path = Path("..")
    skill_dir: Path = Path("../skills/author-skin")
    capability_manifest: Path = Path("../skin-schema/capabilities-v2.json")
    direction: Path = Path("direction/design-direction.md")
    gate_manifest: Path = Path("config/gates.yaml")
    lama_manifest: Path = Path("lama/manifest.json")
    lama_model: Path = Path("var/lama/big-lama-v0.1.0.pt")
    lama_python: Path = Path("var/lama-venv/bin/python")


class ModelRole(ConfigModel):
    provider: Literal["gemini", "openai_compatible", "local_lama", "fake"]
    model: str | None = None
    thinking_level: Literal["minimal", "low", "medium", "high"] | None = None
    base_url: str | None = None
    api_key_env: str | None = None
    timeout_seconds: float = Field(default=120, ge=1, le=1800)
    cost_per_million_input_micros: int = Field(default=0, ge=0)
    cost_per_million_output_micros: int = Field(default=0, ge=0)
    cost_per_image_micros: int = Field(default=0, ge=0)

    def secret(self) -> str | None:
        return os.environ.get(self.api_key_env) if self.api_key_env else None


class ModelsConfig(ConfigModel):
    task_worker: ModelRole
    smart_text: ModelRole
    visual_judge: ModelRole
    image_generator: ModelRole
    image_editor: ModelRole


class BudgetConfig(ConfigModel):
    max_concurrent_attempts: int = Field(default=1, ge=1, le=32)
    max_pending_prototype_reviews: int = Field(default=12, ge=1)
    max_pending_final_reviews: int = Field(default=8, ge=1)
    prototypes_per_attempt: int = Field(default=3, ge=1, le=6)
    provider_retries: int = Field(default=2, ge=0, le=8)
    wall_seconds_per_run: int = Field(default=1800, ge=30)
    max_cost_micros_per_attempt: int = Field(default=5_000_000, ge=0)
    max_cost_micros_per_day: int = Field(default=25_000_000, ge=0)
    max_cost_micros_program: int = Field(default=500_000_000, ge=0)


class ServiceConfig(ConfigModel):
    base_url: str = "http://localhost:8080"
    service_token_env: str = "SNAKETRON_FACTORY_SERVICE_TOKEN"
    operator_token_env: str = "SNAKETRON_FACTORY_OPERATOR_TOKEN"
    request_timeout_seconds: float = Field(default=60, ge=1)


class BrowserConfig(ConfigModel):
    base_url: str = "http://localhost:3000"
    capture_command: list[str] = Field(
        default_factory=lambda: [
            "node",
            "client/web/tests/capture-authored-skin.mjs",
        ]
    )
    timeout_seconds: int = Field(default=90, ge=5)


class WorkerConfig(ConfigModel):
    adapter: Literal["openai_compatible", "fake"] = "openai_compatible"
    endpoint: str = "http://localhost:1234/v1"
    api_key_env: str | None = None
    max_output_tokens: int = Field(default=16_384, ge=1_024)


class ReviewConfig(ConfigModel):
    bind: str = "127.0.0.1"
    port: int = Field(default=8765, ge=1, le=65_535)
    operator_secret_env: str = "SKIN_FACTORY_REVIEW_TOKEN"
    sampled_reject_rate: float = Field(default=0.1, ge=0, le=1)


class JudgeCalibrationConfig(ConfigModel):
    """Fail-closed requirements for allowing a visual judge to route rejects."""

    minimum_labeled_sample: int = Field(default=30, ge=20)
    confidence_level: float = Field(default=0.95, gt=0.5, lt=1)
    max_false_approve_upper: float = Field(default=0.20, ge=0, le=1)
    max_false_reject_upper: float = Field(default=0.20, ge=0, le=1)
    max_reversal_upper: float = Field(default=0.20, ge=0, le=1)
    max_uncertainty_upper: float = Field(default=0.35, ge=0, le=1)
    max_label_age_days: int = Field(default=30, ge=1)


class OutboxConfig(ConfigModel):
    batch_size: int = Field(default=20, ge=1, le=500)
    max_attempts: int = Field(default=8, ge=1, le=100)
    initial_backoff_seconds: int = Field(default=30, ge=1)
    max_backoff_seconds: int = Field(default=3600, ge=1)
    request_timeout_seconds: float = Field(default=10, ge=1, le=300)
    webhook_url_env: str | None = "SKIN_FACTORY_OUTBOX_WEBHOOK_URL"
    webhook_token_env: str | None = "SKIN_FACTORY_OUTBOX_WEBHOOK_TOKEN"

    @model_validator(mode="after")
    def check_backoff(self) -> OutboxConfig:
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("outbox max_backoff_seconds must be at least initial_backoff_seconds")
        return self

    def webhook_url(self) -> str | None:
        return os.environ.get(self.webhook_url_env) if self.webhook_url_env else None

    def webhook_token(self) -> str | None:
        return os.environ.get(self.webhook_token_env) if self.webhook_token_env else None


class OptimizerConfig(ConfigModel):
    enabled: bool = True
    # Only feedback explicitly routed to the bounded playbook target with at
    # least this confidence may become optimizer or technique-mining input.
    feedback_min_confidence: float = Field(default=0.8, ge=0, le=1)
    expected_property_min_score: float = Field(default=0.75, ge=0, le=1)
    expected_property_max_regression: float = Field(default=0.0, ge=0, le=1)
    generation_min_labels: int = Field(default=10, ge=1)
    promotion_min_pairs: int = Field(default=12, ge=3)
    promotion_min_effect: float = Field(default=0.04, ge=0, le=1)
    promotion_max_pair_regression: float = Field(default=0.10, ge=0, le=1)
    promotion_confidence: float = Field(default=0.95, gt=0.5, lt=1)
    holdout_fraction: float = Field(default=0.2, gt=0, lt=0.5)
    development_fraction: float = Field(default=0.2, gt=0, lt=0.5)
    max_metric_calls: int = Field(default=80, ge=1)
    gepa_generations: int = Field(default=2, ge=1, le=5)
    gepa_candidates_per_generation: int = Field(default=3, ge=1, le=5)
    technique_min_fixtures: int = Field(default=3, ge=2)
    # Bumping this version is the explicit human authorization to retire the
    # current sealed holdout and deterministically create a fresh partition.
    # A concept can be queried at most once inside an epoch.
    holdout_epoch: str = Field(default="v1", min_length=1, max_length=80, pattern=r"^[A-Za-z0-9._-]+$")
    active_skill_ref: str = "HEAD"
    promotion_remote: str = "origin"


class FactoryConfig(ConfigModel):
    config_version: int = 1
    mode: Literal["shadow", "production"] = "shadow"
    lease_seconds: int = Field(default=3600, ge=60)
    paths: PathsConfig
    models: ModelsConfig
    budgets: BudgetConfig = BudgetConfig()
    service: ServiceConfig = ServiceConfig()
    browser: BrowserConfig = BrowserConfig()
    worker: WorkerConfig = WorkerConfig()
    review: ReviewConfig = ReviewConfig()
    judge_calibration: JudgeCalibrationConfig = JudgeCalibrationConfig()
    outbox: OutboxConfig = OutboxConfig()
    optimizer: OptimizerConfig = OptimizerConfig()
    source_path: Path | None = Field(default=None, exclude=True)
    version_sha256: str = Field(default="", exclude=True)

    @model_validator(mode="after")
    def enforce_smart_teacher(self) -> FactoryConfig:
        smart = self.models.smart_text
        judge = self.models.visual_judge
        if smart.provider != "gemini" or smart.model != "gemini-3.7-flash":
            raise ValueError("smart_text must default to Gemini 3.7 Flash")
        if judge.provider != "gemini" or judge.model != "gemini-3.7-flash":
            raise ValueError("visual_judge must default to Gemini 3.7 Flash")
        editor = self.models.image_editor
        if editor.provider != "local_lama" or editor.model != "simple-lama":
            raise ValueError("image_editor is the deterministic local simple-lama forge role")
        names = self.service_environment_names() | self.human_authority_environment_names()
        invalid = sorted(name for name in names if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None)
        if invalid:
            raise ValueError(f"invalid environment capability names: {invalid}")
        overlap = sorted(self.service_environment_names().intersection(self.human_authority_environment_names()))
        if overlap:
            raise ValueError(f"service and human authority environment names overlap: {overlap}")
        return self

    def resolve_paths(self, config_file: Path) -> FactoryConfig:
        base = config_file.resolve().parent.parent
        raw = self.model_dump()
        paths = raw["paths"]
        for key, value in paths.items():
            path = Path(value)
            paths[key] = str(path if path.is_absolute() else (base / path).resolve())
        updated = FactoryConfig.model_validate(raw)
        updated.source_path = config_file.resolve()
        updated.version_sha256 = self.version_sha256
        return updated

    def public_snapshot(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        for role in value["models"].values():
            role.pop("api_key_env", None)
        value["service"].pop("service_token_env", None)
        value["service"].pop("operator_token_env", None)
        value["review"].pop("operator_secret_env", None)
        value["worker"].pop("api_key_env", None)
        value["outbox"].pop("webhook_url_env", None)
        value["outbox"].pop("webhook_token_env", None)
        return value

    def human_authority_environment_names(self) -> frozenset[str]:
        """Environment capabilities that a service identity must never own."""

        return frozenset(
            {
                self.review.operator_secret_env,
                self.service.operator_token_env,
                "SKIN_FACTORY_REVIEW_ACTOR",
            }
        )

    def service_environment_names(self) -> frozenset[str]:
        """Config-declared environment inputs permitted in service JSON."""

        names = {role["api_key_env"] for role in self.models.model_dump().values() if role.get("api_key_env")}
        names.update(
            {
                self.worker.api_key_env,
                self.service.service_token_env,
                self.outbox.webhook_url_env,
                self.outbox.webhook_token_env,
            }
        )
        return frozenset(str(name) for name in names if name)

    def required_service_environment_names(self) -> frozenset[str]:
        """Credentials needed by one production/shadow factory cycle."""

        names = {self.service.service_token_env}
        for role_name in ("smart_text", "visual_judge", "image_generator"):
            name = getattr(self.models, role_name).api_key_env
            if name:
                names.add(name)
        return frozenset(names)

    def credential_environment_names(self) -> frozenset[str]:
        """Complete config-derived scrub set for untrusted subprocesses."""

        return self.service_environment_names() | self.human_authority_environment_names()


def load_config(path: Path | str | None = None) -> FactoryConfig:
    selected = Path(path or os.environ.get("SKIN_FACTORY_CONFIG", "config/factory.yaml"))
    if not selected.is_absolute():
        selected = Path.cwd() / selected
    payload = selected.read_bytes()
    raw = yaml.safe_load(payload)
    config = FactoryConfig.model_validate(raw)
    config.version_sha256 = hashlib.sha256(payload).hexdigest()
    return config.resolve_paths(selected)


def snapshot_json(config: FactoryConfig) -> str:
    return json.dumps(config.public_snapshot(), sort_keys=True, separators=(",", ":"))
