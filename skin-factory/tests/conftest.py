from __future__ import annotations

import hashlib
import io
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from snaketron_factory.config import FactoryConfig
from snaketron_factory.db import Database
from snaketron_factory.domain import Purpose, Stage
from snaketron_factory.objects import ObjectStore

GATE_NAMES = (
    "document_schema",
    "reference_integrity",
    "ownership",
    "safety_ip",
    "asset_dimensions",
    "asset_exact_hash",
    "seam",
    "sprite_grid",
    "temporal_loop",
    "palette_chroma",
    "detail_density",
    "operation_budget",
    "renderer_conformance",
    "browser_pixels_ready",
    "contrast_diagnostic",
    "detail_retention_diagnostic",
    "visual_fidelity",
)


@pytest.fixture
def factory_config(tmp_path: Path) -> FactoryConfig:
    repo = tmp_path / "repo"
    skill = repo / "skills" / "author-skin"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("# Test authoring contract\n", encoding="utf-8")
    (skill / "playbook.md").write_text("Prefer clear, small-scale silhouettes.\n", encoding="utf-8")
    references = skill / "references"
    references.mkdir()
    (references / "design-guidelines.md").write_text(
        "# Skin Design Guidelines\nKeep one continuous thin rounded snake body.\n",
        encoding="utf-8",
    )
    schemas = skill / "schemas"
    schemas.mkdir()
    (schemas / "asset-request.schema.json").write_text(
        json.dumps({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}),
        encoding="utf-8",
    )

    direction = repo / "direction.md"
    direction.write_text("Bright, legible, playful snakes.\n", encoding="utf-8")
    capability = repo / "capabilities.json"
    capability.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "limits": {
                    "max_flattened_layers": 24,
                    "max_texture_refs": 4,
                    "max_texture_dimension_px": 2048,
                    "max_texture_variant_bytes": 2_097_152,
                    "max_texture_decoded_bytes": 16_777_216,
                    "max_sprite_frame_rows": 120,
                    "max_sprite_frame_rate_fps": 60,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    gates = repo / "gates.yaml"
    entries = "\n".join(
        f"  - name: {name}\n    version: test-v1\n    blocking: "
        f"{'false' if name in {'visual_fidelity', 'contrast_diagnostic', 'detail_retention_diagnostic'} else 'true'}"
        for name in GATE_NAMES
    )
    gates.write_text(f"version: 1\ngates:\n{entries}\n", encoding="utf-8")

    geometry_dir = repo / "skin-schema"
    geometry_fixtures = geometry_dir / "fixtures"
    geometry_fixtures.mkdir(parents=True)
    guide_buffer = io.BytesIO()
    Image.new("RGB", (16, 9), (255, 255, 255)).save(guide_buffer, format="PNG")
    guide = guide_buffer.getvalue()
    guide_path = geometry_fixtures / "prototype-guide.png"
    guide_path.write_bytes(guide)
    geometry = geometry_dir / "prototype-geometry.json"
    geometry.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "id": "prototype-geometry-test-v1",
                "guide": "fixtures/prototype-guide.png",
                "guide_sha256": hashlib.sha256(guide).hexdigest(),
                "guide_canvas": {"width_px": 16, "height_px": 9},
                "renderer_source": {"fixture": "straight_16"},
                "invariants": ["one continuous capsule"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    # Browser evidence is accepted only against an exact cached HTML/JS/WASM
    # build.  Keep the fixture deliberately tiny while exercising that pin.
    renderer_bundle = repo / "client" / "web" / "dist"
    renderer_bundle.mkdir(parents=True)
    (renderer_bundle / "index.html").write_text(
        '<!doctype html><script src="/main.js"></script>\n',
        encoding="utf-8",
    )
    (renderer_bundle / "main.js").write_text("export const renderer = 'test';\n", encoding="utf-8")
    (renderer_bundle / "client_bg.wasm").write_bytes(b"\x00asm\x01\x00\x00\x00")

    lama_project = repo / "lama"
    lama_project.mkdir()
    (lama_project / "pyproject.toml").write_text("[project]\nname='test-lama'\nversion='1'\n", encoding="utf-8")
    (lama_project / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (lama_project / "sitecustomize.py").write_text("# offline test guard\n", encoding="utf-8")
    (lama_project / "snaketron_lama_runtime.py").write_text("# test loader\n", encoding="utf-8")
    lama_model = tmp_path / "var" / "lama" / "test-lama.pt"
    lama_model.parent.mkdir(parents=True)
    lama_model.write_bytes(b"test-pinned-lama-model")
    lama_model.chmod(0o400)
    (lama_project / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dependency_lock": "uv.lock",
                "model": {
                    "filename": lama_model.name,
                    "sha256": hashlib.sha256(lama_model.read_bytes()).hexdigest(),
                    "size_bytes": lama_model.stat().st_size,
                    "url": "https://models.test/test-lama.pt",
                },
                "runtime_files": ["sitecustomize.py", "snaketron_lama_runtime.py"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    config = FactoryConfig.model_validate(
        {
            "config_version": 1,
            "mode": "shadow",
            "lease_seconds": 60,
            "paths": {
                "data_dir": str(tmp_path / "var"),
                "database": str(tmp_path / "var" / "factory.sqlite3"),
                "objects": str(tmp_path / "var" / "objects"),
                "repo_root": str(repo),
                "skill_dir": str(skill),
                "capability_manifest": str(capability),
                "direction": str(direction),
                "prototype_geometry": str(geometry),
                "gate_manifest": str(gates),
                "lama_manifest": str(lama_project / "manifest.json"),
                "lama_model": str(lama_model),
                "lama_python": sys.executable,
            },
            "models": {
                "task_worker": {
                    "provider": "fake",
                    "model": "worker-test",
                    "base_url": "https://worker.test/v1",
                    "max_output_tokens": 1024,
                },
                "smart_text": {
                    "provider": "gemini",
                    "model": "gemini-3.7-flash",
                    "thinking_level": "high",
                    "api_key_env": "TEST_GEMINI_KEY",
                    "cost_per_million_input_micros": 100,
                    "cost_per_million_output_micros": 200,
                },
                "visual_judge": {
                    "provider": "gemini",
                    "model": "gemini-3.7-flash",
                    "thinking_level": "high",
                    "api_key_env": "TEST_GEMINI_KEY",
                    "cost_per_million_input_micros": 100,
                    "cost_per_million_output_micros": 200,
                },
                "image_generator": {
                    "provider": "gemini",
                    "model": "gemini-3-pro-image",
                    "api_key_env": "TEST_GEMINI_KEY",
                    "cost_per_image_micros": 10_000,
                },
                "image_editor": {"provider": "local_lama", "model": "simple-lama"},
            },
            "budgets": {
                "max_concurrent_attempts": 1,
                "max_pending_prototype_reviews": 4,
                "max_pending_final_reviews": 4,
                "prototypes_per_attempt": 1,
                "provider_retries": 1,
                # Match production so every configured external timeout fits
                # inside one tick's admission budget.
                "wall_seconds_per_run": 1800,
                "max_cost_micros_per_attempt": 1_000_000,
                "max_cost_micros_per_day": 2_000_000,
                "max_cost_micros_program": 3_000_000,
            },
            "service": {"base_url": "https://snaketron.test"},
            "browser": {
                "base_url": "https://client.test",
                "capture_command": ["true"],
                "timeout_seconds": 5,
            },
            "worker": {"adapter": "fake", "max_output_tokens": 1024},
            "optimizer": {"enabled": False},
        }
    )
    config.source_path = repo / "factory.yaml"
    config.version_sha256 = "f" * 64
    return config


@pytest.fixture
def database(factory_config: FactoryConfig) -> Database:
    db = Database(factory_config.paths.database)
    db.migrate()
    return db


@pytest.fixture
def objects(factory_config: FactoryConfig) -> ObjectStore:
    return ObjectStore(factory_config.paths.objects)


@pytest.fixture
def make_attempt(database: Database) -> Callable[..., dict[str, Any]]:
    counter = 0

    def create(
        *,
        stage: Stage = Stage.CONCEPT,
        purpose: Purpose = Purpose.PRODUCTION,
        disposition: str | None = None,
        behavior: dict[str, Any] | None = None,
        approved_prototype_hash: str | None = None,
        prototype_decision_id: str | None = None,
        parent_attempt_id: str | None = None,
    ) -> dict[str, Any]:
        nonlocal counter
        counter += 1
        concept = database.create_concept(
            name=f"Concept {counter}",
            brief="A sufficiently detailed retained concept brief for tests.",
            seed=f"seed-{counter}",
            source="test",
            tags=["test"],
        )
        attempt = database.create_attempt(
            concept_id=concept["id"],
            purpose=purpose,
            stage=stage,
            idempotency_key=f"attempt-key-{counter}",
            behavior=behavior if behavior is not None else {"test": counter},
            direction_sha="1" * 64,
            skill_sha="2" * 64,
            capability_sha="3" * 64,
            gate_sha="4" * 64,
            model_config_sha="5" * 64,
            approved_prototype_hash=approved_prototype_hash,
            prototype_decision_id=prototype_decision_id,
            parent_attempt_id=parent_attempt_id,
        )
        if disposition is not None:
            attempt = database.update_attempt(attempt["id"], attempt["version"], disposition=disposition)
        return attempt

    return create


def add_artifact(
    database: Database,
    objects: ObjectStore,
    attempt_id: str,
    *,
    stage: Stage,
    kind: str,
    value: bytes,
    media_type: str = "application/octet-stream",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stored = objects.put(value)
    return database.add_artifact(
        attempt_id=attempt_id,
        stage=stage,
        kind=kind,
        content_hash=stored.uri,
        object_ref=stored.uri,
        media_type=media_type,
        size_bytes=stored.size,
        metadata=metadata,
    )
