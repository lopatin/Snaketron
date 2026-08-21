from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from conftest import add_artifact

from snaketron_factory.db import (
    MIGRATIONS,
    Database,
    LeaseBusy,
    RecordNotFound,
    VersionConflict,
)
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    OperationStatus,
    Purpose,
    Stage,
)


def test_migrations_are_atomic_rerunnable_and_backup_is_readable(database: Database, tmp_path: Path) -> None:
    database.migrate()
    database.migrate()

    with database.connect() as connection:
        versions = [row[0] for row in connection.execute("SELECT version FROM schema_migration ORDER BY version")]
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    assert versions == list(range(1, len(MIGRATIONS) + 1))
    assert {
        "concept",
        "attempt",
        "operation",
        "operation_resolution",
        "artifact",
        "evaluation",
        "human_decision",
        "feedback_route",
        "technique_candidate",
        "optimization_run",
        "outbox_message",
    } <= table_names
    assert database.integrity_check() == []

    backup = tmp_path / "backup" / "factory.sqlite3"
    database.backup(backup)
    restored = Database(backup)
    assert restored.integrity_check() == []
    with restored.connect() as connection:
        assert connection.execute("SELECT count(*) FROM schema_migration").fetchone()[0] == len(MIGRATIONS)


def test_database_permission_check_migrates_data_directory_and_file(database: Database) -> None:
    os.chmod(database.path.parent, 0o755)
    os.chmod(database.path, 0o644)

    database.assert_file_permissions()

    assert database.path.parent.stat().st_mode & 0o777 == 0o700
    assert database.path.stat().st_mode & 0o777 == 0o600


def test_concept_and_attempt_updates_use_optimistic_versions_and_idempotency(
    database: Database,
) -> None:
    concept = database.create_concept(
        name="Chromatic Current",
        brief="A detailed concept that can be built and retained.",
        seed="seed",
        source="test",
        tags=["water", "animated"],
    )
    updated = database.update_concept(concept["id"], concept["version"], tags_json=["water", "bright"])
    assert json.loads(updated["tags_json"]) == ["water", "bright"]
    assert updated["version"] == concept["version"] + 1
    with pytest.raises(VersionConflict):
        database.update_concept(concept["id"], concept["version"], name="stale")
    with pytest.raises(ValueError):
        database.update_concept(concept["id"], updated["version"], created_at="forbidden")
    with pytest.raises(RecordNotFound):
        database.get_concept("concept_missing")

    values = {
        "concept_id": concept["id"],
        "purpose": Purpose.PRODUCTION,
        "stage": Stage.CONCEPT,
        "idempotency_key": "initial:chromatic",
        "behavior": {"skill": "v1"},
        "direction_sha": "1" * 64,
        "skill_sha": "2" * 64,
        "capability_sha": "3" * 64,
        "gate_sha": "4" * 64,
        "model_config_sha": "5" * 64,
    }
    first = database.create_attempt(**values)
    replay = database.create_attempt(**values)
    assert replay == first
    assert database.get_concept(concept["id"])["current_attempt_id"] == first["id"]

    advanced = database.update_attempt(first["id"], first["version"], stage=Stage.PROTOTYPE)
    assert advanced["stage"] == Stage.PROTOTYPE
    with pytest.raises(VersionConflict):
        database.update_attempt(first["id"], first["version"], stage=Stage.AUTHOR)
    with pytest.raises(ValueError):
        database.update_attempt(first["id"], advanced["version"], purpose=Purpose.OPTIMIZER)


def test_lease_is_exclusive_fenced_and_can_be_taken_after_expiry(database: Database) -> None:
    first = database.acquire_lease("production", "service:first", 60)
    with pytest.raises(LeaseBusy):
        database.acquire_lease("production", "service:second", 60)

    replacement = database.acquire_lease("production", "service:first", 60)
    assert replacement != first
    with pytest.raises(LeaseBusy):
        database.renew_lease("production", first, 60)
    database.renew_lease("production", replacement, 60)

    with database.transaction() as connection:
        expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        connection.execute("UPDATE factory_lease SET expires_at=? WHERE name='production'", (expired,))
    second = database.acquire_lease("production", "service:second", 60)
    assert second != replacement
    database.release_lease("production", replacement)
    with pytest.raises(LeaseBusy):
        database.renew_lease("production", replacement, 60)
    database.release_lease("production", second)
    assert database.acquire_lease("production", "service:third", 60)


def test_artifacts_evaluations_and_human_history_are_immutable_and_blind(
    database: Database, objects, make_attempt
) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE_TRIAGE)
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"prototype pixels",
        media_type="image/png",
    )
    duplicate = database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=artifact["content_hash"],
        object_ref=artifact["object_ref"],
        media_type="image/png",
        size_bytes=artifact["size_bytes"],
    )
    assert duplicate["id"] == artifact["id"]

    evaluation = database.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.MACHINE_REJECTED,
            reasons=["too muddy"],
            measurements={"craft": 0.2},
        ),
        hidden_until_label=True,
    )
    assert database.evaluations_for_attempt(attempt["id"]) == []
    decision = database.add_human_decision(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        action="prototype_label",
        feedback="Readable at game scale",
        tags=["false-reject"],
        actor="human:alex",
        attempt_version=attempt["version"],
        content_hash=artifact["content_hash"],
    )
    assert database.evaluations_for_attempt(attempt["id"])[0]["id"] == evaluation["id"]
    assert database.decisions_for_attempt(attempt["id"])[0]["id"] == decision["id"]

    with pytest.raises(VersionConflict):
        database.add_human_decision(
            artifact_id=artifact["id"],
            attempt_id=attempt["id"],
            action="prototype_label",
            feedback="stale",
            tags=[],
            actor="human:alex",
            attempt_version=attempt["version"] + 1,
        )
    with pytest.raises(PermissionError):
        database.add_human_decision(
            artifact_id=artifact["id"],
            attempt_id=attempt["id"],
            action="prototype_approval",
            feedback="forbidden",
            tags=[],
            actor="service:hermes",
            attempt_version=attempt["version"],
        )


def test_gallery_views_keep_rejects_experiments_published_and_lineage(
    database: Database, objects, make_attempt
) -> None:
    prototype = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    prototype = database.update_attempt(prototype["id"], prototype["version"], review_kind="prototype")
    rejected = make_attempt(stage=Stage.BUILD_GATE, disposition=Disposition.MACHINE_REJECTED)
    published = make_attempt(stage=Stage.COMPLETE, disposition=Disposition.PUBLISHED)
    experiment = make_attempt(
        stage=Stage.COMPLETE,
        purpose=Purpose.OPTIMIZER,
        disposition=Disposition.EXPERIMENT_COMPLETE,
    )
    retained = add_artifact(
        database,
        objects,
        rejected["id"],
        stage=Stage.BUILD_GATE,
        kind=ArtifactKind.SKIN_DOCUMENT,
        value=b'{"version":2}',
        media_type="application/json",
    )

    assert [row["id"] for row in database.list_gallery("prototype_review")] == [prototype["id"]]
    assert [row["id"] for row in database.list_gallery("machine_rejected")] == [rejected["id"]]
    assert [row["id"] for row in database.list_gallery("published")] == [published["id"]]
    assert [row["id"] for row in database.list_gallery("experiments")] == [experiment["id"]]
    all_rows = database.list_gallery("all")
    rejected_row = next(row for row in all_rows if row["id"] == rejected["id"])
    assert rejected_row["latest_artifact_id"] == retained["id"]
    with pytest.raises(ValueError):
        database.list_gallery("secret")


def test_operation_and_outbox_idempotency_are_conditional(database: Database, make_attempt) -> None:
    attempt = make_attempt()
    operation, _created = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="provider:key",
        side_effect="generate",
        provider_role="smart_text",
        request_hash="a" * 64,
        cost_reserved_micros=123,
    )
    replay, replay_created = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="provider:key",
        side_effect="generate",
        provider_role="smart_text",
        request_hash="a" * 64,
        cost_reserved_micros=999,
    )
    assert not replay_created
    assert replay["id"] == operation["id"]
    assert database.get_attempt(attempt["id"])["cost_reserved_micros"] == 123
    with pytest.raises(VersionConflict):
        database.begin_operation(
            attempt_id=attempt["id"],
            stage=Stage.CONCEPT,
            idempotency_key="provider:key",
            side_effect="generate",
            provider_role="smart_text",
            request_hash="b" * 64,
            cost_reserved_micros=123,
        )
    with pytest.raises(VersionConflict):
        database.transition_operation(operation["id"], OperationStatus.RUNNING, OperationStatus.SUCCEEDED)

    message = database.enqueue_outbox(
        idempotency_key="review:1",
        destination="review",
        event_ref="attempt:1",
        payload_ref="sha256:" + "c" * 64,
        payload_hash="c" * 64,
    )
    assert (
        database.enqueue_outbox(
            idempotency_key="review:1",
            destination="review",
            event_ref="attempt:1",
            payload_ref="sha256:" + "c" * 64,
            payload_hash="c" * 64,
        )["id"]
        == message["id"]
    )
    database.update_outbox(message["id"], message["version"], status="retry", error="offline")
    assert database.pending_outbox()[0]["attempts"] == 1
    with pytest.raises(VersionConflict):
        database.update_outbox(message["id"], message["version"], status="sent")
