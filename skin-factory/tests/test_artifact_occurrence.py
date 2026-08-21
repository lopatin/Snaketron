from __future__ import annotations

from pathlib import Path

import snaketron_factory.db as db_module
from snaketron_factory.db import MIGRATIONS, Database, now
from snaketron_factory.domain import ArtifactKind, Purpose, Stage


def test_occurrence_migration_preserves_existing_artifact_foreign_keys(
    tmp_path: Path,
    monkeypatch,
) -> None:
    all_migrations = MIGRATIONS
    occurrence_index = next(
        index for index, migration in enumerate(all_migrations) if "CREATE TABLE artifact_with_occurrence" in migration
    )
    monkeypatch.setattr(db_module, "MIGRATIONS", all_migrations[:occurrence_index])
    database = Database(tmp_path / "legacy.sqlite3")
    database.migrate()
    concept = database.create_concept(
        name="Legacy retained asset",
        brief="A retained pre-occurrence artifact with evaluation history.",
        seed="legacy",
        source="test",
        tags=["legacy"],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.ASSETS,
        idempotency_key="legacy-occurrence-upgrade",
        behavior={},
        direction_sha="1" * 64,
        skill_sha="2" * 64,
        capability_sha="3" * 64,
        gate_sha="4" * 64,
        model_config_sha="5" * 64,
    )
    timestamp = now()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO artifact(id,attempt_id,stage,kind,content_hash,object_ref,media_type,"
            "size_bytes,metadata_json,provenance_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                "artifact_legacy",
                attempt["id"],
                Stage.ASSETS,
                ArtifactKind.TEXTURE_VARIANT,
                "sha256:" + "a" * 64,
                "sha256:" + "a" * 64,
                "image/png",
                7,
                "{}",
                "{}",
                timestamp,
            ),
        )
        connection.execute(
            "INSERT INTO evaluation(id,artifact_id,attempt_id,evaluator,evaluator_version,gate_name,"
            "blocking,verdict,reasons_json,measurements_json,hidden_until_label,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "eval_legacy",
                "artifact_legacy",
                attempt["id"],
                "deterministic",
                "legacy-v1",
                "seam",
                1,
                "fail",
                "[]",
                "{}",
                0,
                timestamp,
            ),
        )

    monkeypatch.setattr(db_module, "MIGRATIONS", all_migrations)
    database.migrate()

    assert database.integrity_check() == []
    with database.connect() as connection:
        assert list(connection.execute("PRAGMA foreign_key_check")) == []
        retained = connection.execute("SELECT * FROM artifact WHERE id='artifact_legacy'").fetchone()
        evaluation = connection.execute("SELECT * FROM evaluation WHERE id='eval_legacy'").fetchone()
    assert retained is not None and retained["occurrence_key"] is None
    assert evaluation is not None and evaluation["artifact_id"] == retained["id"]

    first = database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.ASSETS,
        kind=ArtifactKind.TEXTURE_VARIANT,
        content_hash=retained["content_hash"],
        object_ref=retained["object_ref"],
        media_type="image/png",
        size_bytes=7,
        occurrence_key="asset:0:generation:0:variant:0",
    )
    second = database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.ASSETS,
        kind=ArtifactKind.TEXTURE_VARIANT,
        content_hash=retained["content_hash"],
        object_ref=retained["object_ref"],
        media_type="image/png",
        size_bytes=7,
        occurrence_key="asset:1:generation:0:variant:0",
    )
    assert first["id"] != second["id"]
    assert first["object_ref"] == second["object_ref"] == retained["object_ref"]
