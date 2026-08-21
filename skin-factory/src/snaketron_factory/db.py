"""SQLite durability, conditional transitions, leases, and audit history."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sqlite3
import uuid
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    OperationStatus,
    Purpose,
    Stage,
)


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class _ClosingConnection(sqlite3.Connection):
    """Give ``with database.connect()`` the resource semantics callers expect.

    ``sqlite3.Connection.__exit__`` commits or rolls back but deliberately does
    not close the file descriptor.  Most factory reads use a short-lived
    context manager, so leaving the standard behavior in place accumulates WAL
    handles in Hermes and emits ResourceWarnings in the acceptance suite.
    Explicitly managed connections (migrations, backups, and transactions)
    still close themselves in their existing ``finally`` blocks.
    """

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


class VersionConflict(RuntimeError):
    pass


class LeaseBusy(RuntimeError):
    pass


class RecordNotFound(RuntimeError):
    pass


ATTEMPT_MUTABLE_FIELDS = frozenset(
    {
        "stage",
        "disposition",
        "review_kind",
        "restart_stage",
        "approved_prototype_hash",
        "prototype_decision_id",
        "cost_reserved_micros",
        "cost_charged_micros",
        "production_skin_id",
        "production_revision",
        "production_content_hash",
        "failure_json",
    }
)


MIGRATIONS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS schema_migration (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
    );

    CREATE TABLE factory_lease (
        name TEXT PRIMARY KEY,
        owner TEXT NOT NULL,
        token TEXT NOT NULL,
        acquired_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE concept (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        brief TEXT NOT NULL,
        seed TEXT NOT NULL,
        source TEXT NOT NULL,
        tags_json TEXT NOT NULL,
        current_disposition TEXT NOT NULL,
        current_attempt_id TEXT,
        stable_skin_id TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE attempt (
        id TEXT PRIMARY KEY,
        concept_id TEXT NOT NULL REFERENCES concept(id),
        purpose TEXT NOT NULL,
        parent_attempt_id TEXT REFERENCES attempt(id),
        restart_stage TEXT,
        stage TEXT NOT NULL,
        disposition TEXT NOT NULL,
        review_kind TEXT,
        idempotency_key TEXT NOT NULL UNIQUE,
        approved_prototype_hash TEXT,
        prototype_decision_id TEXT,
        direction_sha TEXT NOT NULL,
        skill_sha TEXT NOT NULL,
        capability_sha TEXT NOT NULL,
        gate_sha TEXT NOT NULL,
        model_config_sha TEXT NOT NULL,
        behavior_json TEXT NOT NULL,
        cost_reserved_micros INTEGER NOT NULL DEFAULT 0,
        cost_charged_micros INTEGER NOT NULL DEFAULT 0,
        experiment_run_id TEXT,
        experiment_candidate TEXT,
        experiment_split TEXT,
        production_skin_id TEXT,
        production_revision TEXT,
        production_content_hash TEXT,
        failure_json TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX attempt_queue ON attempt(disposition, stage, created_at);
    CREATE INDEX attempt_concept ON attempt(concept_id, created_at);

    CREATE TABLE operation (
        id TEXT PRIMARY KEY,
        attempt_id TEXT NOT NULL REFERENCES attempt(id),
        stage TEXT NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        side_effect TEXT NOT NULL,
        provider_role TEXT,
        request_hash TEXT NOT NULL,
        cost_reserved_micros INTEGER NOT NULL DEFAULT 0,
        cost_charged_micros INTEGER NOT NULL DEFAULT 0,
        provider_request_id TEXT,
        resolved_model TEXT,
        status TEXT NOT NULL,
        retry_class TEXT,
        result_hash TEXT,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        failure_json TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX operation_attempt ON operation(attempt_id, stage, created_at);
    CREATE INDEX operation_reconcile ON operation(status, updated_at);

    CREATE TABLE operation_resolution (
        id TEXT PRIMARY KEY,
        operation_id TEXT NOT NULL UNIQUE REFERENCES operation(id),
        resolution TEXT NOT NULL,
        evidence_ref TEXT NOT NULL,
        result_hash TEXT,
        actor TEXT NOT NULL,
        created_at TEXT NOT NULL
    );

    CREATE TABLE artifact (
        id TEXT PRIMARY KEY,
        attempt_id TEXT NOT NULL REFERENCES attempt(id),
        stage TEXT NOT NULL,
        kind TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        object_ref TEXT NOT NULL,
        media_type TEXT NOT NULL,
        size_bytes INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        provenance_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        UNIQUE(attempt_id, stage, kind, content_hash)
    );
    CREATE INDEX artifact_attempt ON artifact(attempt_id, stage, created_at);
    CREATE INDEX artifact_hash ON artifact(content_hash);

    CREATE TABLE evaluation (
        id TEXT PRIMARY KEY,
        artifact_id TEXT NOT NULL REFERENCES artifact(id),
        attempt_id TEXT NOT NULL REFERENCES attempt(id),
        evaluator TEXT NOT NULL,
        evaluator_version TEXT NOT NULL,
        gate_name TEXT NOT NULL,
        blocking INTEGER NOT NULL,
        verdict TEXT NOT NULL,
        reasons_json TEXT NOT NULL,
        measurements_json TEXT NOT NULL,
        hidden_until_label INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    CREATE INDEX evaluation_artifact ON evaluation(artifact_id, created_at);
    CREATE INDEX evaluation_attempt ON evaluation(attempt_id, created_at);

    CREATE TABLE human_decision (
        id TEXT PRIMARY KEY,
        artifact_id TEXT REFERENCES artifact(id),
        attempt_id TEXT NOT NULL REFERENCES attempt(id),
        action TEXT NOT NULL,
        feedback TEXT NOT NULL DEFAULT '',
        tags_json TEXT NOT NULL DEFAULT '[]',
        actor TEXT NOT NULL,
        attempt_version INTEGER NOT NULL,
        revision TEXT,
        content_hash TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX human_decision_attempt ON human_decision(attempt_id, created_at);
    CREATE INDEX human_decision_action ON human_decision(action, created_at);

    CREATE TABLE feedback_route (
        id TEXT PRIMARY KEY,
        decision_id TEXT NOT NULL UNIQUE REFERENCES human_decision(id),
        target TEXT NOT NULL,
        signature TEXT NOT NULL,
        confidence REAL NOT NULL,
        classifier_version TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        created_at TEXT NOT NULL
    );

    CREATE TABLE technique_candidate (
        id TEXT PRIMARY KEY,
        source_artifact_id TEXT REFERENCES artifact(id),
        recipe_ref TEXT NOT NULL,
        recipe_sha TEXT NOT NULL,
        fixture_refs_json TEXT NOT NULL,
        trial_results_json TEXT NOT NULL DEFAULT '[]',
        disposition TEXT NOT NULL,
        run_id TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE optimization_run (
        id TEXT PRIMARY KEY,
        target TEXT NOT NULL,
        dataset_version TEXT NOT NULL,
        teacher_config_json TEXT NOT NULL,
        student_config_json TEXT NOT NULL,
        candidate_refs_json TEXT NOT NULL DEFAULT '[]',
        train_metrics_json TEXT NOT NULL DEFAULT '{}',
        dev_metrics_json TEXT NOT NULL DEFAULT '{}',
        holdout_metrics_json TEXT,
        state TEXT NOT NULL,
        promoted_ref TEXT,
        promoted_sha TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE outbox_message (
        id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        destination TEXT NOT NULL,
        event_ref TEXT NOT NULL,
        payload_ref TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX outbox_pending ON outbox_message(status, created_at);

    CREATE TABLE active_behavior (
        name TEXT PRIMARY KEY,
        git_ref TEXT NOT NULL,
        sha TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE judge_calibration (
        kind TEXT PRIMARY KEY,
        evaluator_version TEXT NOT NULL,
        sample_size INTEGER NOT NULL,
        true_positive INTEGER NOT NULL,
        true_negative INTEGER NOT NULL,
        false_positive INTEGER NOT NULL,
        false_negative INTEGER NOT NULL,
        lower_confidence REAL NOT NULL,
        upper_confidence REAL NOT NULL,
        stale INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL
    );
    """,
    """
    ALTER TABLE outbox_message ADD COLUMN next_attempt_at TEXT;
    ALTER TABLE outbox_message ADD COLUMN delivered_at TEXT;
    ALTER TABLE outbox_message ADD COLUMN dead_lettered_at TEXT;

    CREATE TABLE outbox_delivery_attempt (
        id TEXT PRIMARY KEY,
        message_id TEXT NOT NULL REFERENCES outbox_message(id),
        attempt_number INTEGER NOT NULL,
        outcome TEXT NOT NULL,
        response_code INTEGER,
        error TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(message_id, attempt_number)
    );
    CREATE INDEX outbox_delivery_history ON outbox_delivery_attempt(message_id, attempt_number);

    ALTER TABLE judge_calibration ADD COLUMN precision REAL NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN recall REAL NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN false_approve_rate REAL NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN false_reject_rate REAL NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN reversal_rate REAL NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN uncertainty_rate REAL NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN false_approve_upper REAL NOT NULL DEFAULT 1;
    ALTER TABLE judge_calibration ADD COLUMN false_reject_upper REAL NOT NULL DEFAULT 1;
    ALTER TABLE judge_calibration ADD COLUMN reversal_upper REAL NOT NULL DEFAULT 1;
    ALTER TABLE judge_calibration ADD COLUMN uncertainty_upper REAL NOT NULL DEFAULT 1;
    ALTER TABLE judge_calibration ADD COLUMN uncertain_count INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE judge_calibration ADD COLUMN latest_label_at TEXT;
    """,
    """
    CREATE TABLE optimizer_holdout_use (
        id TEXT PRIMARY KEY,
        target TEXT NOT NULL,
        holdout_epoch TEXT NOT NULL,
        concept_id TEXT NOT NULL REFERENCES concept(id),
        prototype_hash TEXT NOT NULL,
        optimization_run_id TEXT NOT NULL REFERENCES optimization_run(id),
        dataset_version TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(target, holdout_epoch, concept_id)
    );
    CREATE INDEX optimizer_holdout_run
        ON optimizer_holdout_use(optimization_run_id, created_at);
    """,
    """
    ALTER TABLE judge_calibration RENAME TO judge_calibration_singleton;

    CREATE TABLE judge_calibration (
        kind TEXT NOT NULL,
        evaluator_version TEXT NOT NULL,
        sample_size INTEGER NOT NULL,
        true_positive INTEGER NOT NULL,
        true_negative INTEGER NOT NULL,
        false_positive INTEGER NOT NULL,
        false_negative INTEGER NOT NULL,
        lower_confidence REAL NOT NULL,
        upper_confidence REAL NOT NULL,
        stale INTEGER NOT NULL DEFAULT 0,
        precision REAL NOT NULL DEFAULT 0,
        recall REAL NOT NULL DEFAULT 0,
        false_approve_rate REAL NOT NULL DEFAULT 0,
        false_reject_rate REAL NOT NULL DEFAULT 0,
        reversal_rate REAL NOT NULL DEFAULT 0,
        uncertainty_rate REAL NOT NULL DEFAULT 0,
        false_approve_upper REAL NOT NULL DEFAULT 1,
        false_reject_upper REAL NOT NULL DEFAULT 1,
        reversal_upper REAL NOT NULL DEFAULT 1,
        uncertainty_upper REAL NOT NULL DEFAULT 1,
        uncertain_count INTEGER NOT NULL DEFAULT 0,
        latest_label_at TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(kind, evaluator_version)
    );
    INSERT INTO judge_calibration(
        kind,evaluator_version,sample_size,true_positive,true_negative,false_positive,
        false_negative,lower_confidence,upper_confidence,stale,precision,recall,
        false_approve_rate,false_reject_rate,reversal_rate,uncertainty_rate,
        false_approve_upper,false_reject_upper,reversal_upper,uncertainty_upper,
        uncertain_count,latest_label_at,updated_at
    ) SELECT
        kind,evaluator_version,sample_size,true_positive,true_negative,false_positive,
        false_negative,lower_confidence,upper_confidence,stale,precision,recall,
        false_approve_rate,false_reject_rate,reversal_rate,uncertainty_rate,
        false_approve_upper,false_reject_upper,reversal_upper,uncertainty_upper,
        uncertain_count,latest_label_at,updated_at
      FROM judge_calibration_singleton;
    DROP TABLE judge_calibration_singleton;
    CREATE INDEX judge_calibration_current ON judge_calibration(kind, updated_at DESC);
    """,
    """
    ALTER TABLE operation_resolution ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';
    """,
    """
    ALTER TABLE human_decision ADD COLUMN idempotency_key TEXT;
    CREATE UNIQUE INDEX human_decision_idempotency
        ON human_decision(idempotency_key) WHERE idempotency_key IS NOT NULL;
    """,
    """
    CREATE TABLE generation_resume (
        id TEXT PRIMARY KEY,
        halt_key TEXT NOT NULL,
        evidence_at TEXT NOT NULL,
        actor TEXT NOT NULL,
        reason TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(halt_key, evidence_at)
    );
    CREATE INDEX generation_resume_latest ON generation_resume(created_at DESC);
    """,
    """
    -- requires-foreign-keys-off
    CREATE TABLE artifact_with_occurrence (
        id TEXT PRIMARY KEY,
        attempt_id TEXT NOT NULL REFERENCES attempt(id),
        stage TEXT NOT NULL,
        kind TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        object_ref TEXT NOT NULL,
        media_type TEXT NOT NULL,
        size_bytes INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        provenance_json TEXT NOT NULL DEFAULT '{}',
        occurrence_key TEXT,
        created_at TEXT NOT NULL
    );
    INSERT INTO artifact_with_occurrence(
        id,attempt_id,stage,kind,content_hash,object_ref,media_type,size_bytes,
        metadata_json,provenance_json,occurrence_key,created_at
    ) SELECT
        id,attempt_id,stage,kind,content_hash,object_ref,media_type,size_bytes,
        metadata_json,provenance_json,NULL,created_at
      FROM artifact;
    DROP TABLE artifact;
    ALTER TABLE artifact_with_occurrence RENAME TO artifact;
    CREATE INDEX artifact_attempt ON artifact(attempt_id, stage, created_at);
    CREATE INDEX artifact_hash ON artifact(content_hash);
    CREATE UNIQUE INDEX artifact_singleton_content
        ON artifact(attempt_id,stage,kind,content_hash)
        WHERE occurrence_key IS NULL;
    CREATE UNIQUE INDEX artifact_occurrence
        ON artifact(attempt_id,stage,kind,occurrence_key)
        WHERE occurrence_key IS NOT NULL;
    """,
    """
    ALTER TABLE evaluation ADD COLUMN idempotency_key TEXT;
    CREATE UNIQUE INDEX evaluation_idempotency
        ON evaluation(idempotency_key) WHERE idempotency_key IS NOT NULL;
    """,
    """
    ALTER TABLE human_decision ADD COLUMN authority_evaluation_id TEXT REFERENCES evaluation(id);
    CREATE UNIQUE INDEX human_decision_blind_artifact_authority
        ON human_decision(artifact_id, action)
        WHERE authority_evaluation_id IS NOT NULL
          AND action IN ('prototype_label','build_quality_label');
    CREATE UNIQUE INDEX human_decision_blind_evaluation_authority
        ON human_decision(authority_evaluation_id)
        WHERE authority_evaluation_id IS NOT NULL;
    CREATE TRIGGER human_decision_one_label_per_artifact
    BEFORE INSERT ON human_decision
    WHEN NEW.action IN ('prototype_label','build_quality_label')
      AND EXISTS (
        SELECT 1 FROM human_decision d
        WHERE d.artifact_id=NEW.artifact_id AND d.action=NEW.action
      )
    BEGIN
      SELECT RAISE(ABORT, 'human label already exists for exact artifact');
    END;
    """,
)


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            factory=_ClosingConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def migrate(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migration (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            applied = {row[0] for row in connection.execute("SELECT version FROM schema_migration")}
            for version, sql in enumerate(MIGRATIONS, start=1):
                if version in applied:
                    continue
                try:
                    # sqlite3.executescript commits any transaction that was
                    # opened by execute(). Put the transaction boundaries in
                    # the script itself so schema changes and the migration
                    # marker remain one crash-safe unit.
                    applied_at = now().replace("'", "''")
                    foreign_keys_off = "-- requires-foreign-keys-off" in sql
                    if foreign_keys_off:
                        connection.execute("PRAGMA foreign_keys = OFF")
                    connection.executescript(
                        "BEGIN IMMEDIATE;\n"
                        f"{sql}\n"
                        "INSERT OR IGNORE INTO schema_migration(version, applied_at) "
                        f"VALUES ({version}, '{applied_at}');\n"
                        "COMMIT;"
                    )
                    if foreign_keys_off:
                        connection.execute("PRAGMA foreign_keys = ON")
                        violations = list(connection.execute("PRAGMA foreign_key_check"))
                        if violations:
                            raise RuntimeError(f"migration {version} introduced foreign-key violations")
                except BaseException:
                    if connection.in_transaction:
                        connection.execute("ROLLBACK")
                    connection.execute("PRAGMA foreign_keys = ON")
                    raise
        finally:
            connection.close()

    def integrity_check(self) -> list[str]:
        with self.connect() as connection:
            return [row[0] for row in connection.execute("PRAGMA integrity_check") if row[0] != "ok"]

    def backup(self, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        source = self.connect()
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
            destination.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            destination.close()
            source.close()

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def acquire_lease(self, name: str, owner: str, ttl_seconds: int) -> str:
        token = new_id("lease")
        timestamp = datetime.now(UTC)
        expires = timestamp + timedelta(seconds=ttl_seconds)
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM factory_lease WHERE name = ?", (name,)).fetchone()
            if row and datetime.fromisoformat(row["expires_at"]) > timestamp and row["owner"] != owner:
                raise LeaseBusy(f"lease {name} is held by {row['owner']} until {row['expires_at']}")
            if row:
                connection.execute(
                    "UPDATE factory_lease SET owner=?, token=?, acquired_at=?, expires_at=?, "
                    "version=version+1 WHERE name=?",
                    (owner, token, timestamp.isoformat(), expires.isoformat(), name),
                )
            else:
                connection.execute(
                    "INSERT INTO factory_lease(name,owner,token,acquired_at,expires_at) VALUES(?,?,?,?,?)",
                    (name, owner, token, timestamp.isoformat(), expires.isoformat()),
                )
        return token

    def renew_lease(self, name: str, token: str, ttl_seconds: int) -> None:
        expires = (datetime.now(UTC) + timedelta(seconds=ttl_seconds)).isoformat()
        with self.transaction() as connection:
            changed = connection.execute(
                "UPDATE factory_lease SET expires_at=?,version=version+1 WHERE name=? AND token=?",
                (expires, name, token),
            ).rowcount
            if changed != 1:
                raise LeaseBusy(f"lease {name} is no longer owned by this process")

    def release_lease(self, name: str, token: str) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM factory_lease WHERE name=? AND token=?", (name, token))

    def create_concept(
        self,
        *,
        name: str,
        brief: str,
        seed: str,
        source: str,
        tags: Sequence[str],
    ) -> dict[str, Any]:
        concept_id = new_id("concept")
        timestamp = now()
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO concept(id,name,brief,seed,source,tags_json,current_disposition,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    concept_id,
                    name,
                    brief,
                    seed,
                    source,
                    canonical_json(list(tags)),
                    Disposition.ACTIVE,
                    timestamp,
                    timestamp,
                ),
            )
        return self.get_concept(concept_id)

    def get_concept(self, concept_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM concept WHERE id=?", (concept_id,)).fetchone()
        if row is None:
            raise RecordNotFound(concept_id)
        return dict(row)

    def update_concept(
        self,
        concept_id: str,
        expected_version: int,
        **fields: Any,
    ) -> dict[str, Any]:
        allowed = {
            "name",
            "brief",
            "seed",
            "source",
            "tags_json",
            "current_disposition",
            "current_attempt_id",
            "stable_skin_id",
        }
        unknown = set(fields).difference(allowed)
        if unknown:
            raise ValueError(f"concept fields are not mutable: {sorted(unknown)}")
        encoded = {
            name: canonical_json(value) if name == "tags_json" and not isinstance(value, str) else value
            for name, value in fields.items()
        }
        if not encoded:
            return self.get_concept(concept_id)
        with self.transaction() as connection:
            changed = connection.execute(
                f"UPDATE concept SET {','.join(f'{name}=?' for name in encoded)},"
                "version=version+1,updated_at=? WHERE id=? AND version=?",
                (*encoded.values(), now(), concept_id, expected_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(concept_id)
            row = connection.execute("SELECT * FROM concept WHERE id=?", (concept_id,)).fetchone()
        assert row is not None
        return dict(row)

    def create_attempt(
        self,
        *,
        concept_id: str,
        purpose: Purpose,
        stage: Stage,
        idempotency_key: str,
        behavior: Mapping[str, Any],
        direction_sha: str,
        skill_sha: str,
        capability_sha: str,
        gate_sha: str,
        model_config_sha: str,
        parent_attempt_id: str | None = None,
        restart_stage: str | None = None,
        approved_prototype_hash: str | None = None,
        prototype_decision_id: str | None = None,
        experiment_run_id: str | None = None,
        experiment_candidate: str | None = None,
        experiment_split: str | None = None,
        disposition: Disposition | str = Disposition.ACTIVE,
    ) -> dict[str, Any]:
        if purpose == Purpose.PRODUCTION and disposition != Disposition.ACTIVE:
            raise ValueError("production Attempts must start active")
        if purpose == Purpose.CONTROL and disposition != Disposition.EXPERIMENT_COMPLETE:
            raise ValueError("optimizer control Attempts must start terminal")
        attempt_id = new_id("attempt")
        timestamp = now()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM attempt WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                return dict(existing)
            connection.execute(
                """INSERT INTO attempt(
                    id,concept_id,purpose,parent_attempt_id,restart_stage,stage,disposition,
                    idempotency_key,approved_prototype_hash,prototype_decision_id,direction_sha,
                    skill_sha,capability_sha,gate_sha,model_config_sha,behavior_json,
                    experiment_run_id,experiment_candidate,experiment_split,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    attempt_id,
                    concept_id,
                    purpose,
                    parent_attempt_id,
                    restart_stage,
                    stage,
                    disposition,
                    idempotency_key,
                    approved_prototype_hash,
                    prototype_decision_id,
                    direction_sha,
                    skill_sha,
                    capability_sha,
                    gate_sha,
                    model_config_sha,
                    canonical_json(dict(behavior)),
                    experiment_run_id,
                    experiment_candidate,
                    experiment_split,
                    timestamp,
                    timestamp,
                ),
            )
            if purpose == Purpose.PRODUCTION:
                connection.execute(
                    "UPDATE concept SET current_attempt_id=?,current_disposition=?,version=version+1,"
                    "updated_at=? WHERE id=?",
                    (attempt_id, Disposition.ACTIVE, timestamp, concept_id),
                )
            row = connection.execute("SELECT * FROM attempt WHERE id=?", (attempt_id,)).fetchone()
        assert row is not None
        return dict(row)

    def get_attempt(self, attempt_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM attempt WHERE id=?", (attempt_id,)).fetchone()
        if row is None:
            raise RecordNotFound(attempt_id)
        return dict(row)

    def find_attempt_by_key(self, key: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM attempt WHERE idempotency_key=?", (key,)).fetchone()
        return self._dict(row)

    def update_attempt(
        self,
        attempt_id: str,
        expected_version: int,
        **fields: Any,
    ) -> dict[str, Any]:
        unknown = set(fields).difference(ATTEMPT_MUTABLE_FIELDS)
        if unknown:
            raise ValueError(f"attempt fields are not mutable: {sorted(unknown)}")
        if not fields:
            return self.get_attempt(attempt_id)
        assignments = ",".join(f"{name}=?" for name in fields)
        values = [
            canonical_json(value) if name == "failure_json" and value is not None else value
            for name, value in fields.items()
        ]
        timestamp = now()
        with self.transaction() as connection:
            changed = connection.execute(
                f"UPDATE attempt SET {assignments},version=version+1,updated_at=? WHERE id=? AND version=?",
                (*values, timestamp, attempt_id, expected_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(attempt_id)
            row = connection.execute("SELECT * FROM attempt WHERE id=?", (attempt_id,)).fetchone()
            assert row is not None
            if row["purpose"] == Purpose.PRODUCTION:
                connection.execute(
                    "UPDATE concept SET current_disposition=?,version=version+1,updated_at=? "
                    "WHERE id=? AND current_attempt_id=?",
                    (row["disposition"], timestamp, row["concept_id"], row["id"]),
                )
        return dict(row)

    def update_attempt_with_outbox(
        self,
        attempt_id: str,
        expected_version: int,
        *,
        outbox_idempotency_key: str,
        outbox_destination: str,
        outbox_event_ref: str,
        outbox_payload_ref: str,
        outbox_payload_hash: str,
        **fields: Any,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Atomically transition an Attempt and commit its notification intent."""

        unknown = set(fields).difference(ATTEMPT_MUTABLE_FIELDS)
        if unknown:
            raise ValueError(f"attempt fields are not mutable: {sorted(unknown)}")
        if not fields:
            raise ValueError("atomic review transition requires attempt fields")
        assignments = ",".join(f"{name}=?" for name in fields)
        values = [
            canonical_json(value) if name == "failure_json" and value is not None else value
            for name, value in fields.items()
        ]
        timestamp = now()
        with self.transaction() as connection:
            changed = connection.execute(
                f"UPDATE attempt SET {assignments},version=version+1,updated_at=? WHERE id=? AND version=?",
                (*values, timestamp, attempt_id, expected_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(attempt_id)
            attempt = connection.execute("SELECT * FROM attempt WHERE id=?", (attempt_id,)).fetchone()
            assert attempt is not None
            if attempt["purpose"] == Purpose.PRODUCTION:
                connection.execute(
                    "UPDATE concept SET current_disposition=?,version=version+1,updated_at=? "
                    "WHERE id=? AND current_attempt_id=?",
                    (
                        attempt["disposition"],
                        timestamp,
                        attempt["concept_id"],
                        attempt["id"],
                    ),
                )
            message = self._enqueue_outbox_in(
                connection,
                idempotency_key=outbox_idempotency_key,
                destination=outbox_destination,
                event_ref=outbox_event_ref,
                payload_ref=outbox_payload_ref,
                payload_hash=outbox_payload_hash,
                timestamp=timestamp,
            )
        return dict(attempt), dict(message)

    def next_active_attempt(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT a.* FROM attempt a WHERE a.disposition=? AND a.purpose<>? "
                "AND (a.restart_stage IS NULL OR a.restart_stage NOT LIKE 're_evaluate:%' "
                "OR EXISTS (SELECT 1 FROM artifact linked WHERE linked.attempt_id=a.id "
                "AND linked.id=substr(a.restart_stage,length('re_evaluate:')+1))) "
                "ORDER BY a.created_at LIMIT 1",
                (Disposition.ACTIVE, Purpose.CONTROL),
            ).fetchone()
        return self._dict(row)

    def latest_registered_attempt(self, concept_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT a.* FROM attempt a JOIN concept c ON c.id=a.concept_id "
                "WHERE a.concept_id=? AND a.purpose=? AND a.production_skin_id IS NOT NULL "
                "AND a.production_revision IS NOT NULL "
                "AND (c.stable_skin_id IS NULL OR a.production_skin_id=c.stable_skin_id) "
                "ORDER BY CAST(a.production_revision AS INTEGER) DESC,a.updated_at DESC LIMIT 1",
                (concept_id, Purpose.PRODUCTION),
            ).fetchone()
        return self._dict(row)

    def count_attempts(self, *, disposition: str | None = None, review_kind: str | None = None) -> int:
        predicates: list[str] = []
        values: list[Any] = []
        if disposition:
            predicates.append("disposition=?")
            values.append(disposition)
        if review_kind:
            predicates.append("review_kind=?")
            values.append(review_kind)
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with self.connect() as connection:
            row = connection.execute(f"SELECT count(*) FROM attempt{where}", values).fetchone()
        assert row is not None
        return int(row[0])

    def published_concept_count(self) -> int:
        """Count terminal target progress once per production concept."""

        with self.connect() as connection:
            row = connection.execute(
                "SELECT count(DISTINCT concept_id) FROM attempt WHERE purpose=? AND disposition=?",
                (Purpose.PRODUCTION, Disposition.PUBLISHED),
            ).fetchone()
        assert row is not None
        return int(row[0])

    def repeated_blocking_gate_failure(
        self,
        *,
        window: int,
        threshold: int,
        after: str | None = None,
    ) -> dict[str, Any] | None:
        """Return a recent production gate cluster large enough to halt generation."""

        cutoff = after or ""
        with self.connect() as connection:
            row = connection.execute(
                """SELECT gate_name,count(*) AS failures,max(created_at) AS latest_at
                   FROM (
                     SELECT e.gate_name,e.attempt_id,max(e.created_at) AS created_at
                     FROM evaluation e JOIN attempt a ON a.id=e.attempt_id
                     WHERE a.purpose=? AND e.blocking=1 AND e.verdict=? AND e.created_at>?
                     GROUP BY e.gate_name,e.attempt_id
                     ORDER BY created_at DESC LIMIT ?
                   ) recent
                   GROUP BY gate_name HAVING count(*)>=?
                   ORDER BY failures DESC,latest_at DESC,gate_name ASC LIMIT 1""",
                (Purpose.PRODUCTION, GateVerdict.FAIL, cutoff, window, threshold),
            ).fetchone()
        return self._dict(row)

    def repeated_root_cause_after_promotion(
        self,
        *,
        target: str,
        min_confidence: float,
        threshold: int,
        after: str | None = None,
    ) -> dict[str, Any] | None:
        """Return a high-confidence feedback signature recurring after promotion."""

        with self.connect() as connection:
            active = connection.execute("SELECT updated_at FROM active_behavior WHERE name='author-skin'").fetchone()
            if active is None:
                return None
            cutoff = max(str(active["updated_at"]), after or "")
            row = connection.execute(
                """SELECT r.signature,count(*) AS occurrences,max(r.created_at) AS latest_at
                   FROM feedback_route r
                   JOIN human_decision d ON d.id=r.decision_id
                   JOIN attempt a ON a.id=d.attempt_id
                   WHERE a.purpose=? AND r.target=? AND r.confidence>=?
                     AND r.created_at>?
                   GROUP BY r.signature HAVING count(*)>=?
                   ORDER BY occurrences DESC,latest_at DESC,r.signature ASC LIMIT 1""",
                (
                    Purpose.PRODUCTION,
                    target,
                    min_confidence,
                    cutoff,
                    threshold,
                ),
            ).fetchone()
        return self._dict(row)

    def latest_generation_resume(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM generation_resume ORDER BY created_at DESC,id DESC LIMIT 1"
            ).fetchone()
        return self._dict(row)

    def record_generation_resume(
        self,
        *,
        halt_key: str,
        evidence_at: str,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        if not actor.startswith("human:") or not actor.removeprefix("human:").strip():
            raise PermissionError("generation resume requires an authenticated human actor")
        if not halt_key.strip() or not evidence_at.strip() or not reason.strip():
            raise ValueError("halt key, evidence timestamp, and reason are required")
        resume_id = new_id("resume")
        timestamp = now()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM generation_resume WHERE halt_key=? AND evidence_at=?",
                (halt_key, evidence_at),
            ).fetchone()
            if existing is not None:
                if existing["actor"] != actor or existing["reason"] != reason.strip():
                    raise VersionConflict("generation halt acknowledgement already exists with different authority")
                return dict(existing)
            connection.execute(
                "INSERT INTO generation_resume(id,halt_key,evidence_at,actor,reason,created_at) VALUES(?,?,?,?,?,?)",
                (resume_id, halt_key, evidence_at, actor, reason.strip(), timestamp),
            )
            row = connection.execute("SELECT * FROM generation_resume WHERE id=?", (resume_id,)).fetchone()
        assert row is not None
        return dict(row)

    def unresolved_program_halt(self, *, after: str | None = None) -> dict[str, Any] | None:
        """Return the newest explicit program halt not acknowledged by a human."""

        cutoff = after or ""
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT a.* FROM attempt a WHERE a.purpose=? AND a.disposition=? "
                "AND NOT EXISTS (SELECT 1 FROM human_decision d WHERE d.attempt_id=a.id "
                "AND d.action='human_resume') AND a.updated_at>? ORDER BY a.updated_at DESC",
                (Purpose.PRODUCTION, Disposition.BLOCKED, cutoff),
            ).fetchall()
        for row in rows:
            try:
                failure = json.loads(row["failure_json"] or "{}")
            except (TypeError, ValueError):
                continue
            kind = failure.get("program_halt")
            if isinstance(kind, str) and kind:
                return {**dict(row), "program_halt": kind, "failure": failure}
        return None

    def resume_program_halt(self, *, attempt_id: str, actor: str, reason: str) -> dict[str, Any]:
        if not actor.startswith("human:") or not actor.removeprefix("human:").strip():
            raise PermissionError("generation resume requires an authenticated human actor")
        attempt = self.get_attempt(attempt_id)
        active = self.unresolved_program_halt()
        if active is None or active["id"] != attempt_id:
            raise VersionConflict("attempt is not the current unresolved program halt")
        if not reason.strip():
            raise ValueError("generation resume requires a nonempty reason")
        decision = self.add_human_decision(
            artifact_id=None,
            attempt_id=attempt_id,
            action="human_resume",
            feedback=reason.strip(),
            tags=[f"halt:{active['program_halt']}"],
            actor=actor,
            attempt_version=attempt["version"],
            idempotency_key=f"program-resume:{attempt_id}",
        )
        self.record_generation_resume(
            halt_key=f"program_halt:{active['program_halt']}:{attempt_id}",
            evidence_at=str(active["updated_at"]),
            actor=actor,
            reason=reason,
        )
        return decision

    def add_artifact(
        self,
        *,
        attempt_id: str,
        stage: str,
        kind: ArtifactKind | str,
        content_hash: str,
        object_ref: str,
        media_type: str,
        size_bytes: int,
        metadata: Mapping[str, Any] | None = None,
        provenance: Mapping[str, Any] | None = None,
        occurrence_key: str | None = None,
    ) -> dict[str, Any]:
        if occurrence_key is not None and not occurrence_key.strip():
            raise ValueError("artifact occurrence_key must be non-empty when supplied")
        artifact_id = new_id("artifact")
        encoded_metadata = canonical_json(dict(metadata or {}))
        encoded_provenance = canonical_json(dict(provenance or {}))
        with self.transaction() as connection:
            if occurrence_key is None:
                existing = connection.execute(
                    "SELECT * FROM artifact WHERE attempt_id=? AND stage=? AND kind=? "
                    "AND content_hash=? AND occurrence_key IS NULL",
                    (attempt_id, stage, kind, content_hash),
                ).fetchone()
            else:
                existing = connection.execute(
                    "SELECT * FROM artifact WHERE attempt_id=? AND stage=? AND kind=? AND occurrence_key=?",
                    (attempt_id, stage, kind, occurrence_key),
                ).fetchone()
            if existing:
                expected = {
                    "content_hash": content_hash,
                    "object_ref": object_ref,
                    "media_type": media_type,
                    "size_bytes": size_bytes,
                    "metadata_json": encoded_metadata,
                    "provenance_json": encoded_provenance,
                }
                if occurrence_key is not None and any(existing[name] != value for name, value in expected.items()):
                    raise VersionConflict(f"artifact occurrence key reused with different evidence: {occurrence_key}")
                return dict(existing)
            connection.execute(
                """INSERT INTO artifact(
                    id,attempt_id,stage,kind,content_hash,object_ref,media_type,size_bytes,
                    metadata_json,provenance_json,occurrence_key,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    artifact_id,
                    attempt_id,
                    stage,
                    kind,
                    content_hash,
                    object_ref,
                    media_type,
                    size_bytes,
                    encoded_metadata,
                    encoded_provenance,
                    occurrence_key,
                    now(),
                ),
            )
            row = connection.execute("SELECT * FROM artifact WHERE id=?", (artifact_id,)).fetchone()
        assert row is not None
        return dict(row)

    def get_artifact(self, artifact_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM artifact WHERE id=?", (artifact_id,)).fetchone()
        if row is None:
            raise RecordNotFound(artifact_id)
        return dict(row)

    def find_artifact_by_hash(
        self, attempt_id: str, content_hash: str, *, kind: str | None = None
    ) -> dict[str, Any] | None:
        suffix = " AND kind=?" if kind else ""
        values: tuple[Any, ...] = (
            (attempt_id, content_hash, kind)
            if kind
            else (
                attempt_id,
                content_hash,
            )
        )
        with self.connect() as connection:
            row = connection.execute(
                f"SELECT * FROM artifact WHERE attempt_id=? AND content_hash=?{suffix} ORDER BY created_at LIMIT 1",
                values,
            ).fetchone()
        return self._dict(row)

    def artifacts_for_attempt(
        self,
        attempt_id: str,
        *,
        stage: str | None = None,
        kind: str | None = None,
    ) -> list[dict[str, Any]]:
        predicates = ["attempt_id=?"]
        values: list[Any] = [attempt_id]
        if stage:
            predicates.append("stage=?")
            values.append(stage)
        if kind:
            predicates.append("kind=?")
            values.append(kind)
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM artifact WHERE {' AND '.join(predicates)} ORDER BY created_at",
                values,
            ).fetchall()
        return [dict(row) for row in rows]

    def add_evaluation(
        self,
        *,
        artifact_id: str,
        attempt_id: str,
        evaluator: str,
        result: GateResult,
        hidden_until_label: bool = False,
    ) -> dict[str, Any]:
        evaluation_id = new_id("eval")
        identity = canonical_json(
            {
                "artifact_id": artifact_id,
                "attempt_id": attempt_id,
                "evaluator": evaluator,
                "result": result.model_dump(mode="json"),
                "hidden_until_label": hidden_until_label,
            }
        )
        idempotency_key = "evaluation:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM evaluation WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                return dict(existing)
            connection.execute(
                """INSERT INTO evaluation(
                    id,artifact_id,attempt_id,evaluator,evaluator_version,gate_name,blocking,
                    verdict,reasons_json,measurements_json,hidden_until_label,created_at,idempotency_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    evaluation_id,
                    artifact_id,
                    attempt_id,
                    evaluator,
                    result.gate_version,
                    result.gate,
                    int(result.blocking),
                    result.verdict,
                    canonical_json(result.reasons),
                    canonical_json(result.measurements),
                    int(hidden_until_label),
                    now(),
                    idempotency_key,
                ),
            )
            row = connection.execute("SELECT * FROM evaluation WHERE id=?", (evaluation_id,)).fetchone()
        assert row is not None
        return dict(row)

    def evaluations_for_attempt(self, attempt_id: str, *, reveal: bool = False) -> list[dict[str, Any]]:
        condition = ""
        if not reveal:
            condition = (
                " AND (e.hidden_until_label=0 OR EXISTS ("
                "SELECT 1 FROM human_decision d WHERE d.artifact_id=e.artifact_id "
                "AND d.action IN ('prototype_label','build_quality_label') "
                "AND d.authority_evaluation_id IS NOT NULL))"
            )
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT e.* FROM evaluation e WHERE e.attempt_id=?{condition} ORDER BY e.created_at",
                (attempt_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def has_hidden_unlabeled_evaluations(self, attempt_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """SELECT 1 FROM evaluation e
                   WHERE e.attempt_id=? AND e.hidden_until_label=1
                     AND NOT EXISTS (
                       SELECT 1 FROM human_decision d WHERE d.artifact_id=e.artifact_id
                         AND d.action IN ('prototype_label','build_quality_label')
                         AND d.authority_evaluation_id IS NOT NULL
                     ) LIMIT 1""",
                (attempt_id,),
            ).fetchone()
        return row is not None

    def begin_operation(
        self,
        *,
        attempt_id: str,
        stage: str,
        idempotency_key: str,
        side_effect: str,
        provider_role: str | None,
        request_hash: str,
        cost_reserved_micros: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        operation_id = new_id("op")
        timestamp = now()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM operation WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                if existing["request_hash"] != request_hash:
                    raise VersionConflict(f"operation key reused with different request: {idempotency_key}")
                return dict(existing), False
            attempt = connection.execute(
                "SELECT version,cost_reserved_micros FROM attempt WHERE id=?", (attempt_id,)
            ).fetchone()
            if attempt is None:
                raise RecordNotFound(attempt_id)
            connection.execute(
                """INSERT INTO operation(
                    id,attempt_id,stage,idempotency_key,side_effect,provider_role,request_hash,
                    cost_reserved_micros,status,metadata_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    operation_id,
                    attempt_id,
                    stage,
                    idempotency_key,
                    side_effect,
                    provider_role,
                    request_hash,
                    cost_reserved_micros,
                    OperationStatus.INTENT,
                    canonical_json(dict(metadata or {})),
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                "UPDATE attempt SET cost_reserved_micros=cost_reserved_micros+?,"
                "version=version+1,updated_at=? WHERE id=?",
                (cost_reserved_micros, timestamp, attempt_id),
            )
            row = connection.execute("SELECT * FROM operation WHERE id=?", (operation_id,)).fetchone()
        assert row is not None
        return dict(row), True

    def transition_operation(
        self,
        operation_id: str,
        expected_status: OperationStatus | str,
        status: OperationStatus | str,
        **fields: Any,
    ) -> dict[str, Any]:
        allowed = {
            "provider_request_id",
            "resolved_model",
            "retry_class",
            "result_hash",
            "metadata_json",
            "failure_json",
            "cost_charged_micros",
        }
        unknown = set(fields).difference(allowed)
        if unknown:
            raise ValueError(f"operation fields are not mutable: {sorted(unknown)}")
        encoded = {
            name: canonical_json(value) if name in {"metadata_json", "failure_json"} and value is not None else value
            for name, value in fields.items()
        }
        assignments = ["status=?", *(f"{name}=?" for name in encoded)]
        timestamp = now()
        with self.transaction() as connection:
            before = connection.execute("SELECT * FROM operation WHERE id=?", (operation_id,)).fetchone()
            if before is None:
                raise RecordNotFound(operation_id)
            changed = connection.execute(
                f"UPDATE operation SET {','.join(assignments)},version=version+1,updated_at=? WHERE id=? AND status=?",
                (status, *encoded.values(), timestamp, operation_id, expected_status),
            ).rowcount
            if changed != 1:
                raise VersionConflict(operation_id)
            charged_before = int(before["cost_charged_micros"])
            charged_after = int(encoded.get("cost_charged_micros", charged_before))
            if charged_after != charged_before:
                connection.execute(
                    "UPDATE attempt SET cost_charged_micros=cost_charged_micros+?,"
                    "version=version+1,updated_at=? WHERE id=?",
                    (charged_after - charged_before, timestamp, before["attempt_id"]),
                )
            row = connection.execute("SELECT * FROM operation WHERE id=?", (operation_id,)).fetchone()
        assert row is not None
        return dict(row)

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM operation WHERE id=?", (operation_id,)).fetchone()
        if row is None:
            raise RecordNotFound(operation_id)
        return dict(row)

    def unresolved_operations(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM operation WHERE status=? ORDER BY created_at",
                (OperationStatus.RECONCILIATION_REQUIRED,),
            ).fetchall()
        return [dict(row) for row in rows]

    def resolve_operation(
        self,
        *,
        operation_id: str,
        resolution: str,
        evidence_ref: str,
        actor: str,
        result_hash: str | None = None,
        resolved_model: str | None = None,
        provider_request_id: str | None = None,
        media_type: str | None = None,
    ) -> dict[str, Any]:
        if not actor.startswith("human:") or not actor.removeprefix("human:").strip():
            raise PermissionError("unknown operations require an authenticated human actor")
        allowed = {
            "confirmed_not_executed",
            "executed_result_recovered",
            "executed_output_lost",
            "indeterminate",
        }
        if resolution not in allowed:
            raise ValueError(f"invalid resolution {resolution}")
        if resolution == "executed_result_recovered":
            if (
                not isinstance(result_hash, str)
                or not result_hash.startswith("sha256:")
                or len(result_hash) != 71
                or any(character not in "0123456789abcdef" for character in result_hash[7:])
            ):
                raise ValueError("executed_result_recovered requires a valid sha256 result hash")
            if not isinstance(resolved_model, str) or not resolved_model.strip():
                raise ValueError("executed_result_recovered requires the exact resolved model")
        elif any(value is not None for value in (result_hash, resolved_model, provider_request_id, media_type)):
            raise ValueError(f"{resolution} does not accept recovered result metadata")
        resolution_id = new_id("resolution")
        timestamp = now()
        with self.transaction() as connection:
            operation = connection.execute("SELECT * FROM operation WHERE id=?", (operation_id,)).fetchone()
            if operation is None:
                raise RecordNotFound(operation_id)
            if operation["status"] != OperationStatus.RECONCILIATION_REQUIRED:
                raise VersionConflict("only reconciliation_required operations can be resolved")
            operation_metadata = json.loads(operation["metadata_json"])
            resolution_metadata: dict[str, Any] = {}
            if resolution == "executed_result_recovered":
                image_result = operation["provider_role"] in {"image_generator", "image_editor"}
                if image_result and media_type not in {"image/png", "image/jpeg", "image/webp"}:
                    raise ValueError(
                        "recovered image operations require media_type image/png, image/jpeg, or image/webp"
                    )
                if not image_result and media_type is not None:
                    raise ValueError("media_type is only valid for a recovered image operation")
                resolution_metadata = {
                    "resolved_model": resolved_model,
                    "provider_request_id": provider_request_id,
                    "media_type": media_type,
                }
                operation_metadata["recovery"] = {
                    "evidence_ref": evidence_ref,
                    **resolution_metadata,
                }
                if image_result:
                    operation_metadata["result"] = {"kind": "image", "media_type": media_type}
            connection.execute(
                "INSERT INTO operation_resolution(id,operation_id,resolution,evidence_ref,"
                "result_hash,actor,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?)",
                (
                    resolution_id,
                    operation_id,
                    resolution,
                    evidence_ref,
                    result_hash,
                    actor,
                    timestamp,
                    canonical_json(resolution_metadata),
                ),
            )
            if resolution == "confirmed_not_executed":
                operation_status = OperationStatus.RESOLVED
                retry_class = "retry_safe"
                charged = 0
            elif resolution == "executed_result_recovered":
                operation_status = OperationStatus.SUCCEEDED
                retry_class = "complete"
                charged = int(operation["cost_reserved_micros"])
            else:
                operation_status = OperationStatus.RESOLVED
                retry_class = "terminal"
                charged = int(operation["cost_reserved_micros"])
            connection.execute(
                "UPDATE operation SET status=?,retry_class=?,result_hash=COALESCE(?,result_hash),"
                "resolved_model=COALESCE(?,resolved_model),"
                "provider_request_id=COALESCE(?,provider_request_id),metadata_json=?,"
                "cost_charged_micros=?,version=version+1,updated_at=? WHERE id=?",
                (
                    operation_status,
                    retry_class,
                    result_hash,
                    resolved_model,
                    provider_request_id,
                    canonical_json(operation_metadata),
                    charged,
                    timestamp,
                    operation_id,
                ),
            )
            charged_before = int(operation["cost_charged_micros"])
            charged_delta = charged - charged_before
            if charged_delta:
                connection.execute(
                    "UPDATE attempt SET cost_charged_micros=cost_charged_micros+?,"
                    "version=version+1,updated_at=? WHERE id=?",
                    (charged_delta, timestamp, operation["attempt_id"]),
                )
            if resolution in {"executed_output_lost", "indeterminate"}:
                connection.execute(
                    "UPDATE attempt SET disposition=?,failure_json=?,version=version+1,updated_at=? WHERE id=?",
                    (
                        Disposition.BLOCKED,
                        canonical_json({"operation": operation_id, "resolution": resolution}),
                        timestamp,
                        operation["attempt_id"],
                    ),
                )
            row = connection.execute("SELECT * FROM operation_resolution WHERE id=?", (resolution_id,)).fetchone()
        assert row is not None
        return dict(row)

    def create_review_child(
        self,
        *,
        parent_attempt_id: str,
        expected_parent_version: int,
        decision_idempotency_key: str,
        child_idempotency_key: str,
        action: str,
        decision_artifact_id: str | None,
        decision_content_hash: str | None,
        feedback: str,
        tags: Sequence[str],
        actor: str,
        stage: Stage,
        restart_stage: str,
        behavior: Mapping[str, Any],
        direction_sha: str,
        skill_sha: str,
        capability_sha: str,
        gate_sha: str,
        model_config_sha: str,
        approved_prototype_hash: str | None,
        prototype_decision_id: str | None,
        linked_source_artifact_id: str | None = None,
        linked_metadata: Mapping[str, Any] | None = None,
        linked_provenance: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        """Atomically append a human retry request and its runnable child.

        The decision and child keys describe one immutable request.  A replay
        returns the already committed rows; a changed request using either key
        fails closed.  Re-evaluation links retained bytes in this same
        transaction, so the queue can never observe a ``link_pending`` child.
        """

        timestamp = now()
        decision_id = new_id("decision")
        child_id = new_id("attempt")
        linked_id = new_id("artifact") if linked_source_artifact_id is not None else None
        encoded_tags = canonical_json(list(tags))
        encoded_behavior = canonical_json(dict(behavior))
        encoded_link_metadata = canonical_json(dict(linked_metadata or {}))
        encoded_link_provenance = canonical_json(dict(linked_provenance or {}))

        def replay_rows(
            connection: sqlite3.Connection, decision: sqlite3.Row
        ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
            expected_decision = {
                "artifact_id": decision_artifact_id,
                "attempt_id": parent_attempt_id,
                "action": action,
                "feedback": feedback,
                "tags_json": encoded_tags,
                "actor": actor,
                "content_hash": decision_content_hash,
            }
            if any(decision[name] != value for name, value in expected_decision.items()):
                raise VersionConflict(f"review request key reused with different decision: {decision_idempotency_key}")
            child = connection.execute(
                "SELECT * FROM attempt WHERE idempotency_key=?", (child_idempotency_key,)
            ).fetchone()
            if child is None:
                raise VersionConflict("committed review decision has no atomic child")
            expected_child = {
                "concept_id": connection.execute(
                    "SELECT concept_id FROM attempt WHERE id=?", (parent_attempt_id,)
                ).fetchone()[0],
                "purpose": connection.execute(
                    "SELECT purpose FROM attempt WHERE id=?", (parent_attempt_id,)
                ).fetchone()[0],
                "parent_attempt_id": parent_attempt_id,
                "stage": stage,
            }
            if any(child[name] != value for name, value in expected_child.items()):
                raise VersionConflict(f"review child key reused with different request: {child_idempotency_key}")
            linked: sqlite3.Row | None = None
            if linked_source_artifact_id is None:
                if child["restart_stage"] != restart_stage:
                    raise VersionConflict("review retry replay changed its restart stage")
            else:
                prefix = "re_evaluate:"
                if not str(child["restart_stage"] or "").startswith(prefix):
                    raise VersionConflict("re-evaluation replay has no linked artifact authority")
                existing_link_id = str(child["restart_stage"])[len(prefix) :]
                linked = connection.execute(
                    "SELECT * FROM artifact WHERE id=? AND attempt_id=?",
                    (existing_link_id, child["id"]),
                ).fetchone()
                source = connection.execute(
                    "SELECT * FROM artifact WHERE id=? AND attempt_id=?",
                    (linked_source_artifact_id, parent_attempt_id),
                ).fetchone()
                if linked is None or source is None:
                    raise VersionConflict("re-evaluation replay does not bind retained parent bytes")
                expected_link = {
                    "stage": source["stage"],
                    "kind": source["kind"],
                    "content_hash": source["content_hash"],
                    "object_ref": source["object_ref"],
                    "media_type": source["media_type"],
                    "size_bytes": source["size_bytes"],
                    "metadata_json": encoded_link_metadata,
                    "provenance_json": encoded_link_provenance,
                }
                if any(linked[name] != value for name, value in expected_link.items()):
                    raise VersionConflict("re-evaluation replay changed its retained-byte link")
            return dict(decision), dict(child), dict(linked) if linked is not None else None

        with self.transaction() as connection:
            existing_decision = connection.execute(
                "SELECT * FROM human_decision WHERE idempotency_key=?",
                (decision_idempotency_key,),
            ).fetchone()
            if existing_decision is not None:
                return replay_rows(connection, existing_decision)
            if connection.execute("SELECT 1 FROM attempt WHERE idempotency_key=?", (child_idempotency_key,)).fetchone():
                raise VersionConflict(f"review child exists without its decision: {child_idempotency_key}")

            parent = connection.execute("SELECT * FROM attempt WHERE id=?", (parent_attempt_id,)).fetchone()
            if parent is None:
                raise RecordNotFound(parent_attempt_id)
            if parent["version"] != expected_parent_version:
                raise VersionConflict(parent_attempt_id)
            if decision_artifact_id is not None:
                decision_artifact = connection.execute(
                    "SELECT * FROM artifact WHERE id=? AND attempt_id=?",
                    (decision_artifact_id, parent_attempt_id),
                ).fetchone()
                if decision_artifact is None:
                    raise RecordNotFound(decision_artifact_id)
                if decision_content_hash and decision_artifact["content_hash"] != decision_content_hash:
                    raise VersionConflict("review decision changed its exact artifact bytes")

            source: sqlite3.Row | None = None
            final_restart_stage = restart_stage
            if linked_source_artifact_id is not None:
                source = connection.execute(
                    "SELECT * FROM artifact WHERE id=? AND attempt_id=?",
                    (linked_source_artifact_id, parent_attempt_id),
                ).fetchone()
                if source is None:
                    raise RecordNotFound(linked_source_artifact_id)
                assert linked_id is not None
                final_restart_stage = f"re_evaluate:{linked_id}"

            connection.execute(
                """INSERT INTO human_decision(
                    id,artifact_id,attempt_id,action,feedback,tags_json,actor,attempt_version,
                    revision,content_hash,created_at,idempotency_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision_id,
                    decision_artifact_id,
                    parent_attempt_id,
                    action,
                    feedback,
                    encoded_tags,
                    actor,
                    expected_parent_version,
                    None,
                    decision_content_hash,
                    timestamp,
                    decision_idempotency_key,
                ),
            )
            connection.execute(
                """INSERT INTO attempt(
                    id,concept_id,purpose,parent_attempt_id,restart_stage,stage,disposition,
                    idempotency_key,approved_prototype_hash,prototype_decision_id,direction_sha,
                    skill_sha,capability_sha,gate_sha,model_config_sha,behavior_json,
                    created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    child_id,
                    parent["concept_id"],
                    parent["purpose"],
                    parent_attempt_id,
                    final_restart_stage,
                    stage,
                    Disposition.ACTIVE,
                    child_idempotency_key,
                    approved_prototype_hash,
                    prototype_decision_id,
                    direction_sha,
                    skill_sha,
                    capability_sha,
                    gate_sha,
                    model_config_sha,
                    encoded_behavior,
                    timestamp,
                    timestamp,
                ),
            )
            if source is not None:
                assert linked_id is not None
                connection.execute(
                    """INSERT INTO artifact(
                        id,attempt_id,stage,kind,content_hash,object_ref,media_type,size_bytes,
                        metadata_json,provenance_json,created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        linked_id,
                        child_id,
                        source["stage"],
                        source["kind"],
                        source["content_hash"],
                        source["object_ref"],
                        source["media_type"],
                        source["size_bytes"],
                        encoded_link_metadata,
                        encoded_link_provenance,
                        timestamp,
                    ),
                )
            if parent["purpose"] == Purpose.PRODUCTION:
                connection.execute(
                    "UPDATE concept SET current_attempt_id=?,current_disposition=?,version=version+1,"
                    "updated_at=? WHERE id=?",
                    (child_id, Disposition.ACTIVE, timestamp, parent["concept_id"]),
                )
            decision = connection.execute("SELECT * FROM human_decision WHERE id=?", (decision_id,)).fetchone()
            child = connection.execute("SELECT * FROM attempt WHERE id=?", (child_id,)).fetchone()
            linked = (
                connection.execute("SELECT * FROM artifact WHERE id=?", (linked_id,)).fetchone()
                if linked_id is not None
                else None
            )
            assert decision is not None and child is not None
            return dict(decision), dict(child), dict(linked) if linked is not None else None

    def add_human_decision(
        self,
        *,
        artifact_id: str | None,
        attempt_id: str,
        action: str,
        feedback: str,
        tags: Sequence[str],
        actor: str,
        attempt_version: int,
        revision: str | None = None,
        content_hash: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        if actor.startswith("service:") and action in {
            "prototype_approval",
            "publish_approval",
            "soft_triage_override",
            "human_resume",
        }:
            raise PermissionError(f"service identities cannot create {action}")
        decision_id = new_id("decision")
        with self.transaction() as connection:
            if idempotency_key is not None:
                existing = connection.execute(
                    "SELECT * FROM human_decision WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if existing is not None:
                    expected = {
                        "artifact_id": artifact_id,
                        "attempt_id": attempt_id,
                        "action": action,
                        "feedback": feedback,
                        "tags_json": canonical_json(list(tags)),
                        "actor": actor,
                        "revision": revision,
                        "content_hash": content_hash,
                    }
                    if any(existing[name] != value for name, value in expected.items()):
                        raise VersionConflict(f"human decision key reused with different request: {idempotency_key}")
                    return dict(existing)
            attempt = connection.execute("SELECT * FROM attempt WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise RecordNotFound(attempt_id)
            if attempt["version"] != attempt_version:
                raise VersionConflict(attempt_id)
            if action in {"prototype_label", "build_quality_label"}:
                prior_label = connection.execute(
                    "SELECT 1 FROM human_decision WHERE artifact_id=? AND action=? LIMIT 1",
                    (artifact_id, action),
                ).fetchone()
                if prior_label is not None:
                    raise VersionConflict("human label already exists for exact artifact")
            if artifact_id:
                artifact = connection.execute(
                    "SELECT * FROM artifact WHERE id=? AND attempt_id=?", (artifact_id, attempt_id)
                ).fetchone()
                if artifact is None:
                    raise RecordNotFound(artifact_id)
                if content_hash and artifact["content_hash"] != content_hash:
                    raise VersionConflict("decision content hash does not name the selected artifact")
            connection.execute(
                """INSERT INTO human_decision(
                    id,artifact_id,attempt_id,action,feedback,tags_json,actor,attempt_version,
                    revision,content_hash,created_at,idempotency_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision_id,
                    artifact_id,
                    attempt_id,
                    action,
                    feedback,
                    canonical_json(list(tags)),
                    actor,
                    attempt_version,
                    revision,
                    content_hash,
                    now(),
                    idempotency_key,
                ),
            )
            row = connection.execute("SELECT * FROM human_decision WHERE id=?", (decision_id,)).fetchone()
        assert row is not None
        return dict(row)

    def add_blind_human_label(
        self,
        *,
        artifact_id: str,
        attempt_id: str,
        action: str,
        feedback: str,
        tags: Sequence[str],
        actor: str,
        content_hash: str,
    ) -> dict[str, Any]:
        """Atomically bind one blind human label to pre-existing judge evidence.

        The deterministic key makes an exact browser/CLI retry a read.  Any
        changed duplicate is rejected, and the state/evaluation checks live in
        the same immediate transaction as the insert so two reviewers cannot
        both create apparent ground truth for one artifact.
        """

        contracts: dict[str, tuple[str, str, set[tuple[str, str]]]] = {
            "prototype_label": (
                ArtifactKind.PROTOTYPE,
                Stage.PROTOTYPE_REVIEW,
                {
                    (Disposition.NEEDS_HUMAN, "prototype"),
                    (Disposition.MACHINE_REJECTED, "prototype_label"),
                },
            ),
            "build_quality_label": (
                ArtifactKind.CONTACT_SHEET,
                Stage.FINAL_REVIEW,
                {
                    (Disposition.NEEDS_HUMAN, "final"),
                    (Disposition.MACHINE_REJECTED, "build_label"),
                },
            ),
        }
        if action not in contracts:
            raise ValueError("blind labels are prototype_label or build_quality_label")
        if not actor.startswith("human:") or not actor.removeprefix("human:").strip():
            raise PermissionError("blind-label authority requires a nonempty human actor")
        expected_kind, expected_stage, allowed_states = contracts[action]
        encoded_tags = canonical_json(list(tags))
        idempotency_key = f"blind-label:v1:{attempt_id}:{artifact_id}:{action}"
        decision_id = new_id("decision")
        timestamp = now()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM human_decision WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                expected = {
                    "artifact_id": artifact_id,
                    "attempt_id": attempt_id,
                    "action": action,
                    "feedback": feedback,
                    "tags_json": encoded_tags,
                    "actor": actor,
                    "revision": None,
                    "content_hash": content_hash,
                }
                if existing["authority_evaluation_id"] is None or any(
                    existing[name] != value for name, value in expected.items()
                ):
                    raise VersionConflict("blind label already exists with different exact input")
                return dict(existing)

            attempt = connection.execute("SELECT * FROM attempt WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise RecordNotFound(attempt_id)
            if (
                attempt["stage"] != expected_stage
                or (attempt["disposition"], attempt["review_kind"]) not in allowed_states
            ):
                raise VersionConflict(f"{action} requires its exact eligible {expected_stage} review state")
            artifact = connection.execute(
                "SELECT * FROM artifact WHERE id=? AND attempt_id=?",
                (artifact_id, attempt_id),
            ).fetchone()
            if artifact is None:
                raise RecordNotFound(artifact_id)
            if artifact["kind"] != expected_kind:
                raise VersionConflict(f"{action} must name the exact retained {expected_kind} artifact")
            if artifact["content_hash"] != content_hash:
                raise VersionConflict("blind label content hash does not name the exact artifact")

            prior = connection.execute(
                "SELECT * FROM human_decision WHERE artifact_id=? "
                "AND action IN ('prototype_label','build_quality_label') ORDER BY created_at LIMIT 1",
                (artifact_id,),
            ).fetchone()
            if prior is not None:
                raise VersionConflict("artifact was already revealed by a different blind-label request")

            visible_evaluation = connection.execute(
                """SELECT 1 FROM evaluation
                   WHERE artifact_id=? AND attempt_id=? AND evaluator='visual_judge'
                     AND gate_name='visual_fidelity' AND hidden_until_label=0
                   LIMIT 1""",
                (artifact_id, attempt_id),
            ).fetchone()
            if visible_evaluation is not None:
                raise VersionConflict("blind label rejects an artifact with an unblinded visual_judge evaluation")

            evaluation = connection.execute(
                """SELECT * FROM evaluation
                   WHERE artifact_id=? AND attempt_id=? AND evaluator='visual_judge'
                     AND gate_name='visual_fidelity' AND hidden_until_label=1
                   ORDER BY created_at DESC,id DESC LIMIT 1""",
                (artifact_id, attempt_id),
            ).fetchone()
            if evaluation is None:
                raise VersionConflict(
                    "blind label requires a hidden pre-existing visual_judge evaluation of the exact artifact"
                )
            if evaluation["created_at"] > timestamp:
                raise VersionConflict("blind label cannot precede its visual_judge evaluation")

            connection.execute(
                """INSERT INTO human_decision(
                    id,artifact_id,attempt_id,action,feedback,tags_json,actor,attempt_version,
                    revision,content_hash,created_at,idempotency_key,authority_evaluation_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision_id,
                    artifact_id,
                    attempt_id,
                    action,
                    feedback,
                    encoded_tags,
                    actor,
                    attempt["version"],
                    None,
                    content_hash,
                    timestamp,
                    idempotency_key,
                    evaluation["id"],
                ),
            )
            row = connection.execute("SELECT * FROM human_decision WHERE id=?", (decision_id,)).fetchone()
        assert row is not None
        return dict(row)

    def authoritative_blind_label(self, *, artifact_id: str, action: str) -> dict[str, Any] | None:
        """Return the one label created against retained hidden judge evidence."""

        with self.connect() as connection:
            row = connection.execute(
                """SELECT d.* FROM human_decision d
                   JOIN evaluation e ON e.id=d.authority_evaluation_id
                   WHERE d.artifact_id=? AND d.action=? AND d.actor LIKE 'human:%'
                     AND e.artifact_id=d.artifact_id AND e.attempt_id=d.attempt_id
                     AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                     AND e.hidden_until_label=1 AND e.created_at<=d.created_at
                   LIMIT 1""",
                (artifact_id, action),
            ).fetchone()
        return self._dict(row)

    def has_hidden_visual_evaluation(self, artifact_id: str) -> bool:
        """Return whether exact pixels carry blind visual-fidelity evidence."""

        with self.connect() as connection:
            row = connection.execute(
                """SELECT 1 FROM evaluation
                   WHERE artifact_id=? AND evaluator='visual_judge'
                     AND gate_name='visual_fidelity' AND hidden_until_label=1
                   LIMIT 1""",
                (artifact_id,),
            ).fetchone()
        return row is not None

    def decisions_for_attempt(self, attempt_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM human_decision WHERE attempt_id=? ORDER BY created_at", (attempt_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def find_exact_decision(
        self,
        *,
        attempt_id: str,
        action: str,
        content_hash: str | None,
        revision: str | None = None,
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM human_decision WHERE attempt_id=? AND action=? "
                "AND content_hash IS ? AND revision IS ? ORDER BY created_at LIMIT 1",
                (attempt_id, action, content_hash, revision),
            ).fetchone()
        return self._dict(row)

    def add_feedback_route(
        self,
        *,
        decision_id: str,
        target: str,
        signature: str,
        confidence: float,
        classifier_version: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        route_id = new_id("route")
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO feedback_route(id,decision_id,target,signature,confidence,"
                "classifier_version,evidence_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    route_id,
                    decision_id,
                    target,
                    signature,
                    confidence,
                    classifier_version,
                    canonical_json(dict(evidence)),
                    now(),
                ),
            )
            row = connection.execute("SELECT * FROM feedback_route WHERE id=?", (route_id,)).fetchone()
        assert row is not None
        return dict(row)

    def list_gallery(self, view: str = "all", limit: int = 100) -> list[dict[str, Any]]:
        predicates: list[str] = []
        values: list[Any] = []
        if view == "needs_review":
            predicates = ["a.disposition=?"]
            values = [Disposition.NEEDS_HUMAN]
        elif view == "prototype_review":
            predicates = ["a.disposition=?", "a.review_kind='prototype'"]
            values = [Disposition.NEEDS_HUMAN]
        elif view == "final_review":
            predicates = ["a.disposition=?", "a.review_kind='final'"]
            values = [Disposition.NEEDS_HUMAN]
        elif view in {"machine_rejected", "human_rejected", "published", "blocked"}:
            predicates = ["a.disposition=?"]
            values = [view]
        elif view == "experiments":
            predicates = ["a.purpose<>?"]
            values = [Purpose.PRODUCTION]
        elif view != "all":
            raise ValueError(f"unknown gallery view {view}")
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        query = f"""
            SELECT a.*,c.name AS concept_name,c.brief AS concept_brief,c.tags_json,
              (SELECT ar.id FROM artifact ar WHERE ar.attempt_id=a.id
               ORDER BY ar.created_at DESC LIMIT 1) AS latest_artifact_id
            FROM attempt a JOIN concept c ON c.id=a.concept_id{where}
            ORDER BY a.updated_at DESC LIMIT ?
        """
        with self.connect() as connection:
            rows = connection.execute(query, (*values, limit)).fetchall()
        return [dict(row) for row in rows]

    def enqueue_outbox(
        self,
        *,
        idempotency_key: str,
        destination: str,
        event_ref: str,
        payload_ref: str,
        payload_hash: str,
    ) -> dict[str, Any]:
        timestamp = now()
        with self.transaction() as connection:
            row = self._enqueue_outbox_in(
                connection,
                idempotency_key=idempotency_key,
                destination=destination,
                event_ref=event_ref,
                payload_ref=payload_ref,
                payload_hash=payload_hash,
                timestamp=timestamp,
            )
        return dict(row)

    @staticmethod
    def _enqueue_outbox_in(
        connection: sqlite3.Connection,
        *,
        idempotency_key: str,
        destination: str,
        event_ref: str,
        payload_ref: str,
        payload_hash: str,
        timestamp: str,
    ) -> sqlite3.Row:
        existing = connection.execute(
            "SELECT * FROM outbox_message WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if existing:
            expected = (destination, event_ref, payload_ref, payload_hash)
            actual = (
                existing["destination"],
                existing["event_ref"],
                existing["payload_ref"],
                existing["payload_hash"],
            )
            if actual != expected:
                raise VersionConflict(f"outbox key reused with different payload: {idempotency_key}")
            return existing
        message_id = new_id("outbox")
        connection.execute(
            "INSERT INTO outbox_message(id,idempotency_key,destination,event_ref,payload_ref,"
            "payload_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                message_id,
                idempotency_key,
                destination,
                event_ref,
                payload_ref,
                payload_hash,
                timestamp,
                timestamp,
            ),
        )
        row = connection.execute("SELECT * FROM outbox_message WHERE id=?", (message_id,)).fetchone()
        assert row is not None
        return row

    def pending_outbox(self, limit: int = 20, *, at: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM outbox_message WHERE status IN ('pending','retry') "
                "AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
                "ORDER BY COALESCE(next_attempt_at,created_at),created_at LIMIT ?",
                (at or now(), limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_outbox(self, message_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM outbox_message WHERE id=?", (message_id,)).fetchone()
        if row is None:
            raise RecordNotFound(message_id)
        return dict(row)

    def outbox_delivery_attempts(self, message_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM outbox_delivery_attempt WHERE message_id=? ORDER BY attempt_number",
                (message_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_outbox_delivery(
        self,
        message_id: str,
        expected_version: int,
        *,
        status: str,
        outcome: str,
        error: str | None = None,
        response_code: int | None = None,
        next_attempt_at: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"delivered", "retry", "dead_letter"}:
            raise ValueError(f"invalid outbox delivery status {status}")
        if outcome not in {"delivered", "transient_failure", "permanent_failure", "attempts_exhausted"}:
            raise ValueError(f"invalid outbox delivery outcome {outcome}")
        timestamp = now()
        with self.transaction() as connection:
            current = connection.execute(
                "SELECT * FROM outbox_message WHERE id=? AND version=?", (message_id, expected_version)
            ).fetchone()
            if current is None:
                raise VersionConflict(message_id)
            attempt_number = int(current["attempts"]) + 1
            connection.execute(
                "INSERT INTO outbox_delivery_attempt(id,message_id,attempt_number,outcome,"
                "response_code,error,created_at) VALUES(?,?,?,?,?,?,?)",
                (
                    new_id("delivery"),
                    message_id,
                    attempt_number,
                    outcome,
                    response_code,
                    error,
                    timestamp,
                ),
            )
            changed = connection.execute(
                "UPDATE outbox_message SET status=?,attempts=attempts+1,last_error=?,"
                "next_attempt_at=?,delivered_at=?,dead_lettered_at=?,"
                "version=version+1,updated_at=? WHERE id=? AND version=?",
                (
                    status,
                    error,
                    next_attempt_at if status == "retry" else None,
                    timestamp if status == "delivered" else None,
                    timestamp if status == "dead_letter" else None,
                    timestamp,
                    message_id,
                    expected_version,
                ),
            ).rowcount
            if changed != 1:
                raise VersionConflict(message_id)
            row = connection.execute("SELECT * FROM outbox_message WHERE id=?", (message_id,)).fetchone()
        assert row is not None
        return dict(row)

    def update_outbox(
        self,
        message_id: str,
        expected_version: int,
        *,
        status: str,
        error: str | None = None,
    ) -> None:
        """Compatibility wrapper; dispatchers should retain richer delivery outcomes."""

        current = self.get_outbox(message_id)
        if int(current["version"]) != expected_version:
            raise VersionConflict(message_id)
        normalized = "delivered" if status == "sent" else status
        outcome = "delivered" if normalized == "delivered" else "transient_failure"
        self.record_outbox_delivery(
            message_id,
            expected_version,
            status=normalized,
            outcome=outcome,
            error=error,
        )

    def total_cost(self, *, since: str | None = None) -> int:
        """Return immutable charged-cost audit totals."""

        where = " WHERE created_at>=?" if since else ""
        values = (since,) if since else ()
        with self.connect() as connection:
            row = connection.execute(
                f"SELECT coalesce(sum(cost_charged_micros),0) FROM operation{where}", values
            ).fetchone()
        assert row is not None
        return int(row[0])

    def cost_exposure(
        self,
        *,
        attempt_id: str | None = None,
        since: str | None = None,
    ) -> int:
        """Return actual terminal charges plus live uncertain reservations.

        The cumulative reservation columns remain immutable audit history. For
        admission control, a completed operation contributes its actual charge
        while INTENT/RUNNING/reconciliation-required work contributes the
        greater of its reservation and any conservative unknown-outcome charge.
        """

        predicates: list[str] = []
        values: list[Any] = []
        if attempt_id is not None:
            predicates.append("attempt_id=?")
            values.append(attempt_id)
        if since is not None:
            predicates.append("created_at>=?")
            values.append(since)
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        outstanding = (
            OperationStatus.INTENT,
            OperationStatus.RUNNING,
            OperationStatus.RECONCILIATION_REQUIRED,
        )
        placeholders = ",".join("?" for _ in outstanding)
        with self.connect() as connection:
            row = connection.execute(
                f"""SELECT coalesce(sum(
                        CASE WHEN status IN ({placeholders})
                             THEN max(cost_reserved_micros,cost_charged_micros)
                             ELSE cost_charged_micros END
                    ),0) FROM operation{where}""",
                (*outstanding, *values),
            ).fetchone()
        assert row is not None
        return int(row[0])

    def set_active_behavior(self, name: str, git_ref: str, sha: str, expected_sha: str | None = None) -> None:
        with self.transaction() as connection:
            current = connection.execute("SELECT * FROM active_behavior WHERE name=?", (name,)).fetchone()
            if current and expected_sha is not None and current["sha"] != expected_sha:
                raise VersionConflict(f"active {name} moved from expected SHA")
            if current:
                connection.execute(
                    "UPDATE active_behavior SET git_ref=?,sha=?,version=version+1,updated_at=? WHERE name=?",
                    (git_ref, sha, now(), name),
                )
            else:
                connection.execute(
                    "INSERT INTO active_behavior(name,git_ref,sha,updated_at) VALUES(?,?,?,?)",
                    (name, git_ref, sha, now()),
                )

    def active_behavior(self, name: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM active_behavior WHERE name=?", (name,)).fetchone()
        return self._dict(row)

    def human_label_count(self, action: str = "build_quality_label") -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT count(*) FROM human_decision WHERE action=? AND authority_evaluation_id IS NOT NULL",
                (action,),
            ).fetchone()
        assert row is not None
        return int(row[0])

    def judge_calibration_examples(
        self,
        *,
        action: str,
        evaluator_version: str,
    ) -> list[dict[str, Any]]:
        """Return labels transactionally authorized by exact hidden judge evidence.

        The calibration service still performs final exact-pixel deduplication
        because linked attempts can retain the same content-addressed pixels.
        Service-authored, pre-judge, unblinded, and legacy direct rows have no
        authority link and are excluded defensively.
        """

        with self.connect() as connection:
            rows = connection.execute(
                """SELECT d.id AS decision_id,d.artifact_id,a.content_hash,d.action,d.tags_json,
                          d.actor,d.created_at AS label_created_at,
                          e.id AS evaluation_id,e.evaluator_version,e.verdict,
                          e.created_at AS evaluation_created_at
                   FROM human_decision d
                   JOIN artifact a ON a.id=d.artifact_id
                   JOIN evaluation e ON e.id=d.authority_evaluation_id
                   WHERE d.action=? AND d.actor LIKE 'human:%'
                     AND e.evaluator='visual_judge' AND e.evaluator_version=?
                     AND e.gate_name='visual_fidelity' AND e.hidden_until_label=1
                     AND e.artifact_id=d.artifact_id AND e.attempt_id=d.attempt_id
                     AND e.created_at<=d.created_at
                   ORDER BY d.created_at ASC""",
                (action, evaluator_version),
            ).fetchall()
        return [dict(row) for row in rows]

    def judge_evaluator_versions(self, *, action: str) -> list[str]:
        """List exact resolved-model/rubric identities with blind human labels."""

        with self.connect() as connection:
            rows = connection.execute(
                """SELECT e.evaluator_version,max(d.created_at) AS latest_label
                   FROM human_decision d
                   JOIN artifact a ON a.id=d.artifact_id
                   JOIN evaluation e ON e.id=d.authority_evaluation_id
                   WHERE d.action=? AND d.actor LIKE 'human:%'
                     AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                     AND e.hidden_until_label=1
                     AND e.artifact_id=d.artifact_id AND e.attempt_id=d.attempt_id
                     AND e.created_at<=d.created_at
                   GROUP BY e.evaluator_version
                   ORDER BY latest_label ASC,e.evaluator_version ASC""",
                (action,),
            ).fetchall()
        return [str(row["evaluator_version"]) for row in rows]

    def latest_judge_evaluator_version(self, *, rubric_suffix: str) -> str | None:
        """Return the actual model/rubric identity most recently used on pixels."""

        with self.connect() as connection:
            row = connection.execute(
                "SELECT evaluator_version FROM evaluation WHERE evaluator='visual_judge' "
                "AND gate_name='visual_fidelity' AND evaluator_version LIKE ? "
                "ORDER BY created_at DESC,id DESC LIMIT 1",
                (f"%{rubric_suffix}",),
            ).fetchone()
        return str(row[0]) if row is not None else None

    def unlabeled_feedback_routes(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT d.*,a.concept_id FROM human_decision d JOIN attempt a ON a.id=d.attempt_id "
                "LEFT JOIN feedback_route r ON r.decision_id=d.id WHERE r.id IS NULL "
                "AND (d.action NOT IN ('prototype_label','build_quality_label') "
                "OR d.authority_evaluation_id IS NOT NULL) "
                "AND length(trim(d.feedback))>0 ORDER BY d.created_at LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def optimizer_examples(
        self,
        action: str = "build_quality_label",
        *,
        target: str = "authoring_playbook",
        min_confidence: float = 0.8,
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """SELECT d.*,a.concept_id,a.approved_prototype_hash,a.prototype_decision_id,
                    c.name AS concept_name,c.brief AS concept_brief,r.target,r.signature,r.confidence
                   FROM human_decision d
                   JOIN attempt a ON a.id=d.attempt_id
                   JOIN concept c ON c.id=a.concept_id
                   JOIN feedback_route r ON r.decision_id=d.id
                   JOIN evaluation e ON e.id=d.authority_evaluation_id
                   WHERE d.action=? AND r.target=? AND r.confidence>=?
                     AND d.actor LIKE 'human:%'
                     AND e.artifact_id=d.artifact_id AND e.attempt_id=d.attempt_id
                     AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                     AND e.hidden_until_label=1 AND e.created_at<=d.created_at
                   ORDER BY d.created_at""",
                (action, target, min_confidence),
            ).fetchall()
        return [dict(row) for row in rows]

    def used_holdout_concepts(self, *, target: str, holdout_epoch: str) -> set[str]:
        """Return concepts whose sealed metric was already queried in an epoch."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT concept_id FROM optimizer_holdout_use WHERE target=? AND holdout_epoch=?",
                (target, holdout_epoch),
            ).fetchall()
        return {str(row[0]) for row in rows}

    def reserve_holdout(
        self,
        *,
        target: str,
        holdout_epoch: str,
        optimization_run_id: str,
        dataset_version: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Immutably reserve exact holdout concepts before their first query.

        Replaying the same run/dataset/fixture is idempotent. Any attempt to
        query a concept again in the same human-controlled epoch fails closed.
        """

        timestamp = now()
        reserved: list[dict[str, Any]] = []
        with self.transaction() as connection:
            for item in rows:
                concept_id = str(item["concept_id"])
                prototype_hash = str(item["prototype_hash"])
                existing = connection.execute(
                    "SELECT * FROM optimizer_holdout_use WHERE target=? AND holdout_epoch=? AND concept_id=?",
                    (target, holdout_epoch, concept_id),
                ).fetchone()
                if existing is not None:
                    same_query = (
                        existing["optimization_run_id"] == optimization_run_id
                        and existing["dataset_version"] == dataset_version
                        and existing["prototype_hash"] == prototype_hash
                    )
                    if not same_query:
                        raise VersionConflict(
                            f"sealed holdout concept {concept_id} was already queried in epoch {holdout_epoch}"
                        )
                    reserved.append(dict(existing))
                    continue
                use_id = new_id("holdout")
                connection.execute(
                    "INSERT INTO optimizer_holdout_use(id,target,holdout_epoch,concept_id,"
                    "prototype_hash,optimization_run_id,dataset_version,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (
                        use_id,
                        target,
                        holdout_epoch,
                        concept_id,
                        prototype_hash,
                        optimization_run_id,
                        dataset_version,
                        timestamp,
                    ),
                )
                row = connection.execute("SELECT * FROM optimizer_holdout_use WHERE id=?", (use_id,)).fetchone()
                assert row is not None
                reserved.append(dict(row))
        return reserved

    def create_optimization_run(
        self,
        *,
        target: str,
        dataset_version: str,
        teacher_config: Mapping[str, Any],
        student_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        run_id = new_id("optimization")
        timestamp = now()
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO optimization_run(id,target,dataset_version,teacher_config_json,"
                "student_config_json,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    target,
                    dataset_version,
                    canonical_json(dict(teacher_config)),
                    canonical_json(dict(student_config)),
                    "queued",
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute("SELECT * FROM optimization_run WHERE id=?", (run_id,)).fetchone()
        assert row is not None
        return dict(row)

    def judge_calibration(
        self,
        kind: str,
        evaluator_version: str | None = None,
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            if evaluator_version is None:
                row = connection.execute(
                    "SELECT * FROM judge_calibration WHERE kind=? ORDER BY updated_at DESC LIMIT 1",
                    (kind,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM judge_calibration WHERE kind=? AND evaluator_version=?",
                    (kind, evaluator_version),
                ).fetchone()
        return self._dict(row)

    def set_judge_calibration(
        self,
        *,
        kind: str,
        evaluator_version: str,
        sample_size: int,
        true_positive: int,
        true_negative: int,
        false_positive: int,
        false_negative: int,
        lower_confidence: float,
        upper_confidence: float,
        stale: bool,
        precision: float = 0.0,
        recall: float = 0.0,
        false_approve_rate: float = 0.0,
        false_reject_rate: float = 0.0,
        reversal_rate: float = 0.0,
        uncertainty_rate: float = 0.0,
        false_approve_upper: float = 1.0,
        false_reject_upper: float = 1.0,
        reversal_upper: float = 1.0,
        uncertainty_upper: float = 1.0,
        uncertain_count: int = 0,
        latest_label_at: str | None = None,
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO judge_calibration(
                    kind,evaluator_version,sample_size,true_positive,true_negative,false_positive,
                    false_negative,lower_confidence,upper_confidence,stale,precision,recall,
                    false_approve_rate,false_reject_rate,reversal_rate,uncertainty_rate,
                    false_approve_upper,false_reject_upper,reversal_upper,uncertainty_upper,
                    uncertain_count,latest_label_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(kind,evaluator_version) DO UPDATE SET
                    sample_size=excluded.sample_size,true_positive=excluded.true_positive,
                    true_negative=excluded.true_negative,false_positive=excluded.false_positive,
                    false_negative=excluded.false_negative,
                    lower_confidence=excluded.lower_confidence,
                    upper_confidence=excluded.upper_confidence,stale=excluded.stale,
                    precision=excluded.precision,recall=excluded.recall,
                    false_approve_rate=excluded.false_approve_rate,
                    false_reject_rate=excluded.false_reject_rate,
                    reversal_rate=excluded.reversal_rate,
                    uncertainty_rate=excluded.uncertainty_rate,
                    false_approve_upper=excluded.false_approve_upper,
                    false_reject_upper=excluded.false_reject_upper,
                    reversal_upper=excluded.reversal_upper,
                    uncertainty_upper=excluded.uncertainty_upper,
                    uncertain_count=excluded.uncertain_count,
                    latest_label_at=excluded.latest_label_at,
                    updated_at=excluded.updated_at""",
                (
                    kind,
                    evaluator_version,
                    sample_size,
                    true_positive,
                    true_negative,
                    false_positive,
                    false_negative,
                    lower_confidence,
                    upper_confidence,
                    int(stale),
                    precision,
                    recall,
                    false_approve_rate,
                    false_reject_rate,
                    reversal_rate,
                    uncertainty_rate,
                    false_approve_upper,
                    false_reject_upper,
                    reversal_upper,
                    uncertainty_upper,
                    uncertain_count,
                    latest_label_at,
                    now(),
                ),
            )

    def ready_optimization_run(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM optimization_run WHERE state IN "
                "('queued','running','evaluating_development','evaluating_holdout') "
                "ORDER BY created_at LIMIT 1"
            ).fetchone()
        return self._dict(row)

    def get_optimization_run(self, run_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM optimization_run WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise RecordNotFound(run_id)
        return dict(row)

    def optimization_run_for_dataset(self, target: str, dataset_version: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM optimization_run WHERE target=? AND dataset_version=? ORDER BY created_at DESC LIMIT 1",
                (target, dataset_version),
            ).fetchone()
        return self._dict(row)

    def latest_optimization_run(self, target: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM optimization_run WHERE target=? ORDER BY created_at DESC LIMIT 1",
                (target,),
            ).fetchone()
        return self._dict(row)

    def update_optimization_run(self, run_id: str, expected_version: int, **fields: Any) -> dict[str, Any]:
        allowed = {
            "candidate_refs_json",
            "train_metrics_json",
            "dev_metrics_json",
            "holdout_metrics_json",
            "state",
            "promoted_ref",
            "promoted_sha",
        }
        unknown = set(fields).difference(allowed)
        if unknown:
            raise ValueError(f"optimization fields are not mutable: {sorted(unknown)}")
        encoded = {
            name: canonical_json(value)
            if name.endswith("_json") and value is not None and not isinstance(value, str)
            else value
            for name, value in fields.items()
        }
        with self.transaction() as connection:
            changed = connection.execute(
                f"UPDATE optimization_run SET {','.join(f'{name}=?' for name in encoded)},"
                "version=version+1,updated_at=? WHERE id=? AND version=?",
                (*encoded.values(), now(), run_id, expected_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(run_id)
            row = connection.execute("SELECT * FROM optimization_run WHERE id=?", (run_id,)).fetchone()
        assert row is not None
        return dict(row)

    def experiment_attempts(self, run_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM attempt WHERE experiment_run_id=? ORDER BY created_at", (run_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def create_technique_candidate(
        self,
        *,
        source_artifact_id: str | None,
        recipe_ref: str,
        recipe_sha: str,
        fixture_refs: Sequence[str],
        run_id: str | None = None,
    ) -> dict[str, Any]:
        candidate_id = new_id("technique")
        timestamp = now()
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO technique_candidate(id,source_artifact_id,recipe_ref,recipe_sha,"
                "fixture_refs_json,disposition,run_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    candidate_id,
                    source_artifact_id,
                    recipe_ref,
                    recipe_sha,
                    canonical_json(list(fixture_refs)),
                    "experimental",
                    run_id,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute("SELECT * FROM technique_candidate WHERE id=?", (candidate_id,)).fetchone()
        assert row is not None
        return dict(row)

    def ready_technique_candidate(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM technique_candidate WHERE disposition IN "
                "('experimental','trials_running') ORDER BY created_at LIMIT 1"
            ).fetchone()
        return self._dict(row)

    def technique_source_exists(self, source_artifact_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM technique_candidate WHERE source_artifact_id=? LIMIT 1",
                (source_artifact_id,),
            ).fetchone()
        return row is not None

    def successful_novel_traces(
        self,
        limit: int = 20,
        *,
        min_confidence: float = 0.8,
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """SELECT ar.*,a.concept_id,a.approved_prototype_hash,a.prototype_decision_id,
                           d.feedback,d.tags_json,d.id AS decision_id,
                           r.target AS route_target,r.signature AS route_signature,
                           r.confidence AS route_confidence
                   FROM artifact ar
                   JOIN attempt a ON a.id=ar.attempt_id
                   JOIN human_decision d ON d.attempt_id=a.id AND d.action='build_quality_label'
                   JOIN feedback_route r ON r.decision_id=d.id
                   JOIN evaluation e ON e.id=d.authority_evaluation_id
                   WHERE ar.kind=? AND a.purpose=?
                     AND a.stage IN ('final_review','complete')
                     AND a.production_revision IS NOT NULL
                     AND a.production_content_hash IS NOT NULL
                     AND d.actor LIKE 'human:%'
                     AND e.artifact_id=d.artifact_id AND e.attempt_id=d.attempt_id
                     AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                     AND e.hidden_until_label=1 AND e.created_at<=d.created_at
                     AND d.tags_json LIKE '%"outcome:accept"%'
                     AND r.target='authoring_playbook' AND r.confidence>=?
                     AND (
                       lower(d.feedback) LIKE '%novel%'
                       OR lower(d.feedback) LIKE '%technique%'
                       OR lower(d.tags_json) LIKE '%novel%'
                       OR json_extract(ar.metadata_json,'$.novelty_candidate')=1
                       OR (
                         lower(r.signature) LIKE '%novel%'
                         OR lower(r.signature) LIKE '%animation%'
                         OR lower(r.signature) LIKE '%effect%'
                         OR lower(r.signature) LIKE '%asset%'
                         OR lower(r.signature) LIKE '%sprite%'
                         OR lower(r.signature) LIKE '%texture%'
                       )
                     )
                   ORDER BY d.created_at DESC LIMIT ?""",
                (ArtifactKind.WORKER_TRACE, Purpose.PRODUCTION, min_confidence, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_technique_candidate(
        self, candidate_id: str, expected_version: int, *, disposition: str, trial_results: Any
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            changed = connection.execute(
                "UPDATE technique_candidate SET disposition=?,trial_results_json=?,"
                "version=version+1,updated_at=? WHERE id=? AND version=?",
                (disposition, canonical_json(trial_results), now(), candidate_id, expected_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(candidate_id)
            row = connection.execute("SELECT * FROM technique_candidate WHERE id=?", (candidate_id,)).fetchone()
        assert row is not None
        return dict(row)

    def close(self) -> None:
        """Connections are per operation; retained for lifecycle symmetry."""

    def assert_file_permissions(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path.parent, 0o700)
        if not self.path.exists():
            return
        mode = self.path.stat().st_mode & 0o777
        if mode & 0o077:
            os.chmod(self.path, mode & ~0o077)
