from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from conftest import add_artifact

from snaketron_factory.db import Database, VersionConflict
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    ProviderError,
    ProviderFailureKind,
    ProviderResult,
    Purpose,
    Stage,
)
from snaketron_factory.objects import ObjectStore
from snaketron_factory.operations import ExistingOperation, OperationJournal
from snaketron_factory.persistence import ResultPersistence
from snaketron_factory.review import ReviewService


class FakeApi:
    def __init__(self) -> None:
        self.publish_calls: list[dict[str, Any]] = []
        self.request_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[dict[str, Any]] = []
        self.cancel_error: ProviderError | None = None
        self.publish_errors: list[ProviderError] = []
        self.request_errors: list[ProviderError] = []
        self.cancel_errors: list[ProviderError] = []

    async def publish_exact(self, **request: Any) -> ProviderResult:
        self.publish_calls.append(request)
        if self.publish_errors:
            raise self.publish_errors.pop(0)
        return ProviderResult(
            value={"publication": "published", **request},
            request_id=f"publish-{len(self.publish_calls)}",
            resolved_model="snaketron-api",
        )

    async def request_publication_exact(self, **request: Any) -> ProviderResult:
        self.request_calls.append(request)
        if self.request_errors:
            raise self.request_errors.pop(0)
        return ProviderResult(
            value={"pending": True, **request},
            request_id=f"request-{len(self.request_calls)}",
            resolved_model="snaketron-api",
        )

    async def cancel_publication_request_exact(self, **request: Any) -> ProviderResult:
        self.cancel_calls.append(request)
        if self.cancel_errors:
            raise self.cancel_errors.pop(0)
        if self.cancel_error is not None:
            raise self.cancel_error
        return ProviderResult(
            value={"cancelled": True, **request},
            request_id=f"cancel-{len(self.cancel_calls)}",
            resolved_model="snaketron-api",
        )


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


@pytest.fixture
def review(database: Database, objects: ObjectStore, api: FakeApi) -> ReviewService:
    return ReviewService(
        database,
        OperationJournal(database),
        api,  # type: ignore[arg-type]
        ResultPersistence(objects),
        lambda: {
            "direction_sha": "a" * 64,
            "skill_sha": "b" * 64,
            "capability_sha": "c" * 64,
            "gate_sha": "d" * 64,
            "model_config_sha": "e" * 64,
        },
        provider_retries=1,
    )


def test_labels_are_human_only_training_records_and_do_not_advance_attempt(
    database, objects, make_attempt, review: ReviewService
) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"pixels",
    )
    decision = review.label(
        attempt_id=attempt["id"],
        artifact_id=artifact["id"],
        kind="prototype_label",
        outcome="accept",
        feedback="blind quality label",
        tags=["readable"],
        actor="human:alex",
    )
    current = database.get_attempt(attempt["id"])
    assert decision["action"] == "prototype_label"
    assert current["stage"] == Stage.PROTOTYPE_REVIEW
    assert current["disposition"] == Disposition.NEEDS_HUMAN
    assert current["approved_prototype_hash"] is None
    with pytest.raises(PermissionError):
        review.label(
            attempt_id=attempt["id"],
            artifact_id=artifact["id"],
            kind="prototype_label",
            outcome="accept",
            feedback="judge pretending to be human",
            tags=[],
            actor="service:judge",
        )
    with pytest.raises(ValueError):
        review.label(
            attempt_id=attempt["id"],
            artifact_id=artifact["id"],
            kind="publish_approval",
            outcome="accept",
            feedback="wrong API",
            tags=[],
            actor="human:alex",
        )


@pytest.mark.asyncio
async def test_exact_prototype_approval_is_the_only_transition_into_authoring(
    database, objects, make_attempt, review: ReviewService
) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    prototype = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"selected prototype",
        media_type="image/png",
    )
    with pytest.raises(VersionConflict, match="triage override"):
        review.approve_prototype(
            attempt_id=attempt["id"],
            artifact_id=prototype["id"],
            content_hash=prototype["content_hash"],
            feedback="must be explicit",
            actor="human:alex",
        )
    override = await review.override_triage(
        attempt_id=attempt["id"],
        artifact_id=prototype["id"],
        feedback="The exact prototype is worth human direction review.",
        actor="human:alex",
    )
    assert override["decision"]["action"] == "soft_triage_override"
    assert override["attempt"]["disposition"] == Disposition.NEEDS_HUMAN
    assert override["attempt"]["review_kind"] == "prototype"
    result = review.approve_prototype(
        attempt_id=attempt["id"],
        artifact_id=prototype["id"],
        content_hash=prototype["content_hash"],
        feedback="soft-triage override",
        actor="human:alex",
    )
    assert result["decision"]["action"] == "prototype_approval"
    assert result["decision"]["content_hash"] == prototype["content_hash"]
    assert result["attempt"]["stage"] == Stage.AUTHOR
    assert result["attempt"]["disposition"] == Disposition.ACTIVE
    assert result["attempt"]["prototype_decision_id"] == result["decision"]["id"]
    replay = review.approve_prototype(
        attempt_id=attempt["id"],
        artifact_id=prototype["id"],
        content_hash=prototype["content_hash"],
        feedback="double click",
        actor="human:alex",
    )
    assert replay["decision"]["id"] == result["decision"]["id"]
    assert replay["attempt"]["version"] == result["attempt"]["version"]
    assert [row["action"] for row in database.decisions_for_attempt(attempt["id"])] == [
        "soft_triage_override",
        "prototype_approval",
    ]
    alternate = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"alternate pixels",
    )
    with pytest.raises(VersionConflict, match="prototype review state"):
        review.approve_prototype(
            attempt_id=attempt["id"],
            artifact_id=alternate["id"],
            content_hash=alternate["content_hash"],
            feedback="too late to replace active authority",
            actor="human:alex",
        )

    other = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    with pytest.raises(VersionConflict):
        review.approve_prototype(
            attempt_id=other["id"],
            artifact_id=prototype["id"],
            content_hash=prototype["content_hash"],
            feedback="cross-attempt",
            actor="human:alex",
        )
    with pytest.raises(VersionConflict):
        review.approve_prototype(
            attempt_id=other["id"],
            artifact_id=add_artifact(
                database,
                objects,
                other["id"],
                stage=Stage.PROTOTYPE,
                kind=ArtifactKind.PROTOTYPE,
                value=b"other",
            )["id"],
            content_hash="sha256:" + "0" * 64,
            feedback="wrong hash",
            actor="human:alex",
        )

    trial = make_attempt(
        stage=Stage.PROTOTYPE_REVIEW,
        purpose=Purpose.OPTIMIZER,
        disposition=Disposition.NEEDS_HUMAN,
    )
    trial_artifact = add_artifact(
        database,
        objects,
        trial["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"trial",
    )
    with pytest.raises(PermissionError, match="cannot enter production"):
        review.approve_prototype(
            attempt_id=trial["id"],
            artifact_id=trial_artifact["id"],
            content_hash=trial_artifact["content_hash"],
            feedback="forbidden",
            actor="human:alex",
        )


@pytest.mark.asyncio
async def test_human_can_override_final_soft_triage_and_publish_exact_revision(
    database, objects, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-override",
        production_revision="4",
        production_content_hash="sha256:" + "4" * 64,
    )
    contact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"exact rejected render",
        media_type="image/png",
    )

    overridden = await review.override_triage(
        attempt_id=attempt["id"],
        artifact_id=contact["id"],
        feedback="The judge missed the intentional high-contrast effect.",
        actor="human:alex",
    )
    assert overridden["decision"]["content_hash"] == contact["content_hash"]
    assert overridden["attempt"]["disposition"] == Disposition.NEEDS_HUMAN
    assert overridden["attempt"]["review_kind"] == "final"
    assert api.request_calls == [
        {
            "skin_id": "skin-override",
            "revision": 4,
            "content_ref": "sha256:" + "4" * 64,
            "operator": True,
        }
    ]

    published = await review.publish(
        attempt_id=attempt["id"],
        revision="4",
        content_hash="sha256:" + "4" * 64,
        feedback="Human override approved these exact bytes.",
        actor="human:alex",
    )
    assert published["attempt"]["disposition"] == Disposition.PUBLISHED
    assert api.publish_calls[0]["revision"] == 4


@pytest.mark.asyncio
async def test_final_override_retries_only_known_safe_exact_publication_request(
    database, objects, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-request-retry",
        production_revision="3",
        production_content_hash="sha256:" + "3" * 64,
    )
    contact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"request retry render",
        media_type="image/png",
    )
    api.request_errors.append(
        ProviderError(ProviderFailureKind.UNAVAILABLE, "connect failed before send", outcome_known=True)
    )

    result = await review.override_triage(
        attempt_id=attempt["id"],
        artifact_id=contact["id"],
        feedback="open exact final review",
        actor="human:alex",
    )

    assert result["attempt"]["disposition"] == Disposition.NEEDS_HUMAN
    assert len(api.request_calls) == 2
    with database.connect() as connection:
        operations = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM operation WHERE attempt_id=? ORDER BY idempotency_key",
                (attempt["id"],),
            )
        ]
    assert [row["status"] for row in operations] == ["failed_retryable", "succeeded"]
    assert operations[1]["idempotency_key"] == operations[0]["idempotency_key"] + ":retry:1"
    assert operations[0]["request_hash"] == operations[1]["request_hash"]


@pytest.mark.asyncio
async def test_final_rejection_retries_only_known_safe_exact_cancellation(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-cancel-retry",
        production_revision="6",
        production_content_hash="sha256:" + "6" * 64,
    )
    api.cancel_errors.append(ProviderError(ProviderFailureKind.TIMEOUT, "connect timeout", outcome_known=True))

    result = await review.reject(
        attempt_id=attempt["id"],
        artifact_id=None,
        feedback="cancel this exact pending publication",
        tags=[],
        actor="human:alex",
    )

    assert result["attempt"]["disposition"] == Disposition.HUMAN_REJECTED
    assert len(api.cancel_calls) == 2
    with database.connect() as connection:
        operations = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM operation WHERE attempt_id=? ORDER BY idempotency_key",
                (attempt["id"],),
            )
        ]
    assert [row["status"] for row in operations] == ["failed_retryable", "succeeded"]
    assert operations[1]["idempotency_key"] == operations[0]["idempotency_key"] + ":retry:1"
    assert operations[0]["request_hash"] == operations[1]["request_hash"]


@pytest.mark.parametrize(
    ("stage", "review_kind", "artifact_kind"),
    [
        (Stage.PROTOTYPE_REVIEW, "prototype", ArtifactKind.PROTOTYPE),
        (Stage.FINAL_REVIEW, "final", ArtifactKind.CONTACT_SHEET),
    ],
)
@pytest.mark.asyncio
async def test_reject_records_only_owned_artifacts_for_exact_human_review_queue(
    database,
    objects,
    make_attempt,
    review: ReviewService,
    stage: Stage,
    review_kind: str,
    artifact_kind: ArtifactKind,
) -> None:
    attempt = make_attempt(stage=stage, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(attempt["id"], attempt["version"], review_kind=review_kind)
    if stage == Stage.FINAL_REVIEW:
        attempt = database.update_attempt(
            attempt["id"],
            attempt["version"],
            production_skin_id="skin-final",
            production_revision="7",
            production_content_hash="sha256:" + "7" * 64,
        )
    selected = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=stage,
        kind=artifact_kind,
        value=b"reviewed bytes",
    )
    other = make_attempt(stage=stage, disposition=Disposition.NEEDS_HUMAN)
    other = database.update_attempt(other["id"], other["version"], review_kind=review_kind)
    foreign = add_artifact(
        database,
        objects,
        other["id"],
        stage=stage,
        kind=artifact_kind,
        value=b"foreign bytes",
    )

    with pytest.raises(VersionConflict, match="does not belong"):
        await review.reject(
            attempt_id=attempt["id"],
            artifact_id=foreign["id"],
            feedback="must not bind cross-attempt bytes",
            tags=[],
            actor="human:alex",
        )
    assert database.decisions_for_attempt(attempt["id"]) == []
    assert database.get_attempt(attempt["id"])["disposition"] == Disposition.NEEDS_HUMAN

    result = await review.reject(
        attempt_id=attempt["id"],
        artifact_id=selected["id"],
        feedback="not ready",
        tags=["readability"],
        actor="human:alex",
    )
    assert result["decision"]["content_hash"] == selected["content_hash"]
    assert result["attempt"]["disposition"] == Disposition.HUMAN_REJECTED
    assert result["attempt"]["review_kind"] is None
    expected_cancel = (
        [
            {
                "skin_id": "skin-final",
                "revision": 7,
                "content_ref": "sha256:" + "7" * 64,
            }
        ]
        if stage == Stage.FINAL_REVIEW
        else []
    )
    assert review.api.cancel_calls == expected_cancel  # type: ignore[attr-defined]
    if stage == Stage.FINAL_REVIEW:
        assert result["operation"]["side_effect"] == "cancel_exact_publication_request"


@pytest.mark.asyncio
async def test_final_rejection_stays_pending_when_exact_server_cancel_conflicts(
    database,
    make_attempt,
    review: ReviewService,
    api: FakeApi,
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-final",
        production_revision="7",
        production_content_hash="sha256:" + "7" * 64,
    )
    api.cancel_error = ProviderError(
        ProviderFailureKind.INVALID_OUTPUT,
        "HTTP 409: a newer exact request is pending",
    )

    with pytest.raises(ProviderError, match="newer exact request"):
        await review.reject(
            attempt_id=attempt["id"],
            artifact_id=None,
            feedback="reject old rendered revision",
            tags=[],
            actor="human:alex",
        )

    current = database.get_attempt(attempt["id"])
    assert current["disposition"] == Disposition.NEEDS_HUMAN
    assert current["review_kind"] == "final"
    assert database.decisions_for_attempt(attempt["id"]) == []
    with database.connect() as connection:
        operation = connection.execute("SELECT * FROM operation WHERE attempt_id=?", (attempt["id"],)).fetchone()
    assert operation is not None
    assert operation["side_effect"] == "cancel_exact_publication_request"
    assert operation["status"] == "failed_terminal"


@pytest.mark.parametrize(
    ("stage", "disposition", "review_kind"),
    [
        (Stage.AUTHOR, Disposition.ACTIVE, None),
        (Stage.PROTOTYPE_REVIEW, Disposition.MACHINE_REJECTED, None),
        (Stage.PROTOTYPE_REVIEW, Disposition.NEEDS_HUMAN, "prototype_label"),
        (Stage.FINAL_REVIEW, Disposition.NEEDS_HUMAN, "build_label"),
        (Stage.COMPLETE, Disposition.PUBLISHED, None),
    ],
)
@pytest.mark.asyncio
async def test_reject_cannot_replace_training_labels_machine_triage_or_takedown(
    database,
    make_attempt,
    review: ReviewService,
    stage: Stage,
    disposition: Disposition,
    review_kind: str | None,
) -> None:
    attempt = make_attempt(stage=stage, disposition=disposition)
    if review_kind is not None:
        attempt = database.update_attempt(attempt["id"], attempt["version"], review_kind=review_kind)
    with pytest.raises(VersionConflict, match="waiting in prototype or final review"):
        await review.reject(
            attempt_id=attempt["id"],
            artifact_id=None,
            feedback="invalid rejection state",
            tags=[],
            actor="human:alex",
        )
    assert database.decisions_for_attempt(attempt["id"]) == []
    assert database.get_attempt(attempt["id"])["disposition"] == disposition


@pytest.mark.asyncio
async def test_reject_cannot_turn_an_experiment_into_a_human_production_decision(
    database, make_attempt, review: ReviewService
) -> None:
    attempt = make_attempt(
        stage=Stage.FINAL_REVIEW,
        purpose=Purpose.TECHNIQUE,
        disposition=Disposition.NEEDS_HUMAN,
    )
    attempt = database.update_attempt(attempt["id"], attempt["version"], review_kind="final")
    with pytest.raises(PermissionError, match="production review decisions"):
        await review.reject(
            attempt_id=attempt["id"],
            artifact_id=None,
            feedback="invalid trial review",
            tags=[],
            actor="human:alex",
        )
    assert database.decisions_for_attempt(attempt["id"]) == []


def test_retry_creates_linked_child_and_never_rewrites_parent_history(
    database, objects, make_attempt, review: ReviewService
) -> None:
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.HUMAN_REJECTED)
    parent_before = dict(parent)
    child_result = review.retry(
        attempt_id=parent["id"],
        from_stage="prototype",
        feedback="try brighter colors",
        actor="human:alex",
    )
    child = child_result["attempt"]
    assert child["id"] != parent["id"]
    assert child["parent_attempt_id"] == parent["id"]
    assert child["restart_stage"] == "prototype"
    assert child["stage"] == Stage.PROTOTYPE
    assert child["approved_prototype_hash"] is None
    assert child["prototype_decision_id"] is None
    assert database.get_attempt(parent["id"])["stage"] == parent_before["stage"]
    assert database.decisions_for_attempt(parent["id"])[0]["feedback"] == "try brighter colors"

    unauthorized = make_attempt(stage=Stage.BUILD_GATE, disposition=Disposition.MACHINE_REJECTED)
    decisions_before = database.decisions_for_attempt(unauthorized["id"])
    with pytest.raises(VersionConflict, match="no exact prototype authority"):
        review.retry(
            attempt_id=unauthorized["id"],
            from_stage="assets",
            feedback="this must not persist",
            actor="human:alex",
        )
    assert database.decisions_for_attempt(unauthorized["id"]) == decisions_before


def test_retry_double_submit_and_lost_response_return_one_paid_child(
    database, make_attempt, review: ReviewService
) -> None:
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.HUMAN_REJECTED)

    def submit() -> dict[str, Any]:
        return review.retry(
            attempt_id=parent["id"],
            from_stage="prototype",
            feedback="try one brighter pass",
            actor="human:alex",
            idempotency_key="browser-form-7",
        )

    # Two concurrent browser POSTs serialize at BEGIN IMMEDIATE. The second
    # observes and returns the exact committed request rather than appending.
    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = [future.result() for future in [pool.submit(submit), pool.submit(submit)]]
    assert first["decision"]["id"] == second["decision"]["id"]
    assert first["attempt"]["id"] == second["attempt"]["id"]

    # Simulate a response being lost after COMMIT and replay the same form.
    replayed = submit()
    assert replayed["decision"]["id"] == first["decision"]["id"]
    assert replayed["attempt"]["id"] == first["attempt"]["id"]
    with database.connect() as connection:
        child_count = connection.execute(
            "SELECT count(*) FROM attempt WHERE parent_attempt_id=?", (parent["id"],)
        ).fetchone()[0]
    assert child_count == 1
    assert len(database.decisions_for_attempt(parent["id"])) == 1

    with pytest.raises(VersionConflict, match="different decision"):
        review.retry(
            attempt_id=parent["id"],
            from_stage="prototype",
            feedback="changed payload under the same request key",
            actor="human:alex",
            idempotency_key="browser-form-7",
        )


def test_re_evaluation_link_is_atomic_and_replayable_after_response_loss(
    database, objects, make_attempt, review: ReviewService
) -> None:
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    prototype = add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"retained exact pixels",
    )
    # Abort at the final linked-artifact INSERT. Decision and child INSERTs
    # precede it in SQL, so their absence proves the entire unit rolled back.
    with database.connect() as connection:
        connection.execute(
            f"""CREATE TRIGGER fail_re_evaluation_link BEFORE INSERT ON artifact
                WHEN NEW.attempt_id <> '{parent["id"]}'
                BEGIN SELECT RAISE(ABORT, 'injected link crash'); END"""
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected link crash"):
        review.re_evaluate(
            attempt_id=parent["id"],
            artifact_id=prototype["id"],
            feedback="use the current rubric",
            actor="human:alex",
            idempotency_key="reeval-form-3",
        )
    with database.connect() as connection:
        assert (
            connection.execute("SELECT count(*) FROM attempt WHERE parent_attempt_id=?", (parent["id"],)).fetchone()[0]
            == 0
        )
    assert database.decisions_for_attempt(parent["id"]) == []

    with database.connect() as connection:
        connection.execute("DROP TRIGGER fail_re_evaluation_link")
    committed = review.re_evaluate(
        attempt_id=parent["id"],
        artifact_id=prototype["id"],
        feedback="use the current rubric",
        actor="human:alex",
        idempotency_key="reeval-form-3",
    )
    replayed = review.re_evaluate(
        attempt_id=parent["id"],
        artifact_id=prototype["id"],
        feedback="use the current rubric",
        actor="human:alex",
        idempotency_key="reeval-form-3",
    )
    assert replayed["decision"]["id"] == committed["decision"]["id"]
    assert replayed["attempt"]["id"] == committed["attempt"]["id"]
    assert replayed["artifact"]["id"] == committed["artifact"]["id"]
    assert committed["attempt"]["restart_stage"] == f"re_evaluate:{committed['artifact']['id']}"
    assert "link_pending" not in committed["attempt"]["restart_stage"]


def test_malformed_re_evaluation_children_are_never_schedulable(database, make_attempt, review: ReviewService) -> None:
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    snapshot = review.behavior_snapshot()
    malformed = database.create_attempt(
        concept_id=parent["concept_id"],
        purpose=Purpose.PRODUCTION,
        parent_attempt_id=parent["id"],
        restart_stage="re_evaluate:link_pending",
        stage=Stage.PROTOTYPE_TRIAGE,
        idempotency_key="legacy-malformed-link",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    assert malformed["disposition"] == Disposition.ACTIVE
    assert database.next_active_attempt() is None


def test_bulk_retry_resumes_a_committed_prefix_without_duplicate_children(
    database, make_attempt, review: ReviewService
) -> None:
    parents = [make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.HUMAN_REJECTED) for _ in range(3)]

    def submit(parent: dict[str, Any]) -> dict[str, Any]:
        return review.retry(
            attempt_id=parent["id"],
            from_stage="prototype",
            feedback="bulk retry retained rejects",
            actor="human:alex",
            idempotency_key="bulk-request-11",
        )

    committed_prefix = [submit(parent) for parent in parents[:2]]
    # A process crash loses the prefix response. Re-running the whole ordered
    # request replays those rows and appends only the missing suffix.
    resumed = [submit(parent) for parent in parents]
    assert [row["attempt"]["id"] for row in resumed[:2]] == [row["attempt"]["id"] for row in committed_prefix]
    with database.connect() as connection:
        children = connection.execute(
            "SELECT parent_attempt_id,count(*) AS n FROM attempt "
            "WHERE parent_attempt_id IN (?,?,?) GROUP BY parent_attempt_id",
            tuple(parent["id"] for parent in parents),
        ).fetchall()
    assert {row["parent_attempt_id"]: row["n"] for row in children} == {parent["id"]: 1 for parent in parents}
    assert all(len(database.decisions_for_attempt(parent["id"])) == 1 for parent in parents)


def test_retry_and_re_evaluation_preserve_trial_namespace_and_non_publishability(
    database, objects, make_attempt, review: ReviewService
) -> None:
    trial = make_attempt(
        stage=Stage.PROTOTYPE_REVIEW,
        purpose=Purpose.OPTIMIZER,
        disposition=Disposition.MACHINE_REJECTED,
    )
    prototype = add_artifact(
        database,
        objects,
        trial["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"optimizer pixels",
    )
    reevaluated = review.re_evaluate(
        attempt_id=trial["id"],
        artifact_id=prototype["id"],
        feedback="measure with current rubric",
        actor="human:alex",
    )
    assert reevaluated["attempt"]["purpose"] == Purpose.OPTIMIZER
    with pytest.raises(PermissionError, match="cannot enter production"):
        review.approve_prototype(
            attempt_id=reevaluated["attempt"]["id"],
            artifact_id=reevaluated["artifact"]["id"],
            content_hash=reevaluated["artifact"]["content_hash"],
            feedback="must remain isolated",
            actor="human:alex",
        )

    retried = review.retry(
        attempt_id=trial["id"],
        from_stage="prototype",
        feedback="new isolated rollout",
        actor="human:alex",
    )["attempt"]
    assert retried["purpose"] == Purpose.OPTIMIZER
    assert retried["parent_attempt_id"] == trial["id"]


def test_asset_retry_inherits_only_exact_prototype_authority(
    database, objects, make_attempt, review: ReviewService
) -> None:
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    prototype = add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"approved",
    )
    approved = review.approve_prototype(
        attempt_id=parent["id"],
        artifact_id=prototype["id"],
        content_hash=prototype["content_hash"],
        feedback="approved",
        actor="human:alex",
    )["attempt"]
    approved = database.update_attempt(approved["id"], approved["version"], disposition=Disposition.MACHINE_REJECTED)
    child = review.retry(
        attempt_id=approved["id"],
        from_stage="assets",
        feedback="regenerate the texture",
        actor="human:alex",
    )["attempt"]
    assert child["stage"] == Stage.ASSETS
    assert child["approved_prototype_hash"] == prototype["content_hash"]
    assert child["prototype_decision_id"] == approved["prototype_decision_id"]


def test_re_evaluate_uses_existing_bytes_and_routes_documents_through_current_hard_gates(
    database, objects, make_attempt, review: ReviewService
) -> None:
    prototype_attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    prototype = add_artifact(
        database,
        objects,
        prototype_attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"old prototype",
    )
    prototype_result = review.re_evaluate(
        attempt_id=prototype_attempt["id"],
        artifact_id=prototype["id"],
        feedback="new judge rubric",
        actor="human:alex",
    )
    prototype_child = prototype_result["attempt"]
    prototype_link = prototype_result["artifact"]
    assert prototype_child["stage"] == Stage.PROTOTYPE_TRIAGE
    assert prototype_child["restart_stage"] == f"re_evaluate:{prototype_link['id']}"
    assert prototype_link["attempt_id"] == prototype_child["id"]
    assert prototype_link["content_hash"] == prototype["content_hash"]
    assert prototype_link["object_ref"] == prototype["object_ref"]
    assert json.loads(prototype_link["metadata_json"])["re_evaluates_artifact_id"] == prototype["id"]

    build_attempt = make_attempt(stage=Stage.BUILD_GATE, disposition=Disposition.MACHINE_REJECTED)
    document = add_artifact(
        database,
        objects,
        build_attempt["id"],
        stage=Stage.BUILD_GATE,
        kind=ArtifactKind.SKIN_DOCUMENT,
        value=b'{"schema_version":2}',
        media_type="application/json",
    )
    build_result = review.re_evaluate(
        attempt_id=build_attempt["id"],
        artifact_id=document["id"],
        feedback="rerun current deterministic gates",
        actor="human:alex",
    )
    build_child = build_result["attempt"]
    assert build_child["stage"] == Stage.BUILD_GATE
    assert build_child["restart_stage"] == f"re_evaluate:{build_result['artifact']['id']}"
    assert build_result["artifact"]["content_hash"] == document["content_hash"]


@pytest.mark.asyncio
async def test_publication_is_human_exact_final_review_only_and_idempotent(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-1",
        production_revision="7",
        production_content_hash="sha256:" + "7" * 64,
    )
    with pytest.raises(VersionConflict, match="exact reviewed revision"):
        await review.publish(
            attempt_id=attempt["id"],
            revision="8",
            content_hash=attempt["production_content_hash"],
            feedback="wrong revision",
            actor="human:alex",
        )
    with pytest.raises(PermissionError):
        await review.publish(
            attempt_id=attempt["id"],
            revision="7",
            content_hash=attempt["production_content_hash"],
            feedback="service cannot publish",
            actor="service:hermes",
        )
    assert api.publish_calls == []

    result = await review.publish(
        attempt_id=attempt["id"],
        revision="7",
        content_hash=attempt["production_content_hash"],
        feedback="Ship it",
        actor="human:alex",
    )
    assert result["decision"]["action"] == "publish_approval"
    assert result["decision"]["revision"] == "7"
    assert result["decision"]["content_hash"] == attempt["production_content_hash"]
    assert result["attempt"]["stage"] == Stage.COMPLETE
    assert result["attempt"]["disposition"] == Disposition.PUBLISHED
    assert api.publish_calls == [
        {
            "skin_id": "skin-1",
            "revision": 7,
            "content_ref": attempt["production_content_hash"],
            "reason": "Ship it",
        }
    ]

    replay = await review.publish(
        attempt_id=attempt["id"],
        revision="7",
        content_hash=attempt["production_content_hash"],
        feedback="replayed click",
        actor="human:alex",
    )
    assert replay["decision"]["id"] == result["decision"]["id"]
    assert replay["operation"]["id"] == result["operation"]["id"]
    assert len(api.publish_calls) == 1


@pytest.mark.asyncio
async def test_publish_retries_known_safe_failure_and_replays_exact_human_decision(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-publish-retry",
        production_revision="8",
        production_content_hash="sha256:" + "8" * 64,
    )
    api.publish_errors.append(
        ProviderError(ProviderFailureKind.UNAVAILABLE, "connect failed before send", outcome_known=True)
    )

    result = await review.publish(
        attempt_id=attempt["id"],
        revision="8",
        content_hash=attempt["production_content_hash"],
        feedback="the exact retained approval reason",
        actor="human:alex",
    )

    assert result["attempt"]["disposition"] == Disposition.PUBLISHED
    assert len(api.publish_calls) == 2
    assert {call["reason"] for call in api.publish_calls} == {"the exact retained approval reason"}
    with database.connect() as connection:
        operations = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM operation WHERE attempt_id=? ORDER BY idempotency_key",
                (attempt["id"],),
            )
        ]
    assert [row["status"] for row in operations] == ["failed_retryable", "succeeded"]
    assert operations[1]["idempotency_key"] == operations[0]["idempotency_key"] + ":retry:1"
    assert operations[0]["request_hash"] == operations[1]["request_hash"]

    replay = await review.publish(
        attempt_id=attempt["id"],
        revision="8",
        content_hash=attempt["production_content_hash"],
        feedback="a later click must not mutate the retained reason",
        actor="human:alex",
    )
    assert replay["decision"]["id"] == result["decision"]["id"]
    assert replay["operation"]["id"] == result["operation"]["id"]
    assert len(api.publish_calls) == 2
    assert len(database.decisions_for_attempt(attempt["id"])) == 1


@pytest.mark.asyncio
async def test_later_publish_click_resumes_numbered_retry_from_persisted_decision(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-resume-click",
        production_revision="12",
        production_content_hash="sha256:" + "c" * 64,
    )
    decision = database.add_human_decision(
        artifact_id=None,
        attempt_id=attempt["id"],
        action="publish_approval",
        feedback="persisted exact reason",
        tags=[],
        actor="human:alex",
        attempt_version=attempt["version"],
        revision="12",
        content_hash=attempt["production_content_hash"],
    )
    base_key = f"publish:{decision['id']}:12:{attempt['production_content_hash']}"
    request = {
        "skin_id": "skin-resume-click",
        "revision": 12,
        "content_ref": attempt["production_content_hash"],
        "reason": "persisted exact reason",
    }

    with pytest.raises(ProviderError):
        await review.journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.FINAL_REVIEW,
            idempotency_key=base_key,
            side_effect="publish_exact_revision",
            provider_role="human_operator",
            request=request,
            reserve_micros=0,
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(ProviderFailureKind.UNAVAILABLE, "known safe", outcome_known=True)
            ),
            persist_result=review.persistence,
        )

    resumed = await review.publish(
        attempt_id=attempt["id"],
        revision="12",
        content_hash=attempt["production_content_hash"],
        feedback="later click cannot replace the persisted reason",
        actor="human:alex",
    )

    assert resumed["decision"]["id"] == decision["id"]
    assert resumed["operation"]["idempotency_key"] == base_key + ":retry:1"
    assert api.publish_calls == [
        {
            "skin_id": "skin-resume-click",
            "revision": 12,
            "content_ref": attempt["production_content_hash"],
            "reason": "persisted exact reason",
        }
    ]


@pytest.mark.asyncio
async def test_publish_unknown_outcome_persists_one_decision_and_requires_reconciliation(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-publish-unknown",
        production_revision="9",
        production_content_hash="sha256:" + "9" * 64,
    )
    api.publish_errors.append(
        ProviderError(
            ProviderFailureKind.UNAVAILABLE,
            "HTTP 503 after request may have reached server",
            outcome_known=False,
        )
    )

    with pytest.raises(ProviderError, match="may have reached"):
        await review.publish(
            attempt_id=attempt["id"],
            revision="9",
            content_hash=attempt["production_content_hash"],
            feedback="retain this exact approval once",
            actor="human:alex",
        )
    assert len(api.publish_calls) == 1
    assert len(database.decisions_for_attempt(attempt["id"])) == 1
    assert database.unresolved_operations()[0]["status"] == "reconciliation_required"

    with pytest.raises(ExistingOperation, match="requires authenticated reconciliation"):
        await review.publish(
            attempt_id=attempt["id"],
            revision="9",
            content_hash=attempt["production_content_hash"],
            feedback="double click",
            actor="human:alex",
        )
    assert len(api.publish_calls) == 1
    assert len(database.decisions_for_attempt(attempt["id"])) == 1


@pytest.mark.asyncio
async def test_attempt_with_revision_fields_cannot_publish_before_final_review(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(stage=Stage.RENDER, disposition=Disposition.ACTIVE)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-private",
        production_revision="1",
        production_content_hash="sha256:" + "1" * 64,
    )
    with pytest.raises(VersionConflict, match="final review"):
        await review.publish(
            attempt_id=attempt["id"],
            revision="1",
            content_hash=attempt["production_content_hash"],
            feedback="bypass",
            actor="human:alex",
        )
    assert api.publish_calls == []


@pytest.mark.asyncio
async def test_experiment_attempt_can_never_publish(
    database, make_attempt, review: ReviewService, api: FakeApi
) -> None:
    attempt = make_attempt(
        stage=Stage.FINAL_REVIEW,
        purpose=Purpose.TECHNIQUE,
        disposition=Disposition.NEEDS_HUMAN,
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="trial-skin",
        production_revision="1",
        production_content_hash="sha256:" + "9" * 64,
    )
    with pytest.raises(PermissionError, match="non-publishable"):
        await review.publish(
            attempt_id=attempt["id"],
            revision="1",
            content_hash=attempt["production_content_hash"],
            feedback="forbidden",
            actor="human:alex",
        )
    assert api.publish_calls == []
