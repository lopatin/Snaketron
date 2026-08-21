from __future__ import annotations

import hashlib
import hmac
from pathlib import Path
from typing import Any

from conftest import add_artifact
from fastapi.testclient import TestClient

from snaketron_factory.db import Database
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    OperationStatus,
    Stage,
)
from snaketron_factory.gallery import create_app
from snaketron_factory.objects import ObjectStore
from snaketron_factory.operations import OperationJournal
from snaketron_factory.persistence import ResultPersistence
from snaketron_factory.review import ReviewService


class NoPublishApi:
    async def publish_exact(self, **_: Any):
        raise AssertionError("gallery authentication tests must not publish")


class Runtime:
    def __init__(self, database: Database, objects: ObjectStore) -> None:
        self.database = database
        self.objects = objects
        self.journal = OperationJournal(database)
        self.api = NoPublishApi()
        self.persistence = ResultPersistence(objects)

    async def close(self) -> None:
        return None

    @staticmethod
    def behavior_snapshot() -> dict[str, str]:
        return {
            "direction_sha": "a" * 64,
            "skill_sha": "b" * 64,
            "capability_sha": "c" * 64,
            "gate_sha": "d" * 64,
            "model_config_sha": "e" * 64,
        }


def review_app(factory_config, database, objects, monkeypatch):
    secret = "test-review-secret-at-least-16"
    monkeypatch.setenv(factory_config.review.operator_secret_env, secret)
    runtime = Runtime(database, objects)
    review = ReviewService(
        database,
        runtime.journal,
        runtime.api,  # type: ignore[arg-type]
        runtime.persistence,
        runtime.behavior_snapshot,
    )
    templates = Path(__file__).resolve().parents[1] / "templates"
    app = create_app(
        factory_config,
        factory=runtime,  # type: ignore[arg-type]
        review_service=review,
        template_dir=templates,
    )
    return app, secret


def auth(secret: str, actor: str = "human:alex") -> dict[str, str]:
    return {"authorization": f"Bearer {secret}", "x-review-actor": actor}


def test_gallery_requires_distinct_human_auth_and_sets_security_headers(
    factory_config, database, objects, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    with TestClient(app) as client:
        health = client.get("/healthz")
        assert health.status_code == 200
        assert health.headers["x-frame-options"] == "DENY"
        assert health.headers["x-content-type-options"] == "nosniff"
        assert "frame-ancestors 'none'" in health.headers["content-security-policy"]

        assert client.get("/api/gallery/all").status_code == 401
        assert client.get("/api/gallery/all", headers={"authorization": "Bearer wrong"}).status_code == 401
        forbidden = client.get("/api/gallery/all", headers=auth(secret, "service:hermes"))
        assert forbidden.status_code == 403
        assert client.get("/api/gallery/all", headers=auth(secret)).status_code == 200
        assert client.get("/api/gallery/not-a-view", headers=auth(secret)).status_code == 404


def test_gallery_exposes_exact_retained_artifacts_but_keeps_shadow_scores_blind(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(attempt["id"], attempt["version"], review_kind="prototype")
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"exact browseable pixels",
        media_type="image/png",
    )
    database.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.MACHINE_REJECTED,
            reasons=["blind until label"],
        ),
        hidden_until_label=True,
    )

    with TestClient(app) as client:
        gallery = client.get("/api/gallery/prototype", headers=auth(secret)).json()
        assert [row["id"] for row in gallery["items"]] == [attempt["id"]]
        detail = client.get(f"/api/attempts/{attempt['id']}", headers=auth(secret)).json()
        assert detail["evaluations_blind"] is True
        assert detail["evaluations"] == []

        response = client.get(f"/artifacts/{artifact['id']}", headers=auth(secret))
        assert response.content == b"exact browseable pixels"
        assert response.headers["etag"] == f'"{artifact["content_hash"].removeprefix("sha256:")}"'
        assert response.headers["cache-control"] == "private, max-age=31536000, immutable"
        assert response.headers["content-disposition"].startswith("inline")

        labeled = client.post(
            "/actions/label",
            headers=auth(secret),
            data={
                "attempt_id": attempt["id"],
                "artifact_id": artifact["id"],
                "kind": "prototype_label",
                "outcome": "reject",
                "feedback": "independent blind label",
                "tags": "muddy, outcome:accept",
            },
        )
        assert labeled.status_code == 200
        detail = client.get(f"/api/attempts/{attempt['id']}", headers=auth(secret)).json()
        assert detail["evaluations_blind"] is False
        assert detail["evaluations"][0]["reasons_json"] == ["blind until label"]
        assert detail["decisions"][0]["tags_json"] == ["muddy", "outcome:reject"]


def test_cookie_sessions_require_csrf_while_bearer_actions_do_not(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"pixels",
    )
    with TestClient(app) as client:
        login = client.post(
            "/login",
            data={"token": secret, "actor": "human:alex"},
            follow_redirects=False,
        )
        assert login.status_code == 303
        assert "httponly" in login.headers["set-cookie"].lower()
        assert "samesite=strict" in login.headers["set-cookie"].lower()

        values = {
            "attempt_id": attempt["id"],
            "artifact_id": artifact["id"],
            "kind": "prototype_label",
            "outcome": "accept",
            "feedback": "label",
        }
        assert client.post("/actions/label", data=values).status_code == 403
        csrf = hmac.new(secret.encode(), b"csrf:human:alex", hashlib.sha256).hexdigest()
        accepted = client.post("/actions/label", data={**values, "csrf": csrf})
        assert accepted.status_code == 200


def test_gallery_maps_not_found_conflict_and_invalid_actions_to_safe_http_statuses(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"pixels",
    )
    with TestClient(app) as client:
        assert client.get("/api/attempts/missing", headers=auth(secret)).status_code == 404
        wrong_hash = client.post(
            "/actions/approve-prototype",
            headers=auth(secret),
            data={
                "attempt_id": attempt["id"],
                "artifact_id": artifact["id"],
                "content_hash": "sha256:" + "0" * 64,
            },
        )
        assert wrong_hash.status_code == 409
        invalid = client.post(
            "/actions/retry",
            headers=auth(secret),
            data={"attempt_id": attempt["id"], "from_stage": "anything"},
        )
        assert invalid.status_code == 422


def test_gallery_recovered_operation_requires_an_existing_verified_cas_object(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.CONCEPT)
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="gallery-recovery",
        side_effect="generate",
        provider_role="smart_text",
        request_hash="a" * 64,
        cost_reserved_micros=100,
    )
    database.transition_operation(
        operation["id"],
        OperationStatus.INTENT,
        OperationStatus.RECONCILIATION_REQUIRED,
    )
    base = {
        "operation_id": operation["id"],
        "resolution": "executed_result_recovered",
        "evidence_ref": "provider:audit",
    }
    with TestClient(app) as client:
        missing = client.post(
            "/actions/resolve-operation",
            headers=auth(secret),
            data=base,
        )
        assert missing.status_code == 422
        absent = client.post(
            "/actions/resolve-operation",
            headers=auth(secret),
            data={**base, "result_hash": "sha256:" + "f" * 64},
        )
        assert absent.status_code == 422
        assert database.get_operation(operation["id"])["status"] == OperationStatus.RECONCILIATION_REQUIRED

        recovered = objects.put(b'{"recovered":true}')
        accepted = client.post(
            "/actions/resolve-operation",
            headers={**auth(secret), "accept": "application/json"},
            data={
                **base,
                "result_hash": recovered.uri,
                "resolved_model": "gemini-3.7-flash-20260801",
                "provider_request_id": "provider-result-123",
            },
        )
        assert accepted.status_code == 200
        resolved = database.get_operation(operation["id"])
        assert resolved["status"] == OperationStatus.SUCCEEDED
        assert resolved["result_hash"] == recovered.uri
        assert resolved["resolved_model"] == "gemini-3.7-flash-20260801"
