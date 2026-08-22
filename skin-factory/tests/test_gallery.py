from __future__ import annotations

import hashlib
import hmac
import json
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
    Purpose,
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

    async def cancel_publication_request_exact(self, **_: Any):
        raise AssertionError("bulk preflight must finish before any final-review cancellation")


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
        mode=factory_config.mode,
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


def prototype_review_attempt_with_evidence(
    database: Database,
    objects: ObjectStore,
    *,
    contract_guide_sha: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
    guide = b"\x89PNG\r\n\x1a\nexact behavior-pinned blank snake guide"
    guide_object = objects.put(guide)
    contract = {
        "schema_version": 1,
        "id": "prototype-geometry-gallery-test-v1",
        "guide": "fixtures/blank-snake.png",
        "guide_sha256": contract_guide_sha or guide_object.sha256,
        "renderer_source": {
            "body_cells": 16,
            "native_cell_px": 15,
            "head_direction": "right",
        },
        "presentation_transform": {"type": "nearest_neighbor_integer_upscale", "scale": 4},
    }
    contract_bytes = json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    contract_object = objects.put(contract_bytes)
    concept = database.create_concept(
        name="Pinned geometry gallery evidence",
        brief="Review the exact candidate against its retained blank snake geometry input.",
        seed="pinned-geometry-gallery",
        source="test",
        tags=["test", "geometry"],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.PROTOTYPE_REVIEW,
        idempotency_key="pinned-geometry-gallery",
        behavior={
            "snapshot_version": 6,
            "prototype_geometry_sha": contract_object.sha256,
            "prototype_geometry_ref": contract_object.uri,
            "prototype_guide_sha": guide_object.sha256,
            "prototype_guide_ref": guide_object.uri,
        },
        direction_sha="1" * 64,
        skill_sha="2" * 64,
        capability_sha="3" * 64,
        gate_sha="4" * 64,
        model_config_sha="5" * 64,
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="prototype",
    )
    candidate = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"exact candidate pixels",
        media_type="image/png",
        metadata={"prototype_index": 0},
    )
    return attempt, candidate, contract_bytes, guide


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


def test_prototype_review_shows_and_serves_exact_behavior_pinned_geometry_evidence(
    factory_config, database, objects, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt, candidate, contract_bytes, guide = prototype_review_attempt_with_evidence(database, objects)
    behavior = json.loads(attempt["behavior_json"])
    contract_hash = f"sha256:{behavior['prototype_geometry_sha']}"
    guide_hash = f"sha256:{behavior['prototype_guide_sha']}"
    guide_url = f"/attempts/{attempt['id']}/prototype-evidence/guide"
    contract_url = f"/attempts/{attempt['id']}/prototype-evidence/contract"

    with TestClient(app) as client:
        assert client.get(guide_url).status_code == 401
        assert client.get(contract_url).status_code == 401

        page = client.get(f"/attempts/{attempt['id']}", headers=auth(secret))
        assert page.status_code == 200
        assert "Prototype geometry evidence" in page.text
        assert "Exact retained blank snake geometry guide" in page.text
        assert "15px native real-render cell" in page.text
        assert "4\u00d7 review presentation" in page.text
        assert contract_hash in page.text
        assert guide_hash in page.text
        assert guide_url in page.text
        assert contract_url in page.text
        assert f"/artifacts/{candidate['id']}" in page.text

        detail = client.get(f"/api/attempts/{attempt['id']}", headers=auth(secret)).json()
        assert detail["prototype_review_evidence"] == {
            "contract_id": "prototype-geometry-gallery-test-v1",
            "contract_hash": contract_hash,
            "guide_hash": guide_hash,
            "contract_url": contract_url,
            "guide_url": guide_url,
            "body_cells": 16,
            "native_cell_px": 15,
            "presentation_scale": 4,
            "head_direction": "right",
        }

        guide_response = client.get(guide_url, headers=auth(secret))
        assert guide_response.content == guide
        assert guide_response.headers["content-type"] == "image/png"
        assert guide_response.headers["etag"] == f'"{behavior["prototype_guide_sha"]}"'
        assert guide_response.headers["cache-control"] == "private, max-age=31536000, immutable"

        contract_response = client.get(contract_url, headers=auth(secret))
        assert contract_response.content == contract_bytes
        assert contract_response.headers["content-type"] == "application/json"
        assert contract_response.headers["etag"] == f'"{behavior["prototype_geometry_sha"]}"'
        assert (
            client.get(
                f"/attempts/{attempt['id']}/prototype-evidence/not-a-part",
                headers=auth(secret),
            ).status_code
            == 404
        )


def test_prototype_review_refuses_a_contract_that_does_not_name_the_retained_guide(
    factory_config, database, objects, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt, _, _, _ = prototype_review_attempt_with_evidence(
        database,
        objects,
        contract_guide_sha="0" * 64,
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        page = client.get(f"/attempts/{attempt['id']}", headers=auth(secret))
        assert page.status_code == 422
        assert page.json()["detail"] == ("pinned prototype geometry contract does not name the retained guide bytes")


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
        before_html = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" in before_html
        assert "Approve exact bytes and start build" not in before_html

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
        after_html = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" not in after_html
        assert "Approve exact bytes and start build" in after_html


def test_shadow_gallery_reveals_publish_form_only_after_exact_build_label(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-shadow-gallery",
        production_revision="4",
        production_content_hash="sha256:" + "4" * 64,
    )
    contact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"exact gallery build pixels",
        media_type="image/png",
    )
    database.add_evaluation(
        artifact_id=contact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
        ),
        hidden_until_label=True,
    )

    with TestClient(app) as client:
        before = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" in before
        assert "Publish exact revision/hash" not in before
        labeled = client.post(
            "/actions/label",
            headers=auth(secret),
            data={
                "attempt_id": attempt["id"],
                "artifact_id": contact["id"],
                "kind": "build_quality_label",
                "outcome": "reject",
                "feedback": "blind build label remains separate from publication authority",
            },
        )
        assert labeled.status_code == 200
        after = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" not in after
        assert "Publish exact revision/hash" in after


def test_production_final_visible_judge_evidence_hides_blind_label_form_and_post_rejects(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    config = factory_config.model_copy(update={"mode": "production"})
    app, secret = review_app(config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-visible-production",
        production_revision="3",
        production_content_hash="sha256:" + "3" * 64,
    )
    contact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"visible production build pixels",
        media_type="image/png",
    )
    database.add_evaluation(
        artifact_id=contact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
        ),
        hidden_until_label=False,
    )

    with TestClient(app) as client:
        page = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" not in page
        assert "Publish exact revision/hash" in page
        assert "Human reject" in page
        rejected = client.post(
            "/actions/label",
            headers={**auth(secret), "accept": "application/json"},
            data={
                "attempt_id": attempt["id"],
                "artifact_id": contact["id"],
                "kind": "build_quality_label",
                "outcome": "accept",
                "feedback": "this visible review must not become blind authority",
            },
        )
        assert rejected.status_code == 409
        assert "unblinded visual_judge" in rejected.json()["detail"]


def test_production_calibration_fallback_uses_hidden_evidence_for_label_and_publish_ui(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    config = factory_config.model_copy(update={"mode": "production"})
    app, secret = review_app(config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        review_kind="final",
        production_skin_id="skin-fallback-production",
        production_revision="5",
        production_content_hash="sha256:" + "5" * 64,
    )
    contact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"hidden fallback build pixels",
        media_type="image/png",
    )
    database.add_evaluation(
        artifact_id=contact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
        ),
        hidden_until_label=True,
    )

    with TestClient(app) as client:
        before = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" in before
        assert "Publish exact revision/hash" not in before
        labeled = client.post(
            "/actions/label",
            headers=auth(secret),
            data={
                "attempt_id": attempt["id"],
                "artifact_id": contact["id"],
                "kind": "build_quality_label",
                "outcome": "accept",
                "feedback": "independent production fallback label",
            },
        )
        assert labeled.status_code == 200
        after = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" not in after
        assert "Publish exact revision/hash" in after


def test_production_sampled_reject_keeps_blind_label_form_but_hides_human_reject(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    config = factory_config.model_copy(update={"mode": "production"})
    app, secret = review_app(config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    attempt = database.update_attempt(attempt["id"], attempt["version"], review_kind="build_label")
    contact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"sampled rejected build pixels",
        media_type="image/png",
    )
    database.add_evaluation(
        artifact_id=contact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.MACHINE_REJECTED,
        ),
        hidden_until_label=True,
    )

    with TestClient(app) as client:
        page = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Record training label" in page
        assert "<h4>Human reject</h4>" not in page
        labeled = client.post(
            "/actions/label",
            headers=auth(secret),
            data={
                "attempt_id": attempt["id"],
                "artifact_id": contact["id"],
                "kind": "build_quality_label",
                "outcome": "reject",
                "feedback": "independent sampled-reject label",
            },
        )
        assert labeled.status_code == 200


def test_rejected_artifact_annotation_form_posts_authenticated_idempotent_feedback_only_decision(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"annotated rejected pixels",
        media_type="image/png",
    )
    values = {
        "attempt_id": attempt["id"],
        "artifact_id": artifact["id"],
        "content_hash": artifact["content_hash"],
        "feedback": "Keep the crisp head and remove the noisy body stripe.",
        "tags": "readability, stripe",
        "idempotency_key": "browser-annotation-1",
    }

    with TestClient(app) as client:
        page = client.get(f"/attempts/{attempt['id']}", headers=auth(secret)).text
        assert "Annotate exact rejected artifact" in page
        assert "<h4>Human reject</h4>" not in page
        assert client.post("/actions/annotate-reject", data=values).status_code == 401

        headers = {**auth(secret), "accept": "application/json"}
        first = client.post("/actions/annotate-reject", headers=headers, data=values)
        replay = client.post("/actions/annotate-reject", headers=headers, data=values)
        assert first.status_code == replay.status_code == 200
        assert replay.json()["decision"]["id"] == first.json()["decision"]["id"]
        current = database.get_attempt(attempt["id"])
        assert current["disposition"] == Disposition.MACHINE_REJECTED
        assert current["version"] == attempt["version"]
        assert len(database.decisions_for_attempt(attempt["id"])) == 1
        assert database.unlabeled_feedback_routes()[0]["action"] == "feedback_only"

        conflict = client.post(
            "/actions/annotate-reject",
            headers=headers,
            data={**values, "feedback": "changed replay"},
        )
        assert conflict.status_code == 409


def test_cookie_sessions_require_csrf_while_bearer_actions_do_not(
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
        value=b"pixels",
    )
    database.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
        ),
        hidden_until_label=True,
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


def test_bulk_retry_preflights_every_final_review_before_any_server_cancellation(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    exact = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    exact = database.update_attempt(
        exact["id"],
        exact["version"],
        review_kind="final",
        production_skin_id="skin-bulk-exact",
        production_revision="5",
        production_content_hash="sha256:" + "5" * 64,
    )
    incomplete = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    incomplete = database.update_attempt(incomplete["id"], incomplete["version"], review_kind="final")

    with TestClient(app) as client:
        response = client.post(
            "/actions/bulk-retry",
            headers={**auth(secret), "accept": "application/json"},
            data={
                "attempt_ids": [exact["id"], incomplete["id"]],
                "from_stage": "prototype",
                "feedback": "preflight the whole batch",
                "idempotency_key": "bulk-final-preflight",
            },
        )
    assert response.status_code == 409
    assert "registered revision authority" in response.json()["detail"]
    assert database.decisions_for_attempt(exact["id"]) == []
    with database.connect() as connection:
        assert (
            connection.execute("SELECT count(*) FROM operation WHERE attempt_id=?", (exact["id"],)).fetchone()[0] == 0
        )


def test_gallery_recovered_operation_requires_an_existing_verified_cas_object(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    app, secret = review_app(factory_config, database, objects, monkeypatch)
    attempt = make_attempt(stage=Stage.CONCEPT)
    request = {"probe": "gallery recovery"}
    retained_request = objects.put(OperationJournal.request_payload(request))
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="gallery-recovery",
        side_effect="generate",
        provider_role="smart_text",
        request_hash=OperationJournal.request_hash(request),
        cost_reserved_micros=100,
        metadata={
            "request_ref": retained_request.uri,
            "request_sha256": retained_request.sha256,
        },
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

        corrupt = objects.put(b"not-json")
        invalid_payload = client.post(
            "/actions/resolve-operation",
            headers=auth(secret),
            data={
                **base,
                "result_hash": corrupt.uri,
                "resolved_model": "gemini-3.7-flash",
            },
        )
        assert invalid_payload.status_code == 422
        assert database.get_operation(operation["id"])["status"] == OperationStatus.RECONCILIATION_REQUIRED

        recovered = objects.put(b'{"recovered":true}')
        wrong_model = client.post(
            "/actions/resolve-operation",
            headers=auth(secret),
            data={
                **base,
                "result_hash": recovered.uri,
                "resolved_model": "unapproved-fallback",
            },
        )
        assert wrong_model.status_code == 422
        assert database.get_operation(operation["id"])["status"] == OperationStatus.RECONCILIATION_REQUIRED

        accepted = client.post(
            "/actions/resolve-operation",
            headers={**auth(secret), "accept": "application/json"},
            data={
                **base,
                "result_hash": recovered.uri,
                "resolved_model": "gemini-3.7-flash",
                "provider_request_id": "provider-result-123",
            },
        )
        assert accepted.status_code == 200
        resolved = database.get_operation(operation["id"])
        assert resolved["status"] == OperationStatus.SUCCEEDED
        assert resolved["result_hash"] == recovered.uri
        assert resolved["resolved_model"] == "gemini-3.7-flash"
