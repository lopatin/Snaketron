"""Authenticated human review gallery and exact-artifact action endpoints."""

import base64
import hashlib
import hmac
import io
import json
import os
import secrets
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from .config import FactoryConfig
from .db import Database, RecordNotFound, VersionConflict, now
from .domain import Purpose, Stage
from .factory import Factory
from .objects import ObjectStore
from .recovery import validate_recovered_result
from .review import ReviewService

VIEW_MAP: dict[str, tuple[str, str]] = {
    "needs-review": ("Needs review", "needs_review"),
    "prototype": ("Prototype needs review", "prototype_review"),
    "final": ("Final build needs review", "final_review"),
    "blind-labels": ("Blind sampled labels", "blind_labels"),
    "machine-rejected": ("Machine rejected", "machine_rejected"),
    "human-rejected": ("Human rejected", "human_rejected"),
    "blocked": ("Blocked", "blocked"),
    "published": ("Published", "published"),
    "experiments": ("Experiments", "experiments"),
    "all": ("All", "all"),
}

SESSION_COOKIE = "snaketron_factory_review"
SESSION_SECONDS = 12 * 60 * 60


def build_review_service(factory: Factory) -> ReviewService:
    return ReviewService(
        factory.database,
        factory.journal,
        factory.api,
        factory.persistence,
        factory.behavior_snapshot,
        provider_retries=factory.config.budgets.provider_retries,
        mode=factory.config.mode,
    )


def create_app(
    config: FactoryConfig,
    *,
    factory: Factory | None = None,
    database: Database | None = None,
    objects: ObjectStore | None = None,
    review_service: ReviewService | None = None,
    template_dir: Path | None = None,
) -> FastAPI:
    """Create the local review application.

    The factory service token is insufficient here.  A distinct review secret
    authenticates the browser/API. Final soft-triage overrides, cancellation,
    and publication additionally require the Snaketron operator token inside
    this separately launched process.
    """

    owned_factory = factory is None
    runtime = factory or Factory(config, database=database, objects=objects)
    db = runtime.database
    store = runtime.objects
    review = review_service or build_review_service(runtime)
    db.migrate()
    db.assert_file_permissions()
    store.assert_permissions()

    review_secret = os.environ.get(config.review.operator_secret_env, "")
    if len(review_secret) < 16:
        raise RuntimeError(f"{config.review.operator_secret_env} must contain at least 16 characters")

    root = config.source_path.parent.parent if config.source_path is not None else Path(__file__).resolve().parents[2]
    templates = Jinja2Templates(directory=str(template_dir or root / "templates"))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        if owned_factory:
            await runtime.close()

    app = FastAPI(
        title="Snaketron Skin Factory Review",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.config = config
    app.state.factory = runtime
    app.state.database = db
    app.state.objects = store
    app.state.review = review

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["x-content-type-options"] = "nosniff"
        response.headers["x-frame-options"] = "DENY"
        response.headers["referrer-policy"] = "no-referrer"
        response.headers["cache-control"] = response.headers.get("cache-control", "no-store")
        response.headers["content-security-policy"] = (
            "default-src 'self'; img-src 'self' data:; media-src 'self'; "
            "style-src 'self' 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'"
        )
        return response

    def issue_session(actor: str) -> str:
        expires = int(time.time()) + SESSION_SECONDS
        body = json.dumps({"actor": actor, "expires": expires}, sort_keys=True, separators=(",", ":")).encode("utf-8")
        encoded = base64.urlsafe_b64encode(body).rstrip(b"=").decode("ascii")
        signature = hmac.new(review_secret.encode(), encoded.encode(), hashlib.sha256).hexdigest()
        return f"{encoded}.{signature}"

    def read_session(value: str) -> str | None:
        try:
            encoded, supplied = value.split(".", 1)
            expected = hmac.new(review_secret.encode(), encoded.encode(), hashlib.sha256).hexdigest()
            if not secrets.compare_digest(supplied, expected):
                return None
            padded = encoded + "=" * (-len(encoded) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded))
            actor = str(payload["actor"])
            if int(payload["expires"]) < int(time.time()):
                return None
            _validate_human(actor)
            return actor
        except (KeyError, TypeError, ValueError, UnicodeDecodeError):
            return None

    def csrf_for(actor: str) -> str:
        return hmac.new(review_secret.encode(), f"csrf:{actor}".encode(), hashlib.sha256).hexdigest()

    async def authenticated_actor(
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
        x_review_actor: Annotated[str | None, Header()] = None,
    ) -> str:
        if authorization:
            scheme, _, supplied = authorization.partition(" ")
            if scheme.lower() == "bearer" and secrets.compare_digest(supplied, review_secret):
                actor = x_review_actor or os.environ.get("SKIN_FACTORY_REVIEW_ACTOR", "human:reviewer")
                _validate_human(actor)
                request.state.auth_kind = "bearer"
                request.state.actor = actor
                return actor
        actor = read_session(request.cookies.get(SESSION_COOKIE, ""))
        if actor:
            request.state.auth_kind = "session"
            request.state.actor = actor
            return actor
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="authenticated human review token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    Actor = Annotated[str, Depends(authenticated_actor)]

    def require_csrf(request: Request, actor: str, supplied: str | None) -> None:
        if getattr(request.state, "auth_kind", "") == "bearer":
            return
        if not supplied or not secrets.compare_digest(supplied, csrf_for(actor)):
            raise HTTPException(status_code=403, detail="invalid CSRF token")

    def context(request: Request, actor: str, **values: Any) -> dict[str, Any]:
        return {
            "request": request,
            "actor": actor,
            "csrf": csrf_for(actor),
            "views": VIEW_MAP,
            "mode": config.mode,
            # One browser-rendered form token survives double-clicks and a
            # lost POST response, while a deliberate page reload creates a
            # fresh human request.
            "action_idempotency_key": secrets.token_urlsafe(24),
            **values,
        }

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/login", response_class=HTMLResponse)
    async def login_form(request: Request):
        return templates.TemplateResponse(request, "login.html", {"error": None})

    @app.post("/login", response_class=HTMLResponse)
    async def login(
        request: Request,
        token: Annotated[str, Form()],
        actor: Annotated[str, Form()] = "human:reviewer",
    ):
        try:
            _validate_human(actor)
        except PermissionError as error:
            return templates.TemplateResponse(request, "login.html", {"error": str(error)}, status_code=403)
        if not secrets.compare_digest(token, review_secret):
            return templates.TemplateResponse(request, "login.html", {"error": "Invalid review token"}, status_code=401)
        response = RedirectResponse(url="/?view=needs-review", status_code=303)
        response.set_cookie(
            SESSION_COOKIE,
            issue_session(actor),
            max_age=SESSION_SECONDS,
            httponly=True,
            secure=request.url.scheme == "https",
            samesite="strict",
        )
        return response

    @app.post("/logout")
    async def logout(
        request: Request,
        actor: Actor,
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        response = RedirectResponse(url="/login", status_code=303)
        response.delete_cookie(SESSION_COOKIE)
        return response

    @app.get("/", response_class=HTMLResponse)
    async def gallery(
        request: Request,
        actor: Actor,
        view: Annotated[str, Query()] = "needs-review",
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ):
        selected = _view(view)
        rows = [_with_preview(db, _decode_attempt(row)) for row in _gallery_rows(db, selected[1], limit)]
        return templates.TemplateResponse(
            request,
            "gallery.html",
            context(
                request,
                actor,
                current_view=view,
                title=selected[0],
                attempts=rows,
            ),
        )

    @app.get("/api/gallery/{view}")
    async def gallery_json(view: str, actor: Actor, limit: int = Query(100, ge=1, le=500)):
        del actor
        selected = _view(view)
        return {
            "view": view,
            "items": [_with_preview(db, _decode_attempt(row)) for row in _gallery_rows(db, selected[1], limit)],
        }

    @app.get("/attempts/{attempt_id}", response_class=HTMLResponse)
    async def attempt_detail(request: Request, attempt_id: str, actor: Actor):
        attempt = _decode_attempt(db.get_attempt(attempt_id))
        concept = _decode_concept(db.get_concept(attempt["concept_id"]))
        artifacts = [_decode_artifact(row) for row in db.artifacts_for_attempt(attempt_id)]
        prototype_artifacts = [artifact for artifact in artifacts if artifact["kind"] == "prototype"]
        prototype_review_evidence = _load_prototype_review_evidence(store, attempt)
        decisions = [_decode_decision(row) for row in db.decisions_for_attempt(attempt_id)]
        authoritative_label_artifact_ids = _authoritative_label_artifact_ids(db, attempt_id)
        eligible_blind_label_artifact_ids = _eligible_blind_label_artifact_ids(db, attempt_id)
        blind_label_required_artifact_ids = _blind_label_required_artifact_ids(db, attempt_id)
        contact_sheet_ids = [artifact["id"] for artifact in artifacts if artifact["kind"] == "contact_sheet"]
        publication_label_required = bool(
            contact_sheet_ids and contact_sheet_ids[-1] in blind_label_required_artifact_ids
        )
        shadow_publication_label_ready = bool(
            contact_sheet_ids and contact_sheet_ids[-1] in authoritative_label_artifact_ids
        )
        blind = _has_hidden_unlabeled_evaluation(db, attempt_id)
        evaluations = [_decode_evaluation(row) for row in db.evaluations_for_attempt(attempt_id, reveal=False)]
        lineage = [_decode_attempt(row) for row in _lineage(db, attempt)]
        with db.connect() as connection:
            children = [
                _decode_attempt(dict(row))
                for row in connection.execute(
                    "SELECT * FROM attempt WHERE parent_attempt_id=? ORDER BY created_at",
                    (attempt_id,),
                ).fetchall()
            ]
        return templates.TemplateResponse(
            request,
            "attempt.html",
            context(
                request,
                actor,
                title=concept["name"],
                attempt=attempt,
                concept=concept,
                artifacts=artifacts,
                prototype_artifacts=prototype_artifacts,
                prototype_review_evidence=prototype_review_evidence,
                evaluations=evaluations,
                evaluations_blind=blind,
                decisions=decisions,
                shadow_mode=config.mode == "shadow",
                authoritative_label_artifact_ids=authoritative_label_artifact_ids,
                eligible_blind_label_artifact_ids=eligible_blind_label_artifact_ids,
                blind_label_required_artifact_ids=blind_label_required_artifact_ids,
                publication_label_required=publication_label_required,
                shadow_publication_label_ready=shadow_publication_label_ready,
                lineage=lineage,
                children=children,
            ),
        )

    @app.get("/api/attempts/{attempt_id}")
    async def attempt_json(attempt_id: str, actor: Actor):
        del actor
        attempt = _decode_attempt(db.get_attempt(attempt_id))
        decisions = [_decode_decision(row) for row in db.decisions_for_attempt(attempt_id)]
        authoritative_label_artifact_ids = _authoritative_label_artifact_ids(db, attempt_id)
        eligible_blind_label_artifact_ids = _eligible_blind_label_artifact_ids(db, attempt_id)
        blind_label_required_artifact_ids = _blind_label_required_artifact_ids(db, attempt_id)
        artifacts = [_decode_artifact(row) for row in db.artifacts_for_attempt(attempt_id)]
        contact_sheet_ids = [artifact["id"] for artifact in artifacts if artifact["kind"] == "contact_sheet"]
        blind = _has_hidden_unlabeled_evaluation(db, attempt_id)
        evaluations = [_decode_evaluation(row) for row in db.evaluations_for_attempt(attempt_id, reveal=False)]
        return {
            "attempt": attempt,
            "concept": _decode_concept(db.get_concept(attempt["concept_id"])),
            "artifacts": artifacts,
            "prototype_review_evidence": _load_prototype_review_evidence(store, attempt),
            "evaluations": evaluations,
            "evaluations_blind": blind,
            "decisions": decisions,
            "shadow_mode": config.mode == "shadow",
            "authoritative_label_artifact_ids": sorted(authoritative_label_artifact_ids),
            "eligible_blind_label_artifact_ids": sorted(eligible_blind_label_artifact_ids),
            "blind_label_required_artifact_ids": sorted(blind_label_required_artifact_ids),
            "publication_label_required": bool(
                contact_sheet_ids and contact_sheet_ids[-1] in blind_label_required_artifact_ids
            ),
            "shadow_publication_label_ready": bool(
                contact_sheet_ids and contact_sheet_ids[-1] in authoritative_label_artifact_ids
            ),
        }

    @app.get("/attempts/{attempt_id}/prototype-evidence/{part}")
    async def prototype_evidence_bytes(attempt_id: str, part: str, actor: Actor):
        """Serve only the exact behavior-pinned geometry evidence for this Attempt."""

        del actor
        attempt = _decode_attempt(db.get_attempt(attempt_id))
        evidence = _load_prototype_review_evidence(store, attempt, include_bytes=True)
        if evidence is None:
            raise HTTPException(status_code=404, detail="Attempt has no pinned prototype geometry evidence")
        payloads = evidence.pop("_payloads")
        if part == "guide":
            value = payloads["guide"]
            media_type = "image/png"
            digest = evidence["guide_hash"].removeprefix("sha256:")
            filename = "prototype-geometry-guide.png"
        elif part == "contract":
            value = payloads["contract"]
            media_type = "application/json"
            digest = evidence["contract_hash"].removeprefix("sha256:")
            filename = "prototype-geometry-contract.json"
        else:
            raise HTTPException(status_code=404, detail=f"unknown prototype evidence part {part}")
        return StreamingResponse(
            io.BytesIO(value),
            media_type=media_type,
            headers={
                "etag": f'"{digest}"',
                "cache-control": "private, max-age=31536000, immutable",
                "content-length": str(len(value)),
                "content-disposition": f'inline; filename="{filename}"',
            },
        )

    @app.get("/artifacts/{artifact_id}")
    async def artifact_bytes(
        artifact_id: str,
        actor: Actor,
        download: bool = Query(False),
    ):
        del actor
        artifact = db.get_artifact(artifact_id)
        value = store.get(artifact["object_ref"])
        digest = artifact["content_hash"].removeprefix("sha256:")
        safe_inline = artifact["media_type"] in {
            "image/png",
            "image/jpeg",
            "image/webp",
            "image/gif",
            "video/webm",
            "video/mp4",
            "application/json",
            "text/plain",
            "text/markdown",
        }
        headers = {
            "etag": f'"{digest}"',
            "cache-control": "private, max-age=31536000, immutable",
            "content-length": str(len(value)),
            "content-disposition": ("attachment" if download or not safe_inline else "inline")
            + f'; filename="{artifact_id}"',
        }
        return StreamingResponse(io.BytesIO(value), media_type=artifact["media_type"], headers=headers)

    @app.get("/operations", response_class=HTMLResponse)
    async def operations(request: Request, actor: Actor):
        unresolved = [_decode_operation(row) for row in db.unresolved_operations()]
        return templates.TemplateResponse(
            request,
            "operations.html",
            context(request, actor, title="Operations requiring reconciliation", operations=unresolved),
        )

    @app.post("/actions/label")
    async def action_label(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        artifact_id: Annotated[str, Form()],
        kind: Annotated[str, Form()],
        outcome: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        tags: Annotated[str, Form()] = "",
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = review.label(
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            kind=kind,
            outcome=outcome,
            feedback=feedback,
            tags=_tags(tags),
            actor=actor,
        )
        return _action_response(request, result, attempt_id)

    @app.post("/actions/approve-prototype")
    async def action_approve_prototype(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        artifact_id: Annotated[str, Form()],
        content_hash: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = review.approve_prototype(
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            content_hash=content_hash,
            feedback=feedback,
            actor=actor,
        )
        return _action_response(request, result, attempt_id)

    @app.post("/actions/reject")
    async def action_reject(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        artifact_id: Annotated[str | None, Form()] = None,
        feedback: Annotated[str, Form()] = "",
        tags: Annotated[str, Form()] = "",
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = await review.reject(
            attempt_id=attempt_id,
            artifact_id=artifact_id or None,
            feedback=feedback,
            tags=_tags(tags),
            actor=actor,
        )
        return _action_response(request, result, attempt_id)

    @app.post("/actions/annotate-reject")
    async def action_annotate_reject(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        artifact_id: Annotated[str, Form()],
        content_hash: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        tags: Annotated[str, Form()] = "",
        idempotency_key: Annotated[str | None, Form()] = None,
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = review.annotate_reject(
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            content_hash=content_hash,
            feedback=feedback,
            tags=_tags(tags),
            actor=actor,
            idempotency_key=idempotency_key,
        )
        return _action_response(request, result, attempt_id)

    @app.post("/actions/override-triage")
    async def action_override_triage(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        artifact_id: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = await review.override_triage(
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            feedback=feedback,
            actor=actor,
        )
        return _action_response(request, result, attempt_id)

    @app.post("/actions/re-evaluate")
    async def action_re_evaluate(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        artifact_id: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        idempotency_key: Annotated[str | None, Form()] = None,
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = await review.re_evaluate(
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            feedback=feedback,
            actor=actor,
            idempotency_key=idempotency_key,
        )
        return _action_response(request, result, result["attempt"]["id"])

    @app.post("/actions/retry")
    async def action_retry(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        from_stage: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        idempotency_key: Annotated[str | None, Form()] = None,
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = await review.retry(
            attempt_id=attempt_id,
            from_stage=from_stage,
            feedback=feedback,
            actor=actor,
            idempotency_key=idempotency_key,
        )
        return _action_response(request, result, result["attempt"]["id"])

    @app.post("/actions/bulk-retry")
    async def action_bulk_retry(
        request: Request,
        actor: Actor,
        attempt_ids: Annotated[list[str], Form()],
        from_stage: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        idempotency_key: Annotated[str | None, Form()] = None,
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        _prevalidate_bulk_retry(db, attempt_ids, from_stage)
        results = [
            await review.retry(
                attempt_id=attempt_id,
                from_stage=from_stage,
                feedback=feedback,
                actor=actor,
                idempotency_key=idempotency_key,
            )
            for attempt_id in attempt_ids
        ]
        return _action_response(request, {"results": results}, results[0]["attempt"]["id"])

    @app.post("/actions/publish")
    async def action_publish(
        request: Request,
        actor: Actor,
        attempt_id: Annotated[str, Form()],
        revision: Annotated[str, Form()],
        content_hash: Annotated[str, Form()],
        feedback: Annotated[str, Form()] = "",
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        result = await review.publish(
            attempt_id=attempt_id,
            revision=revision,
            content_hash=content_hash,
            feedback=feedback,
            actor=actor,
        )
        return _action_response(request, result, attempt_id)

    @app.post("/actions/resolve-operation")
    async def action_resolve_operation(
        request: Request,
        actor: Actor,
        operation_id: Annotated[str, Form()],
        resolution: Annotated[str, Form()],
        evidence_ref: Annotated[str, Form()],
        result_hash: Annotated[str | None, Form()] = None,
        resolved_model: Annotated[str | None, Form()] = None,
        provider_request_id: Annotated[str | None, Form()] = None,
        media_type: Annotated[str | None, Form()] = None,
        csrf: Annotated[str | None, Form()] = None,
    ):
        require_csrf(request, actor, csrf)
        recovered = result_hash or None
        if resolution == "executed_result_recovered":
            if recovered is None:
                raise ValueError("executed_result_recovered requires a result hash")
            if not resolved_model:
                raise ValueError("executed_result_recovered requires the exact resolved model")
            validate_recovered_result(
                config=config,
                operation=db.get_operation(operation_id),
                database=db,
                objects=store,
                result_hash=recovered,
                resolved_model=resolved_model,
                media_type=media_type or None,
            )
        result = db.resolve_operation(
            operation_id=operation_id,
            resolution=resolution,
            evidence_ref=evidence_ref,
            result_hash=recovered,
            resolved_model=resolved_model or None,
            provider_request_id=provider_request_id or None,
            media_type=media_type or None,
            actor=actor,
        )
        return _action_response(request, result, None, fallback="/operations")

    @app.exception_handler(RecordNotFound)
    async def not_found(_: Request, error: RecordNotFound):
        return JSONResponse({"detail": f"record not found: {error}"}, status_code=404)

    @app.exception_handler(VersionConflict)
    async def conflict(_: Request, error: VersionConflict):
        return JSONResponse({"detail": str(error)}, status_code=409)

    @app.exception_handler(PermissionError)
    async def forbidden(_: Request, error: PermissionError):
        return JSONResponse({"detail": str(error)}, status_code=403)

    @app.exception_handler(ValueError)
    async def invalid(_: Request, error: ValueError):
        return JSONResponse({"detail": str(error)}, status_code=422)

    return app


def _validate_human(actor: str) -> None:
    if not actor.startswith("human:") or not actor.removeprefix("human:").strip():
        raise PermissionError("a nonempty human: actor is required")


def _view(name: str) -> tuple[str, str]:
    if name not in VIEW_MAP:
        raise HTTPException(status_code=404, detail=f"unknown gallery view {name}")
    return VIEW_MAP[name]


def _gallery_rows(database: Database, view: str, limit: int) -> list[dict[str, Any]]:
    if view != "blind_labels":
        return database.list_gallery(view, limit=limit)
    with database.connect() as connection:
        rows = connection.execute(
            """SELECT a.*,c.name AS concept_name,c.brief AS concept_brief,c.tags_json,
                 (SELECT ar.id FROM artifact ar WHERE ar.attempt_id=a.id
                  ORDER BY ar.created_at DESC LIMIT 1) AS latest_artifact_id
               FROM attempt a JOIN concept c ON c.id=a.concept_id
               WHERE a.review_kind IN ('prototype_label','build_label')
                 AND EXISTS (
                   SELECT 1 FROM evaluation e
                   WHERE e.attempt_id=a.id AND e.hidden_until_label=1
                     AND NOT EXISTS (
                       SELECT 1 FROM human_decision d
                       WHERE d.attempt_id=a.id AND d.artifact_id=e.artifact_id
                         AND d.action IN ('prototype_label','build_quality_label')
                         AND d.authority_evaluation_id IS NOT NULL
                     )
                 )
               ORDER BY a.updated_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _has_hidden_unlabeled_evaluation(database: Database, attempt_id: str) -> bool:
    helper = getattr(database, "has_hidden_unlabeled_evaluations", None)
    if helper is not None:
        return bool(helper(attempt_id))
    with database.connect() as connection:
        row = connection.execute(
            """SELECT 1 FROM evaluation e
               WHERE e.attempt_id=? AND e.hidden_until_label=1
                 AND NOT EXISTS (
                   SELECT 1 FROM human_decision d
                   WHERE d.attempt_id=e.attempt_id AND d.artifact_id=e.artifact_id
                     AND d.action IN ('prototype_label','build_quality_label')
                     AND d.authority_evaluation_id IS NOT NULL
                 ) LIMIT 1""",
            (attempt_id,),
        ).fetchone()
    return row is not None


def _authoritative_label_artifact_ids(database: Database, attempt_id: str) -> set[str]:
    with database.connect() as connection:
        rows = connection.execute(
            """SELECT DISTINCT d.artifact_id
               FROM human_decision d
               JOIN evaluation e ON e.id=d.authority_evaluation_id
               WHERE d.attempt_id=? AND d.actor LIKE 'human:%'
                 AND d.action IN ('prototype_label','build_quality_label')
                 AND e.artifact_id=d.artifact_id AND e.attempt_id=d.attempt_id
                 AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                 AND e.hidden_until_label=1 AND e.created_at<=d.created_at""",
            (attempt_id,),
        ).fetchall()
    return {str(row[0]) for row in rows}


def _eligible_blind_label_artifact_ids(database: Database, attempt_id: str) -> set[str]:
    """Return artifacts whose current state can create exact blind-label authority."""

    with database.connect() as connection:
        rows = connection.execute(
            """SELECT DISTINCT ar.id
               FROM artifact ar JOIN attempt a ON a.id=ar.attempt_id
               WHERE a.id=?
                 AND (
                   (ar.kind='prototype' AND a.stage='prototype_review' AND (
                     (a.disposition='needs_human' AND a.review_kind='prototype')
                     OR (a.disposition='machine_rejected' AND a.review_kind='prototype_label')
                   ))
                   OR
                   (ar.kind='contact_sheet' AND a.stage='final_review' AND (
                     (a.disposition='needs_human' AND a.review_kind='final')
                     OR (a.disposition='machine_rejected' AND a.review_kind='build_label')
                   ))
                 )
                 AND EXISTS (
                   SELECT 1 FROM evaluation e
                   WHERE e.artifact_id=ar.id AND e.attempt_id=a.id
                     AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                     AND e.hidden_until_label=1 AND e.created_at<=?
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM evaluation e
                   WHERE e.artifact_id=ar.id AND e.attempt_id=a.id
                     AND e.evaluator='visual_judge' AND e.gate_name='visual_fidelity'
                     AND e.hidden_until_label=0
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM human_decision d
                   WHERE d.artifact_id=ar.id
                     AND d.action IN ('prototype_label','build_quality_label')
                 )""",
            (attempt_id, now()),
        ).fetchall()
    return {str(row[0]) for row in rows}


def _blind_label_required_artifact_ids(database: Database, attempt_id: str) -> set[str]:
    with database.connect() as connection:
        rows = connection.execute(
            """SELECT DISTINCT artifact_id FROM evaluation
               WHERE attempt_id=? AND evaluator='visual_judge'
                 AND gate_name='visual_fidelity' AND hidden_until_label=1""",
            (attempt_id,),
        ).fetchall()
    return {str(row[0]) for row in rows}


def _tags(value: str) -> list[str]:
    return [tag.strip() for tag in value.split(",") if tag.strip()]


def _decode_columns(row: Mapping[str, Any], columns: set[str]) -> dict[str, Any]:
    result = dict(row)
    for name in columns:
        value = result.get(name)
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
                result[name] = decoded
                result[name.removesuffix("_json")] = decoded
            except json.JSONDecodeError:
                result[name.removesuffix("_json")] = value
    return result


def _decode_attempt(row: Mapping[str, Any]) -> dict[str, Any]:
    return _decode_columns(row, {"behavior_json", "failure_json", "tags_json"})


def _load_prototype_review_evidence(
    store: ObjectStore,
    attempt: Mapping[str, Any],
    *,
    include_bytes: bool = False,
) -> dict[str, Any] | None:
    """Verify and describe the exact geometry inputs retained by an Attempt.

    Legacy Attempts have no geometry evidence and continue to render without
    this panel. Once any geometry authority is present, every field and both
    CAS objects must verify; review must never substitute current checkout
    files for missing or corrupted behavior-pinned bytes.
    """

    if attempt.get("stage") != Stage.PROTOTYPE_REVIEW:
        return None
    behavior = attempt.get("behavior")
    if not isinstance(behavior, Mapping):
        return None
    fields = {
        "contract_hash": behavior.get("prototype_geometry_sha"),
        "contract_ref": behavior.get("prototype_geometry_ref"),
        "guide_hash": behavior.get("prototype_guide_sha"),
        "guide_ref": behavior.get("prototype_guide_ref"),
    }
    if not any(fields.values()):
        return None
    if not all(isinstance(value, str) and value for value in fields.values()):
        raise ValueError("pinned prototype geometry evidence is incomplete")

    contract_digest = _sha256_digest(fields["contract_hash"], "prototype geometry contract")
    guide_digest = _sha256_digest(fields["guide_hash"], "prototype geometry guide")
    contract_bytes = _verified_behavior_object(
        store,
        fields["contract_ref"],
        contract_digest,
        "prototype geometry contract",
    )
    guide_bytes = _verified_behavior_object(
        store,
        fields["guide_ref"],
        guide_digest,
        "prototype geometry guide",
    )
    try:
        contract = json.loads(contract_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("pinned prototype geometry contract is not exact UTF-8 JSON") from error
    if not isinstance(contract, dict):
        raise ValueError("pinned prototype geometry contract must be a JSON object")
    if contract.get("guide_sha256") != guide_digest:
        raise ValueError("pinned prototype geometry contract does not name the retained guide bytes")

    contract_id = contract.get("id")
    if not isinstance(contract_id, str) or not contract_id:
        contract_id = "pinned prototype geometry"
    renderer_source = contract.get("renderer_source")
    if not isinstance(renderer_source, dict):
        renderer_source = {}
    presentation_transform = contract.get("presentation_transform")
    if not isinstance(presentation_transform, dict):
        presentation_transform = {}
    result: dict[str, Any] = {
        "contract_id": contract_id,
        "contract_hash": f"sha256:{contract_digest}",
        "guide_hash": f"sha256:{guide_digest}",
        "contract_url": f"/attempts/{attempt['id']}/prototype-evidence/contract",
        "guide_url": f"/attempts/{attempt['id']}/prototype-evidence/guide",
        "body_cells": renderer_source.get("body_cells"),
        # `runtime_cell_px` preserves compatibility with the brief-lived draft
        # contract while `native_cell_px` is the stable real-render scale.
        "native_cell_px": renderer_source.get("native_cell_px", renderer_source.get("runtime_cell_px")),
        "presentation_scale": presentation_transform.get("scale"),
        "head_direction": renderer_source.get("head_direction"),
    }
    if include_bytes:
        result["_payloads"] = {"contract": contract_bytes, "guide": guide_bytes}
    return result


def _sha256_digest(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"pinned {name} hash is absent")
    digest = value.removeprefix("sha256:")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"pinned {name} hash is invalid")
    return digest


def _verified_behavior_object(
    store: ObjectStore,
    reference: object,
    expected_digest: str,
    name: str,
) -> bytes:
    if not isinstance(reference, str):
        raise ValueError(f"pinned {name} CAS reference is absent")
    try:
        value = store.get(reference)
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        raise ValueError(f"pinned {name} bytes are unavailable") from error
    actual = hashlib.sha256(value).hexdigest()
    if not hmac.compare_digest(actual, expected_digest):
        raise ValueError(f"pinned {name} hash differs from retained bytes")
    return value


def _with_preview(database: Database, attempt: dict[str, Any]) -> dict[str, Any]:
    with database.connect() as connection:
        row = connection.execute(
            """SELECT id,media_type FROM artifact
               WHERE attempt_id=? AND (media_type LIKE 'image/%' OR media_type LIKE 'video/%')
               ORDER BY created_at DESC LIMIT 1""",
            (attempt["id"],),
        ).fetchone()
    if row is not None:
        attempt["preview_artifact_id"] = row["id"]
        attempt["preview_media_type"] = row["media_type"]
    return attempt


def _decode_concept(row: Mapping[str, Any]) -> dict[str, Any]:
    return _decode_columns(row, {"tags_json"})


def _decode_artifact(row: Mapping[str, Any]) -> dict[str, Any]:
    return _decode_columns(row, {"metadata_json", "provenance_json"})


def _decode_evaluation(row: Mapping[str, Any]) -> dict[str, Any]:
    return _decode_columns(row, {"reasons_json", "measurements_json"})


def _decode_decision(row: Mapping[str, Any]) -> dict[str, Any]:
    return _decode_columns(row, {"tags_json"})


def _decode_operation(row: Mapping[str, Any]) -> dict[str, Any]:
    return _decode_columns(row, {"metadata_json", "failure_json"})


def _lineage(database: Database, attempt: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = [dict(attempt)]
    parent = attempt.get("parent_attempt_id")
    seen = {str(attempt["id"])}
    while parent and parent not in seen:
        row = database.get_attempt(str(parent))
        rows.append(row)
        seen.add(str(parent))
        parent = row.get("parent_attempt_id")
    return list(reversed(rows))


def _prevalidate_bulk_retry(database: Database, attempt_ids: list[str], from_stage: str) -> None:
    if not attempt_ids:
        raise ValueError("select at least one attempt")
    if len(set(attempt_ids)) != len(attempt_ids):
        raise ValueError("duplicate attempt in bulk retry")
    if from_stage not in {"prototype", "assets", "build"}:
        raise ValueError("retry stage must be prototype, assets, or build")
    for attempt_id in attempt_ids:
        attempt = database.get_attempt(attempt_id)
        if from_stage != "prototype" and (
            not attempt["approved_prototype_hash"] or not attempt["prototype_decision_id"]
        ):
            raise VersionConflict(f"attempt {attempt_id} has no exact prototype approval for {from_stage} retry")
        if (
            attempt["stage"] == Stage.FINAL_REVIEW
            and attempt["purpose"] == Purpose.PRODUCTION
            and (
                not attempt["production_skin_id"]
                or not attempt["production_revision"]
                or not attempt["production_content_hash"]
            )
        ):
            raise VersionConflict(
                f"attempt {attempt_id} has no exact registered revision authority for final-review retry"
            )


def _action_response(
    request: Request,
    result: Any,
    attempt_id: str | None,
    *,
    fallback: str = "/",
):
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse(json.loads(json.dumps(result, default=str)))
    target = f"/attempts/{attempt_id}" if attempt_id else fallback
    return RedirectResponse(url=target, status_code=303)
