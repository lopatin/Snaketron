"""Install and runtime preflight checks for the Skin Factory.

The doctor deliberately reports only whether a credential exists.  It never
copies a secret into a diagnostic result, exception, log, or provider request.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import os
import secrets
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Literal

from PIL import Image, ImageDraw, ImageFont

from .config import FactoryConfig
from .db import Database
from .domain import DoctorCheck, DoctorReport, InlineArtifact, Purpose, WorkerRequest, WorkerResult
from .factory import Factory
from .lama import (
    LamaRuntimeError,
    lama_bundle_manifest,
    lama_bundle_sha,
    lama_python,
    lama_subprocess_environment,
)
from .objects import ObjectStore
from .renderer import renderer_bundle_manifest, renderer_bundle_manifest_sha
from .snaketron_api import validate_service_capabilities
from .worker import SkillBundle
from .worker_validation import validate_worker_handoff

DoctorIdentity = Literal["service", "operator", "all"]


class FactoryDoctor:
    """Run deterministic local checks and optional network conformance probes."""

    def __init__(
        self,
        config: FactoryConfig,
        *,
        factory: Factory | None = None,
        database: Database | None = None,
        objects: ObjectStore | None = None,
    ) -> None:
        self.config = config
        self.factory = factory or Factory(config, database=database, objects=objects)
        self.database = self.factory.database
        self.objects = self.factory.objects
        self._owns_factory = factory is None

    async def close(self) -> None:
        if self._owns_factory:
            await self.factory.close()

    async def run(
        self,
        *,
        identity: DoctorIdentity = "service",
        offline: bool = False,
    ) -> DoctorReport:
        checks: list[DoctorCheck] = []

        checks.append(
            DoctorCheck(
                name="config",
                ok=bool(self.config.version_sha256 and self.config.source_path),
                detail=(
                    f"version sha256:{self.config.version_sha256}"
                    if self.config.version_sha256
                    else "configuration was not loaded from a versioned file"
                ),
            )
        )
        checks.extend(self._canonical_file_checks())
        checks.append(self._prototype_geometry_check())
        checks.extend(self._credential_checks(identity))
        checks.append(self._database_check())
        checks.append(self._object_store_check())
        checks.append(self._capture_command_check())
        checks.append(self._renderer_bundle_check())
        checks.append(self._lama_check())
        checks.append(self._git_signing_check())
        checks.append(await self._git_remote_check(offline=offline))
        checks.append(await self._browser_check(offline=offline))

        if offline:
            checks.extend(
                [
                    DoctorCheck(
                        name="task_worker_model",
                        ok=True,
                        required=False,
                        detail="network probe skipped by --offline",
                    ),
                    DoctorCheck(
                        name="task_worker_conformance",
                        ok=True,
                        required=False,
                        detail="side-effect-free worker request skipped by --offline",
                    ),
                    DoctorCheck(
                        name="content_models",
                        ok=True,
                        required=False,
                        detail="network probe skipped by --offline",
                    ),
                    DoctorCheck(
                        name="snaketron_api",
                        ok=True,
                        required=False,
                        detail="network probe skipped by --offline",
                    ),
                    DoctorCheck(
                        name="snaketron_service_capabilities",
                        ok=True,
                        required=False,
                        detail="authenticated least-privilege probe skipped by --offline",
                    ),
                ]
            )
        else:
            worker, worker_conformance, content_models, api, api_capabilities = await asyncio.gather(
                self._worker_check(),
                self._worker_conformance_check(),
                self._content_models_check(),
                self._api_check(),
                self._api_capability_check(),
            )
            checks.extend([worker, worker_conformance, content_models, api, api_capabilities])

        ok = all(check.ok or not check.required for check in checks)
        return DoctorReport(
            ok=ok,
            config_path=str(self.config.source_path or ""),
            checks=checks,
        )

    def _canonical_file_checks(self) -> list[DoctorCheck]:
        paths = {
            "authoring_skill": self.config.paths.skill_dir / "SKILL.md",
            "capability_manifest": self.config.paths.capability_manifest,
            "design_direction": self.config.paths.direction,
            "gate_manifest": self.config.paths.gate_manifest,
            "prototype_geometry_contract": self.config.paths.prototype_geometry,
        }
        return [
            DoctorCheck(
                name=name,
                ok=path.is_file(),
                detail=f"present: {path}" if path.is_file() else f"missing: {path}",
            )
            for name, path in paths.items()
        ]

    def _prototype_geometry_check(self) -> DoctorCheck:
        try:
            contract_payload, guide, contract = self.factory._current_prototype_geometry()
            contract_sha = hashlib.sha256(contract_payload).hexdigest()
            guide_sha = hashlib.sha256(guide).hexdigest()
            source = contract.get("renderer_source", {})
            detail = (
                f"{contract.get('id')} contract sha256:{contract_sha}; guide sha256:{guide_sha}; "
                f"{source.get('body_cells')} cells at {source.get('native_cell_px')}px native"
            )
            return DoctorCheck(name="prototype_geometry", ok=True, detail=detail)
        except (FileNotFoundError, OSError, RuntimeError, ValueError) as error:
            return DoctorCheck(name="prototype_geometry", ok=False, detail=_safe_error(error))

    def _credential_checks(self, identity: DoctorIdentity) -> list[DoctorCheck]:
        required: set[str] = set()
        if identity in {"service", "all"}:
            for role_name in ("smart_text", "visual_judge", "image_generator"):
                role = getattr(self.config.models, role_name)
                if role.api_key_env:
                    required.add(role.api_key_env)
            required.add(self.config.service.service_token_env)
            worker_key = self.config.models.task_worker.api_key_env
            if worker_key:
                # LM Studio commonly permits anonymous localhost access.  If
                # an env name is configured it is still optional unless the
                # deployment explicitly sets it.
                required.discard(worker_key)
        if identity in {"operator", "all"}:
            required.add(self.config.review.operator_secret_env)
            required.add(self.config.service.operator_token_env)
        return [
            DoctorCheck(
                name=f"credential:{name}",
                ok=bool(os.environ.get(name)),
                detail="set" if os.environ.get(name) else "missing",
            )
            for name in sorted(required)
        ]

    def _database_check(self) -> DoctorCheck:
        try:
            self.database.migrate()
            self.database.assert_file_permissions()
            errors = self.database.integrity_check()
            if errors:
                return DoctorCheck(
                    name="database",
                    ok=False,
                    detail="integrity check failed: " + "; ".join(errors[:5]),
                )
            return DoctorCheck(
                name="database",
                ok=True,
                detail=f"migrated, private, and consistent: {self.database.path}",
            )
        except Exception as error:  # diagnostics must collect all failures
            return DoctorCheck(name="database", ok=False, detail=_safe_error(error))

    def _object_store_check(self) -> DoctorCheck:
        try:
            self.objects.assert_permissions()
            with tempfile.NamedTemporaryFile(prefix=".doctor-", dir=self.objects.root, delete=False) as handle:
                handle.write(b"skin-factory-object-store-doctor")
                temporary = Path(handle.name)
            temporary.unlink()
            errors = self.objects.verify_all()
            if errors:
                return DoctorCheck(
                    name="object_store",
                    ok=False,
                    detail=f"{len(errors)} corrupt object(s); first: {errors[0]}",
                )
            return DoctorCheck(
                name="object_store",
                ok=True,
                detail=f"owner-only, writable, and content hashes verify: {self.objects.root}",
            )
        except Exception as error:
            return DoctorCheck(name="object_store", ok=False, detail=_safe_error(error))

    def _capture_command_check(self) -> DoctorCheck:
        command = self.config.browser.capture_command
        if not command:
            return DoctorCheck(name="browser_capture_command", ok=False, detail="command is empty")
        executable = command[0]
        found = shutil.which(executable) if not Path(executable).is_absolute() else executable
        if not found or not Path(found).exists():
            return DoctorCheck(
                name="browser_capture_command",
                ok=False,
                detail=f"executable is unavailable: {executable}",
            )
        missing: list[str] = []
        for argument in command[1:]:
            if argument.startswith("-"):
                continue
            candidate = Path(argument)
            if not candidate.is_absolute():
                candidate = self.config.paths.repo_root / candidate
            if candidate.suffix in {".js", ".mjs", ".cjs", ".py"} and not candidate.is_file():
                missing.append(str(candidate))
        return DoctorCheck(
            name="browser_capture_command",
            ok=not missing,
            detail=(f"available: {' '.join(command)}" if not missing else "missing script(s): " + ", ".join(missing)),
        )

    def _renderer_bundle_check(self) -> DoctorCheck:
        try:
            manifest = renderer_bundle_manifest(self.config)
            assets = manifest["assets"]
            return DoctorCheck(
                name="cached_renderer_bundle",
                ok=True,
                detail=(
                    f"{len(assets)} exact HTML/JS/CSS/WASM assets; sha256:{renderer_bundle_manifest_sha(manifest)}"
                ),
            )
        except Exception as error:
            return DoctorCheck(name="cached_renderer_bundle", ok=False, detail=_safe_error(error))

    def _lama_check(self) -> DoctorCheck:
        if self.config.models.image_editor.provider != "local_lama":
            return DoctorCheck(
                name="lama_helper",
                ok=True,
                required=False,
                detail="not selected by image_editor role",
            )
        python = lama_python(self.config)
        if not python.is_file():
            return DoctorCheck(
                name="lama_helper",
                ok=False,
                detail=f"isolated helper is missing: {python}",
            )
        try:
            bundle = lama_bundle_manifest(self.config)
        except LamaRuntimeError as error:
            return DoctorCheck(name="lama_helper", ok=False, detail=_safe_error(error))
        uv = shutil.which("uv")
        if not uv:
            return DoctorCheck(name="lama_helper", ok=False, detail="uv is unavailable for frozen environment check")
        uv_home = self.config.paths.data_dir / "lama" / "uv-home"
        uv_home.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(uv_home, 0o700)
        uv_environment = {
            "PATH": os.pathsep.join((str(Path(uv).parent), "/usr/bin", "/bin")),
            "HOME": str(uv_home),
            "UV_PROJECT_ENVIRONMENT": str(python.parent.parent.resolve()),
            "UV_OFFLINE": "1",
        }
        synchronized = subprocess.run(
            [
                uv,
                "sync",
                "--project",
                str(self.config.paths.lama_manifest.parent),
                "--frozen",
                "--no-dev",
                "--no-install-project",
                "--offline",
                "--check",
                "--no-python-downloads",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
            env=uv_environment,
        )
        if synchronized.returncode != 0:
            return DoctorCheck(
                name="lama_helper",
                ok=False,
                detail="frozen offline dependency check failed: "
                + (synchronized.stderr or synchronized.stdout).strip()[-1_000:],
            )
        completed = subprocess.run(
            [
                str(python),
                "-m",
                "snaketron_lama_runtime",
            ],
            capture_output=True,
            text=True,
            timeout=min(180, int(self.config.models.image_editor.timeout_seconds)),
            check=False,
            env=lama_subprocess_environment(self.config),
        )
        if completed.returncode != 0:
            return DoctorCheck(
                name="lama_helper",
                ok=False,
                detail=(completed.stderr or completed.stdout).strip()[-1_000:],
            )
        try:
            report = json.loads(completed.stdout.strip())
        except ValueError:
            return DoctorCheck(name="lama_helper", ok=False, detail="LaMa smoke returned malformed JSON")
        if report.get("model_sha256") != bundle["model"]["sha256"] or report.get("loaded") is not True:
            return DoctorCheck(name="lama_helper", ok=False, detail="LaMa smoke loaded a different model identity")
        return DoctorCheck(
            name="lama_helper",
            ok=True,
            detail=(
                f"frozen offline environment, model load, and 32x32 inference succeeded; "
                f"bundle sha256:{lama_bundle_sha(bundle)}"
            ),
        )

    def _git_signing_check(self) -> DoctorCheck:
        repo = self.config.paths.repo_root
        required = self._promotion_required()
        try:
            key = _git(repo, "config", "--get", "user.signingkey")
            if not key:
                return DoctorCheck(
                    name="git_promotion_signing",
                    ok=False,
                    required=required,
                    detail="git user.signingkey is not configured for signed promotion tags",
                )
            signing_format = _git(repo, "config", "--get", "gpg.format") or "openpgp"
            if signing_format == "openpgp":
                gpg = shutil.which("gpg")
                if not gpg:
                    return DoctorCheck(
                        name="git_promotion_signing",
                        ok=False,
                        required=required,
                        detail="gpg is unavailable",
                    )
                completed = subprocess.run(
                    [gpg, "--batch", "--with-colons", "--list-secret-keys", key],
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
                ready = completed.returncode == 0 and "sec:" in completed.stdout
            elif signing_format == "ssh":
                key_path = Path(key).expanduser()
                ready = key_path.is_file() or key.startswith("ssh-")
            else:
                ready = True  # Git owns validation for supported external formats.
            active_ref = self.config.optimizer.active_skill_ref
            active = subprocess.run(
                ["git", "rev-parse", "--verify", f"{active_ref}^{{commit}}"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            details = [
                f"format={signing_format}",
                "secret signing key available" if ready else "secret signing key unavailable",
            ]
            if active.returncode == 0:
                details.append(f"active ref {active_ref}={active.stdout.strip()}")
            else:
                details.append(f"active immutable ref is missing: {active_ref}")
            return DoctorCheck(
                name="git_promotion_signing",
                ok=ready and active.returncode == 0,
                required=required,
                detail="; ".join(details),
            )
        except Exception as error:
            return DoctorCheck(
                name="git_promotion_signing",
                ok=False,
                required=required,
                detail=_safe_error(error),
            )

    async def _git_remote_check(self, *, offline: bool) -> DoctorCheck:
        repo = self.config.paths.repo_root
        remote = self.config.optimizer.promotion_remote
        required = self._promotion_required()
        try:
            url = _git(repo, "remote", "get-url", "--push", remote)
            if not url:
                raise RuntimeError(f"promotion remote is missing: {remote}")
            if offline:
                return DoctorCheck(
                    name="git_promotion_remote",
                    ok=True,
                    required=False,
                    detail=f"configured {remote}; fetch/push probes skipped by --offline",
                )
            fetch = subprocess.run(
                ["git", "ls-remote", "--exit-code", remote, "HEAD"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=45,
                check=False,
            )
            branch = f"refs/heads/bot/skin-authoring-doctor-{os.getpid()}"
            push = subprocess.run(
                ["git", "push", "--dry-run", remote, f"HEAD:{branch}"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=45,
                check=False,
            )
            ok = fetch.returncode == 0 and push.returncode == 0
            detail = (
                f"{remote} fetch and dry-run push are authorized"
                if ok
                else (fetch.stderr or push.stderr or fetch.stdout or push.stdout).strip()[-2_000:]
            )
            return DoctorCheck(name="git_promotion_remote", ok=ok, required=required, detail=detail)
        except Exception as error:
            return DoctorCheck(
                name="git_promotion_remote",
                ok=False,
                required=required,
                detail=_safe_error(error),
            )

    def _promotion_required(self) -> bool:
        if self.config.mode == "production":
            return True
        try:
            return (
                self.database.human_label_count() >= self.config.optimizer.promotion_min_pairs
                or self.database.ready_optimization_run() is not None
            )
        except Exception:
            return False

    async def _browser_check(self, *, offline: bool) -> DoctorCheck:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return DoctorCheck(
                name="playwright_chromium",
                ok=False,
                detail="Playwright is not installed in the production environment",
            )
        if offline:
            # Import proves the optional production extra is installed.  The
            # executable path check catches a sync without `playwright install`.
            try:
                async with async_playwright() as playwright:
                    executable = Path(playwright.chromium.executable_path)
                ok = executable.is_file()
                return DoctorCheck(
                    name="playwright_chromium",
                    ok=ok,
                    detail=(f"installed: {executable}" if ok else f"Chromium executable is missing: {executable}"),
                )
            except Exception as error:
                return DoctorCheck(name="playwright_chromium", ok=False, detail=_safe_error(error))
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                await browser.close()
            return DoctorCheck(name="playwright_chromium", ok=True, detail="headless Chromium launched")
        except Exception as error:
            return DoctorCheck(name="playwright_chromium", ok=False, detail=_safe_error(error))

    async def _worker_check(self) -> DoctorCheck:
        try:
            description = await self.factory.worker.describe_model()
            resolved = str(description.get("id") or description.get("name") or "unknown")
            expected = self.config.models.task_worker.model
            ok = not expected or resolved == expected
            return DoctorCheck(
                name="task_worker_model",
                ok=ok,
                detail=f"expected {expected!r}; resolved {resolved!r}",
            )
        except Exception as error:
            return DoctorCheck(name="task_worker_model", ok=False, detail=_safe_error(error))

    async def _worker_conformance_check(self) -> DoctorCheck:
        """Exercise the real worker contract without granting any side effect."""

        try:
            visual_challenge, card = _worker_conformance_card()
            card_ref = f"sha256:{hashlib.sha256(card).hexdigest()}"
            bundle = SkillBundle.load(self.config.paths.skill_dir)
            capabilities = json.loads(self.config.paths.capability_manifest.read_text(encoding="utf-8"))
            limits = capabilities["limits"]
            try:
                canonical_template = json.loads(bundle.files["templates/skin-v2.template.json"])
                document_template = {
                    "schema_version": canonical_template["schema_version"],
                    "id": canonical_template["id"],
                    "name": canonical_template["name"],
                    "palette": canonical_template["palette"],
                    "head_core": canonical_template["head_core"],
                    "layers": [
                        {
                            "name": "Outline",
                            "type": "ribbon",
                            "region": "contour",
                            "color": {"slot": "outline"},
                        },
                        {
                            "name": "Body",
                            "type": "ribbon",
                            "region": "body",
                            "color": {"slot": "fill"},
                        },
                    ],
                }
            except (KeyError, TypeError, json.JSONDecodeError) as error:
                raise ValueError("canonical author-skin bundle has no valid SkinDoc v2 template") from error
            expected_document = {
                **document_template,
                "id": "doctor-worker-conformance@1",
                "name": f"Doctor Visual {visual_challenge}",
            }
            request = WorkerRequest(
                request_id="doctor-worker-conformance-v2",
                attempt_id="doctor-side-effect-free-fixture",
                purpose=Purpose.CONTROL,
                skill_sha256=bundle.sha256,
                skill_files=bundle.files,
                capability_manifest=capabilities,
                artifact_refs={},
                authoring_inputs={
                    "conformance_fixture": {
                        "name": "Doctor Visual Identifier",
                        "brief": (
                            "Inspect the attached approved_prototype card. Read the six-digit decimal "
                            "identifier printed on it; the identifier is present only in the image. Copy "
                            "skin_document_template exactly, changing only id to "
                            "'doctor-worker-conformance@1' and name to 'Doctor Visual ' followed by those "
                            "exact six digits. Preserve every other value and array exactly, including each "
                            "ColorRef object such as {'slot': 'outline'}; a raw hex string is invalid at a "
                            "ColorRef site. Return no textures, assets, tool requests, or external actions."
                        ),
                        "implementation_path": "layers",
                        "skin_document_template": document_template,
                    }
                },
                inline_artifacts={
                    "approved_prototype": InlineArtifact(
                        content_hash=card_ref,
                        media_type="image/png",
                        base64_data=base64.b64encode(card).decode("ascii"),
                    )
                },
                pure_tools=[],
                budget={
                    "max_layers": int(limits["max_flattened_layers"]),
                    "max_texture_refs": 0,
                },
                output_schemas={"worker_result": WorkerResult.model_json_schema()},
                feedback=["This is a side-effect-free protocol conformance fixture, not a production build."],
            )
            result = await self.factory.worker.execute(request)
            expected = self.config.models.task_worker.model or ""
            if result.resolved_model != expected:
                raise ValueError(
                    f"worker execution resolved {result.resolved_model!r}, expected exact model {expected!r}"
                )
            if not isinstance(result.value, WorkerResult):
                raise ValueError("worker execution did not return a validated WorkerResult")
            validate_worker_handoff(result.value, bundle.files, capabilities)
            if (
                result.value.implementation_plan.path != "layers"
                or result.value.implementation_plan.asset_plan
                or result.value.tool_requests
            ):
                raise ValueError("worker conformance fixture requested asset or tool side effects")
            document = result.value.skin_document
            if document != expected_document:
                raise ValueError(
                    "worker conformance SkinDoc differs from the exact template outside its required identity"
                )
            deterministic = self.factory.gates.validate_document(
                document,
                result.value.implementation_plan,
            )
            if self.factory.gates.blocking_failure(deterministic):
                failures = [
                    f"{gate.gate}: {gate.reasons}" for gate in deterministic if gate.blocking and gate.verdict == "fail"
                ]
                raise ValueError("worker returned a SkinDoc that fails deterministic gates: " + "; ".join(failures))
            layers = document.get("layers")
            if not isinstance(layers, list) or not layers:
                raise ValueError("worker conformance SkinDoc has no procedural layer")
            name = document.get("name")
            if not isinstance(name, str) or visual_challenge not in name:
                raise ValueError("worker did not copy the six-digit image-only identifier into the SkinDoc name")
            return DoctorCheck(
                name="task_worker_conformance",
                ok=True,
                detail=(
                    "image-only challenge, vision WorkerRequest/WorkerResult, and deterministic SkinDoc "
                    f"gates passed with exact model {result.resolved_model!r}"
                ),
            )
        except Exception as error:
            return DoctorCheck(name="task_worker_conformance", ok=False, detail=_safe_error(error))

    async def _content_models_check(self) -> DoctorCheck:
        failures: list[str] = []
        resolved: list[str] = []
        for role_name in ("smart_text", "visual_judge", "image_generator"):
            role = getattr(self.config.models, role_name)
            try:
                description = await self.factory.providers.role(role_name).describe_model()
                identifier = str(description.get("id") or description.get("name") or "")
                actual = identifier.rsplit("/", 1)[-1]
                label = f"{role_name}({role.provider})"
                if actual != role.model:
                    failures.append(f"{label}: expected {role.model}, resolved {actual or identifier}")
                else:
                    resolved.append(f"{label}={actual}")
            except Exception as error:
                failures.append(f"{role_name}({role.provider}): {_safe_error(error)}")
        return DoctorCheck(
            name="content_models",
            ok=not failures,
            detail="; ".join(failures or resolved),
        )

    async def _api_check(self) -> DoctorCheck:
        try:
            result = await self.factory.api.health()
            return DoctorCheck(
                name="snaketron_api",
                ok=True,
                detail=f"healthy at {self.config.service.base_url}: {type(result).__name__}",
            )
        except Exception as error:
            return DoctorCheck(name="snaketron_api", ok=False, detail=_safe_error(error))

    async def _api_capability_check(self) -> DoctorCheck:
        """Prove the scheduled token is useful and cannot publish."""

        try:
            value = validate_service_capabilities(await self.factory.api.service_capabilities())
            identity = value["identity"]
            credential = value["credential"]
            user_id = identity.get("userId")
            return DoctorCheck(
                name="snaketron_service_capabilities",
                ok=True,
                detail=(
                    f"durable revocable credential {credential['credentialId']!r} for account {user_id!r} "
                    "can create private/evaluation skins and upload private forge textures; "
                    "publish/admin authority absent"
                ),
            )
        except Exception as error:
            return DoctorCheck(
                name="snaketron_service_capabilities",
                ok=False,
                detail=_safe_error(error),
            )


def _worker_conformance_card() -> tuple[str, bytes]:
    """Return an OCR-friendly PNG whose random challenge exists only in its pixels."""

    challenge = "".join(secrets.choice("23456789") for _ in range(6))
    image = Image.new("RGB", (768, 320), "#ffffff")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((16, 16, 752, 304), radius=28, fill="#edf6ff", outline="#1e5aa8", width=8)
    heading_font = ImageFont.load_default(size=34)
    challenge_font = ImageFont.load_default(size=112)
    draw.text((384, 70), "VISUAL CHECK", fill="#1e3a5f", font=heading_font, anchor="mm")
    draw.text((384, 198), challenge, fill="#080b10", font=challenge_font, anchor="mm", stroke_width=2)

    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    return challenge, output.getvalue()


def _safe_error(error: Exception) -> str:
    """Bound diagnostics without ever stringifying environment values."""

    message = str(error).strip() or error.__class__.__name__
    for value in os.environ.values():
        if len(value) >= 12 and value in message:
            message = message.replace(value, "<redacted>")
    return f"{error.__class__.__name__}: {message}"[-2_000:]


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""
