"""Real-browser evidence capture against a cached renderer build."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import FactoryConfig
from .domain import GateResult
from .gates import GateManifest

RENDERER_BUNDLE_ROOT = Path("client/web/dist")
RENDERER_BUNDLE_SUFFIXES = frozenset({".css", ".html", ".js", ".wasm"})
RENDERER_BUNDLE_MANIFEST_ENV = "SNAKETRON_FACTORY_RENDERER_BUNDLE_MANIFEST"
RENDERER_BUNDLE_SHA_ENV = "SNAKETRON_FACTORY_RENDERER_BUNDLE_SHA256"


class RendererDrift(RuntimeError):
    """The renderer, cached browser bundle, or execution config changed."""


def renderer_bundle_manifest(config: FactoryConfig) -> dict[str, Any]:
    """Describe the exact cached browser artifacts authorized for capture.

    The browser URL is deliberately not trusted as the renderer identity.  A
    capture is authoritative only when its HTML, JavaScript, CSS, and WASM
    bytes are members of this local, behavior-pinned cache.
    """

    root = (config.paths.repo_root / RENDERER_BUNDLE_ROOT).resolve()
    if not root.is_dir():
        raise RendererDrift(f"cached renderer bundle is missing: {root}")
    assets: dict[str, dict[str, Any]] = {}
    for path in sorted(root.rglob("*")):
        if path.suffix.lower() not in RENDERER_BUNDLE_SUFFIXES:
            continue
        if path.is_symlink() or not path.is_file():
            raise RendererDrift(f"cached renderer bundle contains a non-regular asset: {path}")
        relative = path.relative_to(root).as_posix()
        value = path.read_bytes()
        assets[relative] = {
            "sha256": hashlib.sha256(value).hexdigest(),
            "size_bytes": len(value),
        }
    if "index.html" not in assets:
        raise RendererDrift("cached renderer bundle has no index.html")
    if not any(path.endswith(".js") for path in assets):
        raise RendererDrift("cached renderer bundle has no JavaScript asset")
    if not any(path.endswith(".wasm") for path in assets):
        raise RendererDrift("cached renderer bundle has no WASM asset")
    return {
        "schema_version": 1,
        "root": RENDERER_BUNDLE_ROOT.as_posix(),
        "assets": assets,
    }


def renderer_bundle_manifest_sha(manifest: dict[str, Any]) -> str:
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def renderer_execution_config(config: FactoryConfig) -> dict[str, Any]:
    source = config.source_path
    source_sha = config.version_sha256
    if source is not None and source.is_file():
        source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    bundle = renderer_bundle_manifest(config)
    return {
        "factory_config_sha": config.version_sha256,
        "factory_config_source_sha": source_sha,
        "repo_root": str(config.paths.repo_root.resolve()),
        "browser_base_url": config.browser.base_url,
        "service_base_url": config.service.base_url,
        "capture_command": list(config.browser.capture_command),
        "timeout_seconds": config.browser.timeout_seconds,
        "browser_bundle": bundle,
        "browser_bundle_sha256": renderer_bundle_manifest_sha(bundle),
    }


def renderer_execution_config_sha(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class BrowserEvidence:
    contact_sheet: bytes
    animation: bytes
    manifest: dict
    gate_result: GateResult
    renderer_sha: str
    renderer_config_sha: str | None = None


class BrowserRenderer:
    def __init__(self, config: FactoryConfig) -> None:
        self.config = config
        self.gates = GateManifest(config.paths.gate_manifest)

    def renderer_sha(self) -> str:
        """Hash the exact renderer worktree, including uncommitted content."""

        root = self.config.paths.repo_root.resolve()
        top_level = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if top_level.returncode != 0:
            return self._directory_sha(root)
        repository = Path(top_level.stdout.strip()).resolve()
        tree = subprocess.run(
            ["git", "rev-parse", "HEAD^{tree}"],
            cwd=repository,
            capture_output=True,
            timeout=10,
            check=True,
        ).stdout
        diff = subprocess.run(
            [
                "git",
                "diff",
                "--binary",
                "--full-index",
                "--no-ext-diff",
                "--no-textconv",
                "HEAD",
                "--",
                ".",
            ],
            cwd=repository,
            capture_output=True,
            timeout=30,
            check=True,
        ).stdout
        changed = subprocess.run(
            ["git", "diff", "--name-only", "--no-renames", "-z", "HEAD", "--", "."],
            cwd=repository,
            capture_output=True,
            timeout=30,
            check=True,
        ).stdout
        untracked = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"],
            cwd=repository,
            capture_output=True,
            timeout=30,
            check=True,
        ).stdout
        hasher = hashlib.sha256(b"snaketron-renderer-worktree-v1\0")
        hasher.update(tree)
        hasher.update(b"\0diff\0")
        hasher.update(diff)
        for encoded in sorted(filter(None, changed.split(b"\0"))):
            path = repository / os.fsdecode(encoded)
            hasher.update(b"\0changed-worktree-bytes\0")
            hasher.update(encoded)
            hasher.update(b"\0")
            if path.exists() or path.is_symlink():
                self._hash_path(hasher, path)
            else:
                hasher.update(b"deleted")
        for encoded in sorted(filter(None, untracked.split(b"\0"))):
            relative = os.fsdecode(encoded)
            path = repository / relative
            hasher.update(b"\0untracked\0")
            hasher.update(encoded)
            hasher.update(b"\0")
            self._hash_path(hasher, path)
        return hasher.hexdigest()

    @classmethod
    def _directory_sha(cls, root: Path) -> str:
        """Deterministic fallback used by source bundles without Git metadata."""

        hasher = hashlib.sha256(b"snaketron-renderer-directory-v1\0")
        for path in sorted(item for item in root.rglob("*") if ".git" not in item.parts):
            if path.is_dir() and not path.is_symlink():
                continue
            relative = path.relative_to(root).as_posix().encode("utf-8", errors="surrogateescape")
            hasher.update(relative)
            hasher.update(b"\0")
            cls._hash_path(hasher, path)
        return hasher.hexdigest()

    @staticmethod
    def _hash_path(hasher: Any, path: Path) -> None:
        details = path.lstat()
        hasher.update(str(details.st_mode).encode("ascii"))
        hasher.update(b"\0")
        if path.is_symlink():
            hasher.update(os.fsencode(os.readlink(path)))
        elif not stat.S_ISREG(details.st_mode):
            raise RuntimeError(f"renderer identity cannot hash non-regular path {path}")
        else:
            hasher.update(path.read_bytes())

    def execution_config(self) -> dict[str, Any]:
        return renderer_execution_config(self.config)

    @staticmethod
    def execution_config_sha(snapshot: dict[str, Any]) -> str:
        return renderer_execution_config_sha(snapshot)

    def capture(
        self,
        content_ref: str,
        *,
        expected_renderer_sha: str | None = None,
        expected_config_sha: str | None = None,
    ) -> BrowserEvidence:
        renderer_sha = self.renderer_sha()
        execution_config = self.execution_config()
        execution_config_sha = self.execution_config_sha(execution_config)
        if expected_renderer_sha is not None and renderer_sha != expected_renderer_sha:
            raise RendererDrift("renderer worktree changed before browser capture")
        if expected_config_sha is not None and execution_config_sha != expected_config_sha:
            raise RendererDrift("renderer configuration changed before browser capture")
        with tempfile.TemporaryDirectory(prefix="skin-factory-browser-") as directory:
            output = Path(directory) / "evidence"
            command = [
                *execution_config["capture_command"],
                execution_config["browser_base_url"],
                execution_config["service_base_url"],
                content_ref,
                str(output),
            ]
            environment = os.environ.copy()
            bundle_manifest = execution_config["browser_bundle"]
            bundle_sha = execution_config["browser_bundle_sha256"]
            environment[RENDERER_BUNDLE_MANIFEST_ENV] = json.dumps(
                bundle_manifest,
                sort_keys=True,
                separators=(",", ":"),
            )
            environment[RENDERER_BUNDLE_SHA_ENV] = bundle_sha
            completed = subprocess.run(
                command,
                cwd=execution_config["repo_root"],
                env=environment,
                capture_output=True,
                text=True,
                timeout=execution_config["timeout_seconds"],
                check=False,
            )
            if (
                self.renderer_sha() != renderer_sha
                or self.execution_config_sha(self.execution_config()) != execution_config_sha
            ):
                raise RendererDrift("renderer worktree or configuration changed during browser capture")
            evidence_path = output / "evidence.json"
            attestation_path = output / "renderer-attestation.json"
            attestation = None
            if attestation_path.is_file():
                try:
                    attestation = json.loads(attestation_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as error:
                    raise RendererDrift("served renderer bundle attestation is unreadable") from error
                errors = self._served_bundle_errors(attestation, bundle_manifest, bundle_sha)
                if errors:
                    raise RendererDrift(
                        "served browser bundle does not match pinned cached bundle: " + "; ".join(errors)
                    )
            if completed.returncode != 0 or not evidence_path.is_file():
                manifest = {
                    "schema_version": 1,
                    "content_ref": content_ref,
                    "renderer_exit": completed.returncode,
                    "stdout": completed.stdout[-4_000:],
                    "stderr": completed.stderr[-4_000:],
                }
                result = self.gates.result(
                    "browser_pixels_ready",
                    False,
                    reasons=[completed.stderr[-4_000:] or completed.stdout[-4_000:]],
                    measurements={"exit": completed.returncode},
                )
                return BrowserEvidence(b"", b"", manifest, result, renderer_sha, execution_config_sha)
            manifest = json.loads(evidence_path.read_text(encoding="utf-8"))
            served_renderer = manifest.get("served_renderer")
            if not isinstance(served_renderer, dict):
                raise RendererDrift("browser evidence has no served renderer bundle attestation")
            errors = self._served_bundle_errors(served_renderer, bundle_manifest, bundle_sha)
            if errors:
                raise RendererDrift("served browser bundle does not match pinned cached bundle: " + "; ".join(errors))
            if attestation is not None and served_renderer != attestation:
                raise RendererDrift("browser evidence renderer attestation differs from capture preflight")
            sheet = (output / "contact-sheet.png").read_bytes()
            animation = (output / "animation.webm").read_bytes()
            errors: list[str] = []
            if hashlib.sha256(sheet).hexdigest() != manifest["contact_sheet"]["sha256"]:
                errors.append("contact sheet hash does not match evidence manifest")
            if hashlib.sha256(animation).hexdigest() != manifest["animation"]["sha256"]:
                errors.append("animation hash does not match evidence manifest")
            asset = manifest.get("asset_status", {})
            if asset.get("failed", 0):
                errors.append("one or more image assets failed")
            if asset.get("requested", 0) and not asset.get("drawnImages", 0):
                errors.append("capture contains procedural fallback instead of image pixels")
            result = self.gates.result(
                "browser_pixels_ready",
                not errors,
                reasons=errors,
                measurements={
                    "sheet_bytes": len(sheet),
                    "animation_bytes": len(animation),
                    "asset_status": asset,
                },
            )
            return BrowserEvidence(sheet, animation, manifest, result, renderer_sha, execution_config_sha)

    @staticmethod
    def _served_bundle_errors(
        attestation: dict[str, Any],
        expected_manifest: dict[str, Any],
        expected_sha: str,
    ) -> list[str]:
        errors: list[str] = []
        if attestation.get("schema_version") != 1:
            errors.append("unsupported attestation schema")
        if attestation.get("bundle_manifest_sha256") != expected_sha:
            errors.append("bundle manifest identity differs")
        reported_errors = attestation.get("errors")
        if not isinstance(reported_errors, list):
            errors.append("attestation errors are malformed")
        elif reported_errors:
            errors.extend(f"capture reported {item}" for item in reported_errors if isinstance(item, str))
            if not any(isinstance(item, str) for item in reported_errors):
                errors.append("capture reported an unparseable bundle error")
        observed = attestation.get("assets")
        if not isinstance(observed, list):
            return [*errors, "served asset list is malformed"]
        expected_assets = expected_manifest.get("assets")
        if not isinstance(expected_assets, dict):
            return [*errors, "cached asset manifest is malformed"]
        paths: set[str] = set()
        for item in observed:
            if not isinstance(item, dict):
                errors.append("served asset entry is malformed")
                continue
            path = item.get("path")
            digest = item.get("sha256")
            size = item.get("size_bytes")
            if not isinstance(path, str) or path in paths:
                errors.append("served asset paths are missing or duplicated")
                continue
            paths.add(path)
            expected = expected_assets.get(path)
            if not isinstance(expected, dict):
                errors.append(f"unexpected served renderer asset {path}")
                continue
            if digest != expected.get("sha256") or size != expected.get("size_bytes"):
                errors.append(f"served bytes differ for {path}")
        if "index.html" not in paths:
            errors.append("served index.html was not attested")
        if not any(path.endswith(".js") for path in paths):
            errors.append("no served JavaScript asset was attested")
        if not any(path.endswith(".wasm") for path in paths):
            errors.append("no served WASM asset was attested")
        return errors
