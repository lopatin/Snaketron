"""Side-effect-free task worker protocol and adapters."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx
from pydantic import ValidationError

from .config import FactoryConfig
from .domain import (
    ProviderError,
    ProviderFailureKind,
    ProviderResult,
    WorkerRequest,
    WorkerResult,
)


@dataclass(frozen=True)
class SkillBundle:
    files: dict[str, str]
    sha256: str

    @classmethod
    def load(cls, root: Path) -> SkillBundle:
        if not (root / "SKILL.md").is_file():
            raise FileNotFoundError(f"canonical authoring skill is missing {root / 'SKILL.md'}")
        files: dict[str, str] = {}
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            relative = path.relative_to(root).as_posix()
            if relative.startswith(".") or "__pycache__" in path.parts:
                continue
            data = path.read_bytes()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                # The execution package is instructions, schemas, and small
                # fixtures. Raster bytes travel as artifact refs instead.
                continue
            files[relative] = text
        return cls.from_files(files)

    @classmethod
    def load_git(cls, repo: Path, commit: str, skill_root: Path) -> SkillBundle:
        """Load an immutable promoted package without mutating the Hermes checkout."""

        relative_root = skill_root.resolve().relative_to(repo.resolve()).as_posix()
        listed = subprocess.run(
            ["git", "ls-tree", "-r", "--name-only", commit, "--", relative_root],
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if listed.returncode != 0:
            raise FileNotFoundError(f"cannot read authoring skill at Git commit {commit}")
        files: dict[str, str] = {}
        for repository_path in sorted(filter(None, listed.stdout.splitlines())):
            relative = Path(repository_path).relative_to(relative_root).as_posix()
            if relative.startswith(".") or "__pycache__" in Path(relative).parts:
                continue
            shown = subprocess.run(
                ["git", "show", f"{commit}:{repository_path}"],
                cwd=repo,
                capture_output=True,
                timeout=30,
                check=False,
            )
            if shown.returncode != 0:
                raise FileNotFoundError(f"cannot read {repository_path} at {commit}")
            try:
                files[relative] = shown.stdout.decode("utf-8")
            except UnicodeDecodeError:
                continue
        if "SKILL.md" not in files:
            raise FileNotFoundError(f"Git commit {commit} has no canonical author-skin package")
        return cls.from_files(files)

    @classmethod
    def from_files(cls, files: dict[str, str]) -> SkillBundle:
        hasher = hashlib.sha256()
        for relative, text in sorted(files.items()):
            hasher.update(relative.encode())
            hasher.update(b"\0")
            hasher.update(text.encode())
            hasher.update(b"\0")
        return cls(files=dict(files), sha256=hasher.hexdigest())


class WorkerAdapter(Protocol):
    async def execute(self, request: WorkerRequest) -> ProviderResult: ...


def _is_exact_lmstudio_model_unloaded(response: httpx.Response) -> bool:
    if response.status_code != 400:
        return False
    try:
        payload = response.json()
    except ValueError:
        return False
    return payload == {"error": "Model unloaded."}


def _is_loopback_endpoint(base_url: str) -> bool:
    try:
        endpoint = urlsplit(base_url)
    except ValueError:
        return False
    return endpoint.scheme == "http" and endpoint.hostname in {"127.0.0.1", "::1", "localhost"}


class OpenAICompatibleWorker:
    """JSON-only worker with no tools, credentials, network, or shell delegation."""

    SYSTEM = """You author one Snaketron SkinDoc v2 skin.

The canonical skill bundle below is the complete execution contract. Follow it
exactly. You have no tools and must not claim to call providers, upload files,
write Git, publish, browse the network, or run shell commands. Return only the
requested JSON. Asset work must be expressed as structured tool_requests for
the deterministic factory driver.
"""

    def __init__(
        self,
        config: FactoryConfig,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        role = config.models.task_worker
        self.model = role.model or ""
        self.base_url = (role.base_url or config.worker.endpoint).rstrip("/")
        self.timeout = role.timeout_seconds
        self.api_key = role.secret()
        self.client = client or httpx.AsyncClient(timeout=self.timeout)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def describe_model(self) -> dict[str, Any]:
        response = await self.client.get(f"{self.base_url}/models", headers=self._headers())
        response.raise_for_status()
        payload = response.json()
        models = payload.get("data", [])
        if not any(item.get("id") == self.model for item in models):
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                f"worker model {self.model!r} is not served at {self.base_url}",
            )
        return next(item for item in models if item.get("id") == self.model)

    def _headers(self) -> dict[str, str]:
        headers = {"content-type": "application/json"}
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        return headers

    async def execute(self, request: WorkerRequest) -> ProviderResult:
        skill_text = "\n\n".join(f"--- {name} ---\n{body}" for name, body in request.skill_files.items())
        user = {
            "request": request.model_dump(mode="json", exclude={"skill_files", "inline_artifacts"}),
            "canonical_skill_bundle": skill_text,
        }
        content: list[dict[str, Any]] = [{"type": "text", "text": json.dumps(user, sort_keys=True)}]
        for name, artifact in request.inline_artifacts.items():
            content.append({"type": "text", "text": f"Exact retained artifact: {name}"})
            content.append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{artifact.media_type};base64,{artifact.base64_data}",
                    },
                }
            )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.SYSTEM},
                {"role": "user", "content": content},
            ],
            "temperature": 0.2,
            "max_tokens": self.config.models.task_worker.max_output_tokens,
            "tools": [],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "snaketron_worker_result",
                    "strict": True,
                    "schema": WorkerResult.model_json_schema(),
                },
            },
        }
        try:
            response = await self.client.post(
                f"{self.base_url}/chat/completions", headers=self._headers(), json=payload
            )
        except httpx.ConnectError as error:
            raise ProviderError(ProviderFailureKind.UNAVAILABLE, f"task worker unavailable: {error}") from error
        except httpx.TimeoutException as error:
            known = isinstance(error, httpx.ConnectTimeout | httpx.PoolTimeout)
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                f"task worker timeout: {error}",
                outcome_known=known,
            ) from error
        except httpx.TransportError as error:
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                f"task worker transport outcome is unknown: {error}",
                outcome_known=False,
            ) from error
        request_id = response.headers.get("x-request-id")
        if not response.is_success:
            lmstudio_model_unloaded = _is_loopback_endpoint(self.base_url) and _is_exact_lmstudio_model_unloaded(
                response
            )
            if lmstudio_model_unloaded:
                # LM Studio rejected the request before inference because the
                # selected local model instance is absent. This is a known-safe
                # availability failure, not malformed model output.
                kind = ProviderFailureKind.UNAVAILABLE
            elif response.status_code == 429:
                kind = ProviderFailureKind.QUOTA
            elif response.status_code == 408 or response.status_code >= 500:
                kind = ProviderFailureKind.UNAVAILABLE
            elif response.status_code in {400, 404, 409, 422}:
                kind = ProviderFailureKind.INVALID_OUTPUT
            else:
                kind = ProviderFailureKind.REFUSAL
            raise ProviderError(
                kind,
                f"task worker HTTP {response.status_code}: {response.text[:500]}",
                # The worker endpoint has no documented idempotency key. A
                # retry-looking response therefore cannot prove the model did
                # not already perform the generation.
                outcome_known=(
                    lmstudio_model_unloaded
                    or (response.status_code != 429 and response.status_code != 408 and response.status_code < 500)
                ),
                request_id=request_id,
            )
        try:
            body = response.json()
        except ValueError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"task worker returned invalid response JSON: {error}",
                request_id=request_id,
            ) from error
        if not isinstance(body, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "task worker response JSON must be an object",
                request_id=request_id,
            )
        choices = body.get("choices", [])
        if not choices:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "task worker returned no choices",
                request_id=request_id,
            )
        message = choices[0].get("message", {})
        if message.get("tool_calls"):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "task worker attempted a side-effecting tool call",
                request_id=request_id,
            )
        content = message.get("content")
        structured_output_channel = "content"
        # LM Studio's Qwen thinking parser can place the response-format JSON
        # in reasoning_content while returning an explicitly empty content
        # string. Accept that narrow representation only when the ordinary
        # channel is empty; the candidate still has to validate as the exact
        # closed WorkerResult schema below. Never prefer hidden reasoning over
        # a nonempty content response.
        if isinstance(content, str) and not content.strip():
            reasoning_content = message.get("reasoning_content")
            if isinstance(reasoning_content, str) and reasoning_content.strip():
                content = reasoning_content
                structured_output_channel = "reasoning_content"
        if not isinstance(content, str):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "task worker response is not JSON text",
                request_id=request_id,
            )
        try:
            result = WorkerResult.model_validate_json(content)
        except (ValidationError, ValueError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"task worker violated WorkerResult: {error}",
                request_id=request_id,
            ) from error
        usage = body.get("usage")
        resolved_model = body.get("model")
        if not isinstance(resolved_model, str) or not resolved_model.strip():
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "task worker response omitted its resolved model",
                request_id=request_id,
            )
        usage_result: dict[str, bool | int | float] = {}
        usage_complete = isinstance(usage, dict) and all(
            _valid_usage_count(usage.get(key)) for key in ("prompt_tokens", "completion_tokens")
        )
        usage_result["usage_complete"] = usage_complete
        if isinstance(usage, dict):
            if _valid_usage_count(usage.get("prompt_tokens")):
                usage_result["input_tokens"] = int(usage["prompt_tokens"])
            if _valid_usage_count(usage.get("completion_tokens")):
                usage_result["output_tokens"] = int(usage["completion_tokens"])
        if usage_complete:
            role = self.config.models.task_worker
            usage_result["cost_micros"] = (
                int(usage["prompt_tokens"]) * role.cost_per_million_input_micros
                + int(usage["completion_tokens"]) * role.cost_per_million_output_micros
            ) // 1_000_000
        return ProviderResult(
            value=result,
            request_id=request_id,
            resolved_model=resolved_model,
            sanitized_metadata={
                "finish_reason": choices[0].get("finish_reason"),
                "system_fingerprint": body.get("system_fingerprint"),
                "structured_output_channel": structured_output_channel,
            },
            usage=usage_result,
        )


def _valid_usage_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


class FakeWorker:
    def __init__(self, results: list[WorkerResult] | None = None, *, model: str = "fake-worker-v1") -> None:
        self.results = list(results or [])
        self.requests: list[WorkerRequest] = []
        self.model = model

    async def describe_model(self) -> dict[str, Any]:
        return {"id": "fake-worker", "owned_by": "test"}

    async def execute(self, request: WorkerRequest) -> ProviderResult:
        self.requests.append(request)
        if not self.results:
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "fake worker has no queued result")
        return ProviderResult(
            value=self.results.pop(0),
            request_id=f"fake-worker-{len(self.requests)}",
            resolved_model=self.model,
            sanitized_metadata={"fake": True},
            usage={"cost_micros": 0},
        )


def build_worker(config: FactoryConfig) -> WorkerAdapter:
    if config.worker.adapter == "fake":
        return FakeWorker(model=config.models.task_worker.model or "fake-worker-v1")
    return OpenAICompatibleWorker(config)
