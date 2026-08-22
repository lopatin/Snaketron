"""Bounded, clean-worktree, append-only Git promotion for behavior artifacts."""

from __future__ import annotations

import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from .config import FactoryConfig
from .db import Database
from .environment import scrubbed_subprocess_environment


@dataclass(frozen=True)
class PromotionResult:
    git_ref: str
    sha: str
    branch: str


class GitPromoter:
    def __init__(self, config: FactoryConfig, database: Database) -> None:
        self.config = config
        self.database = database
        # Git/GPG/SSH variables remain available, but no model, webhook,
        # Snaketron, or human-review capability crosses this boundary.
        self._subprocess_env = scrubbed_subprocess_environment(config)

    def promote_playbook(
        self,
        *,
        candidate_playbook: str,
        run_id: str,
        expected_head: str,
        expected_active_sha: str,
        push: bool = True,
    ) -> PromotionResult:
        repo = self.config.paths.repo_root
        deadline = time.monotonic() + self.config.optimizer.promotion_timeout_seconds
        # The scheduled checkout intentionally remains pinned while behavior
        # promotions advance through immutable refs. Verify the expected base
        # object exists; the DB compare-and-swap below detects pointer races.
        _run(
            repo,
            "git",
            "cat-file",
            "-e",
            f"{expected_head}^{{commit}}",
            env=self._subprocess_env,
            deadline=deadline,
        )
        branch = f"bot/skin-authoring/{run_id}"
        tag = f"skin-authoring/{run_id}"
        with tempfile.TemporaryDirectory(prefix="skin-factory-promotion-") as directory:
            worktree = Path(directory) / "worktree"
            _run(
                repo,
                "git",
                "worktree",
                "add",
                "--detach",
                str(worktree),
                expected_head,
                env=self._subprocess_env,
                deadline=deadline,
            )
            try:
                playbook = worktree / "skills/author-skin/references/playbook.md"
                playbook.write_text(candidate_playbook, encoding="utf-8")
                changed = [
                    line
                    for line in _git(
                        worktree,
                        "diff",
                        "--name-only",
                        env=self._subprocess_env,
                        deadline=deadline,
                    ).splitlines()
                    if line
                ]
                if changed != ["skills/author-skin/references/playbook.md"]:
                    raise RuntimeError(f"optimizer diff escaped the playbook boundary: {changed}")
                validator = worktree / "skills/author-skin/scripts/validate_package.py"
                _run(
                    worktree,
                    "python3",
                    str(validator),
                    "--cargo",
                    env=self._subprocess_env,
                    deadline=deadline,
                )
                _run(
                    worktree,
                    "git",
                    "switch",
                    "-c",
                    branch,
                    env=self._subprocess_env,
                    deadline=deadline,
                )
                _run(
                    worktree,
                    "git",
                    "add",
                    "--",
                    "skills/author-skin/references/playbook.md",
                    env=self._subprocess_env,
                    deadline=deadline,
                )
                environment = dict(self._subprocess_env)
                environment.setdefault("GIT_AUTHOR_NAME", "Snaketron Skin Factory")
                environment.setdefault("GIT_AUTHOR_EMAIL", "skin-factory@snaketron.invalid")
                environment.setdefault("GIT_COMMITTER_NAME", "Snaketron Skin Factory")
                environment.setdefault("GIT_COMMITTER_EMAIL", "skin-factory@snaketron.invalid")
                _run(
                    worktree,
                    "git",
                    "commit",
                    "-m",
                    f"Improve skin authoring playbook ({run_id})",
                    env=environment,
                    deadline=deadline,
                )
                sha = _git(
                    worktree,
                    "rev-parse",
                    "HEAD",
                    env=self._subprocess_env,
                    deadline=deadline,
                )
                # Signed, immutable, and unique. Reusing a prior promotion ref
                # is a hard error rather than silently moving behavior history.
                _run(
                    worktree,
                    "git",
                    "tag",
                    "-s",
                    tag,
                    "-m",
                    f"Skin authoring {run_id}",
                    env=self._subprocess_env,
                    deadline=deadline,
                )
                if push:
                    remote = self.config.optimizer.promotion_remote
                    _run(
                        worktree,
                        "git",
                        "push",
                        remote,
                        f"HEAD:refs/heads/{branch}",
                        env=self._subprocess_env,
                        deadline=deadline,
                    )
                    _run(
                        worktree,
                        "git",
                        "push",
                        remote,
                        f"refs/tags/{tag}:refs/tags/{tag}",
                        env=self._subprocess_env,
                        deadline=deadline,
                    )
                    remote_sha = _git(
                        worktree,
                        "ls-remote",
                        remote,
                        f"refs/tags/{tag}^{{}}",
                        env=self._subprocess_env,
                        deadline=deadline,
                    ).split()[0]
                    if remote_sha != sha:
                        raise RuntimeError("remote signed tag does not peel to promoted SHA")
                    self._verify_clean_clone(remote, sha, self._subprocess_env, deadline)
                self.database.set_active_behavior(
                    "author-skin", f"refs/tags/{tag}", sha, expected_sha=expected_active_sha
                )
                return PromotionResult(f"refs/tags/{tag}", sha, branch)
            finally:
                _run(
                    repo,
                    "git",
                    "worktree",
                    "remove",
                    "--force",
                    str(worktree),
                    env=self._subprocess_env,
                    check=False,
                    timeout_seconds=30,
                )

    @staticmethod
    def _verify_clean_clone(
        remote: str,
        sha: str,
        environment: dict[str, str],
        deadline: float,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="skin-factory-verify-") as directory:
            clone = Path(directory) / "clone"
            _run(
                Path(directory),
                "git",
                "clone",
                "--no-checkout",
                remote,
                str(clone),
                env=environment,
                deadline=deadline,
            )
            _run(
                clone,
                "git",
                "cat-file",
                "-e",
                f"{sha}^{{commit}}",
                env=environment,
                deadline=deadline,
            )


def _git(cwd: Path, *arguments: str, env: dict[str, str], deadline: float | None = None) -> str:
    completed = _run(cwd, "git", *arguments, env=env, deadline=deadline)
    return completed.stdout.strip()


def _run(
    cwd: Path,
    *command: str,
    env: dict[str, str],
    check: bool = True,
    deadline: float | None = None,
    timeout_seconds: float = 1800,
) -> subprocess.CompletedProcess[str]:
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout=0)
        timeout_seconds = min(timeout_seconds, remaining)
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
        check=False,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(
            f"{' '.join(command)} failed ({completed.returncode}): "
            f"{completed.stderr[-4_000:] or completed.stdout[-4_000:]}"
        )
    return completed
