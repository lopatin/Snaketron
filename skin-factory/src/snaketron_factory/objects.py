"""Content-addressed object storage used by every retained artifact."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StoredObject:
    sha256: str
    uri: str
    size: int


class ObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, digest: str) -> Path:
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("invalid sha256")
        return self.root / "sha256" / digest[:2] / digest

    def put(self, value: bytes) -> StoredObject:
        digest = hashlib.sha256(value).hexdigest()
        target = self._path(digest)
        self._secure_directory(self.root)
        self._secure_directory(self.root / "sha256")
        self._secure_directory(target.parent)
        if target.is_symlink():
            raise RuntimeError(f"object path cannot be a symlink: {target}")
        if not target.exists():
            fd, name = tempfile.mkstemp(prefix=".object-", dir=target.parent)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(value)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(name, 0o400)
                try:
                    os.replace(name, target)
                except OSError:
                    if not target.exists():
                        raise
            finally:
                if os.path.exists(name):
                    os.unlink(name)
        os.chmod(target, 0o400)
        actual = target.read_bytes()
        if hashlib.sha256(actual).hexdigest() != digest:
            raise RuntimeError(f"object corruption at {target}")
        return StoredObject(digest, f"sha256:{digest}", len(value))

    def get(self, reference: str) -> bytes:
        digest = reference.removeprefix("sha256:")
        value = self._path(digest).read_bytes()
        if hashlib.sha256(value).hexdigest() != digest:
            raise RuntimeError(f"object corruption for {reference}")
        return value

    def exists(self, reference: str) -> bool:
        return self._path(reference.removeprefix("sha256:")).is_file()

    def verify_all(self) -> list[str]:
        errors: list[str] = []
        root = self.root / "sha256"
        if not root.exists():
            return errors
        for path in root.glob("*/*"):
            if not path.is_file():
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if path.name != digest:
                errors.append(str(path))
        return errors

    def assert_permissions(self) -> None:
        """Migrate the private CAS tree to owner-only traversal and reads."""

        self._secure_directory(self.root)
        root = self.root / "sha256"
        self._secure_directory(root)
        for path in root.rglob("*"):
            if path.is_symlink():
                raise RuntimeError(f"object store cannot contain symlinks: {path}")
            if path.is_dir():
                os.chmod(path, 0o700)
            elif path.is_file():
                os.chmod(path, 0o400)

    @staticmethod
    def _secure_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.is_symlink() or not path.is_dir():
            raise RuntimeError(f"object store directory is not a real directory: {path}")
        os.chmod(path, 0o700)
