"""Cryptographically complete Skin Factory backup manifests and verification."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

SHA_REF_LENGTH = len("sha256:") + 64


def _is_sha_ref(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == SHA_REF_LENGTH
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _collect_refs(value: Any, refs: set[str]) -> None:
    if _is_sha_ref(value):
        refs.add(value)
    elif isinstance(value, dict):
        for item in value.values():
            _collect_refs(item, refs)
    elif isinstance(value, list):
        for item in value:
            _collect_refs(item, refs)


def database_object_refs(database_path: Path) -> set[str]:
    """Return every local-CAS authority reference retained by the backup DB."""

    refs: set[str] = set()
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        scalar_queries = (
            "SELECT object_ref FROM artifact",
            "SELECT result_hash FROM operation WHERE result_hash IS NOT NULL",
            "SELECT result_hash FROM operation_resolution WHERE result_hash IS NOT NULL",
            "SELECT recipe_ref FROM technique_candidate",
            "SELECT event_ref FROM outbox_message",
            "SELECT payload_ref FROM outbox_message",
            "SELECT dataset_version FROM optimization_run",
        )
        for query in scalar_queries:
            for (value,) in connection.execute(query):
                if _is_sha_ref(value):
                    refs.add(value)
        json_queries = (
            "SELECT behavior_json FROM attempt",
            "SELECT metadata_json FROM operation",
            "SELECT candidate_refs_json FROM optimization_run",
            "SELECT trial_results_json FROM technique_candidate",
        )
        for query in json_queries:
            for (payload,) in connection.execute(query):
                try:
                    value = json.loads(payload)
                except (TypeError, json.JSONDecodeError) as error:
                    raise RuntimeError(
                        f"backup database contains malformed retained-reference JSON: {error}"
                    ) from error
                _collect_refs(value, refs)
    finally:
        connection.close()
    return refs


def object_entries(objects_root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if objects_root.is_symlink() or not objects_root.is_dir():
        raise RuntimeError("backup object root must be a real directory")
    for path in sorted(objects_root.rglob("*")):
        if path.is_symlink():
            raise RuntimeError(f"backup object tree cannot contain symlinks: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(objects_root).as_posix()
        parts = relative.split("/")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if parts != ["sha256", digest[:2], digest]:
            raise RuntimeError(f"backup contains a non-canonical or corrupt CAS object: {relative}")
        entries.append({"path": relative, "sha256": digest, "bytes": path.stat().st_size})
    return entries


def entries_digest(entries: list[dict[str, Any]]) -> str:
    payload = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_manifest(*, database_path: Path, objects_root: Path, config_sha256: str, created_at: str) -> dict[str, Any]:
    entries = object_entries(objects_root)
    refs = database_object_refs(database_path)
    available = {f"sha256:{entry['sha256']}" for entry in entries}
    missing = sorted(refs - available)
    if missing:
        raise RuntimeError(f"backup is missing {len(missing)} database-referenced objects: {missing[:5]}")
    return {
        "version": 2,
        "created_at": created_at,
        "config_sha256": config_sha256,
        "database_sha256": hashlib.sha256(database_path.read_bytes()).hexdigest(),
        "objects_sha256": entries_digest(entries),
        "object_count": len(entries),
        "object_bytes": sum(int(entry["bytes"]) for entry in entries),
        "referenced_object_count": len(refs),
        "objects": entries,
    }


def verify_backup(root: Path) -> dict[str, Any]:
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("backup must be a real directory")
    manifest_path = root / "manifest.json"
    database_path = root / "factory.sqlite3"
    objects_root = root / "objects"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("version") != 2:
        raise RuntimeError("backup manifest must use version 2")
    if hashlib.sha256(database_path.read_bytes()).hexdigest() != manifest.get("database_sha256"):
        raise RuntimeError("backup database hash differs from manifest")
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        integrity = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    finally:
        connection.close()
    if integrity != ["ok"]:
        raise RuntimeError(f"backup database integrity failed: {integrity}")
    entries = object_entries(objects_root)
    if entries != manifest.get("objects") or entries_digest(entries) != manifest.get("objects_sha256"):
        raise RuntimeError("backup object inventory differs from manifest")
    if len(entries) != manifest.get("object_count") or sum(int(item["bytes"]) for item in entries) != manifest.get(
        "object_bytes"
    ):
        raise RuntimeError("backup object totals differ from manifest")
    refs = database_object_refs(database_path)
    available = {f"sha256:{entry['sha256']}" for entry in entries}
    missing = sorted(refs - available)
    if missing:
        raise RuntimeError(f"backup is missing {len(missing)} database-referenced objects: {missing[:5]}")
    if len(refs) != manifest.get("referenced_object_count"):
        raise RuntimeError("backup referenced-object count differs from manifest")
    return {
        "ok": True,
        "database_sha256": manifest["database_sha256"],
        "objects_sha256": manifest["objects_sha256"],
        "object_count": len(entries),
        "object_bytes": sum(int(item["bytes"]) for item in entries),
        "referenced_object_count": len(refs),
    }
