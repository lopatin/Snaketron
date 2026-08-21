from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from snaketron_factory.domain import ConceptProposal, ProviderResult
from snaketron_factory.objects import ObjectStore
from snaketron_factory.persistence import ResultPersistence


def test_object_store_is_content_addressed_deduplicated_and_read_only(objects: ObjectStore, factory_config) -> None:
    value = b"exact retained bytes\x00\xff"
    expected = hashlib.sha256(value).hexdigest()
    first = objects.put(value)
    second = objects.put(value)

    assert first == second
    assert first.sha256 == expected
    assert first.uri == f"sha256:{expected}"
    assert first.size == len(value)
    assert objects.get(first.uri) == value
    assert objects.exists(first.uri)
    path = factory_config.paths.objects / "sha256" / expected[:2] / expected
    assert path.stat().st_mode & 0o777 == 0o400
    assert factory_config.paths.objects.stat().st_mode & 0o777 == 0o700
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert objects.verify_all() == []


def test_object_store_migrates_existing_world_readable_tree(objects: ObjectStore, factory_config) -> None:
    stored = objects.put(b"private review artifact")
    root = factory_config.paths.objects
    shard = root / "sha256" / stored.sha256[:2]
    path = shard / stored.sha256
    os.chmod(root, 0o755)
    os.chmod(shard, 0o755)
    os.chmod(path, 0o444)

    objects.assert_permissions()

    assert root.stat().st_mode & 0o777 == 0o700
    assert shard.stat().st_mode & 0o777 == 0o700
    assert path.stat().st_mode & 0o777 == 0o400


def test_object_store_refuses_symlinks_inside_private_cas(objects: ObjectStore, factory_config, tmp_path) -> None:
    objects.assert_permissions()
    outside = tmp_path / "outside"
    outside.mkdir()
    shard = factory_config.paths.objects / "sha256" / "aa"
    shard.symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeError, match="symlink"):
        objects.assert_permissions()


def test_object_store_rejects_invalid_references_and_detects_corruption(objects: ObjectStore, factory_config) -> None:
    for invalid in ("sha256:../escape", "sha256:" + "A" * 64, "sha256:abc"):
        with pytest.raises(ValueError, match="invalid sha256"):
            objects.get(invalid)

    stored = objects.put(b"original")
    path = factory_config.paths.objects / "sha256" / stored.sha256[:2] / stored.sha256
    os.chmod(path, 0o644)
    path.write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="object corruption"):
        objects.get(stored.uri)
    assert objects.verify_all() == [str(path)]


def test_result_persistence_preserves_exact_images_and_canonicalizes_json(objects: ObjectStore) -> None:
    persistence = ResultPersistence(objects)
    image = b"\x89PNG\r\n\x1a\nexact"
    image_ref = persistence(
        ProviderResult(
            value={"image": image, "media_type": "image/png", "ignored": "metadata"},
            resolved_model="image-model",
        )
    )
    assert objects.get(image_ref) == image

    raw_ref = persistence(ProviderResult(value=b"raw bytes", resolved_model="worker"))
    assert objects.get(raw_ref) == b"raw bytes"

    json_ref = persistence(ProviderResult(value={"z": 1, "a": [3, 2]}, resolved_model="text-model"))
    assert objects.get(json_ref) == b'{"a":[3,2],"z":1}'
    assert persistence.load_json(json_ref) == {"a": [3, 2], "z": 1}

    model_ref = persistence(
        ProviderResult(
            value=ConceptProposal(
                name="River Glass",
                brief="A sufficiently detailed concept proposal for serialization.",
                tags=["river"],
                seed="seed",
                palette_intent="cyan",
                motion_intent="flow",
                implementation_hint="layers",
                implementation_rationale="The pattern is best expressed procedurally.",
                novelty_score=0.9,
                direction_score=0.9,
                novelty_rationale="The crystalline river rhythm is distinct from the retained examples.",
            ),
            resolved_model="smart",
        )
    )
    assert json.loads(objects.get(model_ref))["name"] == "River Glass"


def test_object_store_put_rechecks_an_existing_corrupt_object(objects: ObjectStore, factory_config: object) -> None:
    value = b"stable"
    stored = objects.put(value)
    root = Path(factory_config.paths.objects)
    path = root / "sha256" / stored.sha256[:2] / stored.sha256
    os.chmod(path, 0o644)
    path.write_bytes(b"broken")
    with pytest.raises(RuntimeError, match="object corruption"):
        objects.put(value)
