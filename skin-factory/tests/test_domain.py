from __future__ import annotations

import pytest
from pydantic import ValidationError

from snaketron_factory.domain import PrototypeManifest


def prototype_manifest_payload() -> dict[str, str]:
    return {
        "brief": "A projected prototype",
        "palette_intent": "Readable role colours",
        "motion_intent": "Static prototype",
        "implementation_hint": "layers",
        "hint_rationale": "Procedural marks carry the identity",
        "prompt": "Fill the exact body mask",
        "model_config": "retained-model-config",
        "image_sha256": "sha256:" + "1" * 64,
        "source_image_sha256": "sha256:" + "2" * 64,
        "geometry_projection": "prototype-body-mask-v1",
        "design_guidelines_sha256": "3" * 64,
        "prototype_geometry_sha256": "4" * 64,
        "prototype_guide_sha256": "5" * 64,
    }


def test_prototype_manifest_retains_exact_projection_provenance() -> None:
    manifest = PrototypeManifest.model_validate(prototype_manifest_payload())

    assert manifest.source_image_sha256 == "sha256:" + "2" * 64
    assert manifest.geometry_projection == "prototype-body-mask-v1"
    dumped = manifest.model_dump(mode="json", by_alias=True)
    assert dumped["model_config"] == "retained-model-config"
    assert dumped["source_image_sha256"] == "sha256:" + "2" * 64
    assert dumped["geometry_projection"] == "prototype-body-mask-v1"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_image_sha256", "2" * 64),
        ("geometry_projection", "legacy-mask"),
    ],
)
def test_prototype_manifest_rejects_invalid_projection_provenance(field: str, value: str) -> None:
    payload = prototype_manifest_payload()
    payload[field] = value

    with pytest.raises(ValidationError):
        PrototypeManifest.model_validate(payload)
