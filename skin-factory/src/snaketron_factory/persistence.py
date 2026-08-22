"""Durably serialize provider results before operation success commits."""

from __future__ import annotations

import json
from typing import Any

from .db import canonical_json
from .domain import ProviderResult
from .objects import ObjectStore

SUPPORTED_IMAGE_MEDIA_TYPES = frozenset({"image/png", "image/jpeg", "image/webp"})


class ResultPersistence:
    def __init__(self, objects: ObjectStore) -> None:
        self.objects = objects

    def __call__(self, result: ProviderResult) -> str:
        value = result.value
        if isinstance(value, dict) and isinstance(value.get("image"), bytes):
            stored = self.objects.put(value["image"])
        elif isinstance(value, bytes):
            stored = self.objects.put(value)
        else:
            if hasattr(value, "model_dump"):
                value = value.model_dump(mode="json", by_alias=True)
            stored = self.objects.put(canonical_json(value).encode("utf-8"))
        return stored.uri

    def load_json(self, reference: str) -> Any:
        return json.loads(self.objects.get(reference))
