#!/usr/bin/env python3
"""Create disposable, deterministic gallery data for documentation captures."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml
from PIL import Image, ImageDraw

import snaketron_factory.db as database_module
from snaketron_factory.config import load_config
from snaketron_factory.db import Database
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    Purpose,
    Stage,
)
from snaketron_factory.objects import ObjectStore

PACKAGE = Path(__file__).resolve().parents[1]
REPO = PACKAGE.parent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--review-token", required=True)
    args = parser.parse_args()
    state = args.state.resolve()
    state.mkdir(parents=True, exist_ok=True)
    install_deterministic_database_clock()

    config = load_config(PACKAGE / "config/factory.yaml")
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    raw["paths"].update(
        {
            "data_dir": str(state / "var"),
            "database": str(state / "var/factory.sqlite3"),
            "objects": str(state / "var/objects"),
            "repo_root": str(REPO),
            "skill_dir": str(REPO / "skills/author-skin"),
            "capability_manifest": str(REPO / "skin-schema/capabilities-v2.json"),
            "direction": str(PACKAGE / "direction/design-direction.md"),
            "gate_manifest": str(PACKAGE / "config/gates.yaml"),
        }
    )
    raw["worker"]["adapter"] = "fake"
    raw["review"].update({"bind": "127.0.0.1"})
    config_dir = state / "config"
    config_dir.mkdir()
    (state / "templates").symlink_to(PACKAGE / "templates", target_is_directory=True)
    config_path = config_dir / "factory.yaml"
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    env_path = state / "operator.json"
    env_path.write_text(
        json.dumps(
            {
                "SKIN_FACTORY_REVIEW_TOKEN": args.review_token,
                "SKIN_FACTORY_REVIEW_ACTOR": "human:documentation",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    env_path.chmod(0o600)

    database = Database(state / "var/factory.sqlite3")
    database.migrate()
    objects = ObjectStore(state / "var/objects")

    review_attempt, review_artifact = add_attempt(
        database,
        objects,
        name="Neon Circuit",
        brief=(
            "Electric cyan traces travel through a midnight-blue snake, with a crisp head mark "
            "and a restrained pulse designed to stay readable at game scale."
        ),
        tags=["electric", "animated", "layers"],
        stage=Stage.PROTOTYPE_REVIEW,
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="prototype",
        image=snake_image("neon"),
        artifact_kind=ArtifactKind.PROTOTYPE,
    )
    database.add_evaluation(
        artifact_id=review_artifact["id"],
        attempt_id=review_attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="gemini-3.7-flash",
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
            reasons=["Strong head/body/tail role clarity", "Animation intent remains buildable"],
            measurements={"fidelity": 0.88, "readability": 0.93, "craft": 0.86},
        ),
        hidden_until_label=True,
    )

    ember, ember_artifact = add_attempt(
        database,
        objects,
        name="Ember Mosaic",
        brief=(
            "A warm tessellated coat inspired by cooling lava. The prototype was retained after "
            "machine triage found the tail silhouette too noisy."
        ),
        tags=["mosaic", "warm", "texture"],
        stage=Stage.PROTOTYPE_REVIEW,
        disposition=Disposition.MACHINE_REJECTED,
        image=snake_image("ember"),
        artifact_kind=ArtifactKind.PROTOTYPE,
    )
    database.add_evaluation(
        artifact_id=ember_artifact["id"],
        attempt_id=ember["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="gemini-3.7-flash",
            blocking=False,
            verdict=GateVerdict.MACHINE_REJECTED,
            reasons=["Tail role loses contrast", "Pattern scale becomes noisy near taper"],
            measurements={"fidelity": 0.66, "readability": 0.58, "craft": 0.79},
        ),
    )

    aurora, aurora_artifact = add_attempt(
        database,
        objects,
        name="Aurora Koi",
        brief=(
            "A flowing pearl-and-violet build that reached real browser rendering, then failed the "
            "temporal gate when the highlight stepped between sprite rows."
        ),
        tags=["aurora", "sprite-sheet", "animated"],
        stage=Stage.BUILD_TRIAGE,
        disposition=Disposition.MACHINE_REJECTED,
        image=contact_sheet(),
        artifact_kind=ArtifactKind.CONTACT_SHEET,
    )
    database.add_evaluation(
        artifact_id=aurora_artifact["id"],
        attempt_id=aurora["id"],
        evaluator="deterministic_gates",
        result=GateResult(
            gate="temporal_continuity",
            gate_version="gates-v2",
            blocking=True,
            verdict=GateVerdict.FAIL,
            reasons=["Frame 7→8 exceeds the declared temporal-delta bound"],
            measurements={"maximum_delta": 0.184, "allowed": 0.12, "frame_rows": 16},
        ),
    )

    midnight, midnight_artifact = add_attempt(
        database,
        objects,
        name="Midnight Checkers",
        brief=(
            "A precise formula-driven checker pattern. It passed deterministic gates but was "
            "retained after human review found the motion visually busy."
        ),
        tags=["checkerboard", "formula", "layers"],
        stage=Stage.FINAL_REVIEW,
        disposition=Disposition.HUMAN_REJECTED,
        image=snake_image("checker"),
        artifact_kind=ArtifactKind.CONTACT_SHEET,
    )
    database.add_human_decision(
        artifact_id=midnight_artifact["id"],
        attempt_id=midnight["id"],
        action="human_rejection",
        feedback="Excellent still frame; reduce the traveling checker velocity before retrying.",
        tags=["motion:busy"],
        actor="human:art-director",
        attempt_version=midnight["version"],
        content_hash=midnight_artifact["content_hash"],
    )

    prism, prism_artifact = add_attempt(
        database,
        objects,
        name="Prism Current",
        brief=(
            "A published hybrid skin with a procedural body sheen and a small authored head mark, "
            "preserved with its complete prototype-to-revision lineage."
        ),
        tags=["published", "hybrid", "shine"],
        stage=Stage.COMPLETE,
        disposition=Disposition.PUBLISHED,
        image=snake_image("prism"),
        artifact_kind=ArtifactKind.CONTACT_SHEET,
    )
    prism = database.update_attempt(
        prism["id"],
        prism["version"],
        production_skin_id="skin_prism_current",
        production_revision="4",
        production_content_hash=prism_artifact["content_hash"],
    )
    database.add_human_decision(
        artifact_id=prism_artifact["id"],
        attempt_id=prism["id"],
        action="publish_approval",
        feedback="Approved exact browser-rendered revision.",
        tags=[],
        actor="human:art-director",
        attempt_version=prism["version"],
        revision="4",
        content_hash=prism_artifact["content_hash"],
    )

    add_attempt(
        database,
        objects,
        name="Comet Wake Trial",
        brief=(
            "An isolated technique experiment testing layered additive wakes across varied fixture "
            "palettes. It can be inspected but cannot enter production review or publication."
        ),
        tags=["experiment", "effect", "isolated"],
        stage=Stage.COMPLETE,
        disposition=Disposition.EXPERIMENT_COMPLETE,
        purpose=Purpose.TECHNIQUE,
        image=snake_image("comet"),
        artifact_kind=ArtifactKind.CONTACT_SHEET,
    )

    (state / "fixture.json").write_text(
        json.dumps(
            {
                "config": str(config_path),
                "env": str(env_path),
                "review_attempt": review_attempt["id"],
                "soft_reject_attempt": ember["id"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(state / "fixture.json")


def install_deterministic_database_clock() -> None:
    """Make ids/timestamps stable so repeated captures produce the same pixels."""

    ids: defaultdict[str, int] = defaultdict(int)
    ticks = 0

    def deterministic_id(prefix: str) -> str:
        ids[prefix] += 1
        return f"{prefix}_documentation_{ids[prefix]:03d}"

    def deterministic_now() -> str:
        nonlocal ticks
        ticks += 1
        value = datetime(2026, 8, 21, 16, 0, tzinfo=UTC) + timedelta(microseconds=ticks)
        return value.isoformat(timespec="microseconds")

    database_module.new_id = deterministic_id
    database_module.now = deterministic_now


def add_attempt(
    database: Database,
    objects: ObjectStore,
    *,
    name: str,
    brief: str,
    tags: list[str],
    stage: Stage,
    disposition: Disposition,
    image: bytes,
    artifact_kind: ArtifactKind,
    purpose: Purpose = Purpose.PRODUCTION,
    review_kind: str | None = None,
) -> tuple[dict, dict]:
    seed = hashlib.sha256(name.encode()).hexdigest()[:16]
    concept = database.create_concept(
        name=name,
        brief=brief,
        seed=seed,
        source="documentation_fixture",
        tags=tags,
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=purpose,
        stage=stage,
        idempotency_key=f"screenshot:{seed}",
        behavior={"fixture": True, "mode": "shadow"},
        direction_sha="fixture-direction-v2",
        skill_sha="fixture-author-skin-v2",
        capability_sha="fixture-capabilities-v2",
        gate_sha="fixture-gates-v2",
        model_config_sha="fixture-model-roles-v1",
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        disposition=disposition,
        review_kind=review_kind,
    )
    stored = objects.put(image)
    artifact = database.add_artifact(
        attempt_id=attempt["id"],
        stage=stage,
        kind=artifact_kind,
        content_hash=stored.uri,
        object_ref=stored.uri,
        media_type="image/png",
        size_bytes=stored.size,
        metadata={"width": 1000, "height": 320, "fixture": True},
        provenance={"source": "deterministic_documentation_fixture"},
    )
    return attempt, artifact


def snake_image(style: str) -> bytes:
    palettes = {
        "neon": ("#071a33", "#00d9ff", "#85f6ff", "#173f68"),
        "ember": ("#291013", "#ff633f", "#ffc05c", "#73253a"),
        "checker": ("#101628", "#6b79ff", "#d6dcff", "#252d55"),
        "prism": ("#16132d", "#b271ff", "#65e8ff", "#423a82"),
        "comet": ("#101b2c", "#7da2ff", "#f4fbff", "#324b7d"),
    }
    background, primary, highlight, shadow = palettes[style]
    image = Image.new("RGB", (1000, 320), "#090c13")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((25, 25, 975, 295), radius=28, fill="#111722", outline="#293750", width=3)
    centers = [(150 + index * 82, 160) for index in range(9)]
    for index, (x, y) in enumerate(reversed(centers)):
        radius = 52 if index < 7 else 46 - (index - 7) * 12
        draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=background, outline=shadow, width=6)
        if style == "checker":
            for offset in (-28, 8):
                draw.rectangle(
                    (x + offset, y - 42, x + offset + 22, y + 42),
                    fill=primary if (index + offset) % 2 else highlight,
                )
        else:
            draw.arc((x - 38, y - 38, x + 38, y + 38), 205, 345, fill=primary, width=13)
            draw.arc((x - 27, y - 27, x + 27, y + 27), 30, 160, fill=highlight, width=6)
    head_x, head_y = centers[0]
    draw.ellipse((head_x - 61, head_y - 61, head_x + 61, head_y + 61), fill=background, outline=primary, width=7)
    draw.ellipse((head_x - 29, head_y - 28, head_x - 14, head_y - 13), fill=highlight)
    draw.ellipse((head_x + 14, head_y - 28, head_x + 29, head_y - 13), fill=highlight)
    draw.arc((head_x - 27, head_y - 9, head_x + 27, head_y + 35), 20, 160, fill=primary, width=5)
    tail_x, tail_y = centers[-1]
    draw.polygon(
        [(tail_x + 33, tail_y - 33), (tail_x + 86, tail_y), (tail_x + 33, tail_y + 33)],
        fill=background,
        outline=primary,
    )
    return encode_png(image)


def contact_sheet() -> bytes:
    image = Image.new("RGB", (1000, 500), "#090c13")
    draw = ImageDraw.Draw(image)
    for row, palette in enumerate(("prism", "neon", "comet")):
        strip = Image.open(io.BytesIO(snake_image(palette)))
        strip = strip.resize((900, 288))
        crop = strip.crop((0, 80, 900, 208)).resize((900, 128))
        image.paste(crop, (50, 32 + row * 148))
        draw.rounded_rectangle((35, 18 + row * 148, 965, 174 + row * 148), radius=18, outline="#34435f", width=2)
    return encode_png(image)


def encode_png(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True, compress_level=9)
    return output.getvalue()


if __name__ == "__main__":
    main()
