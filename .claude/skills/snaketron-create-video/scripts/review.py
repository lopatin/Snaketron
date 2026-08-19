#!/usr/bin/env python3
"""Generate a frame strip and machine-readable QC report for a rendered trailer."""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import subprocess
import sys
from fractions import Fraction
from pathlib import Path
from typing import Any


BLACK_RE = re.compile(
    r"black_start:(?P<start>[0-9.]+).*?black_end:(?P<end>[0-9.]+)", re.DOTALL
)
FREEZE_RE = re.compile(
    r"freeze_start:\s*(?P<start>[0-9.]+).*?freeze_end:\s*(?P<end>[0-9.]+)",
    re.DOTALL,
)
FRAME_MD5_RE = re.compile(
    r"^\s*\d+,\s*-?\d+,\s*-?\d+,\s*\d+,\s*\d+,\s*([0-9a-fA-F]+)\s*$"
)


class ReviewError(RuntimeError):
    pass


def _command(
    args: list[str], *, capture: bool = False
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if result.returncode != 0:
        detail = (result.stderr or "").strip()
        raise ReviewError(f"command failed ({result.returncode}): {detail}")
    return result


def _probe(video: Path, ffprobe: str) -> tuple[dict[str, Any], dict[str, Any]]:
    result = _command(
        [
            ffprobe,
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            str(video),
        ],
        capture=True,
    )
    data = json.loads(result.stdout)
    video_stream = next(
        (
            stream
            for stream in data.get("streams", [])
            if stream.get("codec_type") == "video"
        ),
        None,
    )
    if video_stream is None:
        raise ReviewError("render has no video stream")
    return data, video_stream


def _fraction(value: str | None) -> float:
    if not value or value == "0/0":
        return 0.0
    return float(Fraction(value))


def _intervals(
    video: Path, ffmpeg: str
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    result = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-nostats",
            "-i",
            str(video),
            "-vf",
            "blackdetect=d=0.04:pix_th=0.02,freezedetect=n=0.001:d=0.04",
            "-an",
            "-f",
            "null",
            "-",
        ],
        check=False,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise ReviewError("ffmpeg black/freeze analysis failed")
    black = [
        (float(item.group("start")), float(item.group("end")))
        for item in BLACK_RE.finditer(result.stderr)
    ]
    frozen = [
        (float(item.group("start")), float(item.group("end")))
        for item in FREEZE_RE.finditer(result.stderr)
    ]
    return black, frozen


def _exact_splice_duplicates(
    video: Path, ffmpeg: str, fps: float, splices: list[float]
) -> list[tuple[float, float]]:
    """Find actual repeated decoded frames touching an edit boundary.

    `freezedetect` deliberately uses a perceptual noise threshold and therefore
    labels SnakeTron's legitimate cell-stepped motion as a freeze. The launch
    gate is narrower: catch an accidentally duplicated frame at a splice. Hash
    decoded frames and compare only the adjacent pairs that touch each edit.
    """
    result = _command(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(video),
            "-map",
            "0:v:0",
            "-f",
            "framemd5",
            "-",
        ],
        capture=True,
    )
    hashes = [
        match.group(1).lower()
        for line in result.stdout.splitlines()
        if (match := FRAME_MD5_RE.match(line))
    ]
    duplicates: list[tuple[float, float]] = []
    if fps <= 0:
        return duplicates
    for splice in splices:
        boundary = round(splice * fps)
        for left in (boundary - 1, boundary):
            right = left + 1
            if 0 <= left < right < len(hashes) and hashes[left] == hashes[right]:
                # A deliberately held title/card has many identical frames;
                # that is not an edit duplication. Flag only an isolated pair
                # introduced at the splice boundary.
                same_before = left > 0 and hashes[left - 1] == hashes[left]
                same_after = (
                    right + 1 < len(hashes) and hashes[right + 1] == hashes[right]
                )
                if same_before or same_after:
                    continue
                interval = (left / fps, right / fps)
                if interval not in duplicates:
                    duplicates.append(interval)
    return duplicates


def _near_splice(
    interval: tuple[float, float], splices: list[float], tolerance: float
) -> bool:
    start, end = interval
    return any(start - tolerance <= splice <= end + tolerance for splice in splices)


def _starts_or_ends_at_splice(
    interval: tuple[float, float], splices: list[float], tolerance: float
) -> bool:
    start, end = interval
    return any(
        abs(start - splice) <= tolerance or abs(end - splice) <= tolerance
        for splice in splices
    )


def review(
    video: Path,
    strip: Path,
    compiled_path: Path | None,
    frames: int,
    ffmpeg: str,
    ffprobe: str,
    expected_fps: int | None,
) -> dict[str, Any]:
    if not video.is_file():
        raise ReviewError(f"render not found: {video}")
    probe, stream = _probe(video, ffprobe)
    duration = float(
        stream.get("duration") or probe.get("format", {}).get("duration") or 0
    )
    if duration <= 0:
        raise ReviewError("render duration is unavailable")
    fps = _fraction(stream.get("avg_frame_rate"))
    interval = max(duration / max(1, frames), 1 / max(fps, 1))
    columns = min(5, frames)
    rows = math.ceil(frames / columns)
    strip.parent.mkdir(parents=True, exist_ok=True)
    _command(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(video),
            "-vf",
            f"fps=1/{interval:.9f},scale=384:-2:flags=lanczos,tile={columns}x{rows}:padding=4:margin=4:color=#3F3F41",
            "-frames:v",
            "1",
            str(strip),
        ]
    )

    compiled = None
    splices: list[float] = []
    if compiled_path:
        try:
            compiled = json.loads(compiled_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ReviewError(f"cannot read compiled EDL: {exc}") from exc
        splices = [float(item["offset"]) for item in compiled.get("transitions", [])]
        transition_targets = {
            int(item["to_segment"]) for item in compiled.get("transitions", [])
        }
        splices.extend(
            float(segment["global_start"])
            for index, segment in enumerate(compiled.get("segments", []))
            if index > 0 and index not in transition_targets
        )

    black, _frozen = _intervals(video, ffmpeg)
    frame_tolerance = 1.5 / max(fps, 1)
    splice_black = [
        item for item in black if _near_splice(item, splices, frame_tolerance)
    ]
    splice_frozen = _exact_splice_duplicates(video, ffmpeg, fps, splices)

    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": passed, "detail": detail})

    check(
        "cfr",
        abs(_fraction(stream.get("r_frame_rate")) - fps) < 1e-6,
        f"avg={fps:g}, nominal={_fraction(stream.get('r_frame_rate')):g}",
    )
    if expected_fps:
        check(
            "fps",
            abs(fps - expected_fps) < 1e-6,
            f"expected {expected_fps}, got {fps:g}",
        )
    check(
        "pixel_format", stream.get("pix_fmt") == "yuv420p", str(stream.get("pix_fmt"))
    )
    tags = {
        key: stream.get(key)
        for key in ("color_space", "color_transfer", "color_primaries")
    }
    check(
        "bt709_tags",
        all(value == "bt709" for value in tags.values()),
        json.dumps(tags, sort_keys=True),
    )
    check("splice_black_frames", not splice_black, f"intervals={splice_black}")
    check("splice_duplicate_frames", not splice_frozen, f"intervals={splice_frozen}")
    if compiled:
        expected_duration = float(compiled.get("duration", 0))
        check(
            "timeline_duration",
            abs(duration - expected_duration) <= 2 / max(fps, 1),
            f"expected={expected_duration:.6f}, actual={duration:.6f}",
        )
        check(
            "beat_snap",
            bool(compiled.get("qc", {}).get("all_cut_offsets_within_tolerance", False)),
            f"tolerance={compiled.get('qc', {}).get('beat_snap_tolerance_s')}",
        )
        check(
            "timeline_entries",
            len(compiled.get("segments", [])) > 0,
            f"segments={len(compiled.get('segments', []))}",
        )
    report = {
        "schema_version": 1,
        "video": str(video.resolve()),
        "frame_strip": str(strip.resolve()),
        "duration": duration,
        "fps": fps,
        "resolution": [stream.get("width"), stream.get("height")],
        "checks": checks,
        "passed": all(item["passed"] for item in checks),
        "manual_checks": [
            "Confirm every intended beat and timeline entry appears.",
            "Confirm title and callout text is legible at 1080p.",
            "Confirm music ducks audibly beneath impact SFX.",
            "Confirm motion, camera framing, and brand treatment support the story.",
        ],
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create a trailer frame strip and QC report."
    )
    parser.add_argument("video", type=Path)
    parser.add_argument("--compiled", type=Path)
    parser.add_argument("--strip", type=Path, default=Path("review-strip.jpg"))
    parser.add_argument("--report", type=Path)
    parser.add_argument("--frames", type=int, default=12)
    parser.add_argument("--fps", type=int)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--ffprobe", default="ffprobe")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    if args.frames < 1:
        parser.error("--frames must be positive")
    if not shutil.which(args.ffmpeg) or not shutil.which(args.ffprobe):
        parser.error("ffmpeg and ffprobe must be installed")
    try:
        report = review(
            args.video.resolve(),
            args.strip.resolve(),
            args.compiled.resolve() if args.compiled else None,
            args.frames,
            args.ffmpeg,
            args.ffprobe,
            args.fps,
        )
    except ReviewError as exc:
        print(f"review: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 1 if args.strict and not report["passed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
