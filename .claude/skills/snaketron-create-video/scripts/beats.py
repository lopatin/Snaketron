#!/usr/bin/env python3
"""Create a deterministic beat-grid manifest for SnakeTron video edits."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
import wave
from array import array
from pathlib import Path
from typing import Any


class BeatError(ValueError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pcm_mono(path: Path) -> tuple[list[float], int, float]:
    try:
        with wave.open(str(path), "rb") as wav:
            channels = wav.getnchannels()
            width = wav.getsampwidth()
            sample_rate = wav.getframerate()
            frame_count = wav.getnframes()
            raw = wav.readframes(frame_count)
    except (wave.Error, FileNotFoundError) as exc:
        raise BeatError(f"stdlib backend requires a readable PCM WAV: {path}") from exc
    if channels < 1 or width not in (1, 2, 3, 4) or sample_rate <= 0:
        raise BeatError("unsupported WAV channel count, sample width, or rate")
    if channels > 1:
        # Decode arbitrary channel counts and average without removed/deprecated audioop.
        mono = bytearray()
        frame_width = channels * width
        for offset in range(0, len(raw) - frame_width + 1, frame_width):
            total = 0
            for channel in range(channels):
                start = offset + channel * width
                sample = int.from_bytes(
                    raw[start : start + width],
                    "little",
                    signed=width != 1,
                )
                if width == 1:
                    sample -= 128
                total += sample
            averaged = round(total / channels)
            if width == 1:
                averaged += 128
            mono.extend(int(averaged).to_bytes(width, "little", signed=width != 1))
        raw = bytes(mono)
    scale = float(1 << (width * 8 - 1))
    samples: list[float] = []
    if width == 1:
        samples = [(value - 128) / 128.0 for value in raw]
    elif width == 2:
        values = array("h")
        values.frombytes(raw)
        if sys.byteorder != "little":
            values.byteswap()
        samples = [value / scale for value in values]
    elif width == 4:
        values = array("i")
        values.frombytes(raw)
        if sys.byteorder != "little":
            values.byteswap()
        samples = [value / scale for value in values]
    else:
        for offset in range(0, len(raw) - 2, 3):
            samples.append(
                int.from_bytes(raw[offset : offset + 3], "little", signed=True) / scale
            )
    return samples, sample_rate, len(samples) / sample_rate


def _stdlib_grid(
    path: Path, min_bpm: float, max_bpm: float, hop: int
) -> tuple[list[float], float, float, int]:
    samples, sample_rate, duration = _pcm_mono(path)
    if duration < 0.5:
        raise BeatError(
            "audio must be at least 0.5 seconds for automatic beat detection"
        )
    envelope: list[float] = []
    for offset in range(0, len(samples), hop):
        window = samples[offset : offset + hop]
        envelope.append(
            math.sqrt(sum(value * value for value in window) / max(1, len(window)))
        )
    # Positive spectral-energy novelty against a short deterministic moving baseline.
    novelty: list[float] = []
    baseline_frames = max(2, round(0.20 * sample_rate / hop))
    for index, value in enumerate(envelope):
        start = max(0, index - baseline_frames)
        baseline = statistics.fmean(envelope[start:index] or [0.0])
        novelty.append(max(0.0, value - baseline))
    if max(novelty, default=0.0) <= 1e-9:
        raise BeatError("no usable transients found; pass --bpm for a manual grid")

    frame_rate = sample_rate / hop
    min_lag = max(1, round(frame_rate * 60.0 / max_bpm))
    max_lag = max(min_lag, round(frame_rate * 60.0 / min_bpm))
    scores: list[tuple[float, int]] = []
    for lag in range(min_lag, min(max_lag + 1, len(novelty))):
        score = sum(
            novelty[index] * novelty[index - lag] for index in range(lag, len(novelty))
        )
        scores.append((score, lag))
    if not scores:
        raise BeatError("audio is too short for the requested BPM range")
    # Prefer the faster grid on exact ties so half-time ambiguity is deterministic.
    _, lag = max(scores, key=lambda item: (item[0], -item[1]))
    bpm = 60.0 * frame_rate / lag

    phase_scores = []
    for phase in range(lag):
        phase_scores.append((sum(novelty[phase::lag]), phase))
    _, phase = max(phase_scores, key=lambda item: (item[0], -item[1]))
    offset = phase / frame_rate
    period = 60.0 / bpm
    beats = []
    value = offset
    while value <= duration + 1e-9:
        beats.append(round(value, 6))
        value += period
    return beats, bpm, duration, sample_rate


def _librosa_grid(
    path: Path, min_bpm: float, max_bpm: float
) -> tuple[list[float], float, float, int]:
    try:
        import librosa  # type: ignore
    except ImportError as exc:
        raise BeatError(
            "librosa backend requested but librosa is not installed"
        ) from exc
    y, sample_rate = librosa.load(path, sr=None, mono=True)
    tempo, frames = librosa.beat.beat_track(
        y=y,
        sr=sample_rate,
        bpm=None,
        start_bpm=(min_bpm + max_bpm) / 2,
        tightness=100,
        sparse=True,
    )
    bpm = float(tempo[0] if hasattr(tempo, "__len__") else tempo)
    beats = [
        round(float(value), 6)
        for value in librosa.frames_to_time(frames, sr=sample_rate)
    ]
    return beats, bpm, len(y) / sample_rate, int(sample_rate)


def _aubio_grid(path: Path) -> tuple[list[float], float, float, int]:
    try:
        import aubio  # type: ignore
    except ImportError as exc:
        raise BeatError("aubio backend requested but aubio is not installed") from exc
    source = aubio.source(str(path), 0, 512)
    detector = aubio.tempo("default", 1024, 512, source.samplerate)
    beats: list[float] = []
    total = 0
    while True:
        samples, read = source()
        if detector(samples):
            beats.append(round(float(detector.get_last_s()), 6))
        total += read
        if read < source.hop_size:
            break
    if len(beats) < 2:
        raise BeatError("aubio did not find enough beats")
    periods = [right - left for left, right in zip(beats, beats[1:])]
    bpm = 60.0 / statistics.median(periods)
    return beats, bpm, total / source.samplerate, int(source.samplerate)


def build_manifest(
    path: Path,
    backend: str = "auto",
    min_bpm: float = 70.0,
    max_bpm: float = 180.0,
    bpm: float | None = None,
    offset: float = 0.0,
    duration: float | None = None,
    hop: int = 512,
) -> dict[str, Any]:
    path = path.resolve()
    if not path.is_file():
        raise BeatError(f"audio file not found: {path}")
    if min_bpm <= 0 or max_bpm <= min_bpm:
        raise BeatError("require 0 < min-bpm < max-bpm")
    if bpm is not None:
        if bpm <= 0 or duration is None or duration <= 0 or offset < 0:
            raise BeatError(
                "manual --bpm requires positive --duration and non-negative --offset"
            )
        period = 60.0 / bpm
        beats = []
        value = offset
        while value <= duration + 1e-9:
            beats.append(round(value, 6))
            value += period
        used_backend = "manual"
        sample_rate = None
        detected_duration = duration
        detected_bpm = bpm
    else:
        used_backend = (
            "stdlib" if backend == "auto" and path.suffix.lower() == ".wav" else backend
        )
        if used_backend == "auto":
            try:
                result = _librosa_grid(path, min_bpm, max_bpm)
                used_backend = "librosa"
            except BeatError:
                result = _aubio_grid(path)
                used_backend = "aubio"
        elif used_backend == "stdlib":
            result = _stdlib_grid(path, min_bpm, max_bpm, hop)
        elif used_backend == "librosa":
            result = _librosa_grid(path, min_bpm, max_bpm)
        elif used_backend == "aubio":
            result = _aubio_grid(path)
        else:
            raise BeatError("backend must be auto, stdlib, librosa, or aubio")
        beats, detected_bpm, detected_duration, sample_rate = result
    if not beats:
        raise BeatError("no beats detected")
    return {
        "schema_version": 1,
        "source": str(path),
        "source_sha256": _sha256(path),
        "backend": used_backend,
        "sample_rate": sample_rate,
        "duration": round(float(detected_duration), 6),
        "bpm": round(float(detected_bpm), 6),
        "beats": beats,
        "analysis": {
            "min_bpm": min_bpm,
            "max_bpm": max_bpm,
            "hop": hop if used_backend == "stdlib" else None,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract a deterministic music beat grid."
    )
    parser.add_argument("audio", type=Path)
    parser.add_argument("-o", "--output", type=Path)
    parser.add_argument(
        "--backend", choices=("auto", "stdlib", "librosa", "aubio"), default="auto"
    )
    parser.add_argument("--min-bpm", type=float, default=70.0)
    parser.add_argument("--max-bpm", type=float, default=180.0)
    parser.add_argument(
        "--bpm", type=float, help="skip detection and emit this manual BPM grid"
    )
    parser.add_argument("--offset", type=float, default=0.0)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--hop", type=int, default=512)
    args = parser.parse_args(argv)
    try:
        manifest = build_manifest(
            args.audio,
            backend=args.backend,
            min_bpm=args.min_bpm,
            max_bpm=args.max_bpm,
            bpm=args.bpm,
            offset=args.offset,
            duration=args.duration,
            hop=args.hop,
        )
    except BeatError as exc:
        print(f"beats: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
