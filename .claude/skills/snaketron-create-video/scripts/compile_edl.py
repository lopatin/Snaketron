#!/usr/bin/env python3
"""Validate and compile a SnakeTron EDL into deterministic render instructions."""

from __future__ import annotations

import argparse
import bisect
import json
import math
import re
import sys
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
EFFECTS = {
    "shake",
    "punch_in",
    "rgb_split",
    "glow",
    "grain",
    "vignette",
    "lut",
    "letterbox",
}
# Transition vocabulary. The smooth* and circle* families are eased rather
# than linear, which is what a cut between two paper-ground shots wants — a
# straight wipe reads mechanical. `fadewhite` replaces `fadeblack` on a light
# product: flashing to black punches a hole in the value polarity the brand
# gate enforces. Keep a trailer to two or three of these so the cut reads as
# one visual language (see references/motion.md).
TRANSITIONS = {
    "fadeblack": "fadeblack",
    "fadewhite": "fadewhite",
    "dissolve": "dissolve",
    "pixelize": "pixelize",
    "slices": "hlslice",
    "wipe_left": "wipeleft",
    "wipe_right": "wiperight",
    "wipe_up": "wipeup",
    "wipe_down": "wipedown",
    "smooth_left": "smoothleft",
    "smooth_right": "smoothright",
    "smooth_up": "smoothup",
    "smooth_down": "smoothdown",
    "circle_open": "circleopen",
    "circle_close": "circleclose",
    "zoom_in": "zoomin",
}
ANCHOR_RE = re.compile(
    r"^meta:(?P<name>[A-Za-z0-9_.:\-]+?)(?P<offset>[+-](?:\d+(?:\.\d*)?|\.\d+))?$"
)


class EdlError(ValueError):
    """A user-actionable EDL validation error."""


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EdlError(f"{field} must be a number")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        qualifier = "a positive finite number" if positive else "finite"
        raise EdlError(f"{field} must be {qualifier}")
    return result


def _integer(value: Any, field: str, *, positive: bool = False) -> int:
    result = _number(value, field, positive=positive)
    if not result.is_integer():
        raise EdlError(f"{field} must be an integer")
    return int(result)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise EdlError(f"file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise EdlError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise EdlError(f"{path} must contain a JSON object")
    return data


def _resolve_path(value: str, base: Path) -> Path:
    path = Path(value).expanduser()
    return (path if path.is_absolute() else base / path).resolve()


def _event_time(event: dict[str, Any]) -> float | None:
    for key in ("master_seconds", "time", "at", "t", "timestamp"):
        value = event.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


def _meta_anchors(meta: dict[str, Any]) -> dict[str, float]:
    anchors: dict[str, float] = {}
    for container_key in ("anchors", "markers"):
        container = meta.get(container_key)
        if isinstance(container, dict):
            for name, value in container.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    anchors.setdefault(str(name), float(value))

    for container_key in ("events", "timeline", "cues", "cue_track"):
        container = meta.get(container_key)
        if isinstance(container, dict):
            for name, value in container.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    anchors.setdefault(str(name), float(value))
                elif isinstance(value, dict):
                    timestamp = _event_time(value)
                    if timestamp is not None:
                        anchors.setdefault(str(name), timestamp)
        elif isinstance(container, list):
            counts: dict[str, int] = {}
            for event in container:
                if not isinstance(event, dict):
                    continue
                name = next(
                    (
                        str(event[key])
                        for key in ("id", "name", "key", "cue", "type")
                        if event.get(key) is not None
                    ),
                    None,
                )
                timestamp = _event_time(event)
                if name is None or timestamp is None:
                    continue
                index = counts.get(name, 0)
                counts[name] = index + 1
                anchors.setdefault(name, timestamp)
                anchors[f"{name}-{index}"] = timestamp
    return anchors


def resolve_source_time(value: Any, anchors: dict[str, float], field: str) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _number(value, field)
    if not isinstance(value, str):
        raise EdlError(f"{field} must be source seconds or a meta: anchor")
    match = ANCHOR_RE.fullmatch(value.strip())
    if not match:
        raise EdlError(f"{field} has invalid anchor {value!r}")
    name = match.group("name")
    if name not in anchors:
        available = ", ".join(sorted(anchors)) or "none"
        raise EdlError(
            f"{field} references missing meta anchor {name!r}; available: {available}"
        )
    offset = float(match.group("offset") or 0.0)
    return anchors[name] + offset


def _speed_map(
    entry: dict[str, Any],
    source_in: float,
    source_out: float,
    anchors: dict[str, float],
    field: str,
) -> list[dict[str, float]]:
    raw = entry.get("speed")
    if raw is None:
        raw = [{"rate": 1.0}]
    if not isinstance(raw, list) or not raw:
        raise EdlError(f"{field}.speed must be a non-empty array")

    intervals: list[dict[str, float]] = []
    cursor = source_in
    output_cursor = 0.0
    for index, speed in enumerate(raw):
        item_field = f"{field}.speed[{index}]"
        if not isinstance(speed, dict):
            raise EdlError(f"{item_field} must be an object")
        rate = _number(speed.get("rate"), f"{item_field}.rate", positive=True)
        if "until" in speed:
            end = resolve_source_time(speed["until"], anchors, f"{item_field}.until")
        else:
            if index != len(raw) - 1:
                raise EdlError(f"only the final {field}.speed entry may omit until")
            end = source_out
        if end <= cursor + 1e-9 or end > source_out + 1e-9:
            raise EdlError(
                f"{item_field}.until must increase and stay within [{source_in:g}, {source_out:g}]"
            )
        end = min(end, source_out)
        output_end = output_cursor + (end - cursor) / rate
        intervals.append(
            {
                "source_start": cursor,
                "source_end": end,
                "rate": rate,
                "output_start": output_cursor,
                "output_end": output_end,
            }
        )
        cursor = end
        output_cursor = output_end
    if abs(cursor - source_out) > 1e-6:
        raise EdlError(f"{field}.speed does not cover the full source window")
    return intervals


def map_source_to_output(source_time: float, speed: list[dict[str, float]]) -> float:
    if source_time < speed[0]["source_start"] - 1e-7:
        raise EdlError(f"source time {source_time:g} is before clip in point")
    if source_time > speed[-1]["source_end"] + 1e-7:
        raise EdlError(f"source time {source_time:g} is after clip out point")
    for interval in speed:
        if source_time <= interval["source_end"] + 1e-7:
            clamped = min(
                max(source_time, interval["source_start"]), interval["source_end"]
            )
            return (
                interval["output_start"]
                + (clamped - interval["source_start"]) / interval["rate"]
            )
    return speed[-1]["output_end"]


def _effect_filters(
    effects: list[dict[str, Any]], width: int, height: int
) -> list[str]:
    filters: list[str] = []
    for effect in effects:
        kind = effect["t"]
        start = effect["at_local"]
        end = start + effect["dur_output"]
        enabled = f"between(t\\,{start:.6f}\\,{end:.6f})"
        if kind == "shake":
            amp = float(effect.get("amp", 12))
            decay = float(effect.get("decay", 3))
            # Overscan first; crop expressions settle exactly at the centered crop.
            filters.extend(
                [
                    f"scale={width + 2 * math.ceil(amp)}:{height + 2 * math.ceil(amp)}",
                    (
                        f"crop={width}:{height}:"
                        f"x='{amp:.3f}+if({enabled},{amp:.3f}*sin(79*t)*exp(-{decay:.3f}*(t-{start:.6f})),0)':"
                        f"y='{amp:.3f}+if({enabled},{amp:.3f}*sin(113*t+1.7)*exp(-{decay:.3f}*(t-{start:.6f})),0)'"
                    ),
                ]
            )
        elif kind == "punch_in":
            zoom = float(effect.get("zoom", 1.2))
            filters.extend(
                [
                    (
                        "scale=w='ceil(iw*(1+"
                        f"{zoom - 1:.6f}*if({enabled},1,0))/2)*2':"
                        "h='ceil(ih*(1+"
                        f"{zoom - 1:.6f}*if({enabled},1,0))/2)*2':eval=frame"
                    ),
                    f"crop={width}:{height}:x='(iw-ow)/2':y='(ih-oh)/2'",
                ]
            )
        elif kind == "rgb_split":
            shift = int(effect.get("px", 6))
            filters.append(f"rgbashift=rh={shift}:bh={-shift}:enable='{enabled}'")
        elif kind == "glow":
            amount = float(effect.get("amount", 1.0))
            filters.append(f"unsharp=5:5:{amount:.3f}:5:5:0:enable='{enabled}'")
        elif kind == "grain":
            strength = int(effect.get("strength", 8))
            filters.append(f"noise=alls={strength}:allf=t:enable='{enabled}'")
        elif kind == "vignette":
            filters.append(f"vignette=angle=PI/5:enable='{enabled}'")
        elif kind == "lut":
            filters.append(f"lut3d=file='{effect['src_resolved']}'")
        elif kind == "letterbox":
            size = int(effect.get("size", round(height * 0.07)))
            filters.extend(
                [
                    f"drawbox=x=0:y=0:w=iw:h={size}:color=black:t=fill:enable='{enabled}'",
                    f"drawbox=x=0:y=ih-{size}:w=iw:h={size}:color=black:t=fill:enable='{enabled}'",
                ]
            )
    return filters


def _compile_clip(
    entry: dict[str, Any],
    index: int,
    edl_dir: Path,
    clips_dir: Path,
    output: dict[str, int],
) -> dict[str, Any]:
    slug = entry.get("clip")
    if not isinstance(slug, str) or not slug:
        raise EdlError(f"timeline[{index}].clip must be a non-empty string")
    clip_dir = clips_dir / slug
    master = _resolve_path(str(entry.get("src", clip_dir / "master.mkv")), edl_dir)
    meta_path = _resolve_path(str(entry.get("meta", clip_dir / "meta.json")), edl_dir)
    meta = _load_json(meta_path)
    anchors = _meta_anchors(meta)
    field = f"timeline[{index}]"
    source_in = resolve_source_time(entry.get("in", 0), anchors, f"{field}.in")
    source_out_default = meta.get("duration", meta.get("duration_seconds"))
    if "out" not in entry and source_out_default is None:
        raise EdlError(f"{field}.out is required when meta.json has no duration")
    source_out = resolve_source_time(
        entry.get("out", source_out_default), anchors, f"{field}.out"
    )
    if source_in < 0 or source_out <= source_in:
        raise EdlError(f"{field} must satisfy 0 <= in < out")
    if source_out_default is not None and source_out > float(source_out_default) + 1e-6:
        raise EdlError(f"{field}.out exceeds the master duration")

    speed = _speed_map(entry, source_in, source_out, anchors, field)
    capture_vfps = _number(
        meta.get("capture_vfps"), f"{meta_path}.capture_vfps", positive=True
    )
    required_vfps = math.ceil(output["fps"] / min(item["rate"] for item in speed))
    if min(item["rate"] for item in speed) * capture_vfps + 1e-7 < output["fps"]:
        raise EdlError(
            f"{field} synthesizes slow motion: capture_vfps={capture_vfps:g}, "
            f"minimum rate={min(item['rate'] for item in speed):g}, output fps={output['fps']}; "
            f"recapture at >= {required_vfps} vfps"
        )

    effects: list[dict[str, Any]] = []
    raw_effects = entry.get("effects", [])
    if not isinstance(raw_effects, list):
        raise EdlError(f"{field}.effects must be an array")
    for effect_index, raw in enumerate(raw_effects):
        effect_field = f"{field}.effects[{effect_index}]"
        if not isinstance(raw, dict) or raw.get("t") not in EFFECTS:
            raise EdlError(
                f"{effect_field}.t must be one of {', '.join(sorted(EFFECTS))}"
            )
        effect = dict(raw)
        at_source = resolve_source_time(
            raw.get("at", source_in), anchors, f"{effect_field}.at"
        )
        duration_source = _number(
            raw.get("dur", 0.35), f"{effect_field}.dur", positive=True
        )
        effect_end_source = min(at_source + duration_source, source_out)
        if at_source < source_in or at_source >= source_out:
            raise EdlError(f"{effect_field}.at must fall inside the clip source window")
        effect["at_source"] = at_source
        effect["at_local"] = map_source_to_output(at_source, speed)
        effect["dur_output"] = max(
            1 / output["fps"],
            map_source_to_output(effect_end_source, speed) - effect["at_local"],
        )
        if effect["t"] == "lut":
            src = effect.get("src")
            if not isinstance(src, str):
                raise EdlError(f"{effect_field}.src is required for a LUT")
            effect["src_resolved"] = str(_resolve_path(src, edl_dir))
        effects.append(effect)

    # A shot may carry several captions ("BOOST!" then "COMBOS!"), each with
    # its own anchor and dwell. `text` stays supported as the single-caption
    # spelling; both compile to the same `texts` list.
    raw_texts = entry.get("texts")
    if raw_texts is None:
        raw_texts = [entry["text"]] if "text" in entry else []
    if not isinstance(raw_texts, list):
        raise EdlError(f"{field}.texts must be an array")

    texts: list[dict[str, Any]] = []
    for text_index, raw_text in enumerate(raw_texts):
        label = f"{field}.texts[{text_index}]"
        if not isinstance(raw_text, dict) or not isinstance(raw_text.get("value"), str):
            raise EdlError(f"{label}.value must be a string")
        item = dict(raw_text)
        text_at_source = resolve_source_time(
            raw_text.get("at", source_in), anchors, f"{label}.at"
        )
        duration_source = _number(
            raw_text.get("dur", min(1.5, source_out - text_at_source)),
            f"{label}.dur",
            positive=True,
        )
        item["at_source"] = text_at_source
        item["at_local"] = map_source_to_output(text_at_source, speed)
        item["dur_output"] = max(
            1 / output["fps"],
            map_source_to_output(
                min(text_at_source + duration_source, source_out), speed
            )
            - item["at_local"],
        )
        texts.append(item)
    text = texts[0] if texts else None

    sfx: list[dict[str, Any]] = []
    raw_sfx = entry.get("sfx", [])
    if not isinstance(raw_sfx, list):
        raise EdlError(f"{field}.sfx must be an array")
    for sfx_index, raw in enumerate(raw_sfx):
        sfx_field = f"{field}.sfx[{sfx_index}]"
        if not isinstance(raw, dict) or not isinstance(raw.get("src"), str):
            raise EdlError(f"{sfx_field}.src must be a path")
        item = dict(raw)
        at_source = resolve_source_time(
            raw.get("at", source_in), anchors, f"{sfx_field}.at"
        )
        item["src_resolved"] = str(_resolve_path(raw["src"], edl_dir))
        item["at_source"] = at_source
        item["at_local"] = map_source_to_output(at_source, speed)
        sfx.append(item)

    duration = speed[-1]["output_end"]
    segment = {
        "index": index,
        "kind": "clip",
        "id": str(entry.get("id", slug)),
        "slug": slug,
        "master": str(master),
        "meta": str(meta_path),
        "capture_vfps": capture_vfps,
        "encoded_fps": meta.get("encoded_fps", meta.get("fps")),
        "source_in": source_in,
        "source_out": source_out,
        "speed": speed,
        "base_output_duration": duration,
        "output_duration": duration,
        "tail_adjust": 0.0,
        "effects": effects,
        "text": text,
        "texts": texts,
        "sfx": sfx,
    }
    segment["filters"] = {"video": _effect_filters(effects, output["w"], output["h"])}
    return segment


def _compile_title(entry: dict[str, Any], index: int) -> dict[str, Any]:
    raw = entry.get("title")
    field = f"timeline[{index}].title"
    if not isinstance(raw, dict) or not isinstance(raw.get("text"), str):
        raise EdlError(f"{field}.text must be a string")
    duration = _number(raw.get("duration"), f"{field}.duration", positive=True)
    return {
        "index": index,
        "kind": "title",
        "id": str(entry.get("id", f"title-{index}")),
        "title": raw,
        "base_output_duration": duration,
        "output_duration": duration,
        "tail_adjust": 0.0,
        "effects": [],
        "sfx": [],
        "filters": {"video": []},
    }


def _load_beats(music: dict[str, Any], edl_dir: Path) -> list[float]:
    manifest_value = music.get("manifest") or music.get("beats_manifest")
    if manifest_value is None:
        return []
    manifest = _load_json(_resolve_path(str(manifest_value), edl_dir))
    beats = manifest.get("beats", [])
    if not isinstance(beats, list):
        raise EdlError("music beat manifest beats must be an array")
    result = sorted({_number(beat, "music manifest beat") for beat in beats})
    return [beat for beat in result if beat >= 0]


def _nearest_beat(beats: list[float], value: float, limit: float) -> float | None:
    if not beats:
        return None
    index = bisect.bisect_left(beats, value)
    choices = beats[max(0, index - 1) : min(len(beats), index + 2)]
    best = min(choices, key=lambda beat: (abs(beat - value), beat))
    return best if abs(best - value) <= limit + 1e-9 else None


def compile_edl(edl_path: Path, clips_dir: Path | None = None) -> dict[str, Any]:
    edl_path = edl_path.resolve()
    edl = _load_json(edl_path)
    edl_dir = edl_path.parent
    raw_output = edl.get("output")
    if not isinstance(raw_output, dict):
        raise EdlError("output must be an object")
    output = {
        "w": _integer(raw_output.get("w"), "output.w", positive=True),
        "h": _integer(raw_output.get("h"), "output.h", positive=True),
        "fps": _integer(raw_output.get("fps"), "output.fps", positive=True),
    }
    resolved_clips = (clips_dir or edl_dir / "clips").resolve()
    timeline = edl.get("timeline")
    if not isinstance(timeline, list) or not timeline:
        raise EdlError("timeline must be a non-empty array")

    segments: list[dict[str, Any]] = []
    transitions_by_next: dict[int, dict[str, Any]] = {}
    pending_transition: dict[str, Any] | None = None
    for index, entry in enumerate(timeline):
        if not isinstance(entry, dict):
            raise EdlError(f"timeline[{index}] must be an object")
        kinds = [key for key in ("clip", "title", "transition") if key in entry]
        if len(kinds) != 1:
            raise EdlError(
                f"timeline[{index}] must contain exactly one of clip, title, transition"
            )
        if kinds[0] == "transition":
            if not segments or pending_transition is not None:
                raise EdlError(
                    f"timeline[{index}] transition must sit between two visual entries"
                )
            kind = entry["transition"]
            if kind not in TRANSITIONS:
                raise EdlError(
                    f"timeline[{index}].transition must be one of {', '.join(sorted(TRANSITIONS))}"
                )
            pending_transition = {
                "timeline_index": index,
                "kind": kind,
                "ffmpeg": TRANSITIONS[kind],
                "duration": _number(
                    entry.get("duration"), f"timeline[{index}].duration", positive=True
                ),
            }
            continue
        segment = (
            _compile_clip(entry, index, edl_dir, resolved_clips, output)
            if kinds[0] == "clip"
            else _compile_title(entry, index)
        )
        if pending_transition is not None:
            transitions_by_next[len(segments)] = pending_transition
            pending_transition = None
        segments.append(segment)
    if pending_transition is not None:
        raise EdlError("timeline cannot end with a transition")

    raw_music = edl.get("music")
    music: dict[str, Any] | None = None
    beats: list[float] = []
    if raw_music is not None:
        if not isinstance(raw_music, dict) or not isinstance(raw_music.get("src"), str):
            raise EdlError("music.src must be a path")
        music = dict(raw_music)
        music["src_resolved"] = str(_resolve_path(raw_music["src"], edl_dir))
        if raw_music.get("beat_snap"):
            beats = _load_beats(raw_music, edl_dir)
            if not beats:
                raise EdlError("music.beat_snap requires a non-empty beat manifest")

    transitions: list[dict[str, Any]] = []
    cuts: list[dict[str, Any]] = []
    cursor = 0.0
    max_snap = _number(
        (music or {}).get("beat_snap_max_s", 0.05),
        "music.beat_snap_max_s",
        positive=True,
    )
    for index, segment in enumerate(segments):
        transition = transitions_by_next.get(index)
        duration = transition["duration"] if transition else 0.0
        if index == 0:
            start = 0.0
        else:
            previous = segments[index - 1]
            if duration >= min(previous["output_duration"], segment["output_duration"]):
                raise EdlError(
                    f"transition before {segment['id']} is longer than an adjacent segment"
                )
            start = cursor - duration
            snap_delta = 0.0
            snapped_to = None
            if music and music.get("beat_snap"):
                snapped = _nearest_beat(beats, start, max_snap)
                if snapped is None:
                    raise EdlError(
                        f"cut before {segment['id']} at {start:.6f}s has no beat within "
                        f"{max_snap:.3f}s; adjust the EDL cut or beat grid"
                    )
                delta = snapped - start
                snap_delta = delta
                snapped_to = snapped
                previous["tail_adjust"] += delta
                previous["output_duration"] += delta
                previous["global_end"] += delta
                cursor += delta
                start = snapped
                if transition is not None:
                    transition = dict(transition)
                    transition["beat_snap_delta"] = delta
                    transition["snapped_to"] = snapped
            cuts.append(
                {
                    "from_segment": index - 1,
                    "to_segment": index,
                    "offset": start,
                    "transition": transition is not None,
                    "beat_snap_delta": snap_delta,
                    "snapped_to": snapped_to,
                }
            )
            if transition is not None:
                compiled_transition = dict(transition)
                compiled_transition.update(
                    {
                        "from_segment": index - 1,
                        "to_segment": index,
                        "offset": start,
                    }
                )
                transitions.append(compiled_transition)
        segment["global_start"] = start
        segment["global_end"] = start + segment["output_duration"]
        for effect in segment["effects"]:
            effect["at_global"] = start + effect["at_local"]
        for item in segment.get("texts", []):
            item["at_global"] = start + item["at_local"]
        for sfx in segment["sfx"]:
            sfx["at_global"] = start + sfx["at_local"]
        cursor = segment["global_end"]

    compiled = {
        "schema_version": SCHEMA_VERSION,
        "source_edl": str(edl_path),
        "output": output,
        "music": music,
        "segments": segments,
        "transitions": transitions,
        "cuts": cuts,
        "duration": cursor,
        "qc": {
            "beat_snap_tolerance_s": max_snap,
            "all_cut_offsets_within_tolerance": all(
                abs(item.get("beat_snap_delta", 0.0)) <= max_snap + 1e-9
                for item in cuts
            ),
        },
    }
    return compiled


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate a SnakeTron EDL and resolve source-master timing."
    )
    parser.add_argument("edl", type=Path, help="EDL JSON path")
    parser.add_argument("--clips-dir", type=Path, help="clip library root")
    parser.add_argument(
        "-o", "--output", type=Path, help="compiled JSON path (default: stdout)"
    )
    args = parser.parse_args(argv)
    try:
        compiled = compile_edl(args.edl, args.clips_dir)
    except EdlError as exc:
        print(f"compile_edl: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(compiled, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
