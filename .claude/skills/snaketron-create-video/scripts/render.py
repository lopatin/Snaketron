#!/usr/bin/env python3
"""Render a compiled SnakeTron EDL with CFR segment caching and tagged BT.709 output."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


RENDERER_VERSION = 1
SKILL_DIR = Path(__file__).resolve().parent.parent
DEFAULT_FONT = SKILL_DIR / "assets" / "fonts" / "BarlowCondensed-ExtraBoldItalic.ttf"


class RenderError(RuntimeError):
    pass


def _load(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RenderError(f"cannot read compiled EDL {path}: {exc}") from exc
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise RenderError("compiled EDL must be a schema_version 1 object")
    if not isinstance(data.get("segments"), list) or not data["segments"]:
        raise RenderError("compiled EDL has no segments")
    return data


def _run(command: list[str], dry_run: bool, commands: list[list[str]]) -> None:
    commands.append(command)
    if dry_run:
        return
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RenderError(f"ffmpeg failed ({result.returncode}): {shlex.join(command)}")


def _escape_drawtext(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace(":", "\\:")
        .replace("%", "\\%")
        .replace("\n", "\\n")
    )


def _drawtext(
    text: str,
    font: Path,
    size: int,
    color: str,
    x: str = "(w-text_w)/2",
    y: str = "(h-text_h)/2",
    enable: str | None = None,
    expand: bool = False,
) -> str:
    rendered_text = _escape_drawtext(text)
    if expand:
        rendered_text = (
            text.replace("\\", "\\\\").replace("'", "\\'").replace(":", "\\:")
        )
    result = (
        f"drawtext=fontfile='{font}':text='{rendered_text}':"
        f"fontsize={size}:fontcolor={color}:borderw=2:bordercolor=#3F3F41:"
        f"shadowx=8:shadowy=8:shadowcolor=black@0.8:x='{x}':y='{y}'"
    )
    if enable:
        result += f":enable='{enable}'"
    return result


def _path_signature(path_value: str | None) -> dict[str, Any] | None:
    if not path_value:
        return None
    path = Path(path_value)
    try:
        stat = path.stat()
    except FileNotFoundError as exc:
        raise RenderError(f"input file not found: {path}") from exc
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _cache_key(
    segment: dict[str, Any], output: dict[str, Any], font: Path, text_mode: str
) -> str:
    identity = {
        "renderer_version": RENDERER_VERSION,
        "segment": segment,
        "output": output,
        "master": _path_signature(segment.get("master")),
        "font": _path_signature(str(font)),
        "text_mode": text_mode,
    }
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _tail_filter(segment: dict[str, Any]) -> str | None:
    adjustment = float(segment.get("tail_adjust", 0.0))
    if adjustment > 1e-7:
        return f"tpad=stop_mode=clone:stop_duration={adjustment:.6f}"
    if adjustment < -1e-7:
        return (
            f"trim=duration={float(segment['output_duration']):.6f},setpts=PTS-STARTPTS"
        )
    return None


def _segment_filter(
    segment: dict[str, Any],
    output: dict[str, int],
    font: Path,
    *,
    use_drawtext: bool,
    raster_callout_input: int | None = None,
) -> tuple[list[str], str]:
    width, height, fps = output["w"], output["h"], output["fps"]
    filters: list[str] = []
    if segment["kind"] == "clip":
        labels: list[str] = []
        for index, interval in enumerate(segment["speed"]):
            label = f"speed{index}"
            filters.append(
                f"[0:v]trim=start={interval['source_start']:.6f}:end={interval['source_end']:.6f},"
                f"setpts=(PTS-STARTPTS)/{interval['rate']:.9f},fps={fps},settb=AVTB[{label}]"
            )
            labels.append(f"[{label}]")
        if len(labels) == 1:
            chain_input = labels[0]
        else:
            filters.append(f"{''.join(labels)}concat=n={len(labels)}:v=1:a=0[speed]")
            chain_input = "[speed]"
        chain = [
            f"scale={width}:{height}:force_original_aspect_ratio=decrease:flags=lanczos",
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=#3F3F41",
            "setsar=1",
        ]
        chain.extend(segment.get("filters", {}).get("video", []))
        text = segment.get("text")
        if text and use_drawtext:
            start = float(text["at_local"])
            end = start + float(text["dur_output"])
            style = text.get("style", "impact")
            color = "#F8C84A" if style == "impact" else "white"
            chain.append(
                _drawtext(
                    text["value"].upper(),
                    font,
                    max(48, round(height * 0.085)),
                    color,
                    y="h*0.74-text_h/2",
                    enable=f"between(t\\,{start:.6f}\\,{end:.6f})",
                )
            )
        if text and not use_drawtext:
            if raster_callout_input is None:
                raise RenderError("raster text overlay input is missing")
            filters.append(f"{chain_input}{','.join(chain)}[pretext]")
            start = float(text["at_local"])
            end = start + float(text["dur_output"])
            filters.append(
                f"[{raster_callout_input}:v]format=rgba[callout];"
                f"[pretext][callout]overlay=0:0:enable='between(t,{start:.6f},{end:.6f})'[texted]"
            )
            chain_input = "[texted]"
            chain = []
        tail = _tail_filter(segment)
        if tail:
            chain.append(tail)
        chain.extend([f"fps={fps}", "settb=AVTB", "setpts=PTS-STARTPTS"])
        filters.append(f"{chain_input}{','.join(chain)}[vout]")
    elif segment["kind"] == "title":
        title = segment["title"]
        style = title.get("style", "logo")
        primary = "#FFFFFF" if style == "logo" else "#F8C84A"
        chain = ["format=gbrp"]
        if use_drawtext:
            chain.extend(
                [
                    "drawbox=x=w*0.12:y=h*0.30:w=w*0.76:h=4:color=#3B82F6:t=fill",
                    "drawbox=x=w*0.18:y=h*0.70:w=w*0.64:h=4:color=#EF4444:t=fill",
                    _drawtext(
                        title["text"].upper(),
                        font,
                        max(72, round(height * 0.17)),
                        primary,
                    ),
                ]
            )
        subtitle = title.get("subtitle")
        if use_drawtext and isinstance(subtitle, str) and subtitle:
            chain.append(
                _drawtext(
                    subtitle.upper(),
                    font,
                    max(30, round(height * 0.045)),
                    "#F8C84A",
                    y="h*0.68",
                )
            )
        tail = _tail_filter(segment)
        if tail:
            chain.append(tail)
        chain.extend([f"fps={fps}", "settb=AVTB", "setpts=PTS-STARTPTS"])
        filters.append(f"[0:v]{','.join(chain)}[vout]")
    else:
        raise RenderError(f"unsupported segment kind {segment.get('kind')!r}")
    return filters, "[vout]"


def _ffmpeg_has_filter(ffmpeg: str, name: str) -> bool:
    result = subprocess.run(
        [ffmpeg, "-hide_banner", "-filters"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.returncode == 0 and any(
        line.split()[1:2] == [name]
        for line in result.stdout.splitlines()
        if line.strip()
    )


def _rasterize_text(
    segment: dict[str, Any], output: dict[str, int], font: Path, target: Path
) -> Path:
    try:
        from PIL import Image, ImageDraw, ImageFont  # type: ignore
    except ImportError as exc:
        raise RenderError(
            "ffmpeg lacks drawtext and Pillow is unavailable; install a drawtext-enabled ffmpeg or Pillow"
        ) from exc
    width, height = output["w"], output["h"]
    if segment["kind"] == "title":
        background = segment["title"].get("background", "#3F3F41")
        image = Image.new("RGB", (width, height), background)
    else:
        image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    def centered(text: str, size: int, y: int, color: str) -> None:
        current_size = size
        while current_size > 24:
            face = ImageFont.truetype(str(font), current_size)
            box = draw.textbbox((0, 0), text, font=face, stroke_width=2)
            if box[2] - box[0] <= width * 0.84:
                break
            current_size -= 4
        text_width = box[2] - box[0]
        x = round((width - text_width) / 2)
        draw.text(
            (x + 8, y + 8),
            text,
            font=face,
            fill="#101014" if image.mode == "RGB" else (16, 16, 20, 210),
            stroke_width=2,
            stroke_fill="#101014",
        )
        draw.text(
            (x, y),
            text,
            font=face,
            fill=color,
            stroke_width=2,
            stroke_fill="#3F3F41",
        )

    if segment["kind"] == "title":
        draw.rectangle(
            (
                round(width * 0.12),
                round(height * 0.30),
                round(width * 0.88),
                round(height * 0.30) + 4,
            ),
            fill="#3B82F6",
        )
        draw.rectangle(
            (
                round(width * 0.18),
                round(height * 0.70),
                round(width * 0.82),
                round(height * 0.70) + 4,
            ),
            fill="#EF4444",
        )
        style = segment["title"].get("style", "logo")
        centered(
            segment["title"]["text"].upper(),
            max(72, round(height * 0.17)),
            round(height * 0.40),
            "#FFFFFF" if style == "logo" else "#F8C84A",
        )
        subtitle = segment["title"].get("subtitle")
        if isinstance(subtitle, str) and subtitle:
            centered(
                subtitle.upper(),
                max(30, round(height * 0.045)),
                round(height * 0.65),
                "#F8C84A",
            )
    else:
        centered(
            segment["text"]["value"].upper(),
            max(48, round(height * 0.085)),
            round(height * 0.70),
            "#F8C84A" if segment["text"].get("style") == "impact" else "#FFFFFF",
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    image.save(target)
    return target


def _render_segment(
    ffmpeg: str,
    segment: dict[str, Any],
    output: dict[str, int],
    target: Path,
    font: Path,
    dry_run: bool,
    commands: list[list[str]],
    use_drawtext: bool,
) -> None:
    duration = float(segment["output_duration"])
    command = [ffmpeg, "-hide_banner", "-loglevel", "error", "-y"]
    raster_path = target.with_suffix(".text.png")
    has_raster_callout = False
    if (
        not dry_run
        and not use_drawtext
        and (segment["kind"] == "title" or segment.get("text"))
    ):
        _rasterize_text(segment, output, font, raster_path)
    if segment["kind"] == "clip":
        command += ["-i", segment["master"]]
        if segment.get("text") and not use_drawtext:
            command += [
                "-loop",
                "1",
                "-framerate",
                str(output["fps"]),
                "-i",
                str(raster_path),
            ]
            has_raster_callout = True
    else:
        if use_drawtext:
            color = segment["title"].get("background", "#3F3F41")
            command += [
                "-f",
                "lavfi",
                "-i",
                f"color=c={color}:s={output['w']}x{output['h']}:r={output['fps']}:d={duration:.6f}",
            ]
        else:
            command += [
                "-loop",
                "1",
                "-framerate",
                str(output["fps"]),
                "-i",
                str(raster_path),
            ]
    audio_input = 2 if has_raster_callout else 1
    command += [
        "-f",
        "lavfi",
        "-t",
        f"{duration:.6f}",
        "-i",
        "anullsrc=r=48000:cl=stereo",
    ]
    filters, video_label = _segment_filter(
        segment,
        output,
        font,
        use_drawtext=use_drawtext,
        raster_callout_input=1 if has_raster_callout else None,
    )
    command += [
        "-filter_complex",
        ";".join(filters),
        "-map",
        video_label,
        "-map",
        f"{audio_input}:a",
        "-t",
        f"{duration:.6f}",
        "-r",
        str(output["fps"]),
        "-c:v",
        "ffv1",
        "-level",
        "3",
        "-pix_fmt",
        "gbrp",
        "-c:a",
        "pcm_s16le",
        str(target),
    ]
    target.parent.mkdir(parents=True, exist_ok=True)
    _run(command, dry_run, commands)


def _input_audio_filter(index: int, label: str) -> str:
    return f"[{index}:a]aresample=48000,aformat=sample_fmts=fltp:channel_layouts=stereo,asetpts=PTS-STARTPTS[{label}]"


def _assemble(
    ffmpeg: str,
    compiled: dict[str, Any],
    cached: list[Path],
    destination: Path,
    profile: str,
    font: Path,
    dry_run: bool,
    commands: list[list[str]],
    use_drawtext: bool,
) -> None:
    command = [ffmpeg, "-hide_banner", "-loglevel", "error", "-y"]
    for path in cached:
        command += ["-i", str(path)]

    sfx_items = [
        item for segment in compiled["segments"] for item in segment.get("sfx", [])
    ]
    sfx_input_indexes: list[int] = []
    for item in sfx_items:
        sfx_input_indexes.append(
            len(command)
        )  # placeholder replaced below; index is tracked separately
        command += ["-i", item["src_resolved"]]
    segment_count = len(cached)
    sfx_input_indexes = list(range(segment_count, segment_count + len(sfx_items)))

    music = compiled.get("music")
    music_index = None
    if music:
        music_index = segment_count + len(sfx_items)
        command += ["-stream_loop", "-1", "-i", music["src_resolved"]]

    filters: list[str] = []
    for index in range(segment_count):
        filters.append(
            f"[{index}:v]fps={compiled['output']['fps']},settb=AVTB,setpts=PTS-STARTPTS[v{index}]"
        )
        filters.append(_input_audio_filter(index, f"a{index}"))

    transitions = {
        int(item["to_segment"]): item for item in compiled.get("transitions", [])
    }
    current_v = "v0"
    current_a = "a0"
    for index in range(1, segment_count):
        next_v, next_a = f"v{index}", f"a{index}"
        out_v, out_a = f"joinv{index}", f"joina{index}"
        transition = transitions.get(index)
        if transition:
            duration = float(transition["duration"])
            offset = float(transition["offset"])
            filters.append(
                f"[{current_v}][{next_v}]xfade=transition={transition['ffmpeg']}:"
                f"duration={duration:.6f}:offset={offset:.6f}[{out_v}]"
            )
            filters.append(
                f"[{current_a}][{next_a}]acrossfade=d={duration:.6f}:c1=tri:c2=tri[{out_a}]"
            )
        else:
            filters.append(f"[{current_v}][{next_v}]concat=n=2:v=1:a=0[{out_v}]")
            filters.append(f"[{current_a}][{next_a}]concat=n=2:v=0:a=1[{out_a}]")
        current_v, current_a = out_v, out_a

    sfx_labels: list[str] = []
    for index, (item, input_index) in enumerate(zip(sfx_items, sfx_input_indexes)):
        delay = max(0, round(float(item["at_global"]) * 1000))
        volume = float(item.get("volume", 1.0))
        label = f"sfx{index}"
        filters.append(
            f"[{input_index}:a]aresample=48000,aformat=sample_fmts=fltp:channel_layouts=stereo,"
            f"volume={volume:.6f},adelay=delays={delay}:all=1[{label}]"
        )
        sfx_labels.append(label)
    sfx_bus = None
    if sfx_labels:
        sfx_bus = "sfxbus"
        filters.append(
            f"{''.join(f'[{label}]' for label in sfx_labels)}amix=inputs={len(sfx_labels)}:"
            f"duration=longest:normalize=0[{sfx_bus}]"
        )

    mix_labels = [current_a]
    if music_index is not None:
        filters.append(
            f"[{music_index}:a]aresample=48000,aformat=sample_fmts=fltp:channel_layouts=stereo,"
            f"atrim=duration={float(compiled['duration']):.6f},asetpts=PTS-STARTPTS,volume={float(music.get('volume', 0.7)):.6f}[music]"
        )
        if sfx_bus and music.get("duck_under", "sfx") == "sfx":
            filters.append(
                f"[music][{sfx_bus}]sidechaincompress=threshold=0.025:ratio=8:attack=5:release=250[ducked]"
            )
            mix_labels += ["ducked", sfx_bus]
        else:
            mix_labels.append("music")
            if sfx_bus:
                mix_labels.append(sfx_bus)
    elif sfx_bus:
        mix_labels.append(sfx_bus)
    filters.append(
        f"{''.join(f'[{label}]' for label in mix_labels)}amix=inputs={len(mix_labels)}:"
        "duration=longest:normalize=0,loudnorm=I=-14:TP=-1.5:LRA=11,aresample=48000[aout]"
    )

    if profile == "preview":
        width, height, crf, preset = 640, 360, 28, "veryfast"
        video_filters = [
            f"scale={width}:{height}:flags=lanczos:out_color_matrix=bt709",
            "format=yuv420p",
        ]
        if use_drawtext:
            video_filters.append(
                _drawtext(
                    "%{pts:hms}",
                    font,
                    22,
                    "white",
                    x="20",
                    y="20",
                    expand=True,
                )
            )
    else:
        width, height = compiled["output"]["w"], compiled["output"]["h"]
        crf, preset = 18, "slow"
        video_filters = [
            f"scale={width}:{height}:flags=lanczos:out_color_matrix=bt709",
            "format=yuv420p",
        ]
    filters.append(f"[{current_v}]{','.join(video_filters)}[vout]")

    destination.parent.mkdir(parents=True, exist_ok=True)
    command += [
        "-filter_complex",
        ";".join(filters),
        "-map",
        "[vout]",
        "-map",
        "[aout]",
        "-t",
        f"{float(compiled['duration']):.6f}",
        "-r",
        str(compiled["output"]["fps"]),
        "-c:v",
        "libx264",
        "-x264-params",
        "colorprim=bt709:transfer=bt709:colormatrix=bt709",
        "-crf",
        str(crf),
        "-preset",
        preset,
        "-pix_fmt",
        "yuv420p",
        "-color_primaries",
        "bt709",
        "-color_trc",
        "bt709",
        "-colorspace",
        "bt709",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        str(destination),
    ]
    _run(command, dry_run, commands)


def render(
    compiled_path: Path,
    destination: Path,
    profile: str,
    cache_dir: Path,
    ffmpeg: str,
    font: Path,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    compiled = _load(compiled_path)
    if not shutil.which(ffmpeg):
        raise RenderError(f"ffmpeg executable not found: {ffmpeg}")
    if not font.is_file():
        raise RenderError(f"brand font not found: {font}")
    use_drawtext = _ffmpeg_has_filter(ffmpeg, "drawtext")
    text_mode = "drawtext" if use_drawtext else "pillow"
    commands: list[list[str]] = []
    cached: list[Path] = []
    cache_hits = 0
    for segment in compiled["segments"]:
        key = _cache_key(segment, compiled["output"], font, text_mode)
        target = cache_dir / f"{key}.mkv"
        if target.is_file() and target.stat().st_size > 0 and not force:
            cache_hits += 1
        else:
            _render_segment(
                ffmpeg,
                segment,
                compiled["output"],
                target,
                font,
                dry_run,
                commands,
                use_drawtext,
            )
        cached.append(target)
    _assemble(
        ffmpeg,
        compiled,
        cached,
        destination,
        profile,
        font,
        dry_run,
        commands,
        use_drawtext,
    )
    return {
        "output": str(destination.resolve()),
        "profile": profile,
        "duration": compiled["duration"],
        "segments": len(cached),
        "cache_hits": cache_hits,
        "cache_misses": len(cached) - cache_hits,
        "text_backend": text_mode,
        "commands": commands if dry_run else len(commands),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render a compiled SnakeTron EDL.")
    parser.add_argument("compiled_edl", type=Path)
    parser.add_argument("-o", "--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("preview", "final"), default="preview")
    parser.add_argument("--cache-dir", type=Path, default=Path(".video-cache/segments"))
    parser.add_argument("--ffmpeg", default=os.environ.get("FFMPEG", "ffmpeg"))
    parser.add_argument("--font", type=Path, default=DEFAULT_FONT)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = render(
            args.compiled_edl.resolve(),
            args.output.resolve(),
            args.profile,
            args.cache_dir.resolve(),
            args.ffmpeg,
            args.font.resolve(),
            force=args.force,
            dry_run=args.dry_run,
        )
    except RenderError as exc:
        print(f"render: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
