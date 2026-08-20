#!/usr/bin/env python3
"""Nudge an EDL's clip lengths until every cut lands on the music's beat grid.

Beat-snapped cutting is a hard constraint in `compile_edl.py`: a cut more than
`beat_snap_max_s` from a beat fails the build. Satisfying it by hand is
miserable, because lengthening any shot shifts every downstream cut — so a
0.1s change near the top can break a cut thirty seconds later.

This solves it mechanically. It compiles, reads the first offending cut, moves
the *preceding* clip's `out` to the nearest beat, and repeats. Tail segments
run at rate 1.0 in practice, so a delta in source seconds moves the cut by the
same amount; when it does not, the loop simply converges over more passes.

    python3 scripts/fit_beats.py assets/launch-trailer/launch-trailer.edl.json \\
        --clips-dir tools/video/clips

Edits the EDL in place and prints what it changed. Run it after any retiming,
before rendering.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CUT_ERROR = re.compile(
    r"cut before (?P<clip>\S+) at (?P<at>[0-9.]+)s has no beat within (?P<tol>[0-9.]+)s"
)


def compile_once(edl: Path, clips_dir: Path, out: Path) -> tuple[bool, str]:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "compile_edl.py"),
            str(edl),
            "--clips-dir",
            str(clips_dir),
            "-o",
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0, (result.stderr or result.stdout).strip()


def preceding_clip_index(timeline: list[dict], clip_name: str) -> int | None:
    """Index of the clip entry immediately before `clip_name`'s entry."""
    target = next(
        (i for i, e in enumerate(timeline) if e.get("clip") == clip_name), None
    )
    if target is None:
        return None
    for index in range(target - 1, -1, -1):
        if "clip" in timeline[index]:
            return index
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("edl", type=Path)
    parser.add_argument("--clips-dir", type=Path, required=True)
    parser.add_argument("--beat", type=float, default=0.5, help="beat period, seconds")
    parser.add_argument("--max-iters", type=int, default=60)
    parser.add_argument("--min-out", type=float, default=0.8)
    args = parser.parse_args()

    scratch = args.edl.with_suffix(".fitcheck.json")
    changes: list[str] = []

    for _ in range(args.max_iters):
        ok, message = compile_once(args.edl, args.clips_dir, scratch)
        if ok:
            scratch.unlink(missing_ok=True)
            for line in changes:
                print(line)
            print("beat grid satisfied")
            return 0

        match = CUT_ERROR.search(message)
        if not match:
            print(message, file=sys.stderr)
            return 1

        clip_name = match.group("clip")
        at = float(match.group("at"))
        target = round(at / args.beat) * args.beat
        delta = round(target - at, 6)

        data = json.loads(args.edl.read_text())
        timeline = data["timeline"]
        index = preceding_clip_index(timeline, clip_name)
        if index is None:
            print(f"no clip precedes {clip_name}; cannot fit", file=sys.stderr)
            return 1

        entry = timeline[index]
        new_out = round(float(entry["out"]) + delta, 6)
        if new_out < args.min_out:
            # Shortening this shot past usefulness — push the cut later instead.
            new_out = round(new_out + args.beat, 6)
        changes.append(
            f"{entry['clip']}: out {entry['out']} -> {new_out} "
            f"(cut {at:.3f}s -> {target:.3f}s)"
        )
        entry["out"] = new_out
        args.edl.write_text(json.dumps(data, indent=1) + "\n")

    scratch.unlink(missing_ok=True)
    print("could not satisfy the beat grid; adjust shot lengths by hand", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
