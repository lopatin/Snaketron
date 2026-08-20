#!/usr/bin/env python3
"""Render the trailer bed and the event SFX set.

    python3 build.py --out <dir> [--variation grid|discovery|opus|robot|all]

Deterministic: every voice is seeded, so the same arguments always produce
byte-identical audio. That is the point — the audio this replaces was five
opaque committed binaries whose only provenance was the string "procedurally
synthesized for this repository", which meant nobody could ever adjust them.

Requires `pedalboard` and `soundfile` (see requirements.txt).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import soundfile as sf

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sfx as sfx_mod  # noqa: E402
import song  # noqa: E402
from dsp import SR  # noqa: E402


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True, help='output directory')
    ap.add_argument('--variation', default='all')
    ap.add_argument('--skip-sfx', action='store_true')
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, str] = {}

    for cfg in song.VARIATIONS:
        if args.variation not in ('all', cfg.name):
            continue
        audio = song.build(cfg)
        path = out / f'launch-bed-{cfg.name}.wav'
        sf.write(path, audio.T, SR, subtype='PCM_24')
        peak = float(np.max(np.abs(audio)))
        rms = 20 * np.log10(np.sqrt(np.mean(audio ** 2)) + 1e-12)
        print(f'{cfg.name:10s} peak={20*np.log10(peak):5.1f} dBFS  rms={rms:6.1f} dB  {path.name}')
        manifest[path.name] = sha256(path)

    if not args.skip_sfx:
        sfx_dir = out / 'sfx'
        sfx_dir.mkdir(exist_ok=True)
        sfx_mod.write_all(str(sfx_dir))
        for f in sorted(sfx_dir.glob('*.wav')):
            manifest[f'sfx/{f.name}'] = sha256(f)

    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    print(f'\n{len(manifest)} files -> {out}/manifest.json')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
