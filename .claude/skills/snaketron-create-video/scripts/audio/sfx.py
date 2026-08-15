"""Event SFX, built from the same palette as the bed.

The old set failed measurably: boost.wav was a single narrow 250 Hz-1 kHz band
with nothing above 1 kHz (that file is the "whooshing"), and whoosh.wav was
10 dB too quiet to hear under music. These are built to sit *with* the bed
rather than against it — the music carries the low weight of each impact, so
the SFX carry the crack and the air.
"""

from __future__ import annotations

import numpy as np
import soundfile as sf
from pedalboard import (
    Distortion, HighpassFilter, LadderFilter, LowpassFilter, Pedalboard, Reverb,
)

import kit
from dsp import SR, apply, as_stereo, note, perc_env, saw, sine, supersaw, sweep_ladder

_rng = np.random.default_rng(0x5EED)


def impact(length=1.1) -> np.ndarray:
    """Elimination. Transient crack, body thump, short bright tail."""
    n = int(length * SR)
    crack = _rng.normal(0, 1, n) * np.exp(-np.linspace(0, 26, n))
    body = sine(62, n) * np.exp(-np.linspace(0, 7, n))
    sweep = sine(180, n) * np.exp(-np.linspace(0, 14, n))
    tail = _rng.normal(0, 1, n) * np.exp(-np.linspace(0, 4.5, n)) * 0.25
    raw = crack * 0.9 + body * 0.85 + sweep * 0.3 + tail
    st = np.stack([raw, raw * 0.96])
    out = apply(Pedalboard([
        Distortion(drive_db=8),
        LowpassFilter(cutoff_frequency_hz=13_000),
        Reverb(room_size=0.3, wet_level=0.16, dry_level=1.0),
    ]), st)
    return out / (np.max(np.abs(out)) + 1e-9) * 0.95


def boost(length=1.0) -> np.ndarray:
    """Boost engage. An upward filter sweep on a saw stack with real top end -
    the thing the old file was trying and failing to be."""
    n = int(length * SR)
    stack = supersaw(note('A2'), n, voices=5, detune_cents=30)
    cut = np.geomspace(180, 12_000, 300)
    swept = sweep_ladder(stack, cut, resonance=0.62, drive=2.4)
    air = _rng.normal(0, 1, n) * np.linspace(0, 1, n) ** 2.2
    air_st = apply(Pedalboard([HighpassFilter(cutoff_frequency_hz=3500)]),
                   np.stack([air, air])) * 0.35
    env = np.linspace(0.15, 1.0, n) ** 1.4
    env[-int(0.12 * SR):] *= np.linspace(1, 0, int(0.12 * SR))
    out = (swept + air_st) * env
    return out / (np.max(np.abs(out)) + 1e-9) * 0.9


def bank(length=1.3) -> np.ndarray:
    """Points banked. A bright major arpeggio flourish that resolves upward."""
    n = int(length * SR)
    out = np.zeros((2, n))
    for i, nm in enumerate(['A4', 'C5', 'E5', 'A5']):
        start = int(i * 0.052 * SR)
        seg = n - start
        v = supersaw(note(nm), seg, voices=5, detune_cents=12)
        out[:, start:] += v * perc_env(seg, 0, curve=3.0) * (0.9 - 0.12 * i)
    out = apply(Pedalboard([
        LadderFilter(mode=LadderFilter.Mode.LPF12, cutoff_hz=9000, resonance=0.2),
        Reverb(room_size=0.4, wet_level=0.24, dry_level=1.0),
    ]), out)
    return out / (np.max(np.abs(out)) + 1e-9) * 0.85


def card_push(length=0.75) -> np.ndarray:
    """Transition. Broadband noise sweep for the smooth_left card pushes -
    replaces whoosh.wav, which was simply inaudible."""
    n = int(length * SR)
    # Two *independent* noise streams, not one inverted against itself: an
    # inverted channel is wide in stereo and silent the moment anything sums to
    # mono, which is most phones. Decorrelated noise is just as wide and
    # survives the fold-down.
    left = _rng.normal(0, 1, n)
    right = _rng.normal(0, 1, n)
    cut = np.concatenate([np.geomspace(400, 9000, 200), np.geomspace(9000, 700, 100)])
    st = sweep_ladder(np.stack([left, right]), cut, resonance=0.4, drive=1.0,
                      mode=LadderFilter.Mode.BPF12)
    env = np.sin(np.linspace(0, np.pi, n)) ** 1.6
    out = st * env
    return out / (np.max(np.abs(out)) + 1e-9) * 0.8


def stamp(length=0.5) -> np.ndarray:
    """Rank badge landing. Short, bright, metallic."""
    n = int(length * SR)
    tick = _rng.normal(0, 1, n) * np.exp(-np.linspace(0, 22, n))
    ring = sum(sine(1800 * r, n) * np.exp(-np.linspace(0, 12, n)) * (0.5 ** i)
               for i, r in enumerate([1.0, 1.71, 2.43]))
    raw = tick * 0.7 + ring * 0.5
    st = np.stack([raw, raw])
    out = apply(Pedalboard([
        HighpassFilter(cutoff_frequency_hz=900),
        Reverb(room_size=0.25, wet_level=0.18, dry_level=1.0),
    ]), st)
    return out / (np.max(np.abs(out)) + 1e-9) * 0.8


SET = {
    'impact.wav': impact,
    'boost.wav': boost,
    'bank.wav': bank,
    'whoosh.wav': card_push,
    'stamp.wav': stamp,
}


def write_all(dest: str) -> None:
    import os
    os.makedirs(dest, exist_ok=True)
    for name, fn in SET.items():
        x = fn()
        sf.write(os.path.join(dest, name), np.asarray(x).T, SR, subtype='PCM_24')
        rms = 20 * np.log10(np.sqrt(np.mean(np.asarray(x) ** 2)) + 1e-12)
        print(f'  {name:12s} {np.asarray(x).shape[1]/SR:.2f}s  rms={rms:6.1f} dB')


if __name__ == '__main__':
    import sys
    write_all(sys.argv[1] if len(sys.argv) > 1 else 'sfx-out')
