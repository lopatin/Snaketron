"""Drum kit and bass voices.

House drums are the one place where synthesis is not an imitation of anything:
the TR-808/909 sounds these reference *are* a pitch-enveloped sine and a gated
noise burst, so modelling them in numpy gets you the real article rather than
an approximation of a recording.
"""

from __future__ import annotations

import numpy as np
from pedalboard import Distortion, HighpassFilter, LowpassFilter, Pedalboard, Reverb

from dsp import SR, apply, as_stereo, perc_env, saw, sine, square

_SEED = 0xC0FFEE
_rng = np.random.default_rng(_SEED)


def reset_rng() -> None:
    """Rewind the noise source.

    Every noise-based voice here draws from one module-level generator, so a
    second `song.build()` in the same process got different drums from the
    first — the output was deterministic per *process*, which is not the same
    as deterministic, and hid itself from a check that shelled out each time.
    """
    global _rng
    _rng = np.random.default_rng(_SEED)


def kick(sr=SR, punch=1.0, length=0.42, f_start=118.0, f_end=44.0,
         click=0.55) -> np.ndarray:
    """909-ish: an exponential pitch drop into a sine, plus a transient click.

    The pitch envelope is the whole sound. A sine at a fixed 50 Hz is a test
    tone; the same sine swept from 118 Hz over 45 ms is a kick drum, because
    the ear reads the downward glide as a membrane losing tension.
    """
    n = int(length * sr)
    t = np.arange(n) / sr
    pitch = f_end + (f_start - f_end) * np.exp(-t / 0.032)
    phase = 2 * np.pi * np.cumsum(pitch) / sr
    body = np.sin(phase) * np.exp(-t / (0.115 * punch))

    # Beater click: very short noise, high-passed, so the kick survives a phone.
    cn = int(0.006 * sr)
    tick = _rng.normal(0, 1, cn) * np.exp(-np.linspace(0, 9, cn))
    tick = np.asarray(Pedalboard([HighpassFilter(cutoff_frequency_hz=1400)])(
        np.stack([tick, tick]).astype(np.float32), sr))[0]

    out = body.copy()
    out[:cn] += tick * click
    # Soft-clip for density; house kicks are always driven.
    out = np.tanh(out * 1.7) / 1.25
    return out


def clap(sr=SR, spread=0.011, taps=4, tail=0.22) -> np.ndarray:
    """Four noise bursts a few ms apart, then a short diffuse tail.

    The multi-tap is what makes it a *clap* rather than a snare: a room full of
    hands never lands on one sample.
    """
    n = int((spread * taps + tail) * sr)
    out = np.zeros(n)
    for i in range(taps):
        off = int((spread * i + _rng.uniform(0, 0.0015)) * sr)
        bn = int(0.012 * sr)
        if off + bn > n:
            break
        burst = _rng.normal(0, 1, bn) * np.exp(-np.linspace(0, 7, bn))
        out[off:off + bn] += burst * (1.0 if i == taps - 1 else 0.62)
    tn = int(tail * sr)
    start = int(spread * (taps - 1) * sr)
    seg = min(tn, n - start)
    out[start:start + seg] += _rng.normal(0, 1, seg) * np.exp(-np.linspace(0, 5.5, seg)) * 0.5

    st = np.stack([out, out]).astype(np.float32)
    st = np.asarray(Pedalboard([
        HighpassFilter(cutoff_frequency_hz=750),
        LowpassFilter(cutoff_frequency_hz=7200),
        Reverb(room_size=0.18, wet_level=0.20, dry_level=1.0, width=1.0),
    ])(st, sr))
    return st / (np.max(np.abs(st)) + 1e-9)


def hat(sr=SR, decay=0.035, open_hat=False, tone=9000.0) -> np.ndarray:
    """Metallic noise: six detuned squares summed, then gated and high-passed.

    Pure white noise gives you a 'ts'; the square stack gives the ringing edge
    that reads as a cymbal.
    """
    d = decay * (5.5 if open_hat else 1.0)
    n = int(d * sr)
    ratios = [1.0, 1.342, 1.2312, 1.6532, 1.9523, 2.1523]
    out = np.zeros(n)
    for r in ratios:
        out += square(tone * r / 8, n, sr)
    out += _rng.normal(0, 0.6, n)
    out *= np.exp(-np.linspace(0, 6.5 if not open_hat else 3.2, n))
    st = np.stack([out, out]).astype(np.float32)
    st = np.asarray(Pedalboard([HighpassFilter(cutoff_frequency_hz=tone * 0.72)])(st, sr))
    return st / (np.max(np.abs(st)) + 1e-9)


def snare(sr=SR, tune=196.0, decay=0.17) -> np.ndarray:
    n = int(decay * sr)
    t = np.arange(n) / sr
    body = (np.sin(2 * np.pi * tune * t) + 0.6 * np.sin(2 * np.pi * tune * 1.48 * t))
    body *= np.exp(-t / 0.045)
    wires = _rng.normal(0, 1, n) * np.exp(-t / 0.075)
    out = body * 0.55 + wires * 0.8
    st = np.stack([out, out]).astype(np.float32)
    st = np.asarray(Pedalboard([
        HighpassFilter(cutoff_frequency_hz=180),
        LowpassFilter(cutoff_frequency_hz=9500),
    ])(st, sr))
    return st / (np.max(np.abs(st)) + 1e-9)


def crash(sr=SR, decay=1.6) -> np.ndarray:
    n = int(decay * sr)
    out = _rng.normal(0, 1, n)
    for r in (1.0, 1.41, 1.73, 2.24, 2.83, 3.46):
        out += square(700 * r, n, sr) * 0.28
    out *= np.exp(-np.linspace(0, 4.2, n))
    st = np.stack([out, out * 0.92]).astype(np.float32)
    st = np.asarray(Pedalboard([
        HighpassFilter(cutoff_frequency_hz=3800),
        Reverb(room_size=0.5, wet_level=0.25, dry_level=1.0),
    ])(st, sr))
    return st / (np.max(np.abs(st)) + 1e-9)


def tom(freq: float, sr=SR, decay=0.30) -> np.ndarray:
    n = int(decay * sr)
    t = np.arange(n) / sr
    pitch = freq * (1 + 0.35 * np.exp(-t / 0.05))
    out = np.sin(2 * np.pi * np.cumsum(pitch) / sr) * np.exp(-t / (decay / 3.2))
    out += _rng.normal(0, 0.08, n) * np.exp(-t / 0.02)
    return np.tanh(out * 1.3)


# --- bass ------------------------------------------------------------------

def sub(f0: float, length: float, sr=SR, drive=1.25) -> np.ndarray:
    """Sine sub with a touch of drive so it survives small speakers."""
    n = int(length * sr)
    env = np.ones(n)
    a, r = int(0.006 * sr), int(0.03 * sr)
    env[:a] = np.linspace(0, 1, a)
    env[-r:] = np.linspace(1, 0, r)
    return np.tanh(sine(f0, n, sr) * drive) * env


def reese(f0: float, length: float, sr=SR, detune=0.35) -> np.ndarray:
    """Two saws a hair apart: the beating between them is the growl."""
    n = int(length * sr)
    a = saw(f0, n, sr)
    b = saw(f0 * 2 ** (detune / 12), n, sr, phase=0.7)
    out = (a + b) * 0.5
    env = np.ones(n)
    at, rl = int(0.008 * sr), int(0.04 * sr)
    env[:at] = np.linspace(0, 1, at)
    env[-rl:] = np.linspace(1, 0, rl)
    return out * env


def pluck_bass(f0: float, length: float, sr=SR) -> np.ndarray:
    """Short filtered saw with a fast decay — the French-house bassline voice."""
    n = int(length * sr)
    raw = saw(f0, n, sr) * 0.7 + square(f0, n, sr) * 0.3
    env = perc_env(n, decay=0.0, curve=4.0)
    st = np.stack([raw * env, raw * env]).astype(np.float32)
    st = np.asarray(Pedalboard([LowpassFilter(cutoff_frequency_hz=f0 * 9)])(st, sr))
    return st
