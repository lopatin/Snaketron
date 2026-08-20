"""Synthesis primitives for the SnakeTron trailer bed.

Everything is band-limited additive or physically-motivated noise shaping, so
nothing aliases: a naive `sign(sin)` or ramp oscillator folds harmonics back
down the spectrum and is the single most reliable way to make a synth sound
cheap. Oscillators here sum only the harmonics that fit under Nyquist.

Effects come from `pedalboard` (JUCE under the hood) rather than hand-rolled
biquads. The Ladder filter in particular is the sound of this genre — a
resonant sweep on a detuned saw stack is the Tron Legacy / French house
signature, and a plain scipy Butterworth does not have the resonance or the
saturation that makes it musical.
"""

from __future__ import annotations

import numpy as np
from pedalboard import (
    Chorus, Compressor, Convolution, Delay, Distortion, Gain, HighpassFilter,
    LadderFilter, Limiter, LowpassFilter, Pedalboard, Reverb,
)

SR = 48_000


# --- note helpers ----------------------------------------------------------

_NOTE_BASE = {'C': 0, 'D': 2, 'E': 4, 'F': 5, 'G': 7, 'A': 9, 'B': 11}


def note(name: str) -> float:
    """'A2' / 'F#3' / 'Eb4' -> frequency in Hz (A4 = 440)."""
    letter, rest = name[0].upper(), name[1:]
    semis = _NOTE_BASE[letter]
    while rest and rest[0] in '#b':
        semis += 1 if rest[0] == '#' else -1
        rest = rest[1:]
    midi = 12 * (int(rest) + 1) + semis
    return 440.0 * 2 ** ((midi - 69) / 12)


def secs(n: int) -> np.ndarray:
    return np.zeros(int(n))


# --- envelopes -------------------------------------------------------------

def adsr(n: int, a=0.005, d=0.1, s=0.6, r=0.2, sr=SR) -> np.ndarray:
    """Exponential-ish ADSR. Linear attack, exponential decay/release, which is
    what ears expect from anything struck or plucked."""
    a_n, d_n, r_n = int(a * sr), int(d * sr), int(r * sr)
    sus_n = max(0, n - a_n - d_n - r_n)
    parts = [
        np.linspace(0, 1, a_n, endpoint=False) if a_n else np.array([]),
        s + (1 - s) * np.exp(-np.linspace(0, 5, d_n)) if d_n else np.array([]),
        np.full(sus_n, s),
        s * np.exp(-np.linspace(0, 6, r_n)) if r_n else np.array([]),
    ]
    env = np.concatenate([p for p in parts if len(p)])
    return np.resize(env, n)


def perc_env(n: int, decay: float, sr=SR, curve: float = 5.0) -> np.ndarray:
    """One-shot percussive decay with a 2 ms click-free attack."""
    env = np.exp(-np.linspace(0, curve, n))
    attack = min(int(0.002 * sr), n)
    if attack:
        env[:attack] *= np.linspace(0, 1, attack)
    return env


# --- oscillators (band-limited) --------------------------------------------

# Capped for speed: harmonic 80 of even a low saw is already past 15 kHz and
# 30 dB down. Uncapped, a 55 Hz note costs 435 sine evaluations per voice.
MAX_HARMONICS = 80


def _harmonic_count(f0: float, sr=SR) -> int:
    return max(1, min(MAX_HARMONICS, int((sr / 2) / max(f0, 1e-6)) - 1))


def saw(f0: float, n: int, sr=SR, phase: float = 0.0) -> np.ndarray:
    """Band-limited sawtooth: sum_{k=1..K} (-1)^(k+1) sin(2*pi*k*f0*t) / k."""
    t = np.arange(n) / sr
    out = np.zeros(n)
    k = np.arange(1, _harmonic_count(f0, sr) + 1)
    for kk in k:
        out += ((-1) ** (kk + 1)) * np.sin(2 * np.pi * kk * f0 * t + phase * kk) / kk
    return out * (2 / np.pi)


def square(f0: float, n: int, sr=SR, phase: float = 0.0) -> np.ndarray:
    t = np.arange(n) / sr
    out = np.zeros(n)
    for kk in range(1, _harmonic_count(f0, sr) + 1, 2):
        out += np.sin(2 * np.pi * kk * f0 * t + phase * kk) / kk
    return out * (4 / np.pi)


def sine(f0: float, n: int, sr=SR, phase: float = 0.0) -> np.ndarray:
    t = np.arange(n) / sr
    return np.sin(2 * np.pi * f0 * t + phase)


def supersaw(f0: float, n: int, voices: int = 7, detune_cents: float = 18.0,
             stereo_spread: float = 0.85, sr=SR) -> np.ndarray:
    """The genre's defining voice: N detuned saws, hard-panned outward.

    Detune is symmetric in cents so the stack stays centred in pitch, and each
    voice gets a random start phase — without that the voices align at t=0 and
    the note begins with an ugly impulse instead of a swell.
    """
    rng = np.random.default_rng(int(f0 * 1000) % 99991)
    left = np.zeros(n)
    right = np.zeros(n)
    offsets = np.linspace(-1, 1, voices)
    for i, off in enumerate(offsets):
        f = f0 * 2 ** (off * detune_cents / 1200)
        v = saw(f, n, sr, phase=rng.uniform(0, 2 * np.pi))
        # Outer voices sit wider; the centre voice stays mono for a solid core.
        pan = off * stereo_spread
        left += v * np.sqrt(max(0.0, (1 - pan) / 2))
        right += v * np.sqrt(max(0.0, (1 + pan) / 2))
    stack = np.stack([left, right]) / np.sqrt(voices)
    return stack


# --- effects ---------------------------------------------------------------

def as_stereo(x: np.ndarray) -> np.ndarray:
    if x.ndim == 1:
        return np.stack([x, x])
    return x


def apply(board: Pedalboard, x: np.ndarray, sr=SR) -> np.ndarray:
    """pedalboard wants (channels, samples) float32."""
    st = as_stereo(x).astype(np.float32)
    return np.asarray(board(st, sr))


def sweep_ladder(x: np.ndarray, cutoff_curve: np.ndarray, resonance=0.35,
                 drive=1.6, mode=LadderFilter.Mode.LPF24, sr=SR,
                 block: int = 512) -> np.ndarray:
    """Ladder filter with a per-block automated cutoff.

    Chunked rather than sample-accurate: at 512 samples (~11 ms) a sweep is
    smooth to the ear, and the filter keeps its internal state across blocks so
    there is no zipper noise at the seams.
    """
    st = as_stereo(x).astype(np.float32)
    n = st.shape[1]
    curve = np.interp(np.arange(n), np.linspace(0, n - 1, len(cutoff_curve)), cutoff_curve)
    filt = LadderFilter(mode=mode, cutoff_hz=float(curve[0]), resonance=resonance, drive=drive)
    out = np.zeros_like(st)
    for start in range(0, n, block):
        end = min(start + block, n)
        filt.cutoff_hz = float(np.clip(curve[start], 30.0, 18_000.0))
        out[:, start:end] = filt(st[:, start:end], sr, reset=False)
    return out


def sidechain(n: int, hits: list[float], depth=0.85, attack=0.004, hold=0.02,
              release=0.26, sr=SR) -> np.ndarray:
    """The pump. A gain envelope that slams down on every kick and breathes back.

    This is done as an explicit envelope rather than with a compressor keyed off
    the kick, because at 120 BPM the release curve *is* the groove and it has to
    be authored, not discovered.
    """
    env = np.ones(n)
    a_n, h_n, r_n = int(attack * sr), int(hold * sr), int(release * sr)
    for t in hits:
        i = int(t * sr)
        if i >= n:
            continue
        if a_n:
            seg = min(a_n, n - i)
            env[i:i + seg] = np.minimum(env[i:i + seg], np.linspace(1, 1 - depth, seg))
        j = i + a_n
        if h_n and j < n:
            seg = min(h_n, n - j)
            env[j:j + seg] = np.minimum(env[j:j + seg], 1 - depth)
        j += h_n
        if r_n and j < n:
            seg = min(r_n, n - j)
            # Exponential recovery: fast out of the dip, slow into unity.
            curve = (1 - depth) + depth * (1 - np.exp(-np.linspace(0, 4, seg)))
            env[j:j + seg] = np.minimum(env[j:j + seg], curve)
    return env


def room(size=0.35, damping=0.5, wet=0.16, width=1.0) -> Pedalboard:
    return Pedalboard([Reverb(room_size=size, damping=damping, wet_level=wet,
                              dry_level=1.0, width=width)])


def pingpong(time_s: float, feedback=0.32, mix=0.22) -> Pedalboard:
    return Pedalboard([Delay(delay_seconds=time_s, feedback=feedback, mix=mix)])


def glue(threshold_db=-14.0, ratio=3.0) -> Pedalboard:
    return Pedalboard([Compressor(threshold_db=threshold_db, ratio=ratio,
                                  attack_ms=8, release_ms=140)])


def master_chain(ceiling_db=-1.2) -> Pedalboard:
    return Pedalboard([
        # 18 Hz DC blocker. tanh saturation on the kick and bass is not
        # symmetric, so the mix accumulates a small offset that eats headroom
        # and costs the kick some of its punch for nothing.
        HighpassFilter(cutoff_frequency_hz=18),
        # Catches peaks only. Section-to-section dynamics are authored in the
        # arrangement and must reach the listener intact.
        Compressor(threshold_db=-6, ratio=1.8, attack_ms=15, release_ms=200),
        Limiter(threshold_db=ceiling_db, release_ms=90),
    ])


def block_dc(x: np.ndarray) -> np.ndarray:
    """Remove residual DC per channel.

    An 18 Hz one-pole is 3 dB down at 18 Hz and barely touches true DC, so the
    offset from asymmetric saturation survives it. Subtracting the mean is
    exact and costs nothing.
    """
    return x - np.mean(x, axis=-1, keepdims=True)


def true_peak_safe(x: np.ndarray, ceiling_db: float = -1.0) -> np.ndarray:
    """Hard guarantee on the output ceiling.

    `Limiter` is a threshold, not a brickwall: fast transients overshoot it, and
    every variation was landing samples at full scale despite a -1 dB setting.
    Anything at 0 dBFS clips on conversion to lossy formats, so the final scale
    is applied unconditionally.
    """
    ceiling = 10 ** (ceiling_db / 20)
    peak = float(np.max(np.abs(x)))
    return x * (ceiling / peak) if peak > ceiling else x


# --- mixing ----------------------------------------------------------------

class Mix:
    """A stereo bus with sample-accurate placement."""

    def __init__(self, duration: float, sr=SR):
        self.sr = sr
        self.n = int(duration * sr)
        self.buf = np.zeros((2, self.n))

    def add(self, x: np.ndarray, at: float = 0.0, gain: float = 1.0) -> None:
        st = as_stereo(np.asarray(x, dtype=np.float64))
        i = int(at * self.sr)
        if i >= self.n:
            return
        seg = min(st.shape[1], self.n - i)
        if seg <= 0:
            return
        self.buf[:, i:i + seg] += st[:, :seg] * gain

    def out(self) -> np.ndarray:
        return self.buf


# --- formant / talkbox -----------------------------------------------------

# Vowel formants (F1, F2, F3) in Hz, and their relative gains. Sweeping a saw
# stack between these is the talkbox sound on Around the World / Harder Better
# Faster — the "robot voice" that is Daft Punk's most identifiable signature.
# A true vocoder needs a voice to modulate; a resonant formant bank does not,
# and gets most of the way there because vowels ARE formant positions.
VOWELS = {
    'a': [(730, 1.0), (1090, 0.50), (2440, 0.22)],
    'e': [(530, 1.0), (1840, 0.55), (2480, 0.25)],
    'i': [(270, 1.0), (2290, 0.62), (3010, 0.30)],
    'o': [(570, 1.0), (840, 0.48), (2410, 0.18)],
    'u': [(300, 1.0), (870, 0.42), (2240, 0.14)],
}


def formant_bank(x: np.ndarray, vowel_track: list[tuple[float, str]], total: float,
                 q: float = 11.0, sr=SR, block: int = 1024) -> np.ndarray:
    """Three resonant band-passes at vowel formants, morphing over time.

    `vowel_track` is [(start_seconds, vowel_letter), ...]. Centres are
    interpolated between entries rather than switched, so the lead glides from
    one vowel to the next the way a talkbox actually does.
    """
    from scipy.signal import sosfilt, sosfilt_zi, butter

    st = as_stereo(x).astype(np.float64)
    n = st.shape[1]
    times = np.array([t for t, _ in vowel_track] + [total])
    seqs = [VOWELS[v] for _, v in vowel_track]

    def centres_at(t: float):
        i = max(0, int(np.searchsorted(times, t, side='right') - 1))
        i = min(i, len(seqs) - 1)
        j = min(i + 1, len(seqs) - 1)
        span = max(times[i + 1] - times[i], 1e-6)
        frac = np.clip((t - times[i]) / span, 0, 1)
        return [((1 - frac) * seqs[i][k][0] + frac * seqs[j][k][0],
                 (1 - frac) * seqs[i][k][1] + frac * seqs[j][k][1]) for k in range(3)]

    out = np.zeros_like(st)
    states = [None, None, None]
    for start in range(0, n, block):
        end = min(start + block, n)
        t = start / sr
        for k, (fc, g) in enumerate(centres_at(t)):
            bw = fc / q
            lo = max(30.0, fc - bw / 2)
            hi = min(sr / 2 - 100, fc + bw / 2)
            if hi <= lo:
                continue
            sos = butter(2, [lo, hi], btype='band', fs=sr, output='sos')
            if states[k] is None or states[k][0].shape != sos.shape:
                states[k] = (sos, np.zeros((sos.shape[0], 2, 2)))
            zi = states[k][1]
            seg = st[:, start:end]
            filtered = np.zeros_like(seg)
            new_zi = np.zeros_like(zi)
            for ch in range(2):
                filtered[ch], z = sosfilt(sos, seg[ch], zi=zi[:, :, ch])
                new_zi[:, :, ch] = z
            states[k] = (sos, new_zi)
            out[:, start:end] += filtered * g
    peak = np.max(np.abs(out))
    return out / peak * np.max(np.abs(st)) if peak > 1e-9 else out
