"""The SnakeTron launch trailer bed.

120 BPM, A minor, 30.1 s. The cut is already a musical grid — every picture cut
lands on the 0.5 s line, so a bar is 2 s and the eight cuts fall on bar and
half-bar boundaries. The arrangement agrees with the edit rather than being
laid over it.

Shape (Prydz/Daft Punk long-form compressed into 30 s):

    0.0   intro        filtered arp alone, no drums, logo drops in
    2.0   beat in      kick on the demolition cut; kill at 2.80
    4.0   build A      clap + hats + bass; bank at 6.10
    7.0   build B      arp opens, second layer
   10.0   build C      supersaw chords, 16th hats
   13.0   peak A       full groove under Classic Snake
   16.0   BREAKDOWN    drums stop dead on the rank-up cut; riser begins
   17.26  promotion    the division boundary — dominant chord, big stab
   18.0   THE DROP     lands 0.5 s BEFORE the leaderboard cut, so the music
                       causes the reveal instead of reacting to it
   26.5   outro        fadewhite; drums out, chord rings under the end slate
   30.1   end          tail still sounding — the last frame is held on purpose

Harmony: i - VI - III - VII (Am F C G), one bar each. The breakdown sits on F
(unresolved), the promotion hits G (maximum tension), and the drop lands on Am.
That V->i is doing the emotional work of the promotion.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from pedalboard import (
    Chorus, Distortion, HighpassFilter, LadderFilter, LowpassFilter,
    Pedalboard, Reverb,
)

import kit
from dsp import (
    SR, Mix, apply, as_stereo, formant_bank, glue, master_chain, note, perc_env,
    pingpong, room, saw, sidechain, sine, square, supersaw, sweep_ladder,
    true_peak_safe, block_dc,
)

BPM = 120.0
BEAT = 60.0 / BPM          # 0.5 s
BAR = 4 * BEAT             # 2.0 s
STEP = BEAT / 4            # 0.125 s, one 16th
DURATION = 30.1

# Picture anchors, from the compiled EDL.
T_KILL_1 = 2.80
T_BANK = 6.10
T_KILL_2 = 8.20
T_BOOST = 10.35
T_CLASSIC = 13.0
T_BREAK = 16.0
T_PROMOTION = 17.26
T_DROP = 18.0
T_LADDER = 18.5
T_FADEWHITE = 26.5

# Am  F  C  G, one bar each, looping.
PROGRESSION = ['Am', 'F', 'C', 'G']
CHORD_TONES = {
    'Am': ['A', 'C', 'E'],
    'F': ['F', 'A', 'C'],
    'C': ['C', 'E', 'G'],
    'G': ['G', 'B', 'D'],
    'Em': ['E', 'G', 'B'],
}
ROOTS = {'Am': 'A', 'F': 'F', 'C': 'C', 'G': 'G', 'Em': 'E'}


def chord_at(t: float, prog=PROGRESSION) -> str:
    """Which chord is sounding at time t. The breakdown holds F and the
    promotion holds G, overriding the loop."""
    if T_BREAK <= t < T_PROMOTION:
        return 'F'
    if T_PROMOTION <= t < T_DROP:
        return 'G'
    return prog[int(t // BAR) % len(prog)]


@dataclass
class Voicing:
    """Config knobs that make the three variations different records."""
    name: str
    blurb: str
    # Lead / arp
    arp_octave: int = 4
    arp_pattern: tuple = (0, 1, 2, 1, 2, 0, 1, 2)
    arp_gain: float = 0.30
    arp_detune: float = 14.0
    # Chords
    chord_gain: float = 0.26
    chord_style: str = 'pad'        # 'pad' | 'stab' | 'none'
    stab_steps: tuple = (2, 6, 10, 14)
    # Bass
    bass_style: str = 'sub'         # 'sub' | 'pluck' | 'reese'
    bass_gain: float = 0.62
    bass_octave: int = 1
    # Drums
    hat_16ths: bool = True
    open_hat_offbeat: bool = True
    clap_gain: float = 0.52
    kick_gain: float = 0.95
    # Feel
    pump_depth: float = 0.78
    drop_filter_hz: float = 16_000.0
    build_filter_hz: float = 900.0
    swing: float = 0.0              # fraction of a 16th, applied to odd steps
    lead_style: str = 'saw'         # 'saw' | 'square' | 'none'
    lead_gain: float = 0.34


# The tune. Scale degrees into the sounding chord's tones, as
# (bar_in_phrase, sixteenth, tone_index, octave). Eight bars, so it spans two
# full trips round the progression and does not feel like a two-bar loop.
#
# It is stated three times at rising density: hinted in the build (sparse, an
# octave down, filtered), full in the drop, and answered by a counter-melody.
# A trailer tune nobody hears until 18 s is not a tune, it is a payoff.
MELODY = [
    (0, 0, 0, 5), (0, 6, 2, 4), (0, 10, 1, 5),
    (1, 0, 1, 5), (1, 4, 0, 5), (1, 10, 2, 4),
    (2, 0, 2, 4), (2, 6, 1, 5), (2, 12, 0, 5),
    (3, 0, 0, 5), (3, 8, 2, 5), (3, 12, 1, 5),
    (4, 0, 1, 5), (4, 6, 0, 5), (4, 10, 2, 5),
    (5, 0, 2, 5), (5, 4, 1, 5), (5, 10, 0, 5),
    (6, 0, 0, 5), (6, 6, 2, 4), (6, 12, 1, 5),
    (7, 0, 1, 5), (7, 8, 0, 5),
]

# The answer: lower, sparser, lands in the gaps the melody leaves.
COUNTER = [
    (0, 12, 0, 4), (1, 12, 2, 3), (2, 10, 1, 4), (3, 4, 0, 4),
    (4, 12, 2, 4), (5, 12, 0, 4), (6, 10, 2, 3), (7, 4, 1, 4),
]


def kick_times(cfg: Voicing) -> list[float]:
    """4-on-the-floor from the demolition cut, out for the breakdown, back on
    the drop, out again at the fadewhite."""
    hits = []
    t = 2.0
    while t < DURATION:
        if T_BREAK <= t < T_DROP:
            t += BEAT
            continue
        if t >= T_FADEWHITE:
            break
        hits.append(t)
        t += BEAT
    return hits


def _step_time(bar_start: float, step: int, cfg: Voicing) -> float:
    t = bar_start + step * STEP
    if cfg.swing and step % 2 == 1:
        t += cfg.swing * STEP
    return t


# Section dynamics. A trailer bed lives or dies on the difference between the
# quietest bar and the loudest one; rendering every section at the same RMS is
# the single most common way to make a well-synthesized track feel inert. These
# are pre-master gains, so the contrast survives into the final mix.
SECTION_GAIN = [
    (0.0,  0.30),   # intro: arp alone, deliberately small
    (2.0,  0.46),   # beat in
    (4.0,  0.55),   # build A
    (7.0,  0.64),   # build B
    (10.0, 0.74),   # build C
    (13.0, 0.82),   # peak A
    (16.0, 0.38),   # BREAKDOWN - the floor drops out
    (17.26, 0.52),  # promotion swells
    (18.0, 1.00),   # THE DROP
    (26.5, 0.86),   # outro
]


def section_envelope(n: int, sr=SR) -> np.ndarray:
    """Stepped-but-smoothed gain per section.

    Steps are eased over 120 ms so a section change is felt rather than heard
    as a click, except into the drop where the step is deliberately immediate.
    """
    env = np.ones(n)
    times = [t for t, _ in SECTION_GAIN] + [DURATION]
    for i, (start, gain) in enumerate(SECTION_GAIN):
        a, b = int(start * sr), int(min(times[i + 1], DURATION) * sr)
        env[a:b] = gain
    # Smooth every boundary except the drop, which should hit like a switch.
    ramp = int(0.12 * sr)
    for start, _ in SECTION_GAIN[1:]:
        if abs(start - T_DROP) < 1e-6:
            continue
        i = int(start * sr)
        lo, hi = max(0, i - ramp // 2), min(n, i + ramp // 2)
        if hi > lo:
            env[lo:hi] = np.linspace(env[lo], env[min(hi, n - 1)], hi - lo)
    return env


def build(cfg: Voicing, seed: int = 7) -> np.ndarray:
    # Rewind every noise source, so build() is a pure function of its
    # arguments rather than of how many times it has been called.
    kit.reset_rng()
    rng = np.random.default_rng(seed)
    mix = Mix(DURATION)
    n_total = mix.n

    kicks = kick_times(cfg)

    # --- drums -------------------------------------------------------------
    k = kit.kick()
    cl = kit.clap()
    hc = kit.hat()
    ho = kit.hat(open_hat=True)
    sn = kit.snare()
    cr = kit.crash()

    for t in kicks:
        # The drop kick is fatter than the build kick.
        g = cfg.kick_gain * (1.12 if t >= T_DROP else 1.0)
        mix.add(k, t, g)

    # Clap on 2 and 4 from the first build section.
    t = 4.0 + BEAT
    while t < DURATION:
        if not (T_BREAK <= t < T_DROP) and t < T_FADEWHITE:
            mix.add(cl, t, cfg.clap_gain)
        t += 2 * BEAT

    # Hats: 16ths in the busy sections, 8ths earlier.
    t = 4.0
    while t < T_FADEWHITE:
        if T_BREAK <= t < T_DROP:
            t += STEP
            continue
        busy = cfg.hat_16ths and (t >= 10.0)
        interval = STEP if busy else BEAT / 2
        # Offbeat open hat is the four-to-the-floor signature.
        on_offbeat = abs((t % BEAT) - BEAT / 2) < 1e-6
        if cfg.open_hat_offbeat and on_offbeat and t >= 7.0:
            mix.add(ho, t, 0.16)
        else:
            vel = 0.13 if (t % BEAT) < 1e-6 else 0.085
            mix.add(hc, t, vel * (1.25 if t >= T_DROP else 1.0))
        t += interval

    # Shaker on the 16ths from build C: fills the space between the hats and
    # is what makes the second half feel like it is moving faster without any
    # tempo change.
    shaker = kit.hat(decay=0.018, tone=12_000)
    t = 10.0
    while t < T_FADEWHITE:
        if not (T_BREAK <= t < T_DROP):
            on_beat = (t % BEAT) < 1e-6
            mix.add(shaker, t, 0.045 if on_beat else 0.070)
        t += STEP

    # Crashes on the structural moments.
    for t, g in [(2.0, 0.32), (T_KILL_1, 0.38), (T_DROP, 0.55), (T_FADEWHITE, 0.30)]:
        mix.add(cr, t, g)

    # Tom fills across the last half-bar into each section change. A section
    # that simply starts is a section the ear does not notice arriving.
    toms = [kit.tom(f) for f in (196.0, 165.0, 131.0, 110.0)]
    for target in (7.0, 13.0, T_BREAK):
        for i in range(4):
            ft = target - BEAT + i * (BEAT / 4)
            if ft < 2.0:
                continue
            mix.add(toms[i], ft, 0.30 + 0.06 * i)

    # Breakdown snare roll: accelerating 16ths -> 32nds into the drop.
    t = T_BREAK + BEAT
    step = STEP
    while t < T_DROP:
        frac = (t - T_BREAK) / (T_DROP - T_BREAK)
        mix.add(sn, t, 0.10 + 0.30 * frac)
        step = STEP * (1.0 - 0.55 * frac)
        t += max(step, STEP * 0.35)

    # --- bass --------------------------------------------------------------
    bass = Mix(DURATION)
    t = 2.0
    while t < T_FADEWHITE:
        if T_BREAK <= t < T_DROP:
            t += BEAT
            continue
        ch = chord_at(t)
        beat_in_bar = int(round((t % BAR) / BEAT)) % 4
        # Octave jump on the last beat of each bar: the line moves instead of
        # hammering the root, which is the difference between a bassline and a
        # pedal tone.
        octv = cfg.bass_octave + (1 if beat_in_bar == 3 else 0)
        f = note(f'{ROOTS[ch]}{octv}')
        fifth = note(f'{CHORD_TONES[ch][2]}{cfg.bass_octave}')
        if cfg.bass_style == 'sub':
            bass.add(kit.sub(f, BEAT * 0.92), t, 1.0)
            if beat_in_bar == 2 and t >= T_DROP:
                bass.add(kit.sub(fifth, BEAT * 0.42), t + BEAT / 2, 0.55)
        elif cfg.bass_style == 'reese':
            bass.add(kit.reese(f, BEAT * 0.92), t, 0.8)
            if beat_in_bar in (1, 3):
                bass.add(kit.reese(fifth, BEAT * 0.30), t + BEAT * 0.75, 0.4)
        else:  # pluck: offbeat 16ths, French house
            bass.add(kit.pluck_bass(f, BEAT * 0.42), t, 0.9)
            bass.add(kit.pluck_bass(f, BEAT * 0.30), t + BEAT * 0.5, 0.72)
            if beat_in_bar in (1, 3):
                bass.add(kit.pluck_bass(fifth, BEAT * 0.26), t + BEAT * 0.75, 0.5)
        t += BEAT
    # Sub under the drop gets an octave-down reinforcement.
    t = T_DROP
    while t < T_FADEWHITE:
        ch = chord_at(t)
        bass.add(kit.sub(note(f'{ROOTS[ch]}1'), BAR * 0.98), t, 0.5)
        t += BAR
    bass_buf = apply(Pedalboard([LowpassFilter(cutoff_frequency_hz=2600),
                                 Distortion(drive_db=6)]), bass.out())

    # --- arpeggio ----------------------------------------------------------
    arp = Mix(DURATION)
    bar_start = 0.0
    while bar_start < T_FADEWHITE:
        ch = chord_at(bar_start)
        tones = CHORD_TONES[ch]
        for step in range(16):
            t = _step_time(bar_start, step, cfg)
            if t >= T_FADEWHITE:
                break
            if T_BREAK <= t < T_DROP:
                continue
            idx = cfg.arp_pattern[step % len(cfg.arp_pattern)]
            octv = cfg.arp_octave + (1 if step % 8 >= 4 else 0)
            f = note(f'{tones[idx % len(tones)]}{octv}')
            length = STEP * 1.9
            v = supersaw(f, int(length * SR), voices=5,
                         detune_cents=cfg.arp_detune, stereo_spread=0.7)
            env = perc_env(v.shape[1], 0, curve=3.4)
            arp.add(v * env, t, 0.55)
        bar_start += BAR
    # The filter sweep IS the arrangement: closed in the intro, opening through
    # the builds, wide open on the drop.
    cutoff = np.interp(
        np.linspace(0, DURATION, 2000),
        [0.0, 2.0, 7.0, 10.0, 13.0, T_BREAK, T_PROMOTION, T_DROP, 24.0, DURATION],
        [260, 520, cfg.build_filter_hz, 1500, 2600, 1200, 700,
         cfg.drop_filter_hz, cfg.drop_filter_hz, 3000],
    )
    arp_buf = sweep_ladder(arp.out(), cutoff, resonance=0.42, drive=2.0)
    arp_buf = apply(pingpong(BEAT * 0.75, feedback=0.28, mix=0.20), arp_buf)

    # --- chords ------------------------------------------------------------
    chords = Mix(DURATION)
    if cfg.chord_style != 'none':
        bar_start = 0.0
        while bar_start < T_FADEWHITE:
            ch = chord_at(bar_start)
            tones = CHORD_TONES[ch]
            if cfg.chord_style == 'pad':
                if bar_start >= 10.0 and not (T_BREAK <= bar_start < T_DROP):
                    for i, tn in enumerate(tones):
                        f = note(f'{tn}{3 if i == 0 else 4}')
                        v = supersaw(f, int(BAR * 0.98 * SR), voices=7,
                                     detune_cents=22, stereo_spread=0.95)
                        env = np.ones(v.shape[1])
                        a = int(0.05 * SR)
                        env[:a] = np.linspace(0, 1, a)
                        env[-int(0.12 * SR):] = np.linspace(1, 0, int(0.12 * SR))
                        chords.add(v * env, bar_start, 0.33)
            else:  # stab: short filtered chord on offbeats, French house
                if bar_start >= 7.0 and not (T_BREAK <= bar_start < T_DROP):
                    for step in cfg.stab_steps:
                        t = _step_time(bar_start, step, cfg)
                        for i, tn in enumerate(tones):
                            f = note(f'{tn}{4 if i < 2 else 3}')
                            v = supersaw(f, int(STEP * 2.2 * SR), voices=5,
                                         detune_cents=16, stereo_spread=0.8)
                            chords.add(v * perc_env(v.shape[1], 0, curve=5.5), t, 0.30)
            bar_start += BAR
    # Breakdown pad: the one sustained thing in the record, holding F then G.
    for start, ch, gain in [(T_BREAK, 'F', 0.34), (T_PROMOTION, 'G', 0.46)]:
        length = (T_PROMOTION - T_BREAK) if ch == 'F' else (T_DROP - T_PROMOTION + 0.6)
        for i, tn in enumerate(CHORD_TONES[ch]):
            f = note(f'{tn}{3 if i == 0 else 4}')
            v = supersaw(f, int(length * SR), voices=7, detune_cents=26)
            env = np.ones(v.shape[1])
            a = int(0.18 * SR)
            env[:a] = np.linspace(0, 1, a)
            env[-int(0.25 * SR):] = np.linspace(1, 0, int(0.25 * SR))
            chords.add(v * env, start, gain)
    chord_cut = np.interp(np.linspace(0, DURATION, 800),
                          [0, 7.0, T_BREAK, T_PROMOTION, T_DROP, DURATION],
                          [400, 1400, 900, 2200, 9000, 4000])
    chord_buf = sweep_ladder(chords.out(), chord_cut, resonance=0.30, drive=1.4)
    chord_buf = apply(room(size=0.42, wet=0.20), chord_buf)

    # --- lead --------------------------------------------------------------
    lead = Mix(DURATION)
    if cfg.lead_style != 'none':
        # Three statements: a hint under the gameplay build (quiet, an octave
        # down), the full tune on the drop, and the counter-melody answering it.
        statements = [
            (10.0, MELODY, -1, 0.30, 4),   # build C: hint
            (13.0, MELODY, -1, 0.42, 4),   # peak A: louder hint
            (T_DROP, MELODY, 0, 1.00, 8),  # the drop: full
            (T_DROP, COUNTER, 0, 0.55, 8),  # the answer
        ]
        for origin, phrase, oct_shift, vel, bars in statements:
            for bar_off, step, tone_i, octv in phrase:
                if bar_off >= bars:
                    continue
                t = origin + bar_off * BAR + step * STEP
                if t >= T_FADEWHITE or t < origin:
                    continue
                if T_BREAK <= t < T_DROP:
                    continue
                ch = chord_at(t)
                f = note(f'{CHORD_TONES[ch][tone_i]}{octv + oct_shift}')
                length = STEP * 5
                if cfg.lead_style == 'square':
                    raw = square(f, int(length * SR))
                    v = np.stack([raw, raw])
                elif cfg.lead_style == 'talkbox':
                    ln = int(length * SR)
                    v = supersaw(f, ln, voices=7, detune_cents=16)
                    # One vowel per note, gliding into the next: the line reads
                    # as a robot singing rather than as a filter sweep.
                    vowels = 'aeiou'
                    seq = [(0.0, vowels[(bar_off * 3 + step) % 5]),
                           (length * 0.45, vowels[(bar_off * 3 + step + 2) % 5]),
                           (length * 0.8, vowels[(bar_off + step) % 5])]
                    v = formant_bank(v, seq, length) * 1.5
                else:
                    v = supersaw(f, int(length * SR), voices=7, detune_cents=20)
                env = perc_env(v.shape[1], 0, curve=2.2)
                lead.add(v * env, t, vel)
    lead_buf = apply(Pedalboard([
        LadderFilter(mode=LadderFilter.Mode.LPF12, cutoff_hz=6500, resonance=0.25, drive=1.5),
        Chorus(rate_hz=0.6, depth=0.25, mix=0.3),
        Reverb(room_size=0.45, wet_level=0.22, dry_level=1.0),
    ]), lead.out())

    # --- intro bed ---------------------------------------------------------
    # The logo drop needs something under it. A low drone plus a reverse swell
    # that arrives on the first kick, so bar 1 reads as anticipation rather
    # than as dead air.
    intro = Mix(DURATION)
    root = note('A1')
    drone_n = int(2.4 * SR)
    drone = (sine(root, drone_n) * 0.6 + sine(root * 2, drone_n) * 0.25)
    denv = np.concatenate([np.linspace(0, 1, int(0.5 * SR)),
                           np.ones(drone_n - int(0.5 * SR))])
    denv[-int(0.4 * SR):] *= np.linspace(1, 0, int(0.4 * SR))
    intro.add(np.stack([drone, drone]) * denv, 0.0, 0.55)
    rev_n = int(1.9 * SR)
    rev = rng.normal(0, 1, rev_n) * np.linspace(0, 1, rev_n) ** 3.2
    intro.add(apply(Pedalboard([
        HighpassFilter(cutoff_frequency_hz=1200),
        Reverb(room_size=0.75, wet_level=0.55, dry_level=0.5),
    ]), np.stack([rev, rev])), 2.0 - 1.9, 0.30)
    mix.add(intro.out(), 0.0, 1.0)

    # --- risers and impacts ------------------------------------------------
    fx = Mix(DURATION)
    # Noise riser into the drop.
    rise_len = T_DROP - T_BREAK
    rn = int(rise_len * SR)
    riser = rng.normal(0, 1, rn)
    rise_cut = np.linspace(300, 11_000, 400) ** 1.0
    riser_st = sweep_ladder(np.stack([riser, riser]) * np.linspace(0.05, 1.0, rn) ** 2,
                            rise_cut, resonance=0.5, drive=1.2,
                            mode=LadderFilter.Mode.HPF12)
    fx.add(riser_st, T_BREAK, 0.16)

    # Impacts on the two eliminations and the promotion.
    for t, g in [(T_KILL_1, 0.55), (T_KILL_2, 0.45), (T_PROMOTION, 0.7), (T_DROP, 0.8)]:
        imp_n = int(0.9 * SR)
        imp = rng.normal(0, 1, imp_n) * np.exp(-np.linspace(0, 7, imp_n))
        low = sine(58, imp_n) * np.exp(-np.linspace(0, 5, imp_n))
        body = np.stack([imp * 0.35 + low, imp * 0.35 + low])
        fx.add(apply(Pedalboard([LowpassFilter(cutoff_frequency_hz=2400)]), body), t, g)

    # Reverse swell before the drop.
    sw_n = int(1.2 * SR)
    swell = rng.normal(0, 1, sw_n) * np.linspace(0, 1, sw_n) ** 3
    fx.add(apply(Pedalboard([
        HighpassFilter(cutoff_frequency_hz=2000),
        Reverb(room_size=0.7, wet_level=0.5, dry_level=0.6),
    ]), np.stack([swell, swell])), T_DROP - 1.2, 0.22)

    # Final chord under the end slate: the tonic, struck once at the fadewhite
    # and left to ring past the last frame.
    for i, tn in enumerate(CHORD_TONES['Am']):
        f = note(f'{tn}{3 if i == 0 else 4}')
        v = supersaw(f, int((DURATION - T_FADEWHITE) * SR), voices=7, detune_cents=24)
        env = np.ones(v.shape[1])
        env[:int(0.03 * SR)] = np.linspace(0, 1, int(0.03 * SR))
        chords.add(v * env, T_FADEWHITE, 0.30)
    chord_buf = sweep_ladder(chords.out(), chord_cut, resonance=0.30, drive=1.4)
    chord_buf = apply(room(size=0.42, wet=0.20), chord_buf)

    # --- pump and sum ------------------------------------------------------
    # Release is tied to the tempo: the envelope should still be recovering
    # when the next kick lands, which is what makes the track breathe rather
    # than merely dip. 0.42 s against a 0.5 s beat gap.
    pump = sidechain(n_total, kicks, depth=cfg.pump_depth, release=0.42)
    # Everything melodic ducks; drums and impacts do not.
    mix.add(bass_buf * pump, 0.0, cfg.bass_gain)
    mix.add(arp_buf * pump, 0.0, cfg.arp_gain)
    mix.add(chord_buf * pump, 0.0, cfg.chord_gain)
    mix.add(lead_buf * pump, 0.0, cfg.lead_gain)
    mix.add(fx.out(), 0.0, 1.0)

    out = mix.out() * section_envelope(n_total)

    # Outro: everything decays under the held end slate rather than stopping.
    tail_start = int(T_FADEWHITE * SR)
    tail = np.ones(n_total)
    tail[tail_start:] = np.linspace(1.0, 0.55, n_total - tail_start) ** 1.1
    out = out * tail

    # Gentle glue only. A hard bus compressor here would undo the section
    # envelope above - the loud sections would be pulled down to meet the quiet
    # ones and the arrangement would flatten right back out.
    out = apply(glue(threshold_db=-8, ratio=1.6), out)
    out = apply(master_chain(ceiling_db=-1.0), out)
    return true_peak_safe(block_dc(out), ceiling_db=-1.0)


VARIATIONS = [
    Voicing(
        name='grid',
        blurb='Tron Legacy. Dark, minor, arpeggio-driven, heavy sub, wide supersaw pads.',
        arp_octave=4, arp_pattern=(0, 1, 2, 1, 2, 0, 1, 2), arp_gain=0.34, arp_detune=16,
        chord_style='pad', chord_gain=0.30,
        bass_style='sub', bass_gain=0.70, bass_octave=1,
        hat_16ths=True, clap_gain=0.42, pump_depth=0.90,
        build_filter_hz=800, drop_filter_hz=14_000, lead_style='saw', lead_gain=0.36,
    ),
    Voicing(
        name='discovery',
        blurb='Random Access Memories. Filtered chord stabs on the offbeat, plucked '
              'bassline, brighter and funkier.',
        arp_octave=5, arp_pattern=(0, 2, 1, 2, 0, 1, 2, 1), arp_gain=0.20, arp_detune=10,
        chord_style='stab', chord_gain=0.40, stab_steps=(2, 6, 10, 14),
        bass_style='pluck', bass_gain=0.58, bass_octave=2,
        hat_16ths=True, open_hat_offbeat=True, clap_gain=0.60, pump_depth=0.82,
        build_filter_hz=1200, drop_filter_hz=16_000, swing=0.06,
        lead_style='square', lead_gain=0.26,
    ),
    Voicing(
        name='robot',
        blurb='Daft Punk talkbox. Formant-filtered lead that "speaks" the melody '
              'over French-house stabs - the Around the World / Harder Better signature.',
        arp_octave=5, arp_pattern=(0, 2, 1, 2, 0, 1, 2, 1), arp_gain=0.18, arp_detune=12,
        chord_style='stab', chord_gain=0.36, stab_steps=(2, 6, 10, 14),
        bass_style='pluck', bass_gain=0.62, bass_octave=2,
        hat_16ths=True, open_hat_offbeat=True, clap_gain=0.58, pump_depth=0.86,
        build_filter_hz=1100, drop_filter_hz=15_000, swing=0.05,
        lead_style='talkbox', lead_gain=0.52,
    ),
    Voicing(
        name='opus',
        blurb='Eric Prydz. Hypnotic 16th arp all the way through, long filter opening, '
              'huge sustained drop.',
        arp_octave=4, arp_pattern=(0, 1, 2, 0, 1, 2, 1, 0), arp_gain=0.40, arp_detune=20,
        chord_style='pad', chord_gain=0.34,
        bass_style='reese', bass_gain=0.60, bass_octave=1,
        hat_16ths=True, clap_gain=0.46, pump_depth=0.93,
        build_filter_hz=600, drop_filter_hz=17_000, lead_style='saw', lead_gain=0.40,
    ),
]
