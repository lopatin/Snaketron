# Trailer audio generation

Everything the trailer hears is generated from this directory: the music bed
and the event SFX. Nothing here is a recording, and nothing is fetched.

```bash
pip install -r requirements.txt
python3 build.py --out /tmp/audio --variation all
```

Deterministic — same arguments, byte-identical output, verified by the sha256
manifest `build.py` emits. This exists because the audio it replaces was five
opaque committed binaries whose entire provenance was the string "procedurally
synthesized for this repository": nobody could regenerate them, so nobody could
fix them, so they shipped broken.

## What was broken

Measured on the originals, not asserted:

| file | defect |
|---|---|
| `launch-bed.m4a` | a 57–75 dB spectral cliff at 2.5 kHz, 7.4 dB crest — a low drone with clicks, not music |
| `boost.wav` | one narrow 250 Hz–1 kHz band, nothing above 1 kHz. This file *was* the "weird whooshing" |
| `whoosh.wav` | RMS −25.9 dB — broadband but simply too quiet to hear under music |

## The sound

House and French touch, in the Tron Legacy / *Random Access Memories* / Prydz
register. That genre is almost entirely synthesis, which is why procedural
generation wins here rather than merely coping: a TR-909 kick *is* a
pitch-enveloped sine, a supersaw *is* seven detuned saws, and a filter sweep on
that stack is the whole signature. Nothing here imitates a recording.

Four variations ship, selectable with `--variation`. All share one arrangement
and differ in voicing:

| | character |
|---|---|
| `grid` | Tron Legacy. Dark, minor, arp-driven, heaviest sub. **This is the shipped bed.** |
| `discovery` | *Random Access Memories*. Filtered stabs on the offbeat, plucked bass, swung 16ths. |
| `opus` | Prydz. Hypnotic 16ths, longest filter opening, biggest sustained drop. |
| `robot` | The talkbox. Formant bank on vowels so the lead "speaks" the melody. |

What actually makes it sound like the genre, in rough order of importance:

1. **The sidechain pump.** Release must be tempo-tied — at 120 BPM the beat gap
   is 500 ms, and a 240 ms release leaves half of every beat static. The
   envelope should still be recovering when the next kick lands.
2. **The filter sweep is the arrangement.** A resonant ladder closed in the
   intro and opening through the builds does more structural work than adding
   parts. `pedalboard`'s `LadderFilter` has the resonance and saturation a
   plain Butterworth does not.
3. **Band-limited oscillators.** A naive ramp or `sign(sin)` folds harmonics
   back down the spectrum; that aliasing is the single most reliable way to
   make a synth sound cheap. Everything here sums only harmonics under Nyquist.
4. **Detune with random start phase.** Aligned phases at t=0 give an ugly
   impulse instead of a swell.
5. **Section dynamics.** See the README rules below — this is what separates a
   well-synthesized track from an exciting one.

A talkbox without a voice to modulate is a bank of three resonant band-passes
parked on vowel formants (`dsp.formant_bank`), gliding between vowels per note.
Vowels *are* formant positions, so it gets most of the way there.

## Layout

| file | what |
|---|---|
| `dsp.py` | band-limited oscillators, envelopes, ladder sweeps, sidechain, mixing |
| `kit.py` | drums and bass voices |
| `song.py` | the arrangement, and the four variations |
| `sfx.py` | event SFX, built from the same palette as the bed |
| `build.py` | CLI, manifest, determinism |
| `measure.py` `qc.py` `pump.py` | the measurement harness — see below |

## Measure before you trust your ears

The QC scripts exist because each of them caught a real defect that a
spectrogram does not show:

- `measure.py` — band energy and crest, plus a **mono fold-down check**. It
  caught `whoosh` being built from one inverted channel, which is wide in
  stereo and *silent* the moment anything sums to mono, which is most phones.
- `qc.py` — clipping, DC offset, stereo width. It caught the limiter not
  holding its ceiling (samples at full scale despite a −1 dB threshold, which
  clips on lossy conversion) and a DC offset from asymmetric saturation.
- `loudness.py` — K-weighted (BS.1770) level around each cue. Flat RMS
  underweights 1–6 kHz, which is exactly where a bright effect lives; the goal
  flourish measured *below* the bed on RMS and was still the most prominent
  thing in the film.
- `balance.py` — how far each effect pokes above the bed floor. Aim for the
  effect's sustained level to sit within ~1 dB of the bed and let only its
  transient above; ~+5 dB of sustained level is what "the effects are much
  louder than the music" sounds like.
- `pump.py` — the sidechain recovery ramp **between** kicks.

`song.build()` is a pure function of its arguments. It was not: every
noise-based voice draws from one module-level generator, so a second call in
the same process produced different drums. That is deterministic per *process*,
which is not the same thing, and it hid from a check that shelled out for each
run. `kit.reset_rng()` at the top of `build()` is what makes an A/B of one note
actually differ by one note. Measuring at the
  kick catches the kick's own energy and reads as the opposite of ducking.

Two authoring rules the measurements enforce:

1. **Section dynamics are the arrangement.** The first render sat at one RMS
   from the first kick to the last — 4 dB of range across the whole record,
   which is what "not exciting" means in numbers. `SECTION_GAIN` in `song.py`
   authors the curve; the master chain is deliberately gentle so it survives.
2. **Fix an ending with the smallest change that works.** The last note the
   tune plays is A5 over an F chord — the third, high, which reads as a
   question rather than an answer. The fix is that note, not a new section: a
   cadence, a gap and a weighted tonic button were all built, and all were
   worse, because they added complexity the rest of the track does not have.
   Match the density of what is already there. Check what the final note
   actually is before designing anything.
3. **A pitched effect must agree with the chord it lands on.** The goal
   flourish was A–C–E over a bar of G major: no shared tones and a semitone
   between its C and the chord's B. It read as "too loud" and trimming it 6 dB
   changed nothing, because loudness was never the problem — the ear picks a
   clashing note out of a mix at any level. `sfx.bank()` now takes the chord,
   and `song.chord_at()` says what it is at a given second.
4. **Sidechain release is tempo-tied.** At 120 BPM the beat gap is 500 ms; a
   240 ms release leaves half of every beat static and the groove stops
   breathing.
