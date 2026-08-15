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
