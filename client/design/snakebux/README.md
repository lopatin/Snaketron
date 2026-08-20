# The Snakebux coin

Two drawings, not one file scaled twice.

`snake-bux.png` (256px) is the detailed coin: double rim, fine bevel on the
letter. `snake-bux-small.png` (64px) is drawn for roughly 20 pixels: one rim,
one glare, a thicker brighter letter. `SnakeBuxIcon` picks between them at 24px,
because the detail in the first turns to mud below about a line of text — and
the letter, the only part that says *which* currency this is, goes first.

## How they are made

The metal comes from Gemini (`gemini-3-pro-image`; `gemini-3.1-flash-image` is
the Flash equivalent and also fine — never 2.5, which follows structural
instructions poorly). The letter does not: the model will not reproduce an
exact letterform on request, and this one has to match the favicon the site
already uses. So `trace_s.py` recovers the outline from
`snaketron-favicon-32x32.ico` — collecting the unit edges where a filled pixel
meets an empty one, then dropping the points that lie on a line — and
`strike.py` strikes that outline into a generated blank.

The single rim is also drawn here rather than asked for. Across roughly ten
attempts spanning three models and three prompt framings — including asking for
a "20 pixel icon" in the hope the small framing would drop the detail — every
generated coin came back with a second ring inside the first. At 20 pixels two
dark lines a pixel apart are one muddy line. `strike.py` cuts the face out
inside whatever rings the model drew and puts one rim around it.

Both edges are painted over whatever the model produced. On the detailed coin
the model's inner ring is a hard black line, which at that diameter reads as a
gap cut through the metal; it is repainted as a lit edge running white → gold →
deep gold, and it needs that middle stop because a straight ramp from white to
brown-gold runs through grey and reads as a pencil line. On the small coin the
whole rim is gold rather than black — a black ring thick enough to survive
twenty pixels is most of what you see of the coin — and it is a gradient for a
reason beyond looks: the lit end holds the silhouette against a dark page and
the shaded end holds it against a white one. Measured on the shipped asset,
the dark end is 9.1:1 against white and the light end 10.7:1 against #18181b,
so neither background can swallow it.

Cut the small coin's face *inside* the model's own inner ring
(`face_inset=INNER_RING[0]`), not outside it, or that ring survives as a dark
circle sitting inside the new rim. And note `face_only` crops to the disc it
keeps: without that, resizing it to fit inside a rim scales the transparent
margin too, insetting the face twice and leaving a rim several times thicker
than asked for.

Generate the two blanks in one family and check they agree on colour. Asked
separately, the detailed blank came back amber (219,147,34) and the small one
bright yellow (248,201,6) — a coin that changes hue as it scales. Passing the
small one back as a colour reference for the large one closed it to
(247,199,4).

```bash
# 1. re-trace the letter (only if the favicon changes)
python3 trace_s.py s-ring.json /tmp/proof.png
# 2. strike it into the blanks
python3 strike.py
```

`blank-hi.png` and `blank-lo.png` are the generated blanks, kept so the assets
can be rebuilt without paying for generation again. They sit on flat magenta,
which `strike.py` keys out — the model paints a checkerboard when asked for
transparency rather than producing an alpha channel.

## What an unguided run produces

`notes/unguided-attempt.png` is `gemini-3-pro-image` asked only to "design a
gold coin icon in high and low fidelity, both legible on white and on dark",
with no references and no mention of this project. It is worth keeping because
of how far it lands from anything usable here: a photoreal lion crest for the
detailed one and literal pixel-art for the small one — two different rendering
media rather than two fidelities of one icon — plus an invented "G" and baked-in
caption text. The lesson is that "high and low fidelity" reads to the model as a
question about *style*, where here it is a question about how much detail
survives a downscale. The guided path exists for that reason.
