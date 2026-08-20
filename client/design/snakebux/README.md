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
