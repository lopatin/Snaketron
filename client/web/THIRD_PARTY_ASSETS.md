# Third-party assets

## Crash explosion

- Source: [Sophisticated Explosion by elnineo](https://opengameart.org/content/sophisticated-explosion-0)
- License: [Creative Commons Zero (CC0 1.0)](https://creativecommons.org/publicdomain/zero/1.0/)
- Local file: `public/images/crash-explosion.png`
- Modification: resized from 2048×2048 to 1024×1024. The 8×8 grid remains 64 frames, with 128×128 pixels per frame.

Attribution is not required by CC0; the source is recorded here for provenance and thanks.

## Animal coat textures

The six textures under `public/images/skins/` are built from three third-party
sources by `client/design/tools/build_coat_textures.py`. That script is the only
thing that should ever write those PNGs; it records the source URLs inline and
fetches them on demand into a local cache that is not committed.

Each animal ships twice: the source texture cropped into a tiling strip
(`<name>.v1.png`), and a flat posterised print derived from the same pixels
(`<name>-print.v1.png`).

| Texture | Source | License |
| --- | --- | --- |
| `zebra.v1.png`, `zebra-print.v1.png` | [Fur of Tiger, Giraffe and Zebra](https://opengameart.org/content/fur-of-tiger-giraffe-and-zebra) on OpenGameArt, from publicdomainpictures.net | [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) |
| `tiger.v1.png`, `tiger-print.v1.png` | the same OpenGameArt submission | [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) |
| `jaguar.v1.png`, `jaguar-print.v1.png` | [Leopard print](https://opengameart.org/content/leopard-print) by **diamond-sparrow** on OpenGameArt | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — **attribution required** |

Modifications in every case: cropped to a band, made to tile by inpainting the
join with [LaMa](https://github.com/enesmsahin/simple-lama-inpainting), scaled
to a one-cell-tall strip, and (for the `-print` variants) posterised to two or
three flat tones. Roughly 13% of each shipped strip is model-generated — the
join — and the rest is the source.

The jaguar textures are a **spotted-cat print rather than a jaguar
specifically** — a real jaguar's rosettes carry spots inside them and a
leopard's do not, and no freely licensed game texture makes that distinction.
The skin is named for the animal it dresses a snake as.

## Sprite sheets

The four sheets under `public/images/skins/` whose names end `-live`, plus
`stars-and-stripes.v1.png` and `race-livery.v1.png`, are built by
`client/design/tools/sprite_sheet.py` from images generated with **ChatGPT**
(OpenAI image generation) at the repository owner's request.

| Sheet | Source |
| --- | --- |
| `zebra-live.v1.png` | ChatGPT-generated seamless zebra hide |
| `tiger-live.v1.png` | ChatGPT-generated seamless tiger fur |
| `stars-and-stripes.v1.png` | ChatGPT-generated waving-flag texture |
| `race-livery.v1.png` | ChatGPT-generated racing-decal texture |

The generated sources are committed under `client/design/sprites/`, unlike the
coat textures above. Those are fetched from recorded URLs and so are
reproducible from the script alone; these are one-off generations with no other
copy, and without them the sheets could never be rebuilt at a different cell
size, row count, or rotation.

Modifications: downsampled to 16 texels per cell, rotated where the recipe says
so, both wrap joins measured and repaired with
[LaMa](https://github.com/enesmsahin/simple-lama-inpainting), and quantised to a
128-colour palette. The repaired slice is 8–23px of a 320px axis, so the large
majority of each sheet is the source.
