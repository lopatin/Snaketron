#!/usr/bin/env python3
"""Losslessly recompress the deterministic documentation screenshots."""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    for path in sorted(args.directory.glob("*.png")):
        with Image.open(path) as image:
            image.save(path, format="PNG", optimize=True, compress_level=9)
        print(f"{path}: {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
