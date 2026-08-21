"""Contract tests for the texture forge's Phase 0 command surface."""

from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent))
import forge


def seam(*, cleared: bool = True) -> forge.Measurement:
    return forge.Measurement(
        ratio=0.1 if cleared else 2.0,
        rank=0.2 if cleared else 0.99,
        cleared=cleared,
        complaint=None if cleared else "fine seam still 2.00x",
        structural=None,
    )


class ParseAxesTests(unittest.TestCase):
    def test_axes_are_explicit_and_independent(self):
        self.assertEqual(forge.parse_axes("none"), ())
        self.assertEqual(forge.parse_axes("x"), (1,))
        self.assertEqual(forge.parse_axes("y"), (0,))
        self.assertEqual(forge.parse_axes("x,y"), (1, 0))

    def test_invalid_or_duplicate_axes_are_rejected(self):
        for value in ("", "z", "x,x", "x,none"):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    forge.parse_axes(value)


class ShapeContractTests(unittest.TestCase):
    def test_body_columns_and_frame_rows_are_independent(self):
        valid = Image.new("RGB", (3 * 16, 5 * 16))
        self.assertIsNone(forge.shape_problem(valid, "sheet", 3, 5))

        wrong_width = Image.new("RGB", (5 * 16, 5 * 16))
        self.assertIn(
            "3 body columns",
            forge.shape_problem(wrong_width, "sheet", 3, 5),
        )

        wrong_height = Image.new("RGB", (3 * 16, 3 * 16))
        self.assertIn(
            "5 frame rows",
            forge.shape_problem(wrong_height, "sheet", 3, 5),
        )

    def test_sheet_requires_y_seam_but_not_x_seam(self):
        image = Image.new("RGB", (2 * 16, 3 * 16))
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(forge, "measure") as measure:
                manifest = forge.forge(
                    image, "sheet", 2, 3, (1,), Path(temporary)
                )
        self.assertFalse(manifest.accepted)
        self.assertIn("must check the y seam", manifest.rejection)
        measure.assert_not_called()


class ForgeTests(unittest.TestCase):
    def test_structural_crop_gets_one_tx_t_attempt_and_records_the_method(self):
        image = Image.new("RGB", (2 * 64, 64), (20, 40, 60))
        structural = forge.Measurement(
            ratio=4.0,
            rank=0.99,
            cleared=False,
            complaint="coarse join",
            structural="halves disagree at scale 8",
        )
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch.object(forge, "measure", side_effect=[structural, seam(), seam()]),
                mock.patch.object(forge.sheets, "load_lama", return_value=object()),
                mock.patch.object(forge, "repair_crop_tx_t", return_value=image) as repair,
                mock.patch.object(forge, "build_ladder", return_value=[]),
            ):
                manifest = forge.forge(image, "coat", 2, None, (1,), Path(temporary))

        self.assertTrue(manifest.accepted)
        self.assertTrue(manifest.repaired)
        self.assertEqual(manifest.repair_methods, ["tx_t:x"])
        repair.assert_called_once()

    def test_tx_t_canvas_presents_identical_t_copies_and_restores_alpha(self):
        image = Image.new("RGBA", (64, 32), (10, 20, 30, 128))

        def fake_lama(canvas, mask):
            pixels = np.asarray(canvas).copy()
            selected = np.asarray(mask) > 0
            self.assertTrue(np.array_equal(pixels[:, :64], pixels[:, -64:]))
            self.assertEqual(int(selected.sum()), 32 * 10)
            pixels[selected] = (40, 50, 60)
            return Image.fromarray(pixels)

        repaired = forge.repair_crop_tx_t(image, 1, fake_lama)
        self.assertEqual(repaired.size, image.size)
        self.assertEqual(repaired.mode, "RGBA")
        self.assertEqual(repaired.getchannel("A").getextrema(), (128, 128))

    def test_manifest_records_shape_and_only_checks_requested_axes(self):
        image = Image.new("RGB", (2 * 16, 3 * 16))
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch.object(forge, "measure", return_value=seam()) as measure,
                mock.patch.object(forge, "build_ladder", return_value=[]),
                mock.patch.object(
                    forge.sheets,
                    "frames_from_period",
                    return_value=(image, 16, 3, 0),
                ) as frames,
            ):
                manifest = forge.forge(
                    image, "sheet", 2, 3, (0,), Path(temporary)
                )

        self.assertTrue(manifest.accepted)
        self.assertEqual(manifest.body_columns, 2)
        self.assertEqual(manifest.frame_rows, 3)
        self.assertEqual(manifest.seam_axes, ["y"])
        self.assertEqual(measure.call_args_list, [mock.call(image, 0), mock.call(image, 0)])
        frames.assert_called_once_with(image, 3, forge.sheets.CELL, 2)

    def test_ladder_dimensions_follow_real_image_shape(self):
        image = Image.new("RGB", (2 * 16, 4 * 16), (20, 40, 60))
        with tempfile.TemporaryDirectory() as temporary:
            rungs = forge.build_ladder(image, "sheet", Path(temporary))

        self.assertEqual(
            [(r.texels_per_cell, r.width_px, r.height_px) for r in rungs],
            [(16, 32, 64), (8, 16, 32)],
        )
        self.assertTrue(all(r.bytes > 0 for r in rungs))
        self.assertTrue(all(len(r.sha256) == 64 for r in rungs))

    def test_every_shipping_rung_is_remeasured(self):
        image = Image.new("RGB", (2 * 64, 64), (20, 40, 60))
        # Initial source, final canonical, then the 64 and 32 rungs clear. The
        # 16-texel PNG regresses and must make the whole forge refuse.
        readings = [seam(), seam(), seam(), seam(), seam(cleared=False)]
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(forge, "measure", side_effect=readings) as measure:
                manifest = forge.forge(
                    image, "coat", 2, None, (1,), Path(temporary)
                )

        self.assertFalse(manifest.accepted)
        self.assertIn("shipping 16-texel rung", manifest.rejection)
        self.assertEqual(measure.call_count, 5)


if __name__ == "__main__":
    unittest.main()
