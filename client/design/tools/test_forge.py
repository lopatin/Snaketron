"""Contract tests for the texture forge's Phase 0 command surface."""

from __future__ import annotations

import argparse
import hashlib
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


def moving_band(*, columns: int = 2, rows: int = 4, cell: int = 16) -> Image.Image:
    width = columns * cell
    pixels = np.full((rows * cell, width, 3), (12, 24, 36), dtype=np.uint8)
    x = np.arange(width)
    for frame in range(rows):
        selected = ((x - frame * width // rows) % width) < width // rows
        pixels[frame * cell : (frame + 1) * cell, selected] = (245, 210, 32)
    return Image.fromarray(pixels)


def bounded_band(*, columns: int = 2, rows: int = 4, cell: int = 16, overhang: int = 4) -> Image.Image:
    side = cell * overhang // 16
    row_texels = cell + 2 * side
    width = columns * cell
    pixels = np.zeros((rows * row_texels, width, 4), dtype=np.uint8)
    x = np.arange(width)
    for frame in range(rows):
        top = frame * row_texels
        body_top = top + side
        pixels[body_top : body_top + cell, :, :] = (12, 24, 36, 255)
        selected = ((x - frame * width // rows) % width) < width // rows
        pixels[body_top : body_top + cell, selected, :] = (245, 210, 32, 255)
        # A real semi-transparent apron sentinel proves that RGBA data is not
        # flattened while the first/last scanlines remain hard transparent.
        pixels[top + 1, selected, :] = (255, 40, 90, 128)
    return Image.fromarray(pixels, mode="RGBA")


class ParseAxesTests(unittest.TestCase):
    def test_axes_are_explicit_and_independent(self):
        self.assertEqual(forge.parse_axes("none"), ())
        self.assertEqual(forge.parse_axes("x"), (1,))
        self.assertEqual(forge.parse_axes("y"), (0,))
        self.assertEqual(forge.parse_axes("x,y"), (1, 0))

    def test_invalid_or_duplicate_axes_are_rejected(self):
        for value in ("", "z", "x,x", "x,none"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
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
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(forge, "measure") as measure:
            manifest = forge.forge(image, "sheet", 2, 3, (1,), Path(temporary))
        self.assertFalse(manifest.accepted)
        self.assertIn("must check the y seam", manifest.rejection)
        measure.assert_not_called()

    def test_four_pixel_bleed_is_a_24px_stored_row_not_a_larger_body_cell(self):
        valid = bounded_band(columns=3, rows=5)
        self.assertIsNone(forge.shape_problem(valid, "sheet", 3, 5, raster_overhang_px=4))

        square_rows = Image.new("RGBA", (3 * 16, 5 * 16), (0, 0, 0, 0))
        problem = forge.shape_problem(square_rows, "sheet", 3, 5, raster_overhang_px=4)
        self.assertIn("4px top bleed apron", problem)
        self.assertIn("unchanged 16px logical body cell", problem)

    def test_nonzero_bleed_requires_rgba_and_transverse_clearance(self):
        rgb = Image.new("RGB", (2 * 16, 3 * 24), (20, 40, 60))
        self.assertIn("alpha channel", forge.alpha_problem(rgb, "sheet", 3, 16, 4))

        touched = bounded_band(columns=2, rows=3)
        touched.putpixel((0, 0), (255, 255, 255, 255))
        self.assertIn(
            "touches a transverse stored-row edge",
            forge.alpha_problem(touched, "sheet", 3, 16, 4),
        )


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
        with (
            tempfile.TemporaryDirectory() as temporary,
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
        image = moving_band(rows=3)
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(forge, "measure", return_value=seam()) as measure,
            mock.patch.object(forge, "build_ladder", return_value=[]),
        ):
            manifest = forge.forge(image, "sheet", 2, 3, (0,), Path(temporary))

        self.assertTrue(manifest.accepted)
        self.assertEqual(manifest.body_columns, 2)
        self.assertEqual(manifest.frame_rows, 3)
        self.assertEqual(manifest.seam_axes, ["y"])
        self.assertEqual(measure.call_args_list, [mock.call(image, 0), mock.call(image, 0)])

    def test_static_declared_frame_cells_are_rejected_as_non_animation(self):
        image = Image.new("RGB", (2 * 16, 64 * 16), (20, 40, 60))
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(forge, "measure", return_value=seam()),
            mock.patch.object(forge, "build_ladder") as ladder,
        ):
            manifest = forge.forge(image, "sheet", 2, 64, (0,), Path(temporary))

        self.assertFalse(manifest.accepted)
        self.assertIn("no measurable frame-to-frame animation", manifest.rejection)
        ladder.assert_not_called()

    def test_horizontal_motion_with_constant_row_means_is_valid_temporal_art(self):
        image = moving_band(rows=4)

        # Every scanline has the same mean; the obsolete 1-D period inference
        # therefore called this a one-frame image despite four distinct cells.
        profile = np.asarray(image, dtype=np.float64).mean(axis=(1, 2))
        self.assertTrue(np.allclose(profile, profile[0]))
        self.assertIsNone(forge.temporal_problem(image, 4))

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

    def test_bounded_rgba_ladder_scales_body_and_aprons_independently(self):
        image = bounded_band(columns=2, rows=4)
        with tempfile.TemporaryDirectory() as temporary:
            rungs = forge.build_ladder(
                image,
                "sheet",
                Path(temporary),
                body_columns=2,
                frame_rows=4,
                raster_overhang_px=4,
            )

            self.assertEqual(
                [(r.texels_per_cell, r.row_texels, r.width_px, r.height_px) for r in rungs],
                [(16, 24, 32, 96), (8, 12, 16, 48)],
            )
            for rung in rungs:
                with Image.open(rung.path) as opened:
                    self.assertEqual(opened.mode, "RGBA")
                    alpha = np.asarray(opened.getchannel("A"))
                for row in range(4):
                    self.assertFalse(alpha[row * rung.row_texels].any())
                    self.assertFalse(alpha[(row + 1) * rung.row_texels - 1].any())
                self.assertGreater(int(alpha.max()), 0)

    def test_real_forge_accepts_a_bounded_rgba_clip_sheet(self):
        image = bounded_band(columns=7, rows=4)
        with tempfile.TemporaryDirectory() as temporary:
            manifest = forge.forge(
                image,
                "sheet",
                7,
                4,
                (0,),
                Path(temporary),
                raster_overhang_px=4,
            )

        self.assertTrue(manifest.accepted, manifest.rejection)
        self.assertEqual(manifest.texels_per_cell, 16)
        self.assertEqual(manifest.raster_overhang_px, 4)
        self.assertEqual(manifest.row_texels, 24)
        self.assertEqual([rung.row_texels for rung in manifest.rungs], [24, 12])

    def test_every_shipping_rung_is_remeasured(self):
        image = Image.new("RGB", (2 * 64, 64), (20, 40, 60))
        # Initial source, final canonical, then the 64 and 32 rungs clear. The
        # 16-texel PNG regresses and must make the whole forge refuse.
        readings = [seam(), seam(), seam(), seam(), seam(cleared=False)]
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(forge, "measure", side_effect=readings) as measure,
        ):
            manifest = forge.forge(image, "coat", 2, None, (1,), Path(temporary))

        self.assertFalse(manifest.accepted)
        self.assertIn("shipping 16-texel rung", manifest.rejection)
        self.assertEqual(measure.call_count, 5)
        self.assertEqual([rung.texels_per_cell for rung in manifest.rungs], [64, 32, 16])
        self.assertIsNotNone(manifest.failed_output)

    def test_failed_post_lama_pixels_are_written_and_hashed(self):
        image = Image.new("RGB", (2 * 64, 64), (20, 40, 60))
        repaired = Image.new("RGB", image.size, (211, 17, 99))
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with (
                mock.patch.object(
                    forge,
                    "measure",
                    side_effect=[seam(cleared=False), seam(cleared=False)],
                ),
                mock.patch.object(forge.sheets, "load_lama", return_value=object()),
                mock.patch.object(forge, "repair", return_value=(repaired, True)),
            ):
                manifest = forge.forge(image, "coat", 2, None, (1,), output)

            self.assertFalse(manifest.accepted)
            self.assertTrue(manifest.repaired)
            self.assertEqual(manifest.repair_methods, ["roll:x"])
            self.assertIsNotNone(manifest.failed_output)
            failed = manifest.failed_output
            assert failed is not None
            exact = Path(failed.path).read_bytes()
            self.assertEqual(hashlib.sha256(exact).hexdigest(), failed.sha256)
            self.assertEqual(len(exact), failed.bytes)
            with Image.open(failed.path) as retained:
                self.assertEqual(retained.getpixel((0, 0)), (211, 17, 99))

    def test_exact_inspection_never_repairs_resizes_or_builds_a_ladder(self):
        image = Image.new("RGB", (2 * 64, 64), (20, 40, 60))
        before = np.asarray(image).copy()
        with (
            mock.patch.object(forge, "measure", return_value=seam()),
            mock.patch.object(forge, "repair") as repair,
            mock.patch.object(forge, "repair_crop_tx_t") as crop_repair,
            mock.patch.object(forge, "build_ladder") as ladder,
        ):
            result = forge.inspect_existing(image, "coat", 2, None, (1,), 64)

        self.assertTrue(result["accepted"])
        self.assertEqual(result["mode"], "inspect_existing")
        self.assertTrue(np.array_equal(np.asarray(image), before))
        repair.assert_not_called()
        crop_repair.assert_not_called()
        ladder.assert_not_called()


if __name__ == "__main__":
    unittest.main()
