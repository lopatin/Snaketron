from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import wave
from pathlib import Path


SKILL = Path(__file__).resolve().parent.parent
SCRIPTS = SKILL / "scripts"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(SCRIPTS))

import beats  # noqa: E402
import compile_edl  # noqa: E402
import render  # noqa: E402
import review  # noqa: E402


class CompileEdlTests(unittest.TestCase):
    def test_resolves_meta_through_speed_and_transition_timebases(self) -> None:
        compiled = compile_edl.compile_edl(
            FIXTURES / "fixture.edl.json", FIXTURES / "clips"
        )
        clip = compiled["segments"][1]
        self.assertAlmostEqual(clip["global_start"], 0.4)
        self.assertAlmostEqual(clip["effects"][0]["at_local"], 1.2)
        self.assertAlmostEqual(clip["effects"][0]["at_global"], 1.6)
        self.assertAlmostEqual(clip["output_duration"], 2.4)
        self.assertAlmostEqual(compiled["duration"], 2.8)
        self.assertEqual(compiled["transitions"][0]["ffmpeg"], "fadeblack")

    def test_rejects_synthesized_slow_motion_with_recapture_instruction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(FIXTURES / "clips", root / "clips")
            meta_path = root / "clips" / "fixture" / "meta.json"
            meta = json.loads(meta_path.read_text())
            meta["capture_vfps"] = 59
            meta_path.write_text(json.dumps(meta))
            edl = json.loads((FIXTURES / "fixture.edl.json").read_text())
            edl.pop("music")
            edl["timeline"][2].pop("sfx")
            edl_path = root / "fixture.edl.json"
            edl_path.write_text(json.dumps(edl))
            with self.assertRaisesRegex(
                compile_edl.EdlError, r"recapture at >= 60 vfps"
            ):
                compile_edl.compile_edl(edl_path, root / "clips")

    def test_rejects_unknown_effect(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(FIXTURES / "clips", root / "clips")
            edl = json.loads((FIXTURES / "fixture.edl.json").read_text())
            edl.pop("music")
            edl["timeline"][2]["effects"] = [{"t": "lens_flare", "at": 1.0}]
            edl["timeline"][2].pop("sfx")
            path = root / "fixture.edl.json"
            path.write_text(json.dumps(edl))
            with self.assertRaisesRegex(compile_edl.EdlError, "must be one of"):
                compile_edl.compile_edl(path, root / "clips")

    def test_launch_trailer_is_beat_aligned_and_30_to_45_seconds(self) -> None:
        launch = SKILL / "assets" / "launch-trailer"
        with tempfile.TemporaryDirectory() as directory:
            clips = Path(directory) / "clips"
            clips.mkdir()
            for contract in (launch / "clip-contracts").glob("*.meta.json"):
                destination = clips / contract.name.removesuffix(".meta.json")
                destination.mkdir()
                shutil.copy(contract, destination / "meta.json")
            compiled = compile_edl.compile_edl(
                launch / "launch-trailer.edl.json", clips
            )
            self.assertGreaterEqual(compiled["duration"], 30)
            self.assertLessEqual(compiled["duration"], 45)
            self.assertTrue(compiled["qc"]["all_cut_offsets_within_tolerance"])
            self.assertTrue(
                all(
                    abs(item.get("beat_snap_delta", 0.0)) <= 0.05
                    for item in compiled["transitions"]
                )
            )

    def test_html_fixture_cards_expose_virtual_capture_contract(self) -> None:
        for name in ("rank-up.html", "leaderboard.html"):
            source = (SKILL / "assets" / "cards" / name).read_text()
            self.assertIn("window.__SNAKETRON_CAPTURE__", source)
            self.assertIn("stepMs:", source)
            self.assertIn("cueTrack:", source)

    def test_beat_snapped_hard_cut_does_not_become_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "beats.json").write_text(json.dumps({"beats": [0.0, 0.5, 1.0]}))
            edl = {
                "output": {"w": 320, "h": 180, "fps": 30},
                "music": {
                    "src": "unused.wav",
                    "manifest": "beats.json",
                    "beat_snap": True,
                },
                "timeline": [
                    {"title": {"text": "ONE", "duration": 0.5}},
                    {"title": {"text": "TWO", "duration": 0.5}},
                ],
            }
            path = root / "edl.json"
            path.write_text(json.dumps(edl))
            compiled = compile_edl.compile_edl(path)
            self.assertEqual(compiled["transitions"], [])
            self.assertEqual(len(compiled["cuts"]), 1)
            self.assertFalse(compiled["cuts"][0]["transition"])


class BeatTests(unittest.TestCase):
    @staticmethod
    def _click_track(path: Path, seconds: int = 6, bpm: int = 120) -> None:
        sample_rate = 8000
        period = round(sample_rate * 60 / bpm)
        samples = []
        for index in range(sample_rate * seconds):
            phase = index % period
            value = int(22000 * math.exp(-phase / 20)) if phase < 100 else 0
            samples.append(value)
        with wave.open(str(path), "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(sample_rate)
            wav.writeframes(
                b"".join(value.to_bytes(2, "little", signed=True) for value in samples)
            )

    def test_stdlib_manifest_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            audio = Path(directory) / "click.wav"
            self._click_track(audio)
            first = beats.build_manifest(audio, backend="stdlib")
            second = beats.build_manifest(audio, backend="stdlib")
            self.assertEqual(first, second)
            self.assertGreaterEqual(first["bpm"], 70)
            self.assertLessEqual(first["bpm"], 180)
            self.assertGreater(len(first["beats"]), 4)

    def test_manual_grid_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            audio = Path(directory) / "click.wav"
            self._click_track(audio, seconds=2)
            manifest = beats.build_manifest(audio, bpm=120, duration=2, offset=0.25)
            self.assertEqual(manifest["beats"], [0.25, 0.75, 1.25, 1.75])


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg required"
)
class RenderIntegrationTests(unittest.TestCase):
    def test_preview_final_cache_and_review(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clip_dir = root / "clips" / "fixture"
            clip_dir.mkdir(parents=True)
            shutil.copy(
                FIXTURES / "clips" / "fixture" / "meta.json", clip_dir / "meta.json"
            )
            subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=s=320x180:r=30:d=2",
                    "-c:v",
                    "libx264rgb",
                    "-qp",
                    "0",
                    str(clip_dir / "master.mkv"),
                ],
                check=True,
            )
            subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=110:sample_rate=48000:duration=4",
                    "-c:a",
                    "pcm_s16le",
                    str(root / "fixture.wav"),
                ],
                check=True,
            )
            shutil.copy(FIXTURES / "beats.json", root / "beats.json")
            edl = json.loads((FIXTURES / "fixture.edl.json").read_text())
            edl["timeline"][2]["sfx"][0]["src"] = str(
                SKILL / "assets" / "sfx" / "impact.wav"
            )
            edl_path = root / "fixture.edl.json"
            edl_path.write_text(json.dumps(edl))
            compiled = compile_edl.compile_edl(edl_path, root / "clips")
            compiled_path = root / "compiled.json"
            compiled_path.write_text(json.dumps(compiled))
            cache = root / "cache"
            preview = root / "preview.mp4"
            final = root / "final.mp4"
            first = render.render(
                compiled_path, preview, "preview", cache, "ffmpeg", render.DEFAULT_FONT
            )
            second = render.render(
                compiled_path, final, "final", cache, "ffmpeg", render.DEFAULT_FONT
            )
            self.assertTrue(preview.is_file())
            self.assertTrue(final.is_file())
            self.assertEqual(first["cache_misses"], 2)
            self.assertEqual(second["cache_hits"], 2)
            report = review.review(
                final,
                root / "strip.jpg",
                compiled_path,
                6,
                "ffmpeg",
                "ffprobe",
                30,
            )
            names = {item["name"]: item["passed"] for item in report["checks"]}
            self.assertTrue(names["cfr"])
            self.assertTrue(names["fps"])
            self.assertTrue(names["bt709_tags"])
            self.assertTrue((root / "strip.jpg").is_file())


@unittest.skipUnless(
    os.environ.get("SNAKETRON_CARD_CAPTURE_SMOKE") == "1",
    "set SNAKETRON_CARD_CAPTURE_SMOKE=1 to run browser capture smoke",
)
class CardCaptureSmokeTests(unittest.TestCase):
    def test_lossless_deterministic_master_and_compiler_interop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = [
                "node",
                str(SCRIPTS / "capture_card.mjs"),
                "--card",
                "rank-up",
                "--capture-vfps",
                "10",
                "--duration-ms",
                "400",
                "--width",
                "320",
                "--height",
                "180",
                "--virtual-time",
                "--param",
                "headline=Victory",
            ]
            first = root / "rank-up-card"
            second = root / "rank-up-card-second"
            subprocess.run([*command, "--out", str(first)], check=True)
            subprocess.run([*command, "--out", str(second)], check=True)
            first_meta = json.loads((first / "meta.json").read_text())
            second_meta = json.loads((second / "meta.json").read_text())
            self.assertEqual(first_meta["master_sha256"], second_meta["master_sha256"])
            self.assertEqual(first_meta["capture_vfps"], 10)
            self.assertEqual(first_meta["encoded_fps"], 10)
            self.assertEqual(first_meta["duration"], 0.4)

            probe = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=pix_fmt,width,height,avg_frame_rate",
                    "-of",
                    "json",
                    str(first / "master.mkv"),
                ],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            stream = json.loads(probe.stdout)["streams"][0]
            self.assertEqual(stream["pix_fmt"], "gbrp")
            self.assertEqual(stream["avg_frame_rate"], "10/1")
            self.assertEqual([stream["width"], stream["height"]], [320, 180])

            compiled = compile_edl.compile_edl(FIXTURES / "card.edl.json", root)
            self.assertEqual(compiled["duration"], 0.4)
            self.assertEqual(compiled["segments"][0]["capture_vfps"], 10)


if __name__ == "__main__":
    unittest.main()
