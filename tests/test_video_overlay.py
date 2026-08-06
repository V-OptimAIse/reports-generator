import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import mock_open, patch

from reports.video.video_overlay import ShiftVideoOverlayGenerator


class ShiftVideoOverlayGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.image_root = Path("history-root")

    def _generator(self, windows):
        generator = ShiftVideoOverlayGenerator("01-01-2026", "C", windows=windows)
        generator.image_root = self.image_root
        generator.text_root = self.image_root
        return generator

    def _frame_path(self, folder_date: str, timestamp: datetime):
        return str(
            self.image_root /
            folder_date /
            "Shift_C_img" /
            f"frame_{timestamp:%d-%m-%Y-%H-%M-%S}.jpeg"
        )

    def test_collect_frames_skips_duplicate_copied_frames_across_date_folders(self):
        start = datetime(2026, 1, 1, 23, 59, 58)
        end = datetime(2026, 1, 2, 0, 0, 3)
        generator = self._generator([{"start": start, "end": end, "label": "midnight"}])

        expected_timestamps = [
            datetime(2026, 1, 1, 23, 59, 58),
            datetime(2026, 1, 1, 23, 59, 59),
            datetime(2026, 1, 2, 0, 0, 0),
            datetime(2026, 1, 2, 0, 0, 1),
            datetime(2026, 1, 2, 0, 0, 2),
            datetime(2026, 1, 2, 0, 0, 3),
        ]

        frames_by_day = {
            "2026_01_01": [
                self._frame_path("2026_01_01", timestamp)
                for timestamp in expected_timestamps[:4]
            ],
            "2026_01_02": [
                self._frame_path("2026_01_02", timestamp)
                for timestamp in expected_timestamps[2:]
            ],
        }

        def fake_glob(pattern):
            pattern = str(pattern)
            for folder_date, paths in frames_by_day.items():
                if folder_date in pattern:
                    return paths
            return []

        with patch("reports.video.video_overlay.glob.glob", side_effect=fake_glob):
            frames = generator._collect_frames()

        self.assertEqual(
            [frame["timestamp"] for frame in frames],
            expected_timestamps,
        )

    def test_collect_frames_orders_unsorted_glob_results_and_windows_by_timestamp(self):
        start = datetime(2026, 1, 1, 22, 0, 0)
        generator = self._generator([
            {
                "start": start + timedelta(seconds=30),
                "end": start + timedelta(seconds=40),
                "label": "later",
            },
            {
                "start": start,
                "end": start + timedelta(seconds=10),
                "label": "earlier",
            },
        ])

        expected_timestamps = [
            start,
            start + timedelta(seconds=5),
            start + timedelta(seconds=35),
        ]
        paths = [
            self._frame_path("2026_01_01", expected_timestamps[2]),
            self._frame_path("2026_01_01", expected_timestamps[1]),
            self._frame_path("2026_01_01", expected_timestamps[0]),
        ]

        with patch("reports.video.video_overlay.glob.glob", return_value=paths):
            frames = generator._collect_frames()

        self.assertEqual(
            [frame["timestamp"] for frame in frames],
            expected_timestamps,
        )

    def test_overlapping_windows_are_merged_before_frame_collection(self):
        start = datetime(2026, 1, 1, 22, 0, 0)
        generator = self._generator([
            {
                "start": start,
                "end": start + timedelta(seconds=20),
                "label": "pipe-1",
            },
            {
                "start": start + timedelta(seconds=10),
                "end": start + timedelta(seconds=30),
                "label": "pipe-2",
            },
            {
                "start": start + timedelta(minutes=2),
                "end": start + timedelta(minutes=3),
                "label": "pipe-3",
            },
        ])

        self.assertEqual(len(generator.windows), 2)
        self.assertEqual(generator.windows[0]["start"], start)
        self.assertEqual(generator.windows[0]["end"], start + timedelta(seconds=30))
        self.assertEqual(generator.windows[0]["label"], "pipe-1, pipe-2")

    def test_load_rois_reads_named_polygon_yaml(self):
        start = datetime(2026, 1, 1, 22, 0, 0)
        generator = self._generator([{"start": start, "end": start + timedelta(seconds=1)}])
        roi_yaml = "\n".join([
            "roi_loadcell:",
            "- [10, 20]",
            "- [30, 40]",
            "roi_invalid:",
            "- [bad]",
        ])

        with (
            self.assertLogs("reports.video.video_overlay", level="WARNING"),
            patch.object(Path, "exists", return_value=True),
            patch("builtins.open", mock_open(read_data=roi_yaml)),
        ):
            rois = generator._load_rois(Path("rois.yaml"))

        self.assertEqual(rois, [{
            "name": "roi_loadcell",
            "points": [(10.0, 20.0), (30.0, 40.0)],
        }])

    def test_load_rois_once_reuses_cached_rois_for_same_path(self):
        start = datetime(2026, 1, 1, 22, 0, 0)
        generator = self._generator([{"start": start, "end": start + timedelta(seconds=1)}])
        path = Path("constant-rois.yaml")
        expected = [{"name": "roi_loadcell", "points": [(10.0, 20.0), (30.0, 40.0)]}]
        ShiftVideoOverlayGenerator._ROI_CACHE.pop(path, None)

        with patch.object(generator, "_load_rois", return_value=expected) as load_rois:
            self.assertIs(generator._load_rois_once(path), expected)
            self.assertIs(generator._load_rois_once(path), expected)

        load_rois.assert_called_once_with(path)

    def test_scale_roi_points_resizes_and_clamps_to_output_frame(self):
        points = [(0, 0), (1309, 598), (1500, -5)]

        scaled = ShiftVideoOverlayGenerator._scale_roi_points(
            points,
            output_w=1280,
            output_h=720,
            source_w=1310,
            source_h=599,
        )

        self.assertEqual(scaled, [(0, 0), (1279, 719), (1279, 0)])

    def test_scale_rois_uses_configured_original_canvas_for_half_saved_frames(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        generator.roi_source_size = (2620, 1216)
        generator.rois = [{
            "name": "roi_right_origin",
            "points": [(0.0, 0.0), (2618.0, 1214.0)],
        }]

        rois = generator._scale_rois(
            output_w=1310,
            output_h=608,
            source_w=1310,
            source_h=608,
        )

        self.assertEqual(rois[0]["points"], [(0, 0), (1309, 607)])

    def test_scale_rois_applies_source_coordinate_offset(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        generator.roi_source_size = (2620, 1216)
        generator.roi_coordinate_offset = (-60.0, 0.0)
        generator.rois = [{
            'name': 'roi_gate2_closed',
            'points': [(200.0, 200.0), (600.0, 400.0)],
        }]

        rois = generator._scale_rois(
            output_w=1310,
            output_h=608,
            source_w=1310,
            source_h=608,
        )

        self.assertEqual(rois[0]['points'], [(70, 100), (270, 200)])

    def test_scale_rois_uses_test_py_reference_size_for_saved_history_frame(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        generator.roi_source_size = (1440, 1080)
        generator.rois = [{
            "name": "roi_gate2_closed",
            "points": [
                (56.0, 567.0),
                (651.0, 582.0),
                (650.0, 735.0),
                (22.0, 784.0),
            ],
        }]

        rois = generator._scale_rois(
            output_w=1310,
            output_h=608,
            source_w=1310,
            source_h=608,
        )

        self.assertEqual(rois[0]["points"], [(51, 319), (592, 328), (591, 414), (20, 441)])

    def test_scale_rois_uses_configured_original_canvas_when_points_fit_frame(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        generator.roi_source_size = (2620, 1216)
        generator.rois = [{
            "name": "roi_top_left",
            "points": [(100.0, 200.0), (300.0, 400.0)],
        }]

        rois = generator._scale_rois(
            output_w=1310,
            output_h=608,
            source_w=1310,
            source_h=608,
        )

        self.assertEqual(rois[0]["points"], [(50, 100), (150, 200)])

    def test_scale_rois_falls_back_to_saved_frame_size_without_config(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        generator.roi_source_size = None
        generator.rois = [{
            "name": "roi_saved_size",
            "points": [(100.0, 200.0), (300.0, 400.0)],
        }]

        with self.assertLogs("reports.video.video_overlay", level="WARNING"):
            rois = generator._scale_rois(
                output_w=1310,
                output_h=608,
                source_w=1310,
                source_h=608,
            )

        self.assertEqual(rois[0]["points"], [(100, 200), (300, 400)])

    def test_scale_rois_infers_test_py_reference_size_without_config(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        generator.roi_source_size = None
        generator.rois = [{
            "name": "roi_gate2_closed",
            "points": [
                (56.0, 567.0),
                (651.0, 582.0),
                (650.0, 735.0),
                (22.0, 784.0),
            ],
        }]

        with self.assertLogs("reports.video.video_overlay", level="WARNING"):
            rois = generator._scale_rois(
                output_w=1310,
                output_h=608,
                source_w=1310,
                source_h=608,
            )

        self.assertEqual(rois[0]["points"], [(51, 319), (592, 328), (591, 414), (20, 441)])

    def test_configured_roi_source_size_reads_rois_source_resolution(self):
        generator = object.__new__(ShiftVideoOverlayGenerator)
        size = generator._configured_roi_source_size({
            "rois": {
                "path": "rois.yaml",
                "source_resolution": {
                    "width": 2620,
                    "height": 1216,
                },
            },
        })

        self.assertEqual(size, (2620, 1216))

    def test_configured_roi_coordinate_offset_defaults_to_zero_and_parses_values(self):
        self.assertEqual(
            ShiftVideoOverlayGenerator._configured_roi_coordinate_offset({}),
            (0.0, 0.0),
        )
        self.assertEqual(
            ShiftVideoOverlayGenerator._configured_roi_coordinate_offset({
                'rois': {'coordinate_offset': {'x': -60, 'y': 10}},
            }),
            (-60.0, 10.0),
        )

    def test_input_images_have_overlay_infers_producer_publish_overlay(self):
        with TemporaryDirectory() as tmp:
            producer_root = Path(tmp)
            image_root = producer_root / "var" / "history"
            image_root.mkdir(parents=True)
            config_dir = producer_root / "config"
            config_dir.mkdir()
            (config_dir / "runtime.yaml").write_text("publish_overlay: true\n", encoding="utf-8")

            generator = object.__new__(ShiftVideoOverlayGenerator)
            generator.video_cfg = {}
            generator.image_root = image_root

            self.assertTrue(generator._input_images_have_overlay({}))


if __name__ == "__main__":
    unittest.main()
