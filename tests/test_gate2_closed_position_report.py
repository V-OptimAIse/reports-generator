import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

from reports.gates import gate2_closed_position_report as gate2_module
from reports.common.caster_config import CasterConfig
from reports.gates.gate2_closed_position_report import (
    FrameCoverage,
    Gate2ClosedPositionReport,
)


class Gate2ClosedPositionReportTest(unittest.TestCase):
    def _report(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.interval_seconds = 600
        report.threshold_percent = 80.0
        report.cfg = {}
        report.report_cfg = {}
        report.roi_points = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
        report.open_roi_points = [(12.0, 12.0), (20.0, 12.0), (20.0, 20.0), (12.0, 20.0)]
        report.roi_area = 100.0
        report.gate2_class_id = 3
        return report

    def _measure_yolo(self, report, yolo_line: str):
        timestamp = datetime(2026, 6, 30, 12, 0, 0)
        text_path = Path("pipe_30-06-2026-12-00-00-000.txt")
        with patch("builtins.open", mock_open(read_data=yolo_line)):
            return report._measure_frame(timestamp, text_path, frame_width=20, frame_height=20)

    def test_gate2_detection_inside_closed_roi_calculates_detection_inside_roi_percent(self):
        report = self._report()
        frame = self._measure_yolo(report, "3 0.25 0.25 0.5 0.5\n2 0.25 0.25 0.5 0.5\n")

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertEqual(frame.centroid_inside_count, 1)
        self.assertEqual(frame.coverage_percent, 100.0)

    def test_gate2_detection_in_both_closed_and_open_rois_is_not_counted_as_closed(self):
        report = self._report()
        report.open_roi_points = [(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0)]

        frame = self._measure_yolo(report, "3 0.25 0.25 0.5 0.5\n")

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertEqual(frame.centroid_inside_count, 0)
        self.assertIsNone(frame.coverage_percent)

    def test_gate2_detection_half_inside_closed_roi_calculates_50_percent(self):
        report = self._report()
        frame = self._measure_yolo(report, "3 0.5 0.25 0.5 0.5\n")

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertAlmostEqual(frame.coverage_percent, 50.0)

    def test_gate2_detection_outside_closed_roi_counts_zero_percent(self):
        report = self._report()
        frame = self._measure_yolo(report, "3 0.75 0.75 0.25 0.25\n")

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertEqual(frame.centroid_inside_count, 0)
        self.assertIsNone(frame.coverage_percent)

    def test_gate2_detection_larger_than_roi_uses_detection_area_denominator(self):
        report = self._report()
        frame = self._measure_yolo(report, "3 0.5 0.5 1.0 1.0\n")

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertAlmostEqual(frame.coverage_percent, 25.0)

    def test_gate2_detection_centroid_outside_closed_roi_does_not_measure_coverage(self):
        report = self._report()
        frame = self._measure_yolo(report, "3 0.65 0.25 0.5 0.5\n")

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertEqual(frame.centroid_inside_count, 0)
        self.assertIsNone(frame.coverage_percent)

    def test_sample_gate2_line_centroid_is_inside_real_gate2_closed_roi(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.roi_points = [
            (29.0, 244.0),
            (580.0, 260.0),
            (581.0, 342.0),
            (30.0, 359.0),
        ]
        report.open_roi_points = [(700.0, 400.0), (800.0, 400.0), (800.0, 500.0), (700.0, 500.0)]
        report.roi_area = Gate2ClosedPositionReport._polygon_area(report.roi_points)
        report.gate2_class_id = 3
        timestamp = datetime(2026, 6, 14, 17, 22, 41, 667000)
        text_path = Path("pipe_14-06-2026-17-22-41-667.txt")

        with patch("builtins.open", mock_open(read_data="3 0.226321 0.502189 0.403887 0.202011\n")):
            frame = report._measure_frame(timestamp, text_path, frame_width=1310, frame_height=608)

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertEqual(frame.centroid_inside_count, 1)
        self.assertGreater(frame.coverage_percent, 80.0)

    def test_roi_coordinates_use_test_py_reference_size_for_saved_history_frame(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.report_cfg = {}
        report.cfg = {
            "rois": {
                "source_resolution": {
                    "width": 1440,
                    "height": 1080,
                },
            },
        }
        report.raw_rois = {
            "roi_gate2_closed": [
                (56.0, 567.0),
                (651.0, 582.0),
                (650.0, 735.0),
                (22.0, 784.0),
            ],
            "roi_gate2_open": [
                (800.0, 500.0),
                (1000.0, 500.0),
                (1000.0, 700.0),
                (800.0, 700.0),
            ],
        }
        report.roi_name = "roi_gate2_closed"
        report.open_roi_name = "roi_gate2_open"
        report.gate2_class_id = 3
        report.EPSILON = Gate2ClosedPositionReport.EPSILON

        report._prepare_rois_for_source_size(frame_width=1310, frame_height=608)

        self.assertAlmostEqual(report.roi_scale_x, 1310 / 1440)
        self.assertAlmostEqual(report.roi_scale_y, 608 / 1080)
        self.assertEqual(report.roi_points, [(51.0, 319.0), (592.0, 328.0), (591.0, 414.0), (20.0, 441.0)])

        timestamp = datetime(2026, 6, 30, 11, 40, 0)
        text_path = Path("pipe_30-06-2026-11-40-00-000.txt")
        with patch("builtins.open", mock_open(read_data="3 0.25 0.62 0.2 0.1\n")):
            frame = report._measure_frame(timestamp, text_path, frame_width=1310, frame_height=608)

        self.assertEqual(frame.gate2_detection_count, 1)
        self.assertEqual(frame.centroid_inside_count, 1)

    def test_roi_coordinates_infer_test_py_reference_size_without_config(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.report_cfg = {}
        report.cfg = {}
        report.raw_rois = {
            "roi_gate2_closed": [
                (56.0, 567.0),
                (651.0, 582.0),
                (650.0, 735.0),
                (22.0, 784.0),
            ],
            "roi_gate2_open": [
                (800.0, 500.0),
                (1000.0, 500.0),
                (1000.0, 700.0),
                (800.0, 700.0),
            ],
        }
        report.roi_name = "roi_gate2_closed"
        report.open_roi_name = "roi_gate2_open"
        report.EPSILON = Gate2ClosedPositionReport.EPSILON

        with self.assertLogs("reports.gates.gate2_closed_position_report", level="WARNING"):
            report._prepare_rois_for_source_size(frame_width=1310, frame_height=608)

        self.assertAlmostEqual(report.roi_scale_x, 1310 / 1440)
        self.assertAlmostEqual(report.roi_scale_y, 608 / 1080)
        self.assertEqual(report.roi_points, [(51.0, 319.0), (592.0, 328.0), (591.0, 414.0), (20.0, 441.0)])

    def test_roi_coordinates_fall_back_to_saved_frame_when_points_fit(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.report_cfg = {}
        report.cfg = {}
        report.raw_rois = {
            "roi_gate2_closed": [
                (100.0, 200.0),
                (300.0, 200.0),
                (300.0, 400.0),
                (100.0, 400.0),
            ],
            "roi_gate2_open": [
                (400.0, 200.0),
                (500.0, 200.0),
                (500.0, 400.0),
                (400.0, 400.0),
            ],
        }
        report.roi_name = "roi_gate2_closed"
        report.open_roi_name = "roi_gate2_open"
        report.EPSILON = Gate2ClosedPositionReport.EPSILON

        with self.assertLogs("reports.gates.gate2_closed_position_report", level="WARNING"):
            report._prepare_rois_for_source_size(frame_width=1310, frame_height=608)

        self.assertEqual(report.roi_scale_x, 1.0)
        self.assertEqual(report.roi_scale_y, 1.0)
        self.assertEqual(report.roi_points[0], (100.0, 200.0))

    def test_configured_roi_source_size_reads_global_rois_source_resolution(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.report_cfg = {}
        report.cfg = {
            "rois": {
                "path": "rois.yaml",
                "source_resolution": {
                    "width": 1440,
                    "height": 1080,
                },
            },
        }

        self.assertEqual(report._configured_roi_source_size(), (1440, 1080))

    def test_caster3_original_roi_coordinates_are_halved_for_saved_frame(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.report_cfg = {}
        report.cfg = {
            'rois': {
                'source_resolution': {'width': 2620, 'height': 1216},
            },
        }
        report.raw_rois = {
            'roi_gate2_closed': [
                (42.0, 502.0),
                (1184.0, 510.0),
                (1204.0, 696.0),
                (16.0, 734.0),
            ],
            'roi_gate2_open': [
                (1400.0, 500.0),
                (1600.0, 500.0),
                (1600.0, 700.0),
                (1400.0, 700.0),
            ],
        }
        report.roi_name = 'roi_gate2_closed'
        report.open_roi_name = 'roi_gate2_open'
        report.roi_source_size = report._configured_roi_source_size()

        report._prepare_rois_for_source_size(frame_width=1310, frame_height=608)

        self.assertEqual(
            report.roi_points,
            [(21.0, 251.0), (592.0, 255.0), (602.0, 348.0), (8.0, 367.0)],
        )

    def test_interval_average_marks_below_threshold_as_alert(self):
        report = self._report()
        start = datetime(2026, 6, 30, 12, 0, 0)
        end = start + timedelta(minutes=30)
        rows = report._build_interval_rows(
            start,
            end,
            [
                FrameCoverage(start + timedelta(minutes=1), Path("a.txt"), 100.0, 1, 1),
                FrameCoverage(start + timedelta(minutes=5), Path("b.txt"), 80.0, 1, 1),
                FrameCoverage(start + timedelta(minutes=11), Path("c.txt"), 70.0, 1, 1),
            ],
        )

        self.assertEqual(rows[0]["avg_coverage_percent"], 90.0)
        self.assertEqual(rows[0]["avg_detection_inside_roi_percent"], 90.0)
        self.assertEqual(rows[0]["closed_position_sample_count"], 2)
        self.assertEqual(rows[0]["status"], "VIEW_UNCHANGED")
        self.assertFalse(rows[0]["alert"])

        self.assertEqual(rows[1]["avg_coverage_percent"], 70.0)
        self.assertEqual(rows[1]["avg_detection_inside_roi_percent"], 70.0)
        self.assertEqual(rows[1]["status"], "POSSIBLE_VIEW_CHANGE")
        self.assertTrue(rows[1]["alert"])

        self.assertEqual(rows[2]["sample_count"], 0)
        self.assertEqual(rows[2]["closed_position_sample_count"], 0)
        self.assertEqual(rows[2]["status"], "NO_SAMPLES")
        self.assertTrue(rows[2]["alert"])
        self.assertAlmostEqual(report._weighted_avg_coverage(rows), 83.33)

    def test_no_sample_interval_can_stay_non_alert_for_recent_runs(self):
        report = self._report()
        report.alert_on_no_samples = False
        start = datetime(2026, 6, 30, 12, 0, 0)
        end = start + timedelta(minutes=10)

        rows = report._build_interval_rows(start, end, [])

        self.assertEqual(rows[0]["status"], "NO_SAMPLES")
        self.assertFalse(rows[0]["alert"])

    def test_open_gate2_detections_are_excluded_from_coverage_average(self):
        report = self._report()
        start = datetime(2026, 6, 30, 12, 0, 0)
        end = start + timedelta(minutes=10)

        rows = report._build_interval_rows(
            start,
            end,
            [
                FrameCoverage(start + timedelta(minutes=1), Path("open.txt"), None, 1, 0),
                FrameCoverage(start + timedelta(minutes=2), Path("closed.txt"), 90.0, 1, 1),
            ],
        )

        self.assertEqual(rows[0]["sample_count"], 2)
        self.assertEqual(rows[0]["closed_position_sample_count"], 1)
        self.assertEqual(rows[0]["avg_detection_inside_roi_percent"], 90.0)
        self.assertEqual(rows[0]["status"], "VIEW_UNCHANGED")
        self.assertFalse(rows[0]["alert"])

    def test_interval_with_only_open_gate2_detections_has_no_coverage_or_alert(self):
        report = self._report()
        start = datetime(2026, 6, 30, 12, 0, 0)
        end = start + timedelta(minutes=10)

        rows = report._build_interval_rows(
            start,
            end,
            [FrameCoverage(start + timedelta(minutes=1), Path("open.txt"), None, 1, 0)],
        )

        self.assertEqual(rows[0]["closed_position_sample_count"], 0)
        self.assertIsNone(rows[0]["avg_detection_inside_roi_percent"])
        self.assertEqual(rows[0]["status"], "NO_CLOSED_DETECTIONS")
        self.assertFalse(rows[0]["alert"])

        summary = report._build_summary(
            date_label="30-06-2026",
            shift_label="Shift_A",
            start=start,
            end=end,
            text_files=[(start + timedelta(minutes=1), Path("open.txt"))],
            rows=rows,
            window_mode="recent",
        )
        self.assertEqual(summary["status"], "NO_CLOSED_DETECTIONS")
        self.assertEqual(summary["closed_position_sample_count"], 0)
        self.assertIsNone(summary["avg_detection_inside_roi_percent"])
        self.assertIn("Avg detection inside ROI: N/A", report._summary_body(summary))

    def test_recent_export_checks_last_window_and_sends_only_on_alert(self):
        report = object.__new__(Gate2ClosedPositionReport)
        report.interval_seconds = 600
        report.threshold_percent = 80.0
        report.alert_on_no_samples = False
        report.report_cfg = {"send_email": True}
        report.cfg = {}
        report.shifts = {
            "shift_a": ("06:00", "14:00"),
            "shift_b": ("14:00", "22:00"),
            "shift_c": ("22:00", "06:00"),
        }
        report.roi_name = "roi_gate2_closed"
        report.open_roi_name = "roi_gate2_open"
        report.roi_source_size = (1310, 608)
        report.raw_rois = {
            "roi_gate2_closed": [
                (100.0, 100.0),
                (300.0, 100.0),
                (300.0, 300.0),
                (100.0, 300.0),
            ],
            "roi_gate2_open": [
                (400.0, 100.0),
                (600.0, 100.0),
                (600.0, 300.0),
                (400.0, 300.0),
            ],
        }
        report.roi_points = report.raw_rois["roi_gate2_closed"]
        report.roi_area = Gate2ClosedPositionReport._polygon_area(report.roi_points)

        timestamp = datetime(2026, 6, 30, 14, 1, 0)
        frame = FrameCoverage(timestamp, Path("pipe.txt"), 70.0, 1, 1)

        with (
            patch.object(report, "_collect_text_files_for_segments", return_value=[(timestamp, Path("pipe.txt"))]),
            patch.object(report, "_resolve_source_size", return_value=(1310, 608)),
            patch.object(report, "_measure_frame", return_value=frame),
            patch.object(report, "_send_alert_email", return_value={"sent": True}) as send_mail,
        ):
            summary = report.export_recent(
                minutes=10,
                end_time=datetime(2026, 6, 30, 14, 5, 0),
                send_email=True,
            )

        self.assertEqual(summary["window_mode"], "recent")
        self.assertEqual(summary["window_start"], "2026-06-30 13:55:00")
        self.assertEqual(summary["window_end"], "2026-06-30 14:05:00")
        self.assertEqual(summary["shift"], "Shift_A+Shift_B")
        self.assertEqual(summary["alert_count"], 1)
        self.assertEqual(summary["total_sample_count"], 1)
        self.assertEqual(summary["gate2_detection_count"], 1)
        self.assertEqual(summary["gate2_closed_count"], 1)
        self.assertEqual(summary["avg_detection_inside_roi_percent"], 70.0)
        self.assertEqual(summary["avg_area_covered_percent"], 70.0)
        self.assertEqual(summary["threshold"], 80.0)
        self.assertEqual(summary["status"], "BELOW_THRESHOLD")
        body = report._summary_body(summary)
        self.assertIn("Threshold            : 80", body)
        self.assertNotIn("Threshold percent", body)
        send_mail.assert_called_once()

    def test_main_without_args_runs_recent_window_check(self):
        caster = CasterConfig(
            id="caster1",
            number=1,
            name="Caster 1",
            enabled=True,
            cfg={},
        )
        fake_report = Mock()
        fake_report.export_recent.return_value = {"alert_count": 0}

        with (
            patch.object(gate2_module, "load_runtime_config", return_value={"casters": {}}),
            patch.object(gate2_module, "resolve_enabled_casters", return_value=[caster]),
            patch.object(gate2_module, "Gate2ClosedPositionReport", return_value=fake_report) as report_cls,
            patch("sys.argv", ["gate2_closed_position_report.py"]),
        ):
            gate2_module.main()

        report_cls.assert_called_once_with(cfg=caster.cfg, caster=caster)
        fake_report.export_recent.assert_called_once_with(
            minutes=None,
            end_time=None,
            send_email=True,
        )
        fake_report.export.assert_not_called()

    def test_alert_recipients_fall_back_to_diagnosis_recipients(self):
        report = self._report()
        report.report_cfg = {}
        report.cfg = {
            "email": {
                "recipients": ["general@example.com"],
                "diagnosis_recipients": ["diagnosis@example.com"],
            }
        }

        self.assertEqual(report._alert_recipients(), ["diagnosis@example.com"])

    def test_custom_window_resolves_inside_shift(self):
        report = self._report()

        start, stop = report._resolve_custom_window(
            "30-06-2026",
            "11:50",
            "12:00",
            datetime(2026, 6, 30, 6, 0, 0),
            datetime(2026, 6, 30, 14, 0, 0),
        )

        self.assertEqual(start, datetime(2026, 6, 30, 11, 50, 0))
        self.assertEqual(stop, datetime(2026, 6, 30, 12, 0, 0))

    def test_custom_window_supports_overnight_shift(self):
        report = self._report()

        start, stop = report._resolve_custom_window(
            "30-06-2026",
            "23:50",
            "00:00",
            datetime(2026, 6, 30, 22, 0, 0),
            datetime(2026, 7, 1, 6, 0, 0),
        )

        self.assertEqual(start, datetime(2026, 6, 30, 23, 50, 0))
        self.assertEqual(stop, datetime(2026, 7, 1, 0, 0, 0))


if __name__ == "__main__":
    unittest.main()
