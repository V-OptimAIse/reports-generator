import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from cli import report_workflow
from cli.report_workflow import ShiftRun, ShiftWorkflow
from reports.common.caster_config import CasterConfig, deep_merge, resolve_enabled_casters
from reports.gates.gate2_closed_position_report import Gate2ClosedPositionReport
from reports.pipes import gate_cycles_exporter
from reports.pipes.pipe_exporter import PipeExporter
from reports.pipes.verified_pipes import VerifiedPipeExporter
from reports.video.video_generator import ShiftVideoGenerator, _normalize_shift_arg


def _base_cfg():
    return {
        "database": {"path": "legacy/pipes.db"},
        "history": {
            "image_root": "legacy/history",
            "shifts": [
                {"name": "Shift_A", "start": "06:00", "end": "14:00"},
                {"name": "Shift_B", "start": "14:00", "end": "22:00"},
                {"name": "Shift_C", "start": "22:00", "end": "06:00"},
            ],
        },
        "rois": {"path": "legacy/rois.yaml", "source_resolution": {"width": 1440, "height": 1080}},
        "outputs": {"csv_dir": "outputs/csv"},
        "video": {"output_dir": "outputs/videos", "overlay_output_dir": "outputs/videos-overlay"},
        "gdrive": {
            "remote": "gdrive",
            "base_path": "Electrosteel/Daily pipe recordings",
            "pipes_csv_dir": "Pipes_Data_Sheet",
            "videos_dir": "Pipe_count_caster2_vid",
        },
        "email": {
            "sender": "sender@example.com",
            "password": "secret",
            "recipients": ["ops@example.com"],
            "test_recipients": ["test@example.com"],
            "diagnosis_recipients": ["diag@example.com"],
            "send_csv_attachment": True,
            "smtp_server": "smtp.example.com",
            "smtp_port": 587,
        },
        "verified_pipe_records_recipients": ["verified@example.com"],
        "verified_pipes_mode": "loadcell",
        "missing_loadcell_video": {"enabled": True, "delete_after_upload": False},
        "casters": {
            "defaults": {
                "enabled": True,
                "var_root": "../producer/var",
                "database_file": "pipes.db",
                "history_dir": "history",
                "outputs": {
                    "csv_dir_template": "outputs/{caster_id}/csv",
                    "video_dir_template": "outputs/{caster_id}/videos",
                    "overlay_video_dir_template": "outputs/{caster_id}/videos-overlay",
                },
                "gdrive": {
                    "pipes_csv_dir_template": "Pipes_Data_Sheet/{caster_id}",
                    "videos_dir_template": "Pipe_count_{caster_id}_vid",
                },
            },
            "items": [
                {
                    "id": "caster1",
                    "number": 1,
                    "var_dir": "../producer/var/caster1",
                    "rois": {"path": "../producer/config/caster1/rois.yaml"},
                },
                {
                    "id": "caster2",
                    "number": 2,
                    "var_dir": "../producer/var/caster2",
                    "rois": {"path": "../producer/config/caster2/rois.yaml"},
                },
                {
                    "id": "caster3",
                    "number": 3,
                    "enabled": False,
                    "var_dir": "../producer/var/caster3",
                    "rois": {"path": "../producer/config/caster3/rois.yaml"},
                },
            ],
        },
    }


class MultiCasterConfigTest(TestCase):
    def test_deep_merge_and_default_path_resolution_from_var_dir(self):
        cfg = _base_cfg()
        caster = resolve_enabled_casters(cfg, ["caster1"])[0]

        self.assertEqual(Path(caster.cfg["database"]["path"]).as_posix(), "../producer/var/caster1/pipes.db")
        self.assertEqual(Path(caster.cfg["history"]["image_root"]).as_posix(), "../producer/var/caster1/history")
        self.assertEqual(caster.cfg["rois"]["path"], "../producer/config/caster1/rois.yaml")
        self.assertEqual(caster.cfg["rois"]["source_resolution"]["width"], 1440)
        self.assertEqual(caster.cfg["outputs"]["csv_dir"], "outputs/caster1/csv")
        self.assertEqual(caster.cfg["video"]["output_dir"], "outputs/caster1/videos")
        self.assertEqual(caster.cfg["video"]["overlay_output_dir"], "outputs/caster1/videos-overlay")
        self.assertEqual(caster.cfg["gdrive"]["pipes_csv_dir"], "Pipes_Data_Sheet/caster1")
        self.assertEqual(caster.cfg["gdrive"]["videos_dir"], "Pipe_count_caster1_vid")
        self.assertEqual(deep_merge({"a": {"b": 1}}, {"a": {"c": 2}}), {"a": {"b": 1, "c": 2}})

    def test_database_file_template_resolves_from_caster_number(self):
        cfg = _base_cfg()
        cfg["casters"]["defaults"]["database_file"] = "caster_{number}_pipes.db"
        cfg["casters"]["items"][1]["var_dir"] = "../producer/var/caster_2"

        caster = resolve_enabled_casters(cfg, ["caster2"])[0]

        self.assertEqual(
            Path(caster.cfg["database"]["path"]).as_posix(),
            "../producer/var/caster_2/caster_2_pipes.db",
        )

    def test_disabled_casters_are_skipped_and_yaml_order_is_preserved(self):
        casters = resolve_enabled_casters(_base_cfg())
        self.assertEqual([caster.id for caster in casters], ["caster1", "caster2"])

    def test_caster_roi_source_resolution_override_is_isolated(self):
        cfg = _base_cfg()
        cfg['casters']['items'][2]['enabled'] = True
        cfg['casters']['items'][2]['rois']['source_resolution'] = {
            'width': 2620,
            'height': 1216,
        }
        cfg['casters']['items'][2]['rois']['coordinate_offset'] = {'x': -60, 'y': 0}

        caster2, caster3 = resolve_enabled_casters(cfg, ['caster2', 'caster3'])

        self.assertEqual(caster2.cfg['rois']['source_resolution'], {'width': 1440, 'height': 1080})
        self.assertEqual(caster3.cfg['rois']['source_resolution'], {'width': 2620, 'height': 1216})
        self.assertNotIn('coordinate_offset', caster2.cfg['rois'])
        self.assertEqual(caster3.cfg['rois']['coordinate_offset'], {'x': -60, 'y': 0})

    def test_legacy_single_caster_runtime_still_resolves(self):
        cfg = _base_cfg()
        cfg.pop("casters")

        caster = resolve_enabled_casters(cfg)[0]

        self.assertTrue(caster.is_legacy)
        self.assertIsNone(caster.file_token)
        self.assertEqual(caster.cfg["database"]["path"], "legacy/pipes.db")

    def test_state_path_includes_caster_id(self):
        with TemporaryDirectory() as tmp:
            wf = ShiftWorkflow(cfg=_base_cfg(), selected_ids=["caster1"])
            wf.state_dir = Path(tmp)
            run = ShiftRun("02-07-2026", "Shift_A")

            self.assertEqual(
                wf._state_path(run, wf.casters[0]).name,
                "caster1_02072026_shift_a.json",
            )

    def test_output_filenames_include_caster_id(self):
        caster = CasterConfig("caster1", 1, "Caster 1", True, {}, False)

        pipe_exporter = object.__new__(PipeExporter)
        pipe_exporter.caster_file_token = caster.file_token
        self.assertEqual(
            pipe_exporter._filename("pipes", "02-07-2026", "Shift_A", "123456", "csv"),
            "pipes_caster1_02072026_shift_a_123456.csv",
        )
        self.assertEqual(
            pipe_exporter._filename("pipes_diagnosis", "02-07-2026", "Shift_A", "123456", "xlsx"),
            "pipes_diagnosis_caster1_02072026_shift_a_123456.xlsx",
        )

        with TemporaryDirectory() as tmp:
            verified = object.__new__(VerifiedPipeExporter)
            verified.cfg = {"verified_pipes_gate_open_max_interval_seconds": 120}
            verified.output_dir = Path(tmp)
            verified.caster_file_token = caster.file_token

            out_path, _summary = verified.export_from_dataframes(
                "02-07-2026",
                "Shift_A",
                pd.DataFrame([{
                    "pipe_uid": "p1",
                    "pipe_checkpoint": 1,
                    "t_origin": "2026-07-02 06:01:00",
                    "t_loadcell_enter": "",
                    "t_loadcell_exit": "",
                }]),
                pd.DataFrame(columns=["gate_name", "t_open_IST"]),
                mode="loadcell",
                shift_end=None,
            )

        self.assertIn("verified_pipes_caster1_02072026_shift_a_", out_path.name)

    def test_pipe_exporter_defaults_missing_pipe_checkpoint_to_zero(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "pipes.db"
            con = sqlite3.connect(db_path)
            try:
                con.execute(
                    """
                    CREATE TABLE pipes (
                        pipe_uid TEXT,
                        origin TEXT,
                        t_origin INTEGER,
                        t_loadcell_enter INTEGER,
                        t_loadcell_exit INTEGER,
                        weight REAL,
                        weight_quality TEXT,
                        weight_samples INTEGER,
                        state TEXT,
                        last_seen_ts INTEGER
                    )
                    """
                )
                ts = int(datetime(2026, 7, 2, 6, 1).timestamp())
                con.execute(
                    """
                    INSERT INTO pipes (
                        pipe_uid,
                        origin,
                        t_origin,
                        t_loadcell_enter,
                        t_loadcell_exit,
                        weight,
                        weight_quality,
                        weight_samples,
                        state,
                        last_seen_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("p1", "origin-1", ts, None, None, 100.0, "ok", 3, "done", ts),
                )
                con.commit()
            finally:
                con.close()

            cfg = {
                "database": {"path": str(db_path)},
                "history": {"shifts": _base_cfg()["history"]["shifts"]},
                "outputs": {"csv_dir": str(root / "csv")},
            }
            out_path, pipe_count = PipeExporter(cfg=cfg).export("02-07-2026", "Shift_A")

            exported = pd.read_csv(out_path)

        self.assertEqual(pipe_count, 1)
        self.assertEqual(exported["pipe_checkpoint"].tolist(), [0])

    def test_video_generator_cli_shift_normalization_accepts_flag_style_values(self):
        self.assertEqual(_normalize_shift_arg("C"), "C")
        self.assertEqual(_normalize_shift_arg("shift_c"), "C")
        self.assertEqual(_normalize_shift_arg("Shift_C"), "C")

        with self.assertRaises(ValueError):
            _normalize_shift_arg("shift_d")

    def test_video_generator_uses_caster_specific_history_root(self):
        caster = resolve_enabled_casters(_base_cfg(), ["caster2"])[0]
        generator = ShiftVideoGenerator("02-07-2026", "A", cfg=caster.cfg, caster=caster)

        self.assertTrue(str(generator.image_root).endswith(r"producer\var\caster2\history"))
        self.assertEqual(generator.output_path.name, "02-07-2026_caster2_shift_a.mp4")

    def test_gate2_report_uses_caster_specific_history_and_rois(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = root / "history"
            history.mkdir()
            rois = root / "rois.yaml"
            rois.write_text(
                "\n".join([
                    "roi_gate2_closed:",
                    "- [0, 0]",
                    "- [10, 0]",
                    "- [10, 10]",
                    "- [0, 10]",
                ]),
                encoding="utf-8",
            )
            caster = CasterConfig(
                "caster1",
                1,
                "Caster 1",
                True,
                {
                    "history": {
                        "image_root": str(history),
                        "shifts": [{"name": "Shift_A", "start": "06:00", "end": "14:00"}],
                    },
                    "rois": {"path": str(rois)},
                    "gate2_closed_position_report": {"send_email": False},
                },
                False,
            )

            report = Gate2ClosedPositionReport(cfg=caster.cfg, caster=caster)

        self.assertEqual(report.history_root, history)
        self.assertEqual(report.roi_points, [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])

    def test_gate_cycles_exporter_has_no_hardcoded_db_path(self):
        self.assertFalse(hasattr(gate_cycles_exporter, "DB_PATH"))


class WorkflowOrderingTest(TestCase):
    def _workflow(self, tmp, *, test_mode=False, selected_ids=None, cfg=None):
        wf = ShiftWorkflow(cfg=cfg or _base_cfg(), selected_ids=selected_ids, test_mode=test_mode)
        wf.state_dir = Path(tmp)
        return wf

    def test_email_subject_prefix_only_in_test_mode(self):
        with TemporaryDirectory() as tmp:
            normal_wf = self._workflow(tmp)
            test_wf = self._workflow(tmp, test_mode=True)

            self.assertEqual(normal_wf._email_subject("Pipe Report CSV - Caster 2"), "Pipe Report CSV - Caster 2")
            self.assertEqual(test_wf._email_subject("Pipe Report CSV - Caster 2"), "[TEST] Pipe Report CSV - Caster 2")

    def test_truthy_non_boolean_does_not_enable_test_subject_prefix(self):
        wf = ShiftWorkflow(cfg=_base_cfg(), test_mode="true")

        self.assertFalse(wf.test_mode)
        self.assertEqual(wf._email_subject("Pipe Report CSV - Caster 2"), "Pipe Report CSV - Caster 2")

    def test_workflow_orders_raw_verified_diagnosis_missing_links_then_normal_videos(self):
        events = []
        diagnosis_bodies = {}
        tmp_root = None

        class FakePipeExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift):
                path = tmp_root / f"{self.caster.id}.csv"
                path.write_text("raw", encoding="utf-8")
                return path, 10

            def export_diagnosis(self, date_str, shift):
                events.append(f"{self.caster.id}_diagnosis_export")
                path = tmp_root / f"{self.caster.id}.xlsx"
                path.write_text("diagnosis", encoding="utf-8")
                return path, {
                    "pipe_count": 10,
                    "abnormal_count": 0,
                    "t_origin_gap_abnormal_count": 0,
                    "t_origin_gap_too_slow_count": 0,
                    "t_origin_gap_too_fast_count": 0,
                    "loadcell_missing_count": 1,
                }

        class FakeVerifiedExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift, csv_path, mode=None):
                path = tmp_root / f"{self.caster.id}_verified.csv"
                path.write_text("verified", encoding="utf-8")
                return path, {
                    "verified_count": 9,
                    "removed_count": 1,
                    "loadcell_missing_records": [
                        {"pipe_uid": f"{self.caster.id}-p1", "origin_time": "2026-07-02 06:10:00"}
                    ],
                }

        class FakeMailer:
            def __init__(self, cfg=None):
                pass

            def send(self, subject, body, attachments=None, recipients=None):
                if subject.startswith("Raw Pipe Production Report"):
                    events.append("raw_email")
                elif subject.startswith("Verified Pipe Production Report"):
                    events.append("verified_email")
                elif subject.startswith("Pipe Diagnosis Report"):
                    events.append("diagnosis_email")
                    diagnosis_bodies["all"] = body
                    diagnosis_bodies["attachments"] = list(attachments or [])
                else:
                    events.append("unexpected_email")

        class FakeUploader:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def upload_csv(self, path):
                return f"https://drive/{self.caster.id}/csv"

            def upload_video(self, path):
                name = Path(path).name
                kind = "overlay" if "overlay" in name else "normal"
                events.append(f"{self.caster.id}_missing_{kind}_upload")
                return f"https://drive/{self.caster.id}/{name}"

        class FakeOverlayGenerator:
            def __init__(self, date_str, shift, windows=None, output_name=None, normal_output_name=None, cfg=None, caster=None):
                self.caster = caster
                self.output_name = output_name or f"{caster.id}_overlay.mp4"
                self.normal_output_path = Path(normal_output_name or f"{caster.id}_normal.mp4")

            def generate(self):
                events.append(f"{self.caster.id}_missing_video_generate")
                return self.output_name

        class FakeVideoGenerator:
            def __init__(self, date_str, shift, cfg=None, caster=None):
                self.caster = caster

            def generate(self):
                events.append(f"{self.caster.id}_video")
                return f"{self.caster.id}.mp4"

        with (
            TemporaryDirectory() as tmp,
            patch.object(report_workflow, "PipeExporter", FakePipeExporter),
            patch.object(report_workflow, "VerifiedPipeExporter", FakeVerifiedExporter),
            patch.object(report_workflow, "EmailSender", FakeMailer),
            patch.object(report_workflow, "GDriveUploader", FakeUploader),
            patch.object(report_workflow, "ShiftVideoOverlayGenerator", FakeOverlayGenerator),
            patch.object(report_workflow, "ShiftVideoGenerator", FakeVideoGenerator),
        ):
            tmp_root = Path(tmp)
            wf = self._workflow(tmp)
            wf.run(ShiftRun("02-07-2026", "Shift_A"))

        expected_events = [
            "raw_email",
            "verified_email",
            "caster1_diagnosis_export",
            "caster2_diagnosis_export",
            "caster1_missing_video_generate",
            "caster1_missing_overlay_upload",
            "caster1_missing_normal_upload",
            "caster2_missing_video_generate",
            "caster2_missing_overlay_upload",
            "caster2_missing_normal_upload",
            "caster1_video",
            "caster2_video",
            "diagnosis_email",
        ]
        self.assertCountEqual(events, expected_events)
        self.assertEqual(events[:4], expected_events[:4])
        self.assertLess(events.index("caster1_missing_video_generate"), events.index("caster1_video"))
        self.assertLess(events.index("caster2_missing_video_generate"), events.index("caster2_video"))
        self.assertLess(events.index("caster1_missing_overlay_upload"), events.index("diagnosis_email"))
        self.assertLess(events.index("caster1_missing_normal_upload"), events.index("diagnosis_email"))
        self.assertLess(events.index("caster2_missing_overlay_upload"), events.index("diagnosis_email"))
        self.assertLess(events.index("caster2_missing_normal_upload"), events.index("diagnosis_email"))
        self.assertLess(events.index("caster1_video"), events.index("diagnosis_email"))
        self.assertLess(events.index("caster2_video"), events.index("diagnosis_email"))
        diagnosis_body = diagnosis_bodies["all"]
        self.assertIn("### Caster 1: 10 Pipes", diagnosis_body)
        self.assertIn("### Caster 2: 10 Pipes", diagnosis_body)
        self.assertIn("Missing Loadcell Videos", diagnosis_body)
        self.assertIn("Overlay Video Link                  : https://drive/caster1/", diagnosis_body)
        self.assertIn("Normal Video Link                   : https://drive/caster1/", diagnosis_body)
        self.assertEqual(len(diagnosis_bodies["attachments"]), 2)

    def test_missing_video_upload_runs_while_normal_shift_video_generates(self):
        events = []
        upload_started = threading.Event()
        release_upload = threading.Event()
        tmp_root = None
        cfg = _base_cfg()
        cfg["workflow"] = {"max_runtime_seconds": 60, "video_upload_workers": 1}
        cfg["missing_loadcell_video"]["delete_after_upload"] = False

        class FakeUploader:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def upload_video(self, path, **_kwargs):
                kind = "overlay" if "overlay" in Path(path).name else "normal"
                events.append(f"{kind}_upload_started")
                upload_started.set()
                if not release_upload.wait(2):
                    raise RuntimeError("upload was not released by normal video generation")
                events.append(f"{kind}_upload_finished")
                return f"https://drive/{self.caster.id}/{Path(path).name}"

        class FakeOverlayGenerator:
            def __init__(self, date_str, shift, windows=None, output_name=None, normal_output_name=None, cfg=None, caster=None):
                self.normal_output_path = tmp_root / (normal_output_name or "missing_normal.mp4")

            def generate(self):
                events.append("missing_video_generate")
                return str(tmp_root / "missing_overlay.mp4")

        class FakeVideoGenerator:
            def __init__(self, date_str, shift, cfg=None, caster=None):
                self.image_root = tmp_root / "history"

            def generate(self):
                if not upload_started.wait(2):
                    raise RuntimeError("upload did not start before normal shift video generation")
                events.append("normal_shift_video_generate")
                release_upload.set()
                return str(tmp_root / "full_shift.mp4")

        with (
            TemporaryDirectory() as tmp,
            patch.object(report_workflow, "GDriveUploader", FakeUploader),
            patch.object(report_workflow, "ShiftVideoOverlayGenerator", FakeOverlayGenerator),
            patch.object(report_workflow, "ShiftVideoGenerator", FakeVideoGenerator),
            patch.object(report_workflow, "cleanup_shift_sources", lambda *args, **kwargs: {"deleted_dirs": [], "failed_dirs": {}}),
        ):
            tmp_root = Path(tmp)
            wf = self._workflow(tmp, selected_ids=["caster1"], cfg=cfg)
            run = ShiftRun("02-07-2026", "Shift_A")
            caster = wf.casters[0]
            result = report_workflow.CasterRunResult(caster=caster)
            result.state = {"date": run.date_str, "shift": run.shift_name, "caster_id": caster.id, "caster_number": caster.number}
            result.verified_summary = {
                "loadcell_missing_records": [
                    {"pipe_uid": "caster1-p1", "origin_time": "2026-07-02 06:10:00"}
                ]
            }
            wf.results[caster.id] = result

            wf.phase_missing_loadcell_videos(wf.casters, run)
            wf.phase_normal_shift_videos(wf.casters, run)
            wf.wait_for_video_uploads(run)

        self.assertLess(events.index("overlay_upload_started"), events.index("normal_shift_video_generate"))
        self.assertLess(events.index("normal_shift_video_generate"), events.index("overlay_upload_finished"))
        self.assertIn("normal_upload_finished", events)
        self.assertFalse(result.errors)
        self.assertTrue(result.state["missing_loadcell_overlay_video_drive_link"].startswith("https://drive/caster1/"))

    def test_test_mode_routes_full_workflow_mail_to_test_recipients(self):
        deliveries = []
        tmp_root = None

        class FakePipeExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift):
                path = tmp_root / f"{self.caster.id}.csv"
                path.write_text("raw", encoding="utf-8")
                return path, 10

            def export_diagnosis(self, date_str, shift):
                path = tmp_root / f"{self.caster.id}.xlsx"
                path.write_text("diagnosis", encoding="utf-8")
                return path, {
                    "pipe_count": 10,
                    "abnormal_count": 0,
                    "t_origin_gap_abnormal_count": 0,
                    "t_origin_gap_too_slow_count": 0,
                    "t_origin_gap_too_fast_count": 0,
                    "loadcell_missing_count": 0,
                }

        class FakeVerifiedExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift, csv_path, mode=None):
                path = tmp_root / f"{self.caster.id}_verified.csv"
                path.write_text("verified", encoding="utf-8")
                return path, {
                    "verified_count": 9,
                    "removed_count": 1,
                    "loadcell_missing_records": [],
                }

        class FakeMailer:
            def __init__(self, cfg=None):
                pass

            def send(self, subject, body, attachments=None, recipients=None):
                deliveries.append((subject, tuple(recipients or [])))

        class FakeUploader:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def upload_csv(self, path):
                return f"https://drive/{self.caster.id}/csv"

            def upload_video(self, path):
                return f"https://drive/{self.caster.id}/video"

        class FakeVideoGenerator:
            def __init__(self, date_str, shift, cfg=None, caster=None):
                self.caster = caster

            def generate(self):
                return f"{self.caster.id}.mp4"

        with (
            TemporaryDirectory() as tmp,
            patch.object(report_workflow, "PipeExporter", FakePipeExporter),
            patch.object(report_workflow, "VerifiedPipeExporter", FakeVerifiedExporter),
            patch.object(report_workflow, "EmailSender", FakeMailer),
            patch.object(report_workflow, "GDriveUploader", FakeUploader),
            patch.object(report_workflow, "ShiftVideoGenerator", FakeVideoGenerator),
        ):
            tmp_root = Path(tmp)
            wf = self._workflow(tmp, test_mode=True)
            wf.run(ShiftRun("02-07-2026", "Shift_A"))

        self.assertEqual([recipients for _, recipients in deliveries], [("test@example.com",)] * 3)
        self.assertEqual(
            [subject for subject, _ in deliveries],
            [
                "[TEST] Raw Pipe Production Report - 02-07-2026 - Shift A",
                "[TEST] Verified Pipe Production Report - 02-07-2026 - Shift A",
                "[TEST] Pipe Diagnosis Report - 02-07-2026 - Shift A",
            ],
        )

    def test_verified_only_test_mode_routes_csv_mail_to_test_recipients(self):
        deliveries = []
        tmp_root = None

        class FakePipeExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift):
                path = tmp_root / f"{self.caster.id}.csv"
                path.write_text("raw", encoding="utf-8")
                return path, 10

        class FakeVerifiedExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift, csv_path, mode=None):
                path = tmp_root / f"{self.caster.id}_verified.csv"
                path.write_text("verified", encoding="utf-8")
                return path, {
                    "verified_count": 9,
                    "removed_count": 1,
                    "loadcell_missing_records": [],
                }

        class FakeMailer:
            def __init__(self, cfg=None):
                pass

            def send(self, subject, body, attachments=None, recipients=None):
                deliveries.append((subject, body, tuple(recipients or [])))

        with (
            TemporaryDirectory() as tmp,
            patch.object(report_workflow, "PipeExporter", FakePipeExporter),
            patch.object(report_workflow, "VerifiedPipeExporter", FakeVerifiedExporter),
            patch.object(report_workflow, "EmailSender", FakeMailer),
        ):
            tmp_root = Path(tmp)
            wf = ShiftWorkflow(cfg=_base_cfg(), selected_ids=["caster2"], test_mode=True)
            wf.state_dir = Path(tmp)
            wf.run_verified_only(ShiftRun("02-07-2026", "Shift_A"))

        self.assertEqual([recipients for _, _, recipients in deliveries], [("test@example.com",)] * 2)
        self.assertTrue(all(subject.startswith("[TEST] ") for subject, _, _ in deliveries))
        verified_subject, verified_body = deliveries[1][0], deliveries[1][1]
        self.assertIn("Verified Pipe Production Report - 02-07-2026 - Shift A", verified_subject)
        self.assertIn("Caster 2 : 9", verified_body)
        self.assertNotIn("Caster id", verified_body)
        self.assertNotIn("Removed Pipe Count", verified_body)

    def test_verified_only_runs_raw_and_verified_without_raw_email_success(self):
        events = []
        tmp_root = None

        class FakePipeExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift):
                events.append(f"{self.caster.id}_raw_export")
                path = tmp_root / f"{self.caster.id}.csv"
                path.write_text("raw", encoding="utf-8")
                return path, 10

        class FakeVerifiedExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift, csv_path, mode=None):
                events.append(f"{self.caster.id}_verified_export")
                path = tmp_root / f"{self.caster.id}_verified.csv"
                path.write_text("verified", encoding="utf-8")
                return path, {
                    "verified_count": 9,
                    "removed_count": 1,
                    "loadcell_missing_records": [],
                }

        class FakeMailer:
            def __init__(self, cfg=None):
                pass

            def send(self, subject, body, attachments=None, recipients=None):
                if subject.startswith("Verified Pipe Production Report"):
                    events.append("verified_email")

        def skip_raw_email(workflow, casters, run):
            for caster in casters:
                result = workflow.results[caster.id]
                events.append(f"{caster.id}_raw_email_skipped")
                result.raw_email_sent = False
                result.state["emailed_csv"] = False
                workflow._save_state(run, caster, result.state)
            return False

        with (
            TemporaryDirectory() as tmp,
            patch.object(report_workflow, "PipeExporter", FakePipeExporter),
            patch.object(report_workflow, "VerifiedPipeExporter", FakeVerifiedExporter),
            patch.object(report_workflow, "EmailSender", FakeMailer),
            patch.object(ShiftWorkflow, "_send_consolidated_raw_csv_email", skip_raw_email),
        ):
            tmp_root = Path(tmp)
            wf = ShiftWorkflow(cfg=_base_cfg(), selected_ids=["caster1"])
            wf.state_dir = Path(tmp)
            run = ShiftRun("02-07-2026", "Shift_A")
            wf._save_state(run, wf.casters[0], {"status": "success", "diagnosis_summary": {"pipe_count": 10}})
            wf.run_verified_only(run)

        self.assertEqual(
            events,
            [
                "caster1_raw_export",
                "caster1_raw_email_skipped",
                "caster1_verified_export",
                "verified_email",
            ],
        )

    def test_failure_isolation_keeps_later_casters_running(self):
        events = []
        tmp_root = None

        class FakePipeExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift):
                if self.caster.id == "caster1":
                    raise RuntimeError("caster1 failed")
                path = tmp_root / f"{self.caster.id}.csv"
                path.write_text("raw", encoding="utf-8")
                return path, 5

        class FakeVerifiedExporter:
            def __init__(self, cfg=None, caster=None):
                self.caster = caster

            def export(self, date_str, shift, csv_path, mode=None):
                path = tmp_root / f"{self.caster.id}_verified.csv"
                path.write_text("verified", encoding="utf-8")
                return path, {
                    "verified_count": 5,
                    "removed_count": 0,
                    "loadcell_missing_records": [],
                }

        class FakeMailer:
            def __init__(self, cfg=None):
                pass

            def send(self, subject, body, attachments=None, recipients=None):
                events.append(subject)

        with (
            TemporaryDirectory() as tmp,
            patch.object(report_workflow, "PipeExporter", FakePipeExporter),
            patch.object(report_workflow, "VerifiedPipeExporter", FakeVerifiedExporter),
            patch.object(report_workflow, "EmailSender", FakeMailer),
            patch.object(report_workflow, "backoff_retry", lambda fn, **kwargs: fn()),
        ):
            tmp_root = Path(tmp)
            wf = self._workflow(tmp)
            wf.phase_raw_and_verified(wf.casters, ShiftRun("02-07-2026", "Shift_A"))

        self.assertTrue(wf.results["caster1"].errors)
        self.assertEqual(
            events,
            [
                "Raw Pipe Production Report - 02-07-2026 - Shift A",
                "Verified Pipe Production Report - 02-07-2026 - Shift A",
            ],
        )
