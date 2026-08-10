import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from cli.hourly_csv_report import HourlyCasterResult, HourlyCsvWorkflow, HourlyWindow
from reports.pipes.pipe_exporter import PipeExporter
from reports.pipes.verified_pipes import VerifiedPipeExporter


def _cfg():
    return {
        "database": {"path": "unused/pipes.db"},
        "history": {
            "image_root": "unused/history",
            "shifts": [
                {"name": "Shift_A", "start": "06:00", "end": "14:00"},
                {"name": "Shift_B", "start": "14:00", "end": "22:00"},
                {"name": "Shift_C", "start": "22:00", "end": "06:00"},
            ],
        },
        "outputs": {"csv_dir": "outputs/csv"},
        "email": {
            "sender": "sender@example.com",
            "password": "secret",
            "recipients": ["raw@example.com"],
            "test_recipients": ["test@example.com"],
            "smtp_server": "smtp.example.com",
            "smtp_port": 587,
        },
        "verified_pipe_records_recipients": ["verified@example.com"],
        "verified_pipes_mode": "loadcell",
        "casters": {
            "defaults": {
                "enabled": True,
                "var_root": "unused/var",
                "database_file": "pipes.db",
                "history_dir": "history",
                "outputs": {"csv_dir_template": "outputs/{caster_id}/csv"},
            },
            "items": [
                {"id": "caster2", "number": 2, "var_dir": "unused/var/caster2"},
                {"id": "caster3", "number": 3, "var_dir": "unused/var/caster3"},
            ],
        },
    }


class HourlyWindowTest(TestCase):
    def test_manual_window_is_half_open_same_day(self):
        window = HourlyWindow.from_cli("08-08-2026", "01:00", "02:00")

        self.assertEqual(window.start, datetime(2026, 8, 8, 1, 0))
        self.assertEqual(window.stop, datetime(2026, 8, 8, 2, 0))
        self.assertEqual(window.state_token, "20260808_010000_20260808_020000")

    def test_manual_window_can_cross_midnight(self):
        window = HourlyWindow.from_cli("08-08-2026", "23:00", "00:00")

        self.assertEqual(window.start, datetime(2026, 8, 8, 23, 0))
        self.assertEqual(window.stop, datetime(2026, 8, 9, 0, 0))

    def test_previous_completed_hour_handles_midnight(self):
        window = HourlyWindow.previous_completed_hour(datetime(2026, 8, 8, 0, 5))

        self.assertEqual(window.start, datetime(2026, 8, 7, 23, 0))
        self.assertEqual(window.stop, datetime(2026, 8, 8, 0, 0))

    def test_equal_start_and_stop_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must be different"):
            HourlyWindow.from_cli("08-08-2026", "01:00", "01:00")


class HourlyExporterBoundaryTest(TestCase):
    def test_raw_window_includes_start_and_excludes_stop(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            with closing(sqlite3.connect(db_path)) as con:
                con.execute(
                    """
                    CREATE TABLE pipes (
                        pipe_uid TEXT,
                        origin TEXT,
                        pipe_checkpoint INTEGER,
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
                for uid, event_time in (
                    ("at-start", datetime(2026, 8, 8, 1, 0)),
                    ("inside", datetime(2026, 8, 8, 1, 30)),
                    ("at-stop", datetime(2026, 8, 8, 2, 0)),
                ):
                    timestamp = int(event_time.timestamp())
                    con.execute(
                        """
                        INSERT INTO pipes (
                            pipe_uid, origin, pipe_checkpoint, t_origin,
                            t_loadcell_enter, t_loadcell_exit, weight,
                            weight_quality, weight_samples, state, last_seen_ts
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (uid, uid, 1, timestamp, timestamp, timestamp, 1.0, "ok", 1, "done", timestamp),
                    )
                con.commit()

            exporter = object.__new__(PipeExporter)
            exporter.db_path = db_path
            frame = exporter._fetch_window_df(
                datetime(2026, 8, 8, 1, 0),
                datetime(2026, 8, 8, 2, 0),
            )

        self.assertEqual(set(frame["pipe_uid"]), {"at-start", "inside"})

    def test_gate_window_includes_start_and_excludes_stop(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            with closing(sqlite3.connect(db_path)) as con:
                con.execute("CREATE TABLE gate_openings (gate_name TEXT, t_open INTEGER)")
                for event_time in (
                    datetime(2026, 8, 8, 1, 0),
                    datetime(2026, 8, 8, 1, 30),
                    datetime(2026, 8, 8, 2, 0),
                ):
                    con.execute(
                        "INSERT INTO gate_openings(gate_name, t_open) VALUES (?, ?)",
                        ("gate2", int(event_time.timestamp())),
                    )
                con.commit()

            exporter = object.__new__(VerifiedPipeExporter)
            exporter.db_path = db_path
            frame, window_end = exporter._fetch_gate_events_window(
                datetime(2026, 8, 8, 1, 0),
                datetime(2026, 8, 8, 2, 0),
            )

        self.assertEqual(len(frame), 2)
        self.assertEqual(window_end, datetime(2026, 8, 8, 2, 0))

    def test_trolley_window_includes_start_and_excludes_stop(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            with closing(sqlite3.connect(db_path)) as con:
                con.execute(
                    """
                    CREATE TABLE trolley_gate2_intersections (
                        timestamp REAL,
                        trolley_track_id INTEGER,
                        pipe_on_trolley INTEGER
                    )
                    """
                )
                for track_id, event_time in (
                    (1, datetime(2026, 8, 8, 1, 0)),
                    (2, datetime(2026, 8, 8, 1, 30)),
                    (3, datetime(2026, 8, 8, 2, 0)),
                ):
                    con.execute(
                        """
                        INSERT INTO trolley_gate2_intersections(
                            timestamp, trolley_track_id, pipe_on_trolley
                        ) VALUES (?, ?, 1)
                        """,
                        (int(event_time.timestamp()), track_id),
                    )
                con.commit()

            exporter = object.__new__(VerifiedPipeExporter)
            exporter.db_path = db_path
            frame = exporter._fetch_trolley_intersections_window(
                datetime(2026, 8, 8, 1, 0),
                datetime(2026, 8, 8, 2, 0),
            )

        self.assertEqual(frame["trolley_track_id"].tolist(), [1, 2])


class HourlyCsvWorkflowTest(TestCase):
    def test_verified_email_uses_html_table_with_enabled_casters_and_saved_counts(self):
        with TemporaryDirectory() as tmp:
            workflow = HourlyCsvWorkflow(cfg=_cfg())
            workflow.state_dir = Path(tmp) / "state"
            caster2, caster3 = workflow.casters

            saved_counts = {
                ("06:00", "07:00"): {"caster2": 0, "caster3": 12},
                ("07:00", "08:00"): {"caster2": 12, "caster3": 14},
            }
            for (start, stop), counts in saved_counts.items():
                saved_window = HourlyWindow.from_cli("08-08-2026", start, stop)
                for caster in workflow.casters:
                    state_path = workflow._state_path(saved_window, caster)
                    state_path.parent.mkdir(parents=True, exist_ok=True)
                    state_path.write_text(json.dumps({
                        "verified_summary": {"verified_count": counts[caster.id]},
                    }))

            current_window = HourlyWindow.from_cli("08-08-2026", "08:00", "09:00")
            current_results = [
                HourlyCasterResult(caster=caster2, verified_summary={"verified_count": 16}),
                HourlyCasterResult(caster=caster3, verified_summary={"verified_count": 16}),
            ]
            _, body = workflow._email_subject_and_body(
                "verified",
                current_window,
                current_results,
            )

        self.assertTrue(body.startswith("<!doctype html>"))
        self.assertIn("Hourly Verified Pipe Production Report</h1>", body)
        self.assertIn("max-width:680px", body)
        self.assertIn("font-family:Arial, Helvetica, sans-serif", body)
        self.assertIn('<table role="table"', body)
        self.assertIn(">Time Interval</th>", body)
        self.assertIn(">Caster 2</th>", body)
        self.assertIn(">Caster 3</th>", body)
        self.assertIn(">06:00 – 07:00</td>", body)
        self.assertIn(">13:00 – 14:00</td>", body)
        self.assertIn(">Total Count</td>", body)
        self.assertIn(">28</td>", body)
        self.assertIn(">42</td>", body)
        self.assertIn("2 CSV files attached</p>", body)
        self.assertNotIn("+---------------+", body)

    def test_verified_email_builds_rows_for_all_three_configured_shifts(self):
        cases = (
            (
                "08-08-2026", "06:00", "07:00", "08-08-2026", "A",
                "06:00 – 07:00", "13:00 – 14:00",
            ),
            (
                "08-08-2026", "14:00", "15:00", "08-08-2026", "B",
                "14:00 – 15:00", "21:00 – 22:00",
            ),
            (
                "09-08-2026", "01:00", "02:00", "08-08-2026", "C",
                "22:00 – 23:00", "05:00 – 06:00",
            ),
        )
        with TemporaryDirectory() as tmp:
            workflow = HourlyCsvWorkflow(cfg=_cfg(), selected_ids=["caster3"])
            workflow.state_dir = Path(tmp) / "state"
            result = HourlyCasterResult(
                caster=workflow.casters[0],
                verified_summary={"verified_count": 5},
            )

            for date_str, start, stop, shift_date, shift_name, first_row, last_row in cases:
                with self.subTest(shift=shift_name):
                    window = HourlyWindow.from_cli(date_str, start, stop)
                    _, body = workflow._email_subject_and_body("verified", window, [result])

                    self.assertIn(f">Date:</span> {shift_date}</td>", body)
                    self.assertIn(f">Shift:</span> {shift_name}</td>", body)
                    self.assertIn(f">{first_row}</td>", body)
                    self.assertIn(f">{last_row}</td>", body)

    def test_raw_email_body_keeps_the_existing_window_format(self):
        workflow = HourlyCsvWorkflow(cfg=_cfg(), selected_ids=["caster3"])
        window = HourlyWindow.from_cli("08-08-2026", "06:00", "07:00")
        result = HourlyCasterResult(caster=workflow.casters[0], raw_count=4)

        _, body = workflow._email_subject_and_body("raw", window, [result])

        self.assertIn("Window : 06:00:00 to 07:00:00", body)
        self.assertIn("Caster 3 : 4", body)
        self.assertNotIn("SHIFT :", body)

    def test_sends_only_consolidated_raw_and_verified_csv_emails(self):
        events = []
        sent_messages = []

        with TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            class FakePipeExporter:
                def __init__(self, cfg=None, caster=None):
                    self.caster = caster

                def export_window(self, start, stop):
                    events.append(("raw", self.caster.id, start, stop))
                    path = output_root / f"{self.caster.id}_raw.csv"
                    path.write_text("pipe_uid,t_origin\np1,2026-08-08 01:10:00\n")
                    return path, 1

            class FakeVerifiedExporter:
                def __init__(self, cfg=None, caster=None):
                    self.caster = caster

                def export_window(self, start, stop, raw_path, *, mode=None):
                    events.append(("verified", self.caster.id, start, stop, str(raw_path), mode))
                    path = output_root / f"{self.caster.id}_verified.csv"
                    path.write_text("Pipe Number,Origin Time\n1,2026-08-08 01:10:00\n")
                    return path, {"verified_count": 1, "removed_count": 0}

            class FakeMailer:
                def __init__(self, cfg=None):
                    pass

                def send(
                    self,
                    subject,
                    body,
                    attachments=None,
                    recipients=None,
                    html_body=None,
                ):
                    sent_messages.append({
                        "subject": subject,
                        "body": body,
                        "html_body": html_body,
                        "attachments": list(attachments or []),
                        "recipients": list(recipients or []),
                    })

            window = HourlyWindow.from_cli("08-08-2026", "01:00", "02:00")
            with (
                patch("cli.hourly_csv_report.PipeExporter", FakePipeExporter),
                patch("cli.hourly_csv_report.VerifiedPipeExporter", FakeVerifiedExporter),
                patch("cli.hourly_csv_report.EmailSender", FakeMailer),
            ):
                workflow = HourlyCsvWorkflow(cfg=_cfg(), selected_ids=["caster3"])
                workflow.state_dir = output_root / "state"
                self.assertTrue(workflow.run(window))

                # A new systemd/manual invocation for the same successful window is idempotent.
                second_workflow = HourlyCsvWorkflow(cfg=_cfg(), selected_ids=["caster3"])
                second_workflow.state_dir = workflow.state_dir
                self.assertTrue(second_workflow.run(window))

            state_path = workflow._state_path(window, workflow.casters[0])
            state = json.loads(state_path.read_text())

        self.assertEqual([event[0] for event in events], ["raw", "verified"])
        self.assertEqual(len(sent_messages), 2)
        self.assertIn("Hourly Raw Pipe", sent_messages[0]["subject"])
        self.assertEqual(sent_messages[0]["recipients"], ["raw@example.com"])
        self.assertIsNone(sent_messages[0]["html_body"])
        self.assertIn("Hourly Verified Pipe", sent_messages[1]["subject"])
        self.assertEqual(sent_messages[1]["recipients"], ["verified@example.com"])
        self.assertIn("<table", sent_messages[1]["html_body"])
        self.assertNotIn("<table", sent_messages[1]["body"])
        self.assertEqual(state["status"], "success")
        self.assertTrue(state["raw_email_sent"])
        self.assertTrue(state["verified_email_sent"])

    def test_no_email_generates_both_csvs_without_constructing_mailer(self):
        events = []

        with TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            class FakePipeExporter:
                def __init__(self, cfg=None, caster=None):
                    self.caster = caster

                def export_window(self, start, stop):
                    events.append("raw")
                    path = output_root / "raw.csv"
                    path.write_text("pipe_uid,t_origin\n")
                    return path, 0

            class FakeVerifiedExporter:
                def __init__(self, cfg=None, caster=None):
                    self.caster = caster

                def export_window(self, start, stop, raw_path, *, mode=None):
                    events.append("verified")
                    path = output_root / "verified.csv"
                    path.write_text("Pipe Number,Origin Time\n")
                    return path, {"verified_count": 0, "removed_count": 0}

            window = HourlyWindow.from_cli("08-08-2026", "01:00", "02:00")
            with (
                patch("cli.hourly_csv_report.PipeExporter", FakePipeExporter),
                patch("cli.hourly_csv_report.VerifiedPipeExporter", FakeVerifiedExporter),
                patch("cli.hourly_csv_report.EmailSender") as mailer,
            ):
                workflow = HourlyCsvWorkflow(
                    cfg=_cfg(),
                    selected_ids=["caster3"],
                    send_email=False,
                )
                workflow.state_dir = output_root / "state"
                self.assertTrue(workflow.run(window))

            state = json.loads(workflow._state_path(window, workflow.casters[0]).read_text())

        self.assertEqual(events, ["raw", "verified"])
        mailer.assert_not_called()
        self.assertEqual(state["status"], "generated_no_email")


if __name__ == "__main__":
    import unittest

    unittest.main()
