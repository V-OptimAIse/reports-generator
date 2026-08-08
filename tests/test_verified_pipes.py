import sqlite3
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from reports.pipes.verified_pipes import VerifiedPipeExporter


class VerifiedPipeExporterTest(unittest.TestCase):
    def _exporter(self):
        exporter = object.__new__(VerifiedPipeExporter)
        exporter.cfg = {"verified_pipes_gate_open_max_interval_seconds": 120}
        return exporter

    def _db_exporter(self, db_path: Path):
        exporter = self._exporter()
        exporter.db_path = db_path
        exporter.shifts = {"shift_b": ("14:00", "22:00")}
        return exporter

    def test_fetch_gate_events_reads_gate_openings_table(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            con = sqlite3.connect(db_path)
            try:
                con.execute(
                    """
                    CREATE TABLE gate_openings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gate_name TEXT NOT NULL,
                        t_open REAL NOT NULL
                    )
                    """
                )
                ts = datetime(2026, 7, 8, 15, 0).timestamp()
                con.execute("INSERT INTO gate_openings(gate_name, t_open) VALUES(?, ?)", ("gate2", ts))
                con.commit()
            finally:
                con.close()

            gate_df, shift_end = self._db_exporter(db_path)._fetch_gate_events_df("08-07-2026", "B")

        self.assertEqual(gate_df["gate_name"].tolist(), ["gate2"])
        self.assertEqual(len(gate_df), 1)
        self.assertEqual(shift_end, datetime(2026, 7, 8, 22, 0))

    def test_fetch_gate_events_falls_back_to_gate_cycles_table(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            con = sqlite3.connect(db_path)
            try:
                con.execute(
                    """
                    CREATE TABLE gate_openings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gate_name TEXT NOT NULL,
                        t_open REAL NOT NULL
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE gate_cycles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        t_gate1_open REAL,
                        t_gate1_close REAL,
                        t_gate2_open REAL,
                        t_gate2_close REAL
                    )
                    """
                )
                ts = datetime(2026, 7, 8, 15, 0).timestamp()
                con.execute("INSERT INTO gate_cycles(t_gate2_open) VALUES(?)", (ts,))
                con.commit()
            finally:
                con.close()

            gate_df, _shift_end = self._db_exporter(db_path)._fetch_gate_events_df("08-07-2026", "B")

        self.assertEqual(gate_df["gate_name"].tolist(), ["gate2"])
        self.assertEqual(len(self._exporter()._gate2_open_times(gate_df)), 1)

    def test_fetch_trolley_intersections_reads_only_pipe_on_trolley_rows(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            con = sqlite3.connect(db_path)
            try:
                con.execute(
                    """
                    CREATE TABLE trolley_gate2_intersections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL NOT NULL,
                        trolley_track_id INTEGER NOT NULL,
                        pipe_on_trolley INTEGER NOT NULL
                    )
                    """
                )
                positive_ts = datetime(2026, 7, 8, 15, 0).timestamp()
                negative_ts = datetime(2026, 7, 8, 15, 1).timestamp()
                con.execute(
                    """
                    INSERT INTO trolley_gate2_intersections(
                        timestamp, trolley_track_id, pipe_on_trolley
                    ) VALUES (?, ?, ?)
                    """,
                    (positive_ts, 10, 1),
                )
                con.execute(
                    """
                    INSERT INTO trolley_gate2_intersections(
                        timestamp, trolley_track_id, pipe_on_trolley
                    ) VALUES (?, ?, ?)
                    """,
                    (negative_ts, 11, 0),
                )
                con.commit()
            finally:
                con.close()

            trolley_df = self._db_exporter(db_path)._fetch_trolley_intersections_df(
                "08-07-2026",
                "B",
            )

        self.assertEqual(trolley_df["trolley_track_id"].tolist(), [10])
        self.assertEqual(trolley_df["pipe_on_trolley"].tolist(), [1])

    def test_missing_trolley_intersections_table_preserves_legacy_fallback(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pipes.db"
            con = sqlite3.connect(db_path)
            try:
                con.execute("CREATE TABLE gate_openings (gate_name TEXT, t_open REAL)")
                con.commit()
            finally:
                con.close()

            trolley_df = self._db_exporter(db_path)._fetch_trolley_intersections_df(
                "08-07-2026",
                "B",
            )

        self.assertTrue(trolley_df.empty)
        self.assertIn("pipe_on_trolley", trolley_df.columns)

    def test_missing_loadcell_checkpoint_one_passes_without_gate(self):
        pipe_df = pd.DataFrame([
            {
                "pipe_uid": "checkpoint-pass",
                "pipe_checkpoint": 1,
                "t_origin": "2026-06-26 22:00:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
        ])
        gate_df = pd.DataFrame(columns=["gate_name", "t_open_IST"])

        verified_df, summary = self._exporter()._apply_gate_verification(
            pipe_df,
            gate_df,
            mode="loadcell",
            shift_end=datetime(2026, 6, 27, 6, 0, 0),
        )

        self.assertEqual(verified_df["pipe_uid"].tolist(), ["checkpoint-pass"])
        self.assertEqual(summary["removed_count"], 0)
        self.assertEqual(summary["confirmed_by_checkpoint_count"], 1)
        self.assertEqual(summary["gate_fallback_checked_count"], 0)

    def test_missing_checkpoint_column_defaults_to_g2_window(self):
        pipe_df = pd.DataFrame([
            {
                "pipe_uid": "legacy-schema-pass",
                "t_origin": "2026-06-26 22:00:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
        ])
        gate_df = pd.DataFrame([
            {
                "gate_name": "g2",
                "t_open_IST": "2026-06-26 22:01:00",
            },
        ])

        verified_df, summary = self._exporter()._apply_gate_verification(
            pipe_df,
            gate_df,
            mode="loadcell",
            shift_end=datetime(2026, 6, 27, 6, 0, 0),
        )

        self.assertEqual(verified_df["pipe_uid"].tolist(), ["legacy-schema-pass"])
        self.assertEqual(summary["pipe_checkpoint_count"], 0)
        self.assertEqual(summary["confirmed_by_gate2_count"], 1)
    def test_missing_loadcell_checkpoint_zero_uses_g2_window(self):
        pipe_df = pd.DataFrame([
            {
                "pipe_uid": "g2-pass",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:00:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
            {
                "pipe_uid": "gate1-only-fail",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:02:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
            {
                "pipe_uid": "normal-loadcell-pass",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:04:00",
                "t_loadcell_enter": "2026-06-26 22:04:10",
                "t_loadcell_exit": "2026-06-26 22:04:20",
            },
        ])
        gate_df = pd.DataFrame([
            {
                "gate_name": "g2",
                "t_open_IST": "2026-06-26 22:01:00",
            },
            {
                "gate_name": "g1",
                "t_open_IST": "2026-06-26 22:03:00",
            },
        ])

        verified_df, summary = self._exporter()._apply_gate_verification(
            pipe_df,
            gate_df,
            mode="loadcell",
            shift_end=datetime(2026, 6, 27, 6, 0, 0),
        )

        self.assertEqual(
            verified_df["pipe_uid"].tolist(),
            ["g2-pass", "normal-loadcell-pass"],
        )
        self.assertEqual(summary["removed_count"], 1)
        self.assertEqual(summary["confirmed_by_gate2_count"], 1)
        self.assertEqual(summary["gate2_unconfirmed_count"], 1)

    def test_verification_fallback_order_is_checkpoint_then_trolley_then_gate2(self):
        pipe_df = pd.DataFrame([
            {
                "pipe_uid": "checkpoint-pass",
                "pipe_checkpoint": 1,
                "t_origin": "2026-06-26 22:00:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
            {
                "pipe_uid": "trolley-pass",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:02:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
            {
                "pipe_uid": "gate2-pass",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:04:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
            {
                "pipe_uid": "unconfirmed-remove",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:06:00",
                "t_loadcell_enter": "",
                "t_loadcell_exit": "",
            },
            {
                "pipe_uid": "normal-loadcell-pass",
                "pipe_checkpoint": 0,
                "t_origin": "2026-06-26 22:08:00",
                "t_loadcell_enter": "2026-06-26 22:08:10",
                "t_loadcell_exit": "2026-06-26 22:08:20",
            },
        ])
        trolley_df = pd.DataFrame([
            {
                "timestamp_IST": "2026-06-26 22:00:30",
                "trolley_track_id": 1,
                "pipe_on_trolley": 1,
            },
            {
                "timestamp_IST": "2026-06-26 22:02:30",
                "trolley_track_id": 2,
                "pipe_on_trolley": "yes",
            },
            {
                "timestamp_IST": "2026-06-26 22:06:30",
                "trolley_track_id": 3,
                "pipe_on_trolley": 0,
            },
        ])
        gate_df = pd.DataFrame([
            {
                # This gate event must not take credit because trolley confirmation is earlier.
                "gate_name": "gate2",
                "t_open_IST": "2026-06-26 22:02:40",
            },
            {
                "gate_name": "gate2",
                "t_open_IST": "2026-06-26 22:04:30",
            },
        ])

        verified_df, summary = self._exporter()._apply_gate_verification(
            pipe_df,
            gate_df,
            trolley_df,
            mode="loadcell",
            shift_end=datetime(2026, 6, 27, 6, 0, 0),
        )

        self.assertEqual(
            verified_df["pipe_uid"].tolist(),
            [
                "checkpoint-pass",
                "trolley-pass",
                "gate2-pass",
                "normal-loadcell-pass",
            ],
        )
        self.assertEqual(summary["verified_count"], 4)
        self.assertEqual(summary["removed_count"], 1)
        self.assertEqual(summary["confirmed_by_checkpoint_count"], 1)
        self.assertEqual(summary["confirmed_by_trolley_count"], 1)
        self.assertEqual(summary["confirmed_by_gate2_count"], 1)
        self.assertEqual(summary["trolley_fallback_checked_count"], 3)
        self.assertEqual(summary["gate_fallback_checked_count"], 2)
        self.assertEqual(summary["trolley_pipe_intersection_count"], 2)
        self.assertEqual(summary["confirmed_by_any_method_count"], 3)
        self.assertEqual(summary["unconfirmed_count"], 1)


if __name__ == "__main__":
    unittest.main()
