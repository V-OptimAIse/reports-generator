import argparse
import logging
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from reports.common.config_loader import load_runtime_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)


class VerifiedPipeExporter:
    """
    Creates a client-facing pipe CSV after removing unverified pipe records.

    Current mode:
        loadcell - checkpoint/G2 verification is applied only to rows
                   missing loadcell entry and/or exit time.

    Future mode:
        all      - checkpoint + gate verification is applied to every pipe row.
    """

    IST_OFFSET = ("+5 hours", "+30 minutes")
    VALID_MODES = {"loadcell", "all"}
    PIPE_TIME_COLUMN = "t_origin"
    CHECKPOINT_COLUMN = "pipe_checkpoint"
    LOADCELL_COLUMNS = ("t_loadcell_enter", "t_loadcell_exit")
    GATE_OPEN_COLUMNS = ("t_open_IST", "t_gate1_open_IST", "t_gate2_open_IST")
    GATE2_OPEN_COLUMNS = ("t_gate2_open_IST",)
    DEFAULT_GATE_OPEN_MAX_INTERVAL_SECONDS = 120
    CLIENT_COLUMNS = ("Pipe Number", "Origin Time")

    def __init__(self, cfg: dict | None = None, caster=None):
        self.root = Path(__file__).resolve().parents[2]
        self.cfg = cfg or load_runtime_config()
        self.caster = caster
        self.caster_file_token = getattr(caster, "file_token", None)

        self.db_path = (self.root / self.cfg["database"]["path"]).resolve()

        csv_dir = self.cfg.get("outputs", {}).get("csv_dir", "outputs/csv")
        self.output_dir = self.root / csv_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        shifts_cfg = self.cfg.get("history", {}).get("shifts", [])
        if not shifts_cfg:
            raise ValueError("No shifts defined in runtime.yaml")

        self.shifts = {s["name"].lower(): (s["start"], s["end"]) for s in shifts_cfg}

    @staticmethod
    def _load_yaml(path: Path) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _normalize_shift_key(shift: str) -> str:
        s = str(shift).strip().lower()
        if s in {"a", "b", "c"}:
            return f"shift_{s}"
        return s

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        normalized = str(mode or "loadcell").strip().lower()
        if normalized not in VerifiedPipeExporter.VALID_MODES:
            raise ValueError("Invalid verified pipes mode. Use 'loadcell' or 'all'.")
        return normalized

    @staticmethod
    def _parse_positive_seconds(value, *, default_seconds: int) -> int:
        if value is None:
            return default_seconds

        if isinstance(value, (int, float)):
            seconds = int(value)
        else:
            value_text = str(value).strip()
            if not value_text:
                return default_seconds

            if value_text.replace(".", "", 1).isdigit():
                seconds = int(float(value_text))
            else:
                seconds = int(pd.to_timedelta(value_text).total_seconds())

        if seconds <= 0:
            raise ValueError("verified_pipes_gate_open_max_interval_seconds must be greater than 0")

        return seconds

    def _gate_open_max_interval_seconds(self) -> int:
        value = (
            self.cfg.get("verified_pipes_gate_open_max_interval_seconds")
            or self.cfg.get("verified_pipes_gate_open_max_seconds")
        )
        return self._parse_positive_seconds(
            value,
            default_seconds=self.DEFAULT_GATE_OPEN_MAX_INTERVAL_SECONDS,
        )

    def _shift_window(self, date_str: str, shift: str):
        shift_key = self._normalize_shift_key(shift)
        if shift_key not in self.shifts:
            raise ValueError(f"Invalid shift: {shift}")

        start_s, end_s = self.shifts[shift_key]

        start = datetime.strptime(f"{date_str} {start_s}", "%d-%m-%Y %H:%M")
        end = datetime.strptime(f"{date_str} {end_s}", "%d-%m-%Y %H:%M")

        if end <= start:
            end += timedelta(days=1)

        return int(start.timestamp()), int(end.timestamp()), start, end

    @staticmethod
    def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _empty_gate_events_df() -> pd.DataFrame:
        return pd.DataFrame(columns=["gate_name", "t_open", "t_open_IST"])

    def _build_gate_openings_query(
        self,
        table_name: str,
        *,
        inclusive_end: bool = True,
    ) -> str:
        h, m = self.IST_OFFSET
        end_operator = "<=" if inclusive_end else "<"
        return f"""
        SELECT
            gate_name,
            t_open,
            datetime(t_open,'unixepoch','{h}','{m}') AS t_open_IST
        FROM {table_name}
        WHERE t_open >= ? AND t_open {end_operator} ?
        ORDER BY t_open;
        """

    def _build_gate_cycles_query(self, *, inclusive_end: bool = True) -> str:
        h, m = self.IST_OFFSET
        end_operator = "<=" if inclusive_end else "<"
        return f"""
        SELECT
            'gate2' AS gate_name,
            t_gate2_open AS t_open,
            datetime(t_gate2_open,'unixepoch','{h}','{m}') AS t_open_IST,
            datetime(t_gate2_open,'unixepoch','{h}','{m}') AS t_gate2_open_IST
        FROM gate_cycles
        WHERE t_gate2_open >= ? AND t_gate2_open {end_operator} ?
        ORDER BY t_gate2_open;
        """

    def _fetch_gate_events_between(
        self,
        start_ts: int,
        end_ts: int,
        *,
        inclusive_end: bool,
    ) -> pd.DataFrame:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        with closing(sqlite3.connect(self.db_path)) as con:
            gate_df = None
            if self._table_exists(con, "gate_openings"):
                gate_df = pd.read_sql_query(
                    self._build_gate_openings_query(
                        "gate_openings",
                        inclusive_end=inclusive_end,
                    ),
                    con,
                    params=(start_ts, end_ts),
                )
            if (gate_df is None or gate_df.empty) and self._table_exists(con, "gate_open_events"):
                gate_df = pd.read_sql_query(
                    self._build_gate_openings_query(
                        "gate_open_events",
                        inclusive_end=inclusive_end,
                    ),
                    con,
                    params=(start_ts, end_ts),
                )
            if (gate_df is None or gate_df.empty) and self._table_exists(con, "gate_cycles"):
                gate_df = pd.read_sql_query(
                    self._build_gate_cycles_query(inclusive_end=inclusive_end),
                    con,
                    params=(start_ts, end_ts),
                )
            if gate_df is None:
                logger.warning(
                    "No gate opening table found; verified-pipes G2 fallback will have no gate events | db=%s",
                    self.db_path,
                )
                gate_df = self._empty_gate_events_df()

        return gate_df

    def _fetch_gate_events_df(self, date_str: str, shift: str) -> tuple[pd.DataFrame, datetime]:
        start_ts, end_ts, _start_dt, end_dt = self._shift_window(date_str, shift)
        gate_df = self._fetch_gate_events_between(
            start_ts,
            end_ts,
            inclusive_end=True,
        )
        return gate_df, end_dt

    def _fetch_gate_events_window(
        self,
        start_dt: datetime,
        stop_dt: datetime,
    ) -> tuple[pd.DataFrame, datetime]:
        if stop_dt <= start_dt:
            raise ValueError("Report stop time must be after start time")
        gate_df = self._fetch_gate_events_between(
            int(start_dt.timestamp()),
            int(stop_dt.timestamp()),
            inclusive_end=False,
        )
        return gate_df, stop_dt

    @staticmethod
    def _missing_mask(series: pd.Series) -> pd.Series:
        as_text = series.astype("string").str.strip().str.lower()
        return series.isna() | as_text.isna() | as_text.isin({"", "none", "nan", "nat", "null"})

    @staticmethod
    def _parse_datetime_series(series: pd.Series) -> pd.Series:
        as_text = series.astype("string").str.strip()
        parsed = pd.to_datetime(as_text, errors="coerce")

        dayfirst_mask = as_text.str.match(r"^\d{2}[-/]\d{2}[-/]\d{4}\b", na=False)
        if dayfirst_mask.any():
            dayfirst_parsed = pd.to_datetime(as_text[dayfirst_mask], errors="coerce", dayfirst=True)
            parsed.loc[dayfirst_mask] = dayfirst_parsed.loc[dayfirst_mask]

        return parsed

    def _gate_open_times(self, gate_df: pd.DataFrame) -> pd.Series:
        present_columns = [c for c in self.GATE_OPEN_COLUMNS if c in gate_df.columns]
        if not present_columns and "t_open" not in gate_df.columns:
            raise ValueError(
                "Gate open data must include t_open/t_open_IST or old t_gate1_open_IST/t_gate2_open_IST"
            )

        parsed = [
            self._parse_datetime_series(gate_df[column])
            for column in present_columns
        ]
        if "t_open" in gate_df.columns:
            unix_open = pd.to_numeric(gate_df["t_open"], errors="coerce")
            if unix_open.notna().any():
                parsed.append(
                    pd.to_datetime(unix_open, unit="s", errors="coerce", utc=True)
                    .dt.tz_convert("Asia/Kolkata")
                    .dt.tz_localize(None)
                )
            else:
                parsed.append(self._parse_datetime_series(gate_df["t_open"]))

        return (
            pd.concat(parsed, ignore_index=True)
            .dropna()
            .drop_duplicates()
            .sort_values(ignore_index=True)
        )

    @staticmethod
    def _gate2_name_mask(series: pd.Series) -> pd.Series:
        names = series.astype("string").str.strip().str.lower()
        compact = names.str.replace(r"[\s_-]+", "", regex=True)
        return compact.isin({"2", "g2", "gate2"}) | compact.str.endswith("gate2", na=False)

    def _gate2_open_times(self, gate_df: pd.DataFrame) -> pd.Series:
        parsed = []

        for column in self.GATE2_OPEN_COLUMNS:
            if column in gate_df.columns:
                parsed.append(self._parse_datetime_series(gate_df[column]))

        if "gate_name" in gate_df.columns:
            gate2_df = gate_df.loc[self._gate2_name_mask(gate_df["gate_name"])]
            if "t_open_IST" in gate2_df.columns:
                parsed.append(self._parse_datetime_series(gate2_df["t_open_IST"]))
            if "t_open" in gate2_df.columns:
                unix_open = pd.to_numeric(gate2_df["t_open"], errors="coerce")
                if unix_open.notna().any():
                    parsed.append(
                        pd.to_datetime(unix_open, unit="s", errors="coerce", utc=True)
                        .dt.tz_convert("Asia/Kolkata")
                        .dt.tz_localize(None)
                    )
                else:
                    parsed.append(self._parse_datetime_series(gate2_df["t_open"]))

        if not parsed:
            raise ValueError("Gate open data must include gate_name or t_gate2_open_IST for G2 verification")

        return (
            pd.concat(parsed, ignore_index=True)
            .dropna()
            .drop_duplicates()
            .sort_values(ignore_index=True)
        )

    def _loadcell_missing_mask(self, pipe_df: pd.DataFrame) -> pd.Series:
        missing_columns = [c for c in self.LOADCELL_COLUMNS if c not in pipe_df.columns]
        if missing_columns:
            raise ValueError(f"Pipe CSV missing required columns: {', '.join(missing_columns)}")

        missing = pd.Series(False, index=pipe_df.index)
        for column in self.LOADCELL_COLUMNS:
            missing = missing | self._missing_mask(pipe_df[column])
        return missing

    def _pipe_checkpoint_mask(self, pipe_df: pd.DataFrame) -> pd.Series:
        if self.CHECKPOINT_COLUMN not in pipe_df.columns:
            return pd.Series(False, index=pipe_df.index)
        values = pipe_df[self.CHECKPOINT_COLUMN]
        truthy_text = values.astype("string").str.strip().str.lower().isin({"true", "yes"})
        return pd.to_numeric(values, errors="coerce").eq(1) | truthy_text

    def _add_loadcell_missing_columns(self, pipe_df: pd.DataFrame) -> pd.DataFrame:
        missing_columns = [c for c in self.LOADCELL_COLUMNS if c not in pipe_df.columns]
        if missing_columns:
            raise ValueError(f"Pipe CSV missing required columns: {', '.join(missing_columns)}")

        pipe_df["_verified_missing_loadcell_enter"] = self._missing_mask(
            pipe_df["t_loadcell_enter"]
        )
        pipe_df["_verified_missing_loadcell_exit"] = self._missing_mask(
            pipe_df["t_loadcell_exit"]
        )
        pipe_df["_verified_missing_loadcell"] = (
            pipe_df["_verified_missing_loadcell_enter"]
            | pipe_df["_verified_missing_loadcell_exit"]
        )
        return pipe_df

    @staticmethod
    def _safe_text(value) -> str:
        if pd.isna(value):
            return ""
        return str(value)

    def _loadcell_missing_records(self, work: pd.DataFrame) -> list[dict]:
        records = []
        missing_df = work.loc[work["_verified_missing_loadcell"]].copy()
        missing_df = missing_df.sort_values("_verified_t_origin", kind="mergesort")

        for _, row in missing_df.iterrows():
            origin_dt = row["_verified_t_origin"]
            if pd.isna(origin_dt):
                origin_time = ""
            else:
                origin_time = pd.Timestamp(origin_dt).strftime("%Y-%m-%d %H:%M:%S")

            records.append({
                "pipe_uid": self._safe_text(row.get("pipe_uid", "")),
                "origin_time": origin_time,
                "origin_time_raw": self._safe_text(row.get(self.PIPE_TIME_COLUMN, "")),
                "missing_loadcell_enter": bool(row["_verified_missing_loadcell_enter"]),
                "missing_loadcell_exit": bool(row["_verified_missing_loadcell_exit"]),
            })

        return records

    def _apply_gate_verification(
        self,
        pipe_df: pd.DataFrame,
        gate_df: pd.DataFrame,
        *,
        mode: str,
        shift_end: datetime | None,
    ) -> tuple[pd.DataFrame, dict]:
        mode = self._normalize_mode(mode)

        if self.PIPE_TIME_COLUMN not in pipe_df.columns:
            raise ValueError(f"Pipe CSV missing required column: {self.PIPE_TIME_COLUMN}")

        work = pipe_df.copy()
        work["_verified_original_index"] = range(len(work))
        work["_verified_t_origin"] = self._parse_datetime_series(work[self.PIPE_TIME_COLUMN])
        work = self._add_loadcell_missing_columns(work)
        work["_verified_pipe_checkpoint"] = self._pipe_checkpoint_mask(work)

        work = work.sort_values("_verified_t_origin", kind="mergesort").reset_index(drop=True)
        work["_verified_next_t_origin"] = work["_verified_t_origin"].shift(-1)
        max_interval_seconds = self._gate_open_max_interval_seconds()
        work["_verified_max_gate_window_end"] = (
            work["_verified_t_origin"] + pd.to_timedelta(max_interval_seconds, unit="s")
        )

        if shift_end is not None:
            shift_end_ts = pd.Timestamp(shift_end)
            work["_verified_window_end"] = work["_verified_next_t_origin"].fillna(shift_end_ts)
        else:
            work["_verified_window_end"] = work["_verified_next_t_origin"]

        work["_verified_window_end"] = pd.concat(
            [work["_verified_window_end"], work["_verified_max_gate_window_end"]],
            axis=1,
        ).min(axis=1)

        gate2_times = self._gate2_open_times(gate_df)

        verify_mask = pd.Series(mode == "all", index=work.index)
        if mode == "loadcell":
            verify_mask = work["_verified_missing_loadcell"].copy()

        gate_fallback_mask = verify_mask & ~work["_verified_pipe_checkpoint"]
        has_gate2_open = pd.Series(False, index=work.index)
        checked = work.loc[gate_fallback_mask, ["_verified_t_origin", "_verified_window_end"]].dropna()
        checked = checked.loc[checked["_verified_window_end"] > checked["_verified_t_origin"]]
        gate_values = gate2_times.to_numpy(dtype="datetime64[ns]")
        if len(gate_values) and not checked.empty:
            starts = checked["_verified_t_origin"].to_numpy(dtype="datetime64[ns]")
            ends = checked["_verified_window_end"].to_numpy(dtype="datetime64[ns]")
            positions = np.searchsorted(gate_values, starts, side="left")
            matches = positions < len(gate_values)
            matches[matches] = gate_values[positions[matches]] < ends[matches]
            has_gate2_open.loc[checked.index] = matches

        confirmed_by_checkpoint = verify_mask & work["_verified_pipe_checkpoint"]
        confirmed_by_gate2 = gate_fallback_mask & has_gate2_open
        confirmed_mask = confirmed_by_checkpoint | confirmed_by_gate2
        keep_mask = ~verify_mask | confirmed_mask
        removed_df = work.loc[~keep_mask]
        verified_df = (
            work.loc[keep_mask]
            .sort_values("_verified_original_index", kind="mergesort")
            .drop(
                columns=[
                    "_verified_original_index",
                    "_verified_t_origin",
                    "_verified_missing_loadcell_enter",
                    "_verified_missing_loadcell_exit",
                    "_verified_missing_loadcell",
                    "_verified_pipe_checkpoint",
                    "_verified_next_t_origin",
                    "_verified_max_gate_window_end",
                    "_verified_window_end",
                ]
            )
        )

        summary = {
            "mode": mode,
            "input_count": int(len(pipe_df)),
            "verified_count": int(len(verified_df)),
            "removed_count": int(len(removed_df)),
            "loadcell_missing_count": int(work["_verified_missing_loadcell"].sum()),
            "gate_open_count": int(len(gate2_times)),
            "gate2_open_count": int(len(gate2_times)),
            "gate_open_max_interval_seconds": int(max_interval_seconds),
            "checked_count": int(verify_mask.sum()),
            "gate_fallback_checked_count": int(gate_fallback_mask.sum()),
            "confirmed_by_checkpoint_count": int(confirmed_by_checkpoint.sum()),
            "confirmed_by_gate_count": int(confirmed_by_gate2.sum()),
            "confirmed_by_gate2_count": int(confirmed_by_gate2.sum()),
            "confirmed_by_checkpoint_and_gate_count": int(confirmed_mask.sum()),
            "pipe_checkpoint_count": int(work["_verified_pipe_checkpoint"].sum()),
            "checkpoint_unconfirmed_count": int(
                (verify_mask & ~work["_verified_pipe_checkpoint"]).sum()
            ),
            "gate_unconfirmed_count": int((gate_fallback_mask & ~has_gate2_open).sum()),
            "gate2_unconfirmed_count": int((gate_fallback_mask & ~has_gate2_open).sum()),
            "unconfirmed_count": int((verify_mask & ~confirmed_mask).sum()),
            "loadcell_missing_records": self._loadcell_missing_records(work),
        }

        return verified_df, summary

    def _build_client_csv_df(self, verified_df: pd.DataFrame) -> pd.DataFrame:
        sorted_df = verified_df.copy()
        sorted_df["_verified_t_origin"] = self._parse_datetime_series(sorted_df[self.PIPE_TIME_COLUMN])
        sorted_df = sorted_df.sort_values("_verified_t_origin", kind="mergesort").reset_index(drop=True)

        return pd.DataFrame({
            "Pipe Number": range(1, len(sorted_df) + 1),
            "Origin Time": sorted_df[self.PIPE_TIME_COLUMN],
        }, columns=list(self.CLIENT_COLUMNS))

    def export(
        self,
        date_str: str,
        shift: str,
        pipes_csv_path: str | Path,
        *,
        mode: str | None = None,
    ) -> tuple[Path, dict]:
        configured_mode = (
            self.cfg.get("verified_pipes_mode")
            or self.cfg.get("verfied_pipes_mode")
            or "loadcell"
        )
        mode = self._normalize_mode(mode or configured_mode)

        gate_df, shift_end = self._fetch_gate_events_df(date_str, shift)
        return self.export_from_dataframes(
            date_str,
            shift,
            pd.read_csv(pipes_csv_path),
            gate_df,
            mode=mode,
            shift_end=shift_end,
        )

    def export_window(
        self,
        start_dt: datetime,
        stop_dt: datetime,
        pipes_csv_path: str | Path,
        *,
        mode: str | None = None,
    ) -> tuple[Path, dict]:
        """Export verified pipes for a half-open custom window."""
        configured_mode = (
            self.cfg.get("verified_pipes_mode")
            or self.cfg.get("verfied_pipes_mode")
            or "loadcell"
        )
        mode = self._normalize_mode(mode or configured_mode)
        gate_df, window_end = self._fetch_gate_events_window(start_dt, stop_dt)
        window_token = self._window_file_token(start_dt, stop_dt)
        out_path, summary = self.export_from_dataframes(
            start_dt.strftime("%d-%m-%Y"),
            window_token,
            pd.read_csv(pipes_csv_path),
            gate_df,
            mode=mode,
            shift_end=window_end,
        )
        summary["window_start"] = start_dt.isoformat(timespec="seconds")
        summary["window_stop"] = stop_dt.isoformat(timespec="seconds")
        return out_path, summary

    @staticmethod
    def _window_file_token(start_dt: datetime, stop_dt: datetime) -> str:
        if start_dt.date() == stop_dt.date():
            return f"window_{start_dt:%H%M%S}_{stop_dt:%H%M%S}"
        return f"window_{start_dt:%H%M%S}_{stop_dt:%d%m%Y_%H%M%S}"

    def export_from_csvs(
        self,
        date_str: str,
        shift: str,
        pipes_csv_path: str | Path,
        gate_events_csv_path: str | Path,
        *,
        mode: str | None = None,
    ) -> tuple[Path, dict]:
        configured_mode = (
            self.cfg.get("verified_pipes_mode")
            or self.cfg.get("verfied_pipes_mode")
            or "loadcell"
        )
        mode = self._normalize_mode(mode or configured_mode)
        _start_ts, _end_ts, _start_dt, shift_end = self._shift_window(date_str, shift)

        return self.export_from_dataframes(
            date_str,
            shift,
            pd.read_csv(pipes_csv_path),
            pd.read_csv(gate_events_csv_path),
            mode=mode,
            shift_end=shift_end,
        )

    def export_from_dataframes(
        self,
        date_str: str,
        shift: str,
        pipe_df: pd.DataFrame,
        gate_df: pd.DataFrame,
        *,
        mode: str,
        shift_end: datetime | None = None,
    ) -> tuple[Path, dict]:
        verified_df, summary = self._apply_gate_verification(
            pipe_df,
            gate_df,
            mode=mode,
            shift_end=shift_end,
        )

        timestamp = datetime.now().strftime("%H%M%S")
        shift_key = self._normalize_shift_key(shift)
        caster_part = f"_{self.caster_file_token}" if getattr(self, "caster_file_token", None) else ""
        filename = f"verified_pipes{caster_part}_{date_str.replace('-','')}_{shift_key}_{timestamp}.csv"
        out_path = self.output_dir / filename
        client_df = self._build_client_csv_df(verified_df)
        client_df.to_csv(out_path, index=False)

        logger.info(
            "VERIFIED PIPE REPORT | shift=%s | mode=%s | input=%s | verified=%s | removed=%s | saved=%s",
            shift_key.upper(),
            summary["mode"],
            summary["input_count"],
            summary["verified_count"],
            summary["removed_count"],
            out_path,
        )
        return out_path, summary


def main():
    parser = argparse.ArgumentParser(description="Create a verified pipe CSV")
    parser.add_argument("--date", required=True, help="DD-MM-YYYY")
    parser.add_argument("--shift", required=True, help="A/B/C or Shift_A/Shift_B/Shift_C")
    parser.add_argument(
        "--pipes-csv",
        help="Existing pipe CSV. If omitted, raw pipes are exported from the database first.",
    )
    parser.add_argument(
        "--gate-events-csv",
        "--gate-cycles-csv",
        dest="gate_events_csv",
        help="Existing gate open events CSV. If omitted, gate events are read from the database.",
    )
    parser.add_argument("--mode", choices=sorted(VerifiedPipeExporter.VALID_MODES))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    exporter = VerifiedPipeExporter()
    pipes_csv_path = args.pipes_csv

    if not pipes_csv_path:
        from reports.pipes.pipe_exporter import PipeExporter

        shift_key = exporter._normalize_shift_key(args.shift)
        pipes_csv_path, pipe_count = PipeExporter().export(args.date, shift_key)
        print(f"Exported raw pipes to {pipes_csv_path} ({pipe_count} rows)")

    if args.gate_events_csv:
        out_path, summary = exporter.export_from_csvs(
            args.date,
            args.shift,
            pipes_csv_path,
            args.gate_events_csv,
            mode=args.mode,
        )
    else:
        out_path, summary = exporter.export(
            args.date,
            args.shift,
            pipes_csv_path,
            mode=args.mode,
        )

    print(f"Exported verified pipes to {out_path}")
    print(summary)


if __name__ == "__main__":
    main()
