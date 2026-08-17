from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reports.common.caster_config import (
    CasterConfig,
    build_caster_runtime_config,
    caster_label,
    resolve_enabled_casters,
)
from reports.common.config_loader import load_runtime_config
from reports.common.table_image import save_table_image
from reports.pipes.pipe_exporter import PipeExporter
from reports.pipes.verified_pipes import VerifiedPipeExporter


logger = logging.getLogger("reports-generator.hourly")


@dataclass(frozen=True)
class HourlyWindow:
    """A local-time, half-open report window: start <= event time < stop."""

    start: datetime
    stop: datetime

    def __post_init__(self):
        if self.stop <= self.start:
            raise ValueError("Report stop time must be after start time")

    @classmethod
    def from_cli(cls, date_str: str, start_time: str, stop_time: str) -> "HourlyWindow":
        try:
            report_date = datetime.strptime(date_str, "%d-%m-%Y").date()
        except ValueError as exc:
            raise ValueError("--date must use DD-MM-YYYY") from exc

        start_clock = cls._parse_clock(start_time, "--start")
        stop_clock = cls._parse_clock(stop_time, "--stop")
        if start_clock == stop_clock:
            raise ValueError("--start and --stop must be different")

        start = datetime.combine(report_date, start_clock)
        stop = datetime.combine(report_date, stop_clock)
        if stop < start:
            stop += timedelta(days=1)
        return cls(start=start, stop=stop)

    @classmethod
    def previous_completed_hour(cls, now: datetime | None = None) -> "HourlyWindow":
        current = now or datetime.now()
        stop = current.replace(minute=0, second=0, microsecond=0)
        return cls(start=stop - timedelta(hours=1), stop=stop)

    @staticmethod
    def _parse_clock(value: str, argument_name: str):
        text = str(value or "").strip()
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                return datetime.strptime(text, fmt).time()
            except ValueError:
                continue
        raise ValueError(f"{argument_name} must use HH:MM or HH:MM:SS")

    @property
    def state_token(self) -> str:
        return f"{self.start:%Y%m%d_%H%M%S}_{self.stop:%Y%m%d_%H%M%S}"

    @property
    def display(self) -> str:
        if self.start.date() == self.stop.date():
            return f"{self.start:%H:%M:%S} to {self.stop:%H:%M:%S}"
        return f"{self.start:%d-%m-%Y %H:%M:%S} to {self.stop:%d-%m-%Y %H:%M:%S}"


@dataclass(frozen=True)
class HourlyShift:
    name: str
    start: datetime
    stop: datetime

    @property
    def display_name(self) -> str:
        value = self.name.replace("_", " ").strip()
        if value.lower().startswith("shift "):
            value = value[6:].strip()
        return value.upper()

    @property
    def windows(self) -> list[HourlyWindow]:
        windows = []
        start = self.start
        while start < self.stop:
            stop = min(start + timedelta(hours=1), self.stop)
            windows.append(HourlyWindow(start=start, stop=stop))
            start = stop
        return windows


@dataclass
class HourlyCasterResult:
    caster: CasterConfig
    state: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    raw_path: str | None = None
    raw_hour_path: str | None = None
    raw_count: int | str = 0
    verified_path: str | None = None
    verified_summary: dict | None = None


@dataclass(frozen=True)
class HourlyVerifiedTable:
    shift: HourlyShift
    caster_labels: tuple[str, ...]
    rows: tuple[tuple[str, tuple[str, ...]], ...]
    total_values: tuple[str, ...]


class HourlyCsvWorkflow:
    """Save hourly raw/verified CSVs and a verified-count table image locally."""

    VERIFIED_CSV_COLUMNS = ("Pipe Number", "Origin Time")
    DEFAULT_RETENTION_DAYS = 7

    def __init__(
        self,
        cfg: dict | None = None,
        selected_ids: list[str] | None = None,
        *,
        force: bool = False,
    ):
        self.root = PROJECT_ROOT
        self.cfg = cfg or load_runtime_config()
        self.casters = resolve_enabled_casters(self.cfg, selected_ids)
        self.force = force is True
        self.state_dir = self.root / "outputs" / "state" / "hourly"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.results: dict[str, HourlyCasterResult] = {}

    def _state_path(self, window: HourlyWindow, caster: CasterConfig) -> Path:
        return self.state_dir / f"{caster.id}_{window.state_token}.json"

    def _load_state(self, window: HourlyWindow, caster: CasterConfig) -> dict:
        path = self._state_path(window, caster)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            logger.warning("Ignoring unreadable hourly state | path=%s", path)
            return {}

    def _save_state(self, window: HourlyWindow, result: HourlyCasterResult):
        path = self._state_path(window, result.caster)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result.state, indent=2, sort_keys=True))

    def _retention_days(self) -> int:
        hourly_cfg = self.cfg.get("hourly_csv_report", {}) or {}
        value = hourly_cfg.get("retention_days", self.DEFAULT_RETENTION_DAYS)
        try:
            days = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("hourly_csv_report.retention_days must be a positive integer") from exc
        if days <= 0:
            raise ValueError("hourly_csv_report.retention_days must be a positive integer")
        return days

    def _hourly_image_dir(self) -> Path:
        hourly_cfg = self.cfg.get("hourly_csv_report", {}) or {}
        image_dir = Path(str(hourly_cfg.get("image_dir") or "outputs/hourly-report-images"))
        return image_dir if image_dir.is_absolute() else self.root / image_dir

    def _hourly_csv_dirs(self) -> set[Path]:
        directories = set()
        caster_configs = [caster.cfg for caster in self.casters]
        casters_cfg = self.cfg.get("casters")
        if isinstance(casters_cfg, dict):
            defaults = casters_cfg.get("defaults") or {}
            caster_configs.extend(
                build_caster_runtime_config(self.cfg, item, defaults)
                for item in (casters_cfg.get("items") or [])
                if isinstance(item, dict)
            )

        for caster_cfg in caster_configs:
            csv_dir = Path(
                str((caster_cfg.get("outputs", {}) or {}).get("csv_dir") or "outputs/csv")
            )
            directories.add(csv_dir if csv_dir.is_absolute() else self.root / csv_dir)
        return directories

    @staticmethod
    def _delete_expired_files(
        directory: Path,
        *,
        cutoff_timestamp: float,
        matches: Callable[[Path], bool],
    ) -> int:
        if not directory.exists():
            return 0

        try:
            candidates = list(directory.iterdir())
        except OSError:
            logger.warning("Could not inspect hourly report directory | path=%s", directory)
            return 0

        deleted = 0
        for path in candidates:
            try:
                if (
                    path.is_file()
                    and matches(path)
                    and path.stat().st_mtime < cutoff_timestamp
                ):
                    path.unlink()
                    deleted += 1
            except OSError:
                logger.warning("Could not inspect or delete expired hourly report | path=%s", path)
        return deleted

    def cleanup_expired_reports(self, *, now: datetime | None = None) -> dict[str, int]:
        """Delete only hourly-owned report files older than the configured lifespan."""
        current = now or datetime.now()
        cutoff = (current - timedelta(days=self._retention_days())).timestamp()

        deleted_csvs = sum(
            self._delete_expired_files(
                directory,
                cutoff_timestamp=cutoff,
                matches=lambda path: path.suffix.lower() == ".csv" and "_window_" in path.name,
            )
            for directory in self._hourly_csv_dirs()
        )
        deleted_images = self._delete_expired_files(
            self._hourly_image_dir(),
            cutoff_timestamp=cutoff,
            matches=lambda path: (
                path.suffix.lower() == ".png"
                and path.name.startswith("verified_hourly_report_")
            ),
        )
        deleted_states = self._delete_expired_files(
            self.state_dir,
            cutoff_timestamp=cutoff,
            matches=lambda path: path.suffix.lower() == ".json",
        )
        deleted = {
            "csv": deleted_csvs,
            "images": deleted_images,
            "state": deleted_states,
        }
        if any(deleted.values()):
            logger.info(
                "Expired hourly reports deleted | retention_days=%s | csv=%s | "
                "images=%s | state=%s",
                self._retention_days(),
                deleted_csvs,
                deleted_images,
                deleted_states,
            )
        return deleted

    @staticmethod
    def _verified_mode(cfg: dict) -> str:
        return str(
            cfg.get("verified_pipes_mode")
            or cfg.get("verfied_pipes_mode")
            or "loadcell"
        ).strip().lower()

    @staticmethod
    def _retry(
        operation: Callable,
        *,
        what: str,
        tries: int = 4,
        base_delay: float = 2.0,
        should_retry: Callable[[Exception], bool] | None = None,
    ):
        last_error = None
        attempts_made = 0
        for attempt in range(1, tries + 1):
            attempts_made = attempt
            try:
                return operation()
            except Exception as exc:
                last_error = exc
                if attempt == tries:
                    break
                if should_retry is not None and not should_retry(exc):
                    logger.error(
                        "%s failed with a non-retryable error; stopping after "
                        "attempt %s/%s | error=%s",
                        what,
                        attempt,
                        tries,
                        exc,
                    )
                    break
                delay = base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "%s failed (attempt %s/%s); retrying in %.1fs | error=%s",
                    what,
                    attempt,
                    tries,
                    delay,
                    exc,
                )
                time.sleep(delay)
        raise RuntimeError(
            f"{what} failed after {attempts_made} attempt(s): {last_error}"
        ) from last_error

    def _record_error(
        self,
        window: HourlyWindow,
        result: HourlyCasterResult,
        message: str,
        *,
        detail: str | None = None,
    ):
        text = message if not detail else f"{message}:\n{detail}"
        result.errors.append(text)
        result.state["errors"] = result.errors
        self._save_state(window, result)

    def _prepare_result(
        self,
        window: HourlyWindow,
        caster: CasterConfig,
    ) -> HourlyCasterResult | None:
        state = {} if self.force else self._load_state(window, caster)
        if state.get("status") == "success" and not self.force:
            logger.info(
                "Hourly report already generated; skipping | caster=%s | window=%s",
                caster.id,
                window.display,
            )
            return None

        state["errors"] = []
        state.update({
            "date": window.start.strftime("%d-%m-%Y"),
            "window_start": window.start.isoformat(timespec="seconds"),
            "window_stop": window.stop.isoformat(timespec="seconds"),
            "caster_id": caster.id,
            "caster_number": caster.number,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "status": "running",
        })
        result = HourlyCasterResult(caster=caster, state=state)
        self.results[caster.id] = result
        self._save_state(window, result)
        return result

    def _export_raw(self, window: HourlyWindow, result: HourlyCasterResult):
        previous_path = (
            result.state.get("raw_cumulative_csv_path")
            or result.state.get("raw_csv_path")
        )
        previous_hour_path = result.state.get("raw_hour_csv_path") or previous_path
        if (
            not self.force
            and previous_path
            and previous_hour_path
            and Path(previous_path).exists()
            and Path(previous_hour_path).exists()
        ):
            result.raw_path = str(previous_path)
            result.raw_hour_path = str(previous_hour_path)
            result.raw_count = result.state.get("raw_count", 0)
            logger.info(
                "Reusing hourly raw CSV | caster=%s | path=%s",
                result.caster.id,
                previous_path,
            )
            return

        exporter = PipeExporter(cfg=result.caster.cfg, caster=result.caster)
        hourly_path, count = self._retry(
            lambda: exporter.export_window(window.start, window.stop),
            what=f"{result.caster.id} hourly raw CSV export",
        )
        cumulative_path, cumulative_count = self._build_cumulative_raw_csv(
            window,
            result.caster,
            Path(hourly_path),
        )
        result.raw_path = str(cumulative_path)
        result.raw_hour_path = str(hourly_path)
        result.raw_count = int(count)
        result.state["raw_hour_csv_path"] = str(hourly_path)
        result.state["raw_csv_path"] = result.raw_path
        result.state["raw_count"] = result.raw_count
        result.state["raw_cumulative_csv_path"] = result.raw_path
        result.state["raw_cumulative_count"] = cumulative_count
        result.state["raw_exported"] = True
        self._save_state(window, result)

    def _export_verified(self, window: HourlyWindow, result: HourlyCasterResult):
        previous_path = (
            result.state.get("verified_cumulative_csv_path")
            or result.state.get("verified_csv_path")
        )
        previous_summary = result.state.get("verified_summary")
        if (
            not self.force
            and previous_path
            and Path(previous_path).exists()
            and isinstance(previous_summary, dict)
        ):
            result.verified_path = str(previous_path)
            result.verified_summary = previous_summary
            logger.info(
                "Reusing hourly verified CSV | caster=%s | path=%s",
                result.caster.id,
                previous_path,
            )
            return

        if not result.raw_hour_path:
            raise RuntimeError("Current-hour raw CSV is unavailable")
        exporter = VerifiedPipeExporter(cfg=result.caster.cfg, caster=result.caster)
        hourly_path, summary = self._retry(
            lambda: exporter.export_window(
                window.start,
                window.stop,
                result.raw_hour_path,
                mode=self._verified_mode(result.caster.cfg),
            ),
            what=f"{result.caster.id} hourly verified CSV export",
        )
        cumulative_path, cumulative_count = self._build_cumulative_verified_csv(
            window,
            result.caster,
            Path(hourly_path),
        )
        result.verified_path = str(cumulative_path)
        result.verified_summary = summary
        result.state["verified_hour_csv_path"] = str(hourly_path)
        result.state["verified_csv_path"] = result.verified_path
        result.state["verified_cumulative_csv_path"] = result.verified_path
        result.state["verified_cumulative_count"] = cumulative_count
        result.state["verified_summary"] = summary
        result.state["verified_exported"] = True
        self._save_state(window, result)

    def _prior_csv_paths(
        self,
        window: HourlyWindow,
        caster: CasterConfig,
        report_type: str,
    ) -> list[Path]:
        """Return the smallest saved CSV set covering earlier hours in this shift."""
        if report_type not in {"raw", "verified"}:
            raise ValueError(f"Unsupported hourly report type: {report_type}")

        shift = self._shift_for_window(window)
        paths: list[Path] = []
        for row_window in shift.windows:
            if row_window.stop > window.start:
                break

            state = self._load_state(row_window, caster)
            cumulative_value = state.get(f"{report_type}_cumulative_csv_path")
            hourly_value = state.get(f"{report_type}_hour_csv_path")
            legacy_value = state.get(f"{report_type}_csv_path")
            value = cumulative_value or hourly_value or legacy_value
            expected_count = state.get(f"{report_type}_cumulative_count")
            if expected_count is None:
                if report_type == "raw":
                    expected_count = state.get("raw_count")
                else:
                    summary = state.get("verified_summary")
                    if isinstance(summary, dict):
                        expected_count = summary.get("verified_count")
            try:
                expected_count = int(expected_count)
            except (TypeError, ValueError):
                expected_count = None

            if not value:
                if expected_count and expected_count > 0:
                    raise FileNotFoundError(
                        f"Saved {report_type} CSV path is missing for {caster.id} "
                        f"window {row_window.display}"
                    )
                continue

            path = Path(value)
            if not path.exists():
                if expected_count == 0:
                    continue
                raise FileNotFoundError(
                    f"Saved {report_type} CSV is missing for {caster.id} "
                    f"window {row_window.display}: {path}"
                )

            if cumulative_value:
                # This file already contains every earlier completed hour, so it
                # supersedes individual files collected before it.
                paths = [path]
            else:
                paths.append(path)
        return paths

    @staticmethod
    def _csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError(f"CSV has no header: {path}")
            return list(reader.fieldnames), list(reader)

    def _build_cumulative_raw_csv(
        self,
        window: HourlyWindow,
        caster: CasterConfig,
        hourly_path: Path,
    ) -> tuple[Path, int]:
        prior_paths = self._prior_csv_paths(window, caster, "raw")
        current_columns, current_rows = self._csv_rows(hourly_path)
        if not prior_paths:
            return hourly_path, len(current_rows)

        rows: list[dict[str, str]] = []
        expected_columns: list[str] | None = None
        for path in prior_paths:
            columns, source_rows = self._csv_rows(path)
            if expected_columns is None:
                expected_columns = columns
            elif columns != expected_columns:
                raise ValueError(
                    f"Raw CSV columns do not match while accumulating {path}"
                )
            rows.extend(source_rows)
        if current_columns != expected_columns:
            raise ValueError(
                f"Raw CSV columns do not match while accumulating {hourly_path}"
            )
        rows.extend(current_rows)

        cumulative_path = hourly_path.with_name(
            f"{hourly_path.stem}_cumulative{hourly_path.suffix}"
        )
        with cumulative_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=expected_columns or current_columns)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(
            "Cumulative raw CSV built | caster=%s | shift_start=%s | through=%s | "
            "source_files=%s | raw_count=%s | path=%s",
            caster.id,
            self._shift_for_window(window).start,
            window.stop,
            len(prior_paths) + 1,
            len(rows),
            cumulative_path,
        )
        return cumulative_path, len(rows)

    @staticmethod
    def _verified_origin_times(path: Path) -> list[str]:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            missing = [
                column
                for column in HourlyCsvWorkflow.VERIFIED_CSV_COLUMNS
                if column not in (reader.fieldnames or [])
            ]
            if missing:
                raise ValueError(
                    f"Verified CSV {path} is missing columns: {', '.join(missing)}"
                )
            return [str(row.get("Origin Time") or "") for row in reader]

    def _build_cumulative_verified_csv(
        self,
        window: HourlyWindow,
        caster: CasterConfig,
        hourly_path: Path,
    ) -> tuple[Path, int]:
        prior_paths = self._prior_csv_paths(window, caster, "verified")
        if not prior_paths:
            return hourly_path, len(self._verified_origin_times(hourly_path))

        origin_times: list[str] = []
        for path in [*prior_paths, hourly_path]:
            origin_times.extend(self._verified_origin_times(path))

        cumulative_path = hourly_path.with_name(
            f"{hourly_path.stem}_cumulative{hourly_path.suffix}"
        )
        with cumulative_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(self.VERIFIED_CSV_COLUMNS),
            )
            writer.writeheader()
            writer.writerows(
                {
                    "Pipe Number": pipe_number,
                    "Origin Time": origin_time,
                }
                for pipe_number, origin_time in enumerate(origin_times, start=1)
            )

        logger.info(
            "Cumulative verified CSV built | caster=%s | shift_start=%s | through=%s | "
            "source_files=%s | verified_count=%s | path=%s",
            caster.id,
            self._shift_for_window(window).start,
            window.stop,
            len(prior_paths) + 1,
            len(origin_times),
            cumulative_path,
        )
        return cumulative_path, len(origin_times)

    def _shift_for_window(self, window: HourlyWindow) -> HourlyShift:
        configured = ((self.cfg.get("history", {}) or {}).get("shifts", []) or [])
        if not configured:
            raise ValueError("history.shifts is required for the hourly verified report table")

        for item in configured:
            required_keys = ("name", "start", "end")
            if not isinstance(item, dict) or not all(key in item for key in required_keys):
                raise ValueError("Each history.shifts item must define name, start, and end")

            start_clock = HourlyWindow._parse_clock(str(item["start"]), "shift start")
            stop_clock = HourlyWindow._parse_clock(str(item["end"]), "shift end")
            for shift_date in (window.start.date(), window.start.date() - timedelta(days=1)):
                shift_start = datetime.combine(shift_date, start_clock)
                shift_stop = datetime.combine(shift_date, stop_clock)
                if shift_stop <= shift_start:
                    shift_stop += timedelta(days=1)
                if shift_start <= window.start < shift_stop:
                    return HourlyShift(
                        name=str(item["name"]),
                        start=shift_start,
                        stop=shift_stop,
                    )

        raise ValueError(f"No configured shift contains hourly window {window.display}")

    @staticmethod
    def _interval_label(window: HourlyWindow) -> str:
        return f"{window.start:%H:%M} – {window.stop:%H:%M}"

    def _verified_count_for_window(
        self,
        row_window: HourlyWindow,
        caster: CasterConfig,
        current_window: HourlyWindow,
        current_results: dict[str, HourlyCasterResult],
    ) -> str:
        summary = None
        if row_window.start == current_window.start and row_window.stop == current_window.stop:
            result = current_results.get(caster.id)
            if result is not None:
                summary = result.verified_summary or result.state.get("verified_summary")
        if not isinstance(summary, dict):
            summary = self._load_state(row_window, caster).get("verified_summary")
        if not isinstance(summary, dict) or "verified_count" not in summary:
            return ""
        return str(summary["verified_count"])

    def _verified_shift_table(
        self,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
    ) -> HourlyVerifiedTable:
        shift = self._shift_for_window(window)
        current_results = {result.caster.id: result for result in results}
        caster_labels = [caster_label(caster, caster.cfg) for caster in self.casters]
        rows: list[tuple[str, list[str]]] = []
        totals = [0] * len(self.casters)
        has_counts = [False] * len(self.casters)
        for row_window in shift.windows:
            counts = [
                self._verified_count_for_window(
                    row_window,
                    caster,
                    window,
                    current_results,
                )
                for caster in self.casters
            ]
            for index, count in enumerate(counts):
                try:
                    totals[index] += int(count)
                    has_counts[index] = True
                except (TypeError, ValueError):
                    continue
            rows.append((self._interval_label(row_window), counts))
        total_values = [
            str(total) if has_counts[index] else ""
            for index, total in enumerate(totals)
        ]

        return HourlyVerifiedTable(
            shift=shift,
            caster_labels=tuple(caster_labels),
            rows=tuple((interval, tuple(counts)) for interval, counts in rows),
            total_values=tuple(total_values),
        )

    def _verified_table_image_path(
        self,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
    ) -> Path:
        caster_token = "_".join(result.caster.id for result in results)
        caster_token = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in caster_token
        )
        return self._hourly_image_dir() / (
            f"verified_hourly_report_{window.state_token}_{caster_token}.png"
        )

    def _save_verified_table_image(
        self,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
        *,
        table: HourlyVerifiedTable | None = None,
    ) -> Path:
        if not results:
            raise ValueError("At least one verified result is required for the table image")

        table = table or self._verified_shift_table(window, results)
        output_path = self._verified_table_image_path(window, results)
        rows = [
            (interval, *counts)
            for interval, counts in table.rows
        ]
        rows.append(("Total Count", *table.total_values))
        saved_path = save_table_image(
            output_path,
            title="Hourly Verified Pipe Production Report",
            metadata=(
                ("Date", table.shift.start.strftime("%d-%m-%Y")),
                ("Shift", table.shift.display_name),
            ),
            headers=("Time Interval", *table.caster_labels),
            rows=rows,
        )

        for result in results:
            result.state["verified_table_image_path"] = str(saved_path)
            self._save_state(window, result)
        logger.info(
            "Hourly verified table image saved | window=%s | casters=%s | path=%s",
            window.display,
            [result.caster.id for result in results],
            saved_path,
        )
        return saved_path

    def _finish_result(self, window: HourlyWindow, result: HourlyCasterResult):
        image_path = result.state.get("verified_table_image_path")
        exports_ok = bool(
            result.raw_path
            and result.verified_path
            and image_path
            and Path(image_path).exists()
        )
        status = "partial_failure" if result.errors or not exports_ok else "success"
        result.state["errors"] = result.errors
        result.state["status"] = status
        result.state["finished_at"] = datetime.now().isoformat(timespec="seconds")
        self._save_state(window, result)

    def run(self, window: HourlyWindow) -> bool:
        workflow_started = time.perf_counter()
        cleanup_started = time.perf_counter()
        self.cleanup_expired_reports()
        cleanup_seconds = time.perf_counter() - cleanup_started
        logger.info(
            "Hourly local report workflow start | window=%s | casters=%s | "
            "force=%s | retention_days=%s",
            window.display,
            [caster.id for caster in self.casters],
            self.force,
            self._retention_days(),
        )
        active_results = []
        raw_started = time.perf_counter()
        for caster in self.casters:
            result = self._prepare_result(window, caster)
            if result is None:
                continue
            active_results.append(result)
            try:
                self._export_raw(window, result)
            except Exception:
                self._record_error(
                    window,
                    result,
                    "Hourly raw CSV export failed",
                    detail=traceback.format_exc(),
                )
                logger.exception("Hourly raw CSV export failed | caster=%s", caster.id)
        raw_export_seconds = time.perf_counter() - raw_started
        logger.info("PERFORMANCE | raw_export=%.3fs", raw_export_seconds)

        verified_started = time.perf_counter()
        for result in active_results:
            if not result.raw_path:
                continue
            try:
                self._export_verified(window, result)
            except Exception:
                self._record_error(
                    window,
                    result,
                    "Hourly verified CSV export failed",
                    detail=traceback.format_exc(),
                )
                logger.exception("Hourly verified CSV export failed | caster=%s", result.caster.id)
        verified_export_seconds = time.perf_counter() - verified_started
        logger.info("PERFORMANCE | verified_export=%.3fs", verified_export_seconds)

        table_image_seconds = 0.0
        verified_results = [
            result
            for result in active_results
            if result.verified_path and Path(result.verified_path).exists()
        ]
        if verified_results:
            table_image_started = time.perf_counter()
            try:
                verified_table = self._verified_shift_table(window, verified_results)
                self._save_verified_table_image(
                    window,
                    verified_results,
                    table=verified_table,
                )
            except Exception:
                detail = traceback.format_exc()
                for result in verified_results:
                    self._record_error(
                        window,
                        result,
                        "Hourly verified table image export failed",
                        detail=detail,
                    )
                logger.exception("Hourly verified table image export failed")
            table_image_seconds = time.perf_counter() - table_image_started
            logger.info("PERFORMANCE | verified_table_image=%.3fs", table_image_seconds)

        for result in active_results:
            self._finish_result(window, result)

        success = all(result.state.get("status") != "partial_failure" for result in active_results)
        logger.info(
            "Hourly CSV workflow finished | window=%s | active_casters=%s | success=%s",
            window.display,
            [result.caster.id for result in active_results],
            success,
        )
        logger.info(
            "PERFORMANCE | cleanup=%.3fs | raw_export=%.3fs | verified_export=%.3fs | "
            "verified_table_image=%.3fs | total=%.3fs",
            cleanup_seconds,
            raw_export_seconds,
            verified_export_seconds,
            table_image_seconds,
            time.perf_counter() - workflow_started,
        )
        return success


def setup_logging(cfg: dict):
    level_name = (cfg.get("logging", {}) or {}).get("level", "INFO")
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Save raw/verified pipe CSVs and a table image for a custom or "
            "previous-hour window"
        ),
    )
    parser.add_argument("--date", help="Window start date in DD-MM-YYYY")
    parser.add_argument("--start", help="Window start time in HH:MM or HH:MM:SS")
    parser.add_argument("--stop", help="Window stop time in HH:MM or HH:MM:SS")
    caster_group = parser.add_mutually_exclusive_group()
    caster_group.add_argument("--caster", help="Single caster id, for example caster3")
    caster_group.add_argument("--casters", help="Comma-separated caster ids")
    caster_group.add_argument("--all-casters", action="store_true", help="Use every enabled caster")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate an already successful window",
    )
    return parser


def _window_from_args(args, parser: argparse.ArgumentParser) -> HourlyWindow:
    supplied = [args.date is not None, args.start is not None, args.stop is not None]
    if any(supplied) and not all(supplied):
        parser.error("Use --date, --start, and --stop together")
    try:
        if all(supplied):
            return HourlyWindow.from_cli(args.date, args.start, args.stop)
        return HourlyWindow.previous_completed_hour()
    except ValueError as exc:
        parser.error(str(exc))


def _selected_ids_from_args(args) -> list[str] | None:
    if args.all_casters:
        return None
    if args.caster:
        return [args.caster]
    if args.casters:
        return [part.strip() for part in args.casters.split(",") if part.strip()]
    return None


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    window = _window_from_args(args, parser)
    cfg = load_runtime_config()
    setup_logging(cfg)

    try:
        workflow = HourlyCsvWorkflow(
            cfg=cfg,
            selected_ids=_selected_ids_from_args(args),
            force=args.force,
        )
        return 0 if workflow.run(window) else 1
    except Exception:
        logger.exception("Hourly CSV workflow could not start")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
