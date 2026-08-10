from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reports.common.caster_config import CasterConfig, caster_label, resolve_enabled_casters
from reports.common.config_loader import load_runtime_config
from reports.common.email_sender import EmailSender
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
    raw_count: int | str = 0
    verified_path: str | None = None
    verified_summary: dict | None = None


@dataclass(frozen=True)
class HourlyEmailContent:
    subject: str
    text_body: str
    html_body: str | None = None


class HourlyCsvWorkflow:
    """Generate and email only raw and verified CSVs for a custom time window."""

    def __init__(
        self,
        cfg: dict | None = None,
        selected_ids: list[str] | None = None,
        *,
        test_mode: bool = False,
        send_email: bool = True,
        force: bool = False,
    ):
        self.root = PROJECT_ROOT
        self.cfg = cfg or load_runtime_config()
        self.casters = resolve_enabled_casters(self.cfg, selected_ids)
        self.test_mode = test_mode is True
        self.send_email = send_email is True
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

    @staticmethod
    def _normalize_recipients(value) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        return [str(item).strip() for item in value if str(item).strip()]

    def _report_recipients(self, report_type: str, cfg: dict) -> list[str]:
        email_cfg = cfg.get("email", {}) or {}
        if self.test_mode:
            return self._normalize_recipients(email_cfg.get("test_recipients"))

        hourly_cfg = cfg.get("hourly_csv_report", {}) or {}
        if report_type == "raw":
            configured = hourly_cfg.get("raw_recipients")
            return self._normalize_recipients(
                configured if configured is not None else email_cfg.get("recipients")
            )
        configured = hourly_cfg.get("verified_recipients")
        return self._normalize_recipients(
            configured
            if configured is not None
            else cfg.get("verified_pipe_records_recipients")
        )

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
    ):
        last_error = None
        for attempt in range(1, tries + 1):
            try:
                return operation()
            except Exception as exc:
                last_error = exc
                if attempt == tries:
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
        raise RuntimeError(f"{what} failed after {tries} tries: {last_error}") from last_error

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
                "Hourly report already sent; skipping | caster=%s | window=%s",
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
            "email_test_mode": self.test_mode,
            "email_enabled": self.send_email,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "status": "running",
        })
        result = HourlyCasterResult(caster=caster, state=state)
        self.results[caster.id] = result
        self._save_state(window, result)
        return result

    def _export_raw(self, window: HourlyWindow, result: HourlyCasterResult):
        previous_path = result.state.get("raw_csv_path")
        if not self.force and previous_path and Path(previous_path).exists():
            result.raw_path = str(previous_path)
            result.raw_count = result.state.get("raw_count", 0)
            logger.info("Reusing hourly raw CSV | caster=%s | path=%s", result.caster.id, previous_path)
            return

        exporter = PipeExporter(cfg=result.caster.cfg, caster=result.caster)
        path, count = self._retry(
            lambda: exporter.export_window(window.start, window.stop),
            what=f"{result.caster.id} hourly raw CSV export",
        )
        result.raw_path = str(path)
        result.raw_count = int(count)
        result.state["raw_csv_path"] = result.raw_path
        result.state["raw_count"] = result.raw_count
        result.state["raw_exported"] = True
        self._save_state(window, result)

    def _export_verified(self, window: HourlyWindow, result: HourlyCasterResult):
        previous_path = result.state.get("verified_csv_path")
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

        if not result.raw_path:
            raise RuntimeError("Raw CSV is unavailable")
        exporter = VerifiedPipeExporter(cfg=result.caster.cfg, caster=result.caster)
        path, summary = self._retry(
            lambda: exporter.export_window(
                window.start,
                window.stop,
                result.raw_path,
                mode=self._verified_mode(result.caster.cfg),
            ),
            what=f"{result.caster.id} hourly verified CSV export",
        )
        result.verified_path = str(path)
        result.verified_summary = summary
        result.state["verified_csv_path"] = result.verified_path
        result.state["verified_summary"] = summary
        result.state["verified_exported"] = True
        self._save_state(window, result)

    def _email_content(
        self,
        report_type: str,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
    ) -> HourlyEmailContent:
        report_name = "Raw Pipe" if report_type == "raw" else "Verified Pipe"
        subject = (
            f"Hourly {report_name} Production Report - "
            f"{window.start:%d-%m-%Y} - {window.start:%H:%M:%S} to {window.stop:%H:%M:%S}"
        )
        if self.test_mode:
            subject = f"[TEST] {subject}"

        if report_type == "verified":
            text_body, html_body = self._verified_shift_email_bodies(window, results)
            return HourlyEmailContent(
                subject=subject,
                text_body=text_body,
                html_body=html_body,
            )

        lines = [
            f"Hourly {report_name} Production Report",
            "",
            f"Window : {window.display}",
            "",
        ]
        for result in results:
            count = result.raw_count
            lines.append(f"{caster_label(result.caster, result.caster.cfg)} : {count}")
        lines.extend(["", f"{len(results)} CSV file(s) attached."])
        return HourlyEmailContent(subject=subject, text_body="\n".join(lines))

    def _email_subject_and_body(
        self,
        report_type: str,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
    ) -> tuple[str, str]:
        """Compatibility helper returning the primary rendered body."""
        content = self._email_content(report_type, window, results)
        return content.subject, content.html_body or content.text_body

    def _shift_for_window(self, window: HourlyWindow) -> HourlyShift:
        configured = ((self.cfg.get("history", {}) or {}).get("shifts", []) or [])
        if not configured:
            raise ValueError("history.shifts is required for the hourly verified report table")

        for item in configured:
            if not isinstance(item, dict) or not all(key in item for key in ("name", "start", "end")):
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

    def _verified_shift_email_bodies(
        self,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
    ) -> tuple[str, str]:
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
        attachment_count = len(results)
        attachment_text = (
            f"{attachment_count} CSV "
            f"{'file' if attachment_count == 1 else 'files'} attached"
        )

        text_lines = [
            "Hourly Verified Pipe Production Report",
            "",
            f"Date: {shift.start:%d-%m-%Y}",
            f"Shift: {shift.display_name}",
            "",
        ]
        for interval, counts in rows:
            values = ", ".join(
                f"{label}: {count or '-'}"
                for label, count in zip(caster_labels, counts)
            )
            text_lines.append(f"{interval} — {values}")
        total_text = ", ".join(
            f"{label}: {total or '-'}"
            for label, total in zip(caster_labels, total_values)
        )
        text_lines.extend([
            "",
            f"Total Count — {total_text}",
            "",
            attachment_text,
        ])

        header_cells = "".join(
            (
                '<th align="left" style="padding:11px 12px; border:1px solid #cbd5e1; '
                'background-color:#e8eef5; color:#17324d; font-size:13px; '
                'font-weight:700; line-height:18px; text-align:left; white-space:nowrap;">'
                'Time Interval</th>'
                if index == 0
                else '<th align="center" style="padding:11px 12px; border:1px solid #cbd5e1; '
                'background-color:#e8eef5; color:#17324d; font-size:13px; '
                'font-weight:700; line-height:18px; text-align:center; white-space:nowrap;">'
                f'{escape(label)}</th>'
            )
            for index, label in enumerate(["Time Interval", *caster_labels])
        )

        data_rows = []
        for row_index, (interval, counts) in enumerate(rows):
            background = "#ffffff" if row_index % 2 == 0 else "#f7f9fc"
            count_cells = "".join(
                '<td align="center" style="padding:10px 12px; border:1px solid #d6dce3; '
                f'background-color:{background}; color:#263746; font-size:13px; '
                'line-height:18px; text-align:center;">'
                f'{escape(count) if count else "&nbsp;"}</td>'
                for count in counts
            )
            data_rows.append(
                '<tr>'
                '<td align="left" style="padding:10px 12px; border:1px solid #d6dce3; '
                f'background-color:{background}; color:#263746; font-size:13px; '
                'line-height:18px; text-align:left; white-space:nowrap;">'
                f'{escape(interval)}</td>{count_cells}</tr>'
            )

        total_cells = "".join(
            '<td align="center" style="padding:11px 12px; border:1px solid #b8c5d1; '
            'background-color:#dfe8f1; color:#17324d; font-size:13px; font-weight:700; '
            'line-height:18px; text-align:center;">'
            f'{escape(total) if total else "&nbsp;"}</td>'
            for total in total_values
        )
        total_row = (
            '<tr><td align="left" style="padding:11px 12px; border:1px solid #b8c5d1; '
            'background-color:#dfe8f1; color:#17324d; font-size:13px; font-weight:700; '
            'line-height:18px; text-align:left; white-space:nowrap;">Total Count</td>'
            f'{total_cells}</tr>'
        )

        html_body = (
            '<!doctype html>'
            '<html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
            '<title>Hourly Verified Pipe Production Report</title></head>'
            '<body style="margin:0; padding:0; background-color:#f2f5f8; '
            'font-family:Arial, Helvetica, sans-serif; color:#263746;">'
            '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" '
            'style="width:100%; border-collapse:collapse; background-color:#f2f5f8;">'
            '<tr><td align="center" style="padding:24px 12px;">'
            '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" '
            'style="width:100%; max-width:680px; border-collapse:separate; border-spacing:0; '
            'background-color:#ffffff; border:1px solid #d9e0e7; border-radius:10px;">'
            '<tr><td style="padding:28px 24px 24px 24px;">'
            '<h1 style="margin:0 0 20px 0; color:#17324d; font-size:22px; font-weight:700; '
            'line-height:29px;">Hourly Verified Pipe Production Report</h1>'
            '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" '
            'style="width:100%; margin:0 0 20px 0; border-collapse:collapse;">'
            '<tr><td width="50%" style="padding:10px 12px; background-color:#f7f9fc; '
            'border:1px solid #e0e5eb; color:#526474; font-size:13px; line-height:18px;">'
            '<span style="font-weight:700; color:#17324d;">Date:</span> '
            f'{escape(shift.start.strftime("%d-%m-%Y"))}</td>'
            '<td width="50%" style="padding:10px 12px; background-color:#f7f9fc; '
            'border:1px solid #e0e5eb; color:#526474; font-size:13px; line-height:18px;">'
            '<span style="font-weight:700; color:#17324d;">Shift:</span> '
            f'{escape(shift.display_name)}</td></tr></table>'
            '<div style="width:100%; overflow-x:auto; border-radius:7px;">'
            '<table role="table" width="100%" cellspacing="0" cellpadding="0" border="0" '
            'style="width:100%; border-collapse:collapse; border-spacing:0;">'
            f'<thead><tr>{header_cells}</tr></thead>'
            f'<tbody>{"".join(data_rows)}{total_row}</tbody>'
            '</table></div>'
            '<p style="margin:18px 0 0 0; color:#617383; font-size:12px; line-height:18px;">'
            f'{escape(attachment_text)}</p>'
            '</td></tr></table></td></tr></table></body></html>'
        )
        return "\n".join(text_lines), html_body

    def _send_consolidated_email(
        self,
        report_type: str,
        window: HourlyWindow,
        results: list[HourlyCasterResult],
    ) -> bool:
        sent_key = f"{report_type}_email_sent"
        path_attribute = "raw_path" if report_type == "raw" else "verified_path"
        eligible = [result for result in results if not result.state.get(sent_key)]
        if not eligible:
            return True

        attachments = []
        valid_results = []
        for result in eligible:
            path = getattr(result, path_attribute)
            if not path or not Path(path).exists():
                self._record_error(
                    window,
                    result,
                    f"Hourly {report_type} email attachment is missing",
                )
                continue
            attachments.append(path)
            valid_results.append(result)
        if not valid_results:
            return False

        recipients: list[str] = []
        for result in valid_results:
            for recipient in self._report_recipients(report_type, result.caster.cfg):
                if recipient not in recipients:
                    recipients.append(recipient)
        if not recipients:
            reason = (
                "No email.test_recipients configured"
                if self.test_mode
                else f"No hourly {report_type} recipients configured"
            )
            for result in valid_results:
                self._record_error(window, result, reason)
            return False

        content = self._email_content(report_type, window, valid_results)
        send_kwargs = {
            "attachments": attachments,
            "recipients": recipients,
        }
        if content.html_body is not None:
            send_kwargs["html_body"] = content.html_body
        try:
            mailer = EmailSender(cfg=valid_results[0].caster.cfg)
            self._retry(
                lambda: mailer.send(
                    content.subject,
                    content.text_body,
                    **send_kwargs,
                ),
                what=f"hourly {report_type} CSV email",
            )
        except Exception:
            detail = traceback.format_exc()
            for result in valid_results:
                self._record_error(
                    window,
                    result,
                    f"Hourly {report_type} CSV email failed",
                    detail=detail,
                )
            logger.exception("Hourly %s CSV email failed", report_type)
            return False

        for result in valid_results:
            result.state[sent_key] = True
            result.state[f"{report_type}_email_recipients"] = recipients
            self._save_state(window, result)
        logger.info(
            "Hourly %s CSV email sent | window=%s | casters=%s | attachments=%s | recipients=%s",
            report_type,
            window.display,
            [result.caster.id for result in valid_results],
            len(attachments),
            len(recipients),
        )
        return True

    def _finish_result(self, window: HourlyWindow, result: HourlyCasterResult):
        exports_ok = bool(result.raw_path and result.verified_path)
        emails_ok = bool(
            result.state.get("raw_email_sent")
            and result.state.get("verified_email_sent")
        )
        if result.errors or not exports_ok or (self.send_email and not emails_ok):
            status = "partial_failure"
        elif self.send_email:
            status = "success"
        else:
            status = "generated_no_email"
        result.state["errors"] = result.errors
        result.state["status"] = status
        result.state["finished_at"] = datetime.now().isoformat(timespec="seconds")
        self._save_state(window, result)

    def run(self, window: HourlyWindow) -> bool:
        logger.info(
            "Hourly CSV workflow start | window=%s | casters=%s | email=%s | test=%s | force=%s",
            window.display,
            [caster.id for caster in self.casters],
            self.send_email,
            self.test_mode,
            self.force,
        )
        active_results = []
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

        if self.send_email:
            self._send_consolidated_email("raw", window, active_results)

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

        if self.send_email:
            self._send_consolidated_email("verified", window, active_results)

        for result in active_results:
            self._finish_result(window, result)

        success = all(result.state.get("status") != "partial_failure" for result in active_results)
        logger.info(
            "Hourly CSV workflow finished | window=%s | active_casters=%s | success=%s",
            window.display,
            [result.caster.id for result in active_results],
            success,
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
        description="Email raw and verified pipe CSVs for a custom or previous-hour window",
    )
    parser.add_argument("--date", help="Window start date in DD-MM-YYYY")
    parser.add_argument("--start", help="Window start time in HH:MM or HH:MM:SS")
    parser.add_argument("--stop", help="Window stop time in HH:MM or HH:MM:SS")
    caster_group = parser.add_mutually_exclusive_group()
    caster_group.add_argument("--caster", help="Single caster id, for example caster3")
    caster_group.add_argument("--casters", help="Comma-separated caster ids")
    caster_group.add_argument("--all-casters", action="store_true", help="Use every enabled caster")
    parser.add_argument("--test", action="store_true", help="Send both emails only to email.test_recipients")
    parser.add_argument("--no-email", action="store_true", help="Generate both CSVs without sending email")
    parser.add_argument("--force", action="store_true", help="Regenerate and resend an already successful window")
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
            test_mode=args.test,
            send_email=not args.no_email,
            force=args.force,
        )
        return 0 if workflow.run(window) else 1
    except Exception:
        logger.exception("Hourly CSV workflow could not start")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
