import argparse
import inspect
import json
import logging
import math
import os
import sys
import time
import traceback
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reports.common.caster_config import CasterConfig, caster_label, resolve_enabled_casters
from reports.common.config_loader import load_runtime_config
from reports.common.email_sender import EmailSender
from reports.common.gdrive_uploader import GDriveUploader
from reports.pipes.pipe_exporter import PipeExporter
from reports.pipes.verified_pipes import VerifiedPipeExporter
from reports.video.source_cleanup import cleanup_shift_sources
from reports.video.video_generator import ShiftVideoGenerator
from reports.video.video_overlay import ShiftVideoOverlayGenerator


logger = logging.getLogger("reports-generator")

DEFAULT_WORKFLOW_MAX_SECONDS = 7 * 60 * 60
DEFAULT_VIDEO_UPLOAD_WORKERS = 2


@dataclass(frozen=True)
class ShiftRun:
    date_str: str
    shift_name: str


@dataclass
class CasterRunResult:
    caster: CasterConfig
    state: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    csv_path: str | None = None
    csv_drive_link: str | None = None
    pipe_count: int | str = 0
    raw_exported: bool = False
    raw_email_sent: bool = False
    verified_path: str | None = None
    verified_summary: dict | None = None
    verified_exported: bool = False
    diagnosis_path: str | None = None
    diagnosis_summary: dict | None = None
    diagnosis_exported: bool = False
    missing_overlay_link: str | None = None
    missing_normal_link: str | None = None
    full_shift_video_path: str | None = None


@dataclass(frozen=True)
class VideoUploadOutcome:
    kind: str
    path: str
    link: str
    deleted_after_upload: bool | None = None
    delete_error: str | None = None


@dataclass(frozen=True)
class PendingVideoUpload:
    result: CasterRunResult
    kind: str
    path: str
    future: Future


def setup_logging(cfg: dict):
    level_name = (cfg.get("logging", {}) or {}).get("level", "INFO")
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _now():
    return datetime.now()


def detect_shift_for_trigger(now: datetime) -> ShiftRun | None:
    def in_window(target_h: int, target_m: int, minutes_slack: int = 5) -> bool:
        target = target_h * 60 + target_m
        current = now.hour * 60 + now.minute
        return target <= current <= (target + minutes_slack)

    if in_window(6, 0):
        return ShiftRun((now - timedelta(days=1)).strftime("%d-%m-%Y"), "Shift_C")
    if in_window(14, 0):
        return ShiftRun(now.strftime("%d-%m-%Y"), "Shift_A")
    if in_window(22, 0):
        return ShiftRun(now.strftime("%d-%m-%Y"), "Shift_B")
    return None


def backoff_retry(fn, *, tries=4, base_delay=2.0, what="operation", deadline_monotonic=None):
    last_err = None
    for attempt in range(1, tries + 1):
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            raise TimeoutError(f"{what} exceeded workflow deadline before attempt {attempt}")
        try:
            return fn()
        except Exception as exc:
            last_err = exc
            if attempt == tries:
                break
            sleep_s = base_delay * (2 ** (attempt - 1))
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"{what} exceeded workflow deadline after {attempt} attempt(s): {last_err}"
                    ) from last_err
                sleep_s = min(sleep_s, remaining)
            logger.warning(
                "%s failed (attempt %s/%s). Retrying in %.1fs. Error=%s",
                what,
                attempt,
                tries,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"{what} failed after {tries} tries: {last_err}") from last_err


class ShiftWorkflow:
    def __init__(self, cfg: dict | None = None, selected_ids: list[str] | None = None, *, test_mode: bool = False):
        self.root = PROJECT_ROOT
        self.cfg = cfg or load_runtime_config()
        self.test_mode = test_mode is True
        self.multi_caster_mode = isinstance(self.cfg.get("casters"), dict)
        self.casters = resolve_enabled_casters(self.cfg, selected_ids)
        self.state_dir = self.root / "outputs" / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._mailers: dict[str, EmailSender] = {}
        self.results: dict[str, CasterRunResult] = {}
        self._state_lock = threading.RLock()
        self._video_upload_executor: ThreadPoolExecutor | None = None
        self._pending_video_uploads: list[PendingVideoUpload] = []
        self._workflow_started_monotonic: float | None = None
        self._workflow_deadline_monotonic: float | None = None

    def _run_key(self, run: ShiftRun) -> str:
        return f"{run.date_str.replace('-', '')}_{run.shift_name.lower()}"

    def _state_path(self, run: ShiftRun, caster: CasterConfig | None = None) -> Path:
        key = self._run_key(run)
        if caster and caster.file_token:
            return self.state_dir / f"{caster.file_token}_{key}.json"
        return self.state_dir / f"{key}.json"

    def _multi_state_path(self, run: ShiftRun) -> Path:
        return self.state_dir / f"multi_{self._run_key(run)}.json"

    def _load_state(self, run: ShiftRun, caster: CasterConfig) -> dict:
        path = self._state_path(run, caster)
        if path.exists():
            return json.loads(path.read_text())
        return {}

    def _save_state(self, run: ShiftRun, caster: CasterConfig, state: dict):
        with self._state_lock:
            self._state_path(run, caster).write_text(json.dumps(state, indent=2, sort_keys=True))

    def _save_multi_state(self, run: ShiftRun, data: dict):
        with self._state_lock:
            self._multi_state_path(run).write_text(json.dumps(data, indent=2, sort_keys=True))

    def _workflow_cfg(self) -> dict:
        return self.cfg.get("workflow", {}) or {}

    def _workflow_max_seconds(self) -> int:
        value = os.getenv("REPORT_WORKFLOW_MAX_SECONDS")
        if value is None:
            value = self._workflow_cfg().get("max_runtime_seconds", DEFAULT_WORKFLOW_MAX_SECONDS)
        return self._parse_positive_int(value, default=DEFAULT_WORKFLOW_MAX_SECONDS, name="workflow.max_runtime_seconds")

    def _video_upload_workers(self) -> int:
        value = os.getenv("REPORT_VIDEO_UPLOAD_WORKERS")
        if value is None:
            value = self._workflow_cfg().get("video_upload_workers", DEFAULT_VIDEO_UPLOAD_WORKERS)
        return self._parse_positive_int(value, default=DEFAULT_VIDEO_UPLOAD_WORKERS, name="workflow.video_upload_workers")

    def _start_workflow_timer(self):
        if self._workflow_started_monotonic is not None:
            return
        max_seconds = self._workflow_max_seconds()
        self._workflow_started_monotonic = time.monotonic()
        self._workflow_deadline_monotonic = self._workflow_started_monotonic + max_seconds
        logger.info("Workflow runtime limit active | max_runtime_seconds=%s", max_seconds)

    def _remaining_workflow_seconds(self) -> float:
        self._start_workflow_timer()
        if self._workflow_deadline_monotonic is None:
            return math.inf
        return max(0.0, self._workflow_deadline_monotonic - time.monotonic())

    def _video_upload_executor_for_run(self) -> ThreadPoolExecutor:
        self._start_workflow_timer()
        if self._video_upload_executor is None:
            workers = self._video_upload_workers()
            self._video_upload_executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="missing-video-upload",
            )
            logger.info(
                "Async video upload worker pool started | workers=%s | remaining_seconds=%.0f",
                workers,
                self._remaining_workflow_seconds(),
            )
        return self._video_upload_executor

    def _call_upload_video(self, uploader, path: str) -> str:
        method = uploader.upload_video
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            parameters = {}
        accepts_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values())
        if accepts_kwargs or "deadline_monotonic" in parameters:
            return method(path, deadline_monotonic=self._workflow_deadline_monotonic)
        if "timeout_seconds" in parameters:
            return method(path, timeout_seconds=self._remaining_workflow_seconds())
        return method(path)

    def _run_video_upload_task(
        self,
        caster: CasterConfig,
        cfg: dict,
        kind: str,
        path: str,
        delete_after_upload: bool,
    ) -> VideoUploadOutcome:
        uploader = GDriveUploader(cfg=cfg, caster=caster)
        link = backoff_retry(
            lambda: self._call_upload_video(uploader, path),
            what=f"{caster.id} missing-loadcell {kind} video upload",
            deadline_monotonic=self._workflow_deadline_monotonic,
        )
        deleted = None
        delete_error = None
        if delete_after_upload:
            try:
                Path(path).unlink(missing_ok=True)
                deleted = True
            except Exception as exc:
                deleted = False
                delete_error = str(exc)
                logger.warning("Failed to delete %s missing-loadcell video | %s | error=%s", kind, path, exc)
        return VideoUploadOutcome(
            kind=kind,
            path=path,
            link=link,
            deleted_after_upload=deleted,
            delete_error=delete_error,
        )

    @staticmethod
    def _clear_previous_run_outputs(state: dict):
        prefixes = (
            "csv_",
            "verified_pipes_",
            "diagnosis_",
            "missing_loadcell_",
            "normal_shift_video_",
            "final_summary_email_",
            "video_",
        )
        exact_keys = {
            "pipe_count",
            "emailed_csv",
            "emailed_verified_pipes",
            "verified_pipe_records_recipients",
            "diagnosis_recipients",
            "email_test_mode",
            "errors",
        }
        for key in list(state):
            if key in exact_keys or any(key.startswith(prefix) for prefix in prefixes):
                state.pop(key, None)

    def _mailer(self, caster: CasterConfig) -> EmailSender:
        if caster.id not in self._mailers:
            self._mailers[caster.id] = EmailSender(cfg=caster.cfg)
        return self._mailers[caster.id]

    @staticmethod
    def _normalize_recipients(recipients) -> list[str]:
        if recipients is None:
            return []
        if isinstance(recipients, str):
            recipients = [recipients]
        return [str(item).strip() for item in recipients if str(item).strip()]

    def _diagnosis_recipients(self, cfg: dict) -> list[str]:
        return self._normalize_recipients((cfg.get("email", {}) or {}).get("diagnosis_recipients"))

    def _verified_pipe_records_recipients(self, cfg: dict) -> list[str]:
        return self._normalize_recipients(cfg.get("verified_pipe_records_recipients"))

    def _test_recipients(self, cfg: dict) -> list[str]:
        return self._normalize_recipients((cfg.get("email", {}) or {}).get("test_recipients"))

    def _email_recipients(self, cfg: dict) -> list[str]:
        return self._normalize_recipients((cfg.get("email", {}) or {}).get("recipients"))

    def _test_mode_skip_reason(self) -> str:
        return "No email.test_recipients configured for --test"

    def _email_subject(self, subject: str) -> str:
        return f"[TEST] {subject}" if self.test_mode is True else subject

    @staticmethod
    def _verified_pipes_mode(cfg: dict) -> str:
        return str(cfg.get("verified_pipes_mode") or cfg.get("verfied_pipes_mode") or "loadcell").strip().lower()

    @staticmethod
    def _email_password_skip_reason(cfg: dict) -> str | None:
        email_cfg = cfg.get("email", {}) or {}
        password_env = email_cfg.get("password_env")
        if password_env:
            return None if os.getenv(password_env) else f"Email password environment variable {password_env} is not set"
        return None if email_cfg.get("password") else "No email.password or email.password_env configured"

    @staticmethod
    def _parse_positive_int(value, *, default: int, name: str) -> int:
        if value is None:
            return default
        seconds = int(value)
        if seconds <= 0:
            raise ValueError(f"{name} must be greater than 0")
        return seconds

    @staticmethod
    def _format_seconds_hms(seconds) -> str:
        if seconds is None:
            return "N/A"
        total_seconds = abs(int(round(float(seconds))))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _diagnosis_gap_labels(self, diagnosis_summary: dict | None) -> tuple[str, str]:
        summary = diagnosis_summary or {}
        min_label = summary.get("t_origin_gap_min_label")
        if not min_label and summary.get("t_origin_gap_min_seconds") is not None:
            min_label = self._format_seconds_hms(summary.get("t_origin_gap_min_seconds"))
        max_label = summary.get("t_origin_gap_max_label")
        if not max_label and summary.get("t_origin_gap_max_seconds") is not None:
            max_label = self._format_seconds_hms(summary.get("t_origin_gap_max_seconds"))
        return str(min_label or "00:01:50"), str(max_label or "00:03:10")

    def _removed_pipe_reason(self, verified_summary: dict | None) -> str:
        if not verified_summary:
            return "N/A"
        try:
            removed_count = int(verified_summary.get("removed_count", 0))
        except (TypeError, ValueError):
            removed_count = 0
        if removed_count <= 0:
            return "N/A"
        window_seconds = verified_summary.get("gate_open_max_interval_seconds")
        window_text = self._format_seconds_hms(window_seconds) if window_seconds is not None else "configured window"
        return (
            "Pipe checkpoint was not 1, no pipe-on-trolley Gate 2 intersection was "
            f"recorded, and G2 was not open within {window_text} from T-Origin"
        )

    def _missing_loadcell_video_cfg(self, cfg: dict) -> dict:
        video_cfg = cfg.get("missing_loadcell_video", {}) or {}
        return {
            "enabled": bool(video_cfg.get("enabled", True)),
            "pre_origin_seconds": self._parse_positive_int(
                video_cfg.get("pre_origin_seconds"),
                default=60,
                name="missing_loadcell_video.pre_origin_seconds",
            ),
            "clip_duration_seconds": self._parse_positive_int(
                video_cfg.get("clip_duration_seconds"),
                default=300,
                name="missing_loadcell_video.clip_duration_seconds",
            ),
            "delete_after_upload": bool(video_cfg.get("delete_after_upload", True)),
        }

    @staticmethod
    def _normalize_shift_name(shift: str) -> str:
        value = str(shift).strip()
        if value.lower().startswith("shift_"):
            letter = value.split("_", 1)[1].upper()
        else:
            letter = value.upper()
        if letter not in {"A", "B", "C"}:
            raise ValueError("Invalid shift. Use A, B, or C")
        return f"Shift_{letter}"

    def _shift_window(self, cfg: dict, run: ShiftRun) -> tuple[datetime, datetime]:
        shifts_cfg = (cfg.get("history", {}) or {}).get("shifts", [])
        shifts = {str(item["name"]).lower(): (item["start"], item["end"]) for item in shifts_cfg}
        shift_key = run.shift_name.lower()
        if shift_key not in shifts:
            raise ValueError(f"Invalid shift: {run.shift_name}")
        start_s, end_s = shifts[shift_key]
        start = datetime.strptime(f"{run.date_str} {start_s}", "%d-%m-%Y %H:%M")
        end = datetime.strptime(f"{run.date_str} {end_s}", "%d-%m-%Y %H:%M")
        if end <= start:
            end += timedelta(days=1)
        return start, end

    @staticmethod
    def _parse_origin_time(value: str | None) -> datetime | None:
        if not value:
            return None
        text = str(value).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _merge_overlay_labels(existing: str, new: str) -> str:
        parts = [part.strip() for part in f"{existing}, {new}".split(",") if part.strip()]
        return ", ".join(dict.fromkeys(parts))

    def _build_missing_loadcell_windows(
        self,
        cfg: dict,
        run: ShiftRun,
        records: list[dict],
    ) -> tuple[list[dict], int]:
        video_cfg = self._missing_loadcell_video_cfg(cfg)
        shift_start, shift_end = self._shift_window(cfg, run)
        pre_origin = timedelta(seconds=video_cfg["pre_origin_seconds"])
        duration = timedelta(seconds=video_cfg["clip_duration_seconds"])
        windows = []
        skipped = 0
        for idx, record in enumerate(records, start=1):
            origin = self._parse_origin_time(record.get("origin_time") or record.get("origin_time_raw"))
            if origin is None:
                skipped += 1
                continue
            raw_start = origin - pre_origin
            raw_end = raw_start + duration
            start = max(raw_start, shift_start)
            end = min(raw_end, shift_end)
            if end <= start:
                skipped += 1
                continue
            pipe_uid = str(record.get("pipe_uid") or "").strip()
            label = f"{pipe_uid} @ {origin:%H:%M:%S}" if pipe_uid else f"Pipe {idx} @ {origin:%H:%M:%S}"
            windows.append({"start": start, "end": end, "label": label})

        windows = sorted(windows, key=lambda item: item["start"])
        if not windows:
            return [], skipped
        merged = [windows[0].copy()]
        for window in windows[1:]:
            current = merged[-1]
            if window["start"] <= current["end"]:
                current["end"] = max(current["end"], window["end"])
                current["label"] = self._merge_overlay_labels(current["label"], window["label"])
            else:
                merged.append(window.copy())
        return merged, skipped

    @staticmethod
    def _serialize_windows(windows: list[dict]) -> list[dict]:
        return [
            {
                "start": window["start"].isoformat(timespec="seconds"),
                "end": window["end"].isoformat(timespec="seconds"),
                "label": window["label"],
            }
            for window in windows
        ]

    def _record_error(self, run: ShiftRun, result: CasterRunResult, label: str, exc_text: str | None = None):
        message = f"{label}:\n{exc_text or traceback.format_exc()}"
        result.errors.append(message)
        result.state["errors"] = result.errors
        self._save_state(run, result.caster, result.state)

    def _skip_missing_loadcell_videos(self, run: ShiftRun, result: CasterRunResult, reason: str):
        for kind in ("overlay", "normal"):
            result.state[f"missing_loadcell_{kind}_video_skipped"] = True
            result.state[f"missing_loadcell_{kind}_video_skip_reason"] = reason
        self._save_state(run, result.caster, result.state)

    def _submit_missing_loadcell_video_upload(
        self,
        run: ShiftRun,
        result: CasterRunResult,
        kind: str,
        path: str,
        delete_after_upload: bool,
    ):
        if self._remaining_workflow_seconds() <= 0:
            self._mark_video_upload_timeout(run, result, kind, path)
            return

        result.state[f"missing_loadcell_{kind}_video_upload_pending"] = True
        result.state[f"missing_loadcell_{kind}_video_uploaded"] = False
        result.state[f"missing_loadcell_{kind}_video_upload_queued_at"] = datetime.now().isoformat(timespec="seconds")
        result.state.pop(f"missing_loadcell_{kind}_video_upload_error", None)
        result.state.pop(f"missing_loadcell_{kind}_video_upload_timed_out", None)
        self._save_state(run, result.caster, result.state)

        future = self._video_upload_executor_for_run().submit(
            self._run_video_upload_task,
            result.caster,
            result.caster.cfg,
            kind,
            path,
            delete_after_upload,
        )
        self._pending_video_uploads.append(PendingVideoUpload(result=result, kind=kind, path=path, future=future))
        logger.info(
            "Queued async missing-loadcell video upload | caster=%s | kind=%s | path=%s | pending=%s",
            result.caster.id,
            kind,
            path,
            len(self._pending_video_uploads),
        )

    def _apply_video_upload_success(self, run: ShiftRun, item: PendingVideoUpload, outcome: VideoUploadOutcome):
        result = item.result
        kind = outcome.kind
        if kind == "overlay":
            result.missing_overlay_link = outcome.link
        elif kind == "normal":
            result.missing_normal_link = outcome.link

        result.state[f"missing_loadcell_{kind}_video_drive_link"] = outcome.link
        result.state[f"missing_loadcell_{kind}_video_uploaded"] = True
        result.state[f"missing_loadcell_{kind}_video_upload_pending"] = False
        result.state[f"missing_loadcell_{kind}_video_uploaded_at"] = datetime.now().isoformat(timespec="seconds")
        result.state.pop(f"missing_loadcell_{kind}_video_upload_error", None)
        result.state.pop(f"missing_loadcell_{kind}_video_upload_timed_out", None)

        if outcome.deleted_after_upload is not None:
            result.state[f"missing_loadcell_{kind}_video_deleted_after_upload"] = outcome.deleted_after_upload
        if outcome.delete_error:
            result.state[f"missing_loadcell_{kind}_video_delete_error"] = outcome.delete_error

        generated_kinds = [
            upload_kind
            for upload_kind in ("overlay", "normal")
            if result.state.get(f"missing_loadcell_{upload_kind}_video_path")
        ]
        if generated_kinds and all(result.state.get(f"missing_loadcell_{upload_kind}_video_drive_link") for upload_kind in generated_kinds):
            result.state["missing_loadcell_video_uploaded_at"] = datetime.now().isoformat(timespec="seconds")

        self._save_state(run, result.caster, result.state)
        logger.info(
            "Missing-loadcell video upload completed | caster=%s | kind=%s | link=%s",
            result.caster.id,
            kind,
            outcome.link,
        )

    def _apply_video_upload_failure(self, run: ShiftRun, item: PendingVideoUpload, exc_text: str):
        result = item.result
        kind = item.kind
        result.state[f"missing_loadcell_{kind}_video_uploaded"] = False
        result.state[f"missing_loadcell_{kind}_video_upload_pending"] = False
        result.state[f"missing_loadcell_{kind}_video_upload_error"] = exc_text
        result.state[f"missing_loadcell_{kind}_video_upload_failed_at"] = datetime.now().isoformat(timespec="seconds")
        self._record_error(run, result, f"Missing-loadcell {kind} video upload failed", exc_text)
        logger.error("Missing-loadcell video upload failed | caster=%s | kind=%s", result.caster.id, kind)

    def _mark_video_upload_timeout(self, run: ShiftRun, result: CasterRunResult, kind: str, path: str):
        reason = f"Video upload exceeded workflow limit of {self._workflow_max_seconds()} seconds"
        result.state[f"missing_loadcell_{kind}_video_uploaded"] = False
        result.state[f"missing_loadcell_{kind}_video_upload_pending"] = False
        result.state[f"missing_loadcell_{kind}_video_upload_timed_out"] = True
        result.state[f"missing_loadcell_{kind}_video_upload_timeout_reason"] = reason
        result.state[f"missing_loadcell_{kind}_video_upload_failed_at"] = datetime.now().isoformat(timespec="seconds")
        result.state[f"missing_loadcell_{kind}_video_path"] = path
        self._record_error(run, result, f"Missing-loadcell {kind} video upload timed out", reason)
        logger.error("Missing-loadcell video upload timed out | caster=%s | kind=%s | path=%s", result.caster.id, kind, path)

    def wait_for_video_uploads(self, run: ShiftRun):
        if not self._pending_video_uploads:
            self._shutdown_video_upload_executor()
            return

        pending = list(self._pending_video_uploads)
        self._pending_video_uploads = []
        logger.info(
            "Waiting for async missing-loadcell video uploads | pending=%s | remaining_seconds=%.0f",
            len(pending),
            self._remaining_workflow_seconds(),
        )

        while pending:
            remaining = self._remaining_workflow_seconds()
            if remaining <= 0:
                for item in pending:
                    item.future.cancel()
                    self._mark_video_upload_timeout(run, item.result, item.kind, item.path)
                pending = []
                break

            done_futures, _not_done = wait(
                [item.future for item in pending],
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            if not done_futures:
                for item in pending:
                    item.future.cancel()
                    self._mark_video_upload_timeout(run, item.result, item.kind, item.path)
                pending = []
                break

            next_pending = []
            for item in pending:
                if item.future not in done_futures:
                    next_pending.append(item)
                    continue
                try:
                    outcome = item.future.result()
                except Exception:
                    self._apply_video_upload_failure(run, item, traceback.format_exc())
                else:
                    self._apply_video_upload_success(run, item, outcome)
            pending = next_pending

        self._shutdown_video_upload_executor()

    def _shutdown_video_upload_executor(self):
        if self._video_upload_executor is None:
            return
        self._video_upload_executor.shutdown(wait=True, cancel_futures=True)
        self._video_upload_executor = None

    def _generate_missing_loadcell_videos(self, run: ShiftRun, result: CasterRunResult):
        cfg = result.caster.cfg
        video_cfg = self._missing_loadcell_video_cfg(cfg)
        records = (result.verified_summary or {}).get("loadcell_missing_records", []) or []
        result.missing_overlay_link = None
        result.missing_normal_link = None
        for key in list(result.state):
            if key.startswith("missing_loadcell_"):
                result.state.pop(key, None)
        result.state["missing_loadcell_video_strategy"] = "merged_window_compilation_overlay_and_normal"
        result.state["missing_loadcell_video_record_count"] = len(records)

        if not video_cfg["enabled"]:
            self._skip_missing_loadcell_videos(run, result, "missing_loadcell_video.enabled is false")
            return
        if not result.verified_summary:
            self._skip_missing_loadcell_videos(run, result, "Verified pipe summary unavailable")
            return
        if not records:
            self._skip_missing_loadcell_videos(run, result, "No loadcell-missing pipes")
            return

        windows, skipped = self._build_missing_loadcell_windows(cfg, run, records)
        result.state["missing_loadcell_overlay_window_count"] = len(windows)
        result.state["missing_loadcell_overlay_skipped_record_count"] = skipped
        result.state["missing_loadcell_overlay_windows"] = self._serialize_windows(windows)
        self._save_state(run, result.caster, result.state)
        if not windows:
            self._skip_missing_loadcell_videos(run, result, "No valid loadcell-missing video windows")
            return

        shift_key = run.shift_name.lower()
        caster_part = result.caster.file_token or "legacy"
        output_name = f"missing_loadcell_{caster_part}_{run.date_str.replace('-', '')}_{shift_key}_overlay.mp4"
        normal_output_name = f"missing_loadcell_{caster_part}_{run.date_str.replace('-', '')}_{shift_key}_normal.mp4"
        overlay_gen = ShiftVideoOverlayGenerator(
            run.date_str,
            run.shift_name.split("_")[1],
            windows=windows,
            output_name=output_name,
            normal_output_name=normal_output_name,
            cfg=cfg,
            caster=result.caster,
        )
        overlay_path = backoff_retry(lambda: overlay_gen.generate(), what=f"{result.caster.id} missing-loadcell video")
        normal_path = str(overlay_gen.normal_output_path)
        result.state["missing_loadcell_overlay_video_path"] = overlay_path
        result.state["missing_loadcell_normal_video_path"] = normal_path
        self._save_state(run, result.caster, result.state)

        result.state["missing_loadcell_video_uploads_queued_at"] = datetime.now().isoformat(timespec="seconds")
        self._save_state(run, result.caster, result.state)
        for kind, path in (("overlay", overlay_path), ("normal", normal_path)):
            self._submit_missing_loadcell_video_upload(
                run,
                result,
                kind,
                path,
                delete_after_upload=video_cfg["delete_after_upload"],
            )

    @staticmethod
    def _shift_display_name(shift: str) -> str:
        value = str(shift or "").strip()
        if value.lower().startswith("shift_"):
            return value.split("_", 1)[1].upper()
        return value.upper()

    @staticmethod
    def _caster_sort_key(caster: CasterConfig) -> tuple[int, str]:
        try:
            number = int(caster.number)
        except (TypeError, ValueError):
            number = 9999
        return number, str(caster.id)

    def _ordered_results(self, casters: list[CasterConfig]) -> list[CasterRunResult]:
        ordered_casters = sorted(casters, key=self._caster_sort_key)
        return [self.results.setdefault(caster.id, CasterRunResult(caster=caster)) for caster in ordered_casters]

    @staticmethod
    def _display_count(value, *, unavailable: str = "0") -> str:
        if value is None:
            return unavailable
        if isinstance(value, str):
            value = value.strip()
            return value if value else unavailable
        if isinstance(value, float):
            if math.isnan(value):
                return unavailable
            if value.is_integer():
                return str(int(value))
        return str(value)

    @staticmethod
    def _display_text(value) -> str:
        text = "" if value is None else str(value).strip()
        return text or "N/A"

    def _summary_count(self, summary: dict | None, key: str, *, unavailable: str = "0") -> str:
        return self._display_count((summary or {}).get(key), unavailable=unavailable)

    @staticmethod
    def _report_type_label(report_type: str) -> str:
        labels = {
            "raw": "Raw Production Report",
            "verified": "Verified Production Report",
            "diagnosis": "Pipe Diagnosis Report",
        }
        return labels.get(report_type, report_type)

    @staticmethod
    def _report_state_keys(report_type: str) -> tuple[str, str, str]:
        if report_type == "raw":
            return "emailed_csv", "csv_email_skip_reason", "csv_email_recipients"
        if report_type == "verified":
            return "emailed_verified_pipes", "verified_pipes_skip_reason", "verified_pipe_records_recipients"
        if report_type == "diagnosis":
            return "emailed_diagnosis_xlsx", "diagnosis_skip_reason", "diagnosis_recipients"
        raise ValueError(f"Unknown report type: {report_type}")

    def _set_report_email_state(
        self,
        run: ShiftRun,
        result: CasterRunResult,
        report_type: str,
        sent: bool,
        *,
        recipients: list[str] | None = None,
        reason: str | None = None,
    ):
        sent_key, skip_key, recipients_key = self._report_state_keys(report_type)
        result.state[sent_key] = sent
        result.state["email_test_mode"] = self.test_mode
        if sent:
            result.state.pop(skip_key, None)
            result.state[recipients_key] = recipients or []
        elif reason:
            result.state[skip_key] = reason
        if report_type == "raw":
            result.raw_email_sent = sent
        self._save_state(run, result.caster, result.state)

    def _report_recipients(self, report_type: str, cfg: dict) -> list[str]:
        if self.test_mode:
            return self._test_recipients(cfg)
        if report_type == "raw":
            return self._email_recipients(cfg)
        if report_type == "verified":
            return self._verified_pipe_records_recipients(cfg)
        if report_type == "diagnosis":
            return self._diagnosis_recipients(cfg)
        raise ValueError(f"Unknown report type: {report_type}")

    def _no_recipients_reason(self, report_type: str) -> str:
        if self.test_mode:
            return self._test_mode_skip_reason()
        if report_type == "raw":
            return "No email.recipients configured"
        if report_type == "verified":
            return "No verified_pipe_records_recipients configured"
        if report_type == "diagnosis":
            return "No email.diagnosis_recipients configured"
        return "No email recipients configured"

    def _consolidated_send_context(
        self,
        report_type: str,
        results: list[CasterRunResult],
    ) -> tuple[CasterConfig | None, list[str], str | None]:
        recipients: list[str] = []
        mailer_caster = None
        fallback_reason = None
        for result in results:
            cfg = result.caster.cfg
            result_recipients = self._report_recipients(report_type, cfg)
            if not result_recipients:
                fallback_reason = self._no_recipients_reason(report_type)
                continue
            for recipient in result_recipients:
                if recipient not in recipients:
                    recipients.append(recipient)

            password_skip_reason = self._email_password_skip_reason(cfg)
            if password_skip_reason:
                fallback_reason = password_skip_reason
                continue
            if mailer_caster is None:
                mailer_caster = result.caster

        if mailer_caster is not None and recipients:
            return mailer_caster, recipients, None
        return None, [], fallback_reason or self._no_recipients_reason(report_type)

    def _should_send_report_result(self, result: CasterRunResult, report_type: str) -> bool:
        exported_now = {
            "raw": result.raw_exported,
            "verified": result.verified_exported,
            "diagnosis": result.diagnosis_exported,
        }[report_type]
        if exported_now:
            return True
        sent_key, _skip_key, _recipients_key = self._report_state_keys(report_type)
        return not bool(result.state.get(sent_key))

    def _log_missing_attachment(self, run: ShiftRun, result: CasterRunResult, report_type: str, path: str):
        logger.warning(
            "Missing attachment | caster=%s | caster_number=%s | report_type=%s | date=%s | shift=%s | path=%s",
            result.caster.id,
            result.caster.number,
            self._report_type_label(report_type),
            run.date_str,
            run.shift_name,
            path,
        )

    def _report_attachment_items(
        self,
        casters: list[CasterConfig],
        run: ShiftRun,
        report_type: str,
        path_attr: str,
        state_path_key: str,
    ) -> list[tuple[CasterRunResult, str]]:
        items: list[tuple[CasterRunResult, str]] = []
        for result in self._ordered_results(casters):
            if not self._should_send_report_result(result, report_type):
                continue
            if report_type == "raw" and not bool((result.caster.cfg.get("email", {}) or {}).get("send_csv_attachment", True)):
                reason = "email.send_csv_attachment is false"
                self._set_report_email_state(run, result, report_type, False, reason=reason)
                logger.info("Raw CSV email skipped | caster=%s | reason=%s", result.caster.id, reason)
                continue

            path_value = getattr(result, path_attr) or result.state.get(state_path_key)
            if not path_value:
                continue
            attachment_path = str(path_value)
            if not Path(attachment_path).exists():
                reason = f"{self._report_type_label(report_type)} attachment missing: {attachment_path}"
                self._set_report_email_state(run, result, report_type, False, reason=reason)
                self._log_missing_attachment(run, result, report_type, attachment_path)
                continue
            items.append((result, attachment_path))
        return items

    def _record_consolidated_email_failure(
        self,
        run: ShiftRun,
        report_type: str,
        items: list[tuple[CasterRunResult, str]],
        exc: Exception,
    ):
        reason = str(exc) or "Email sending failed"
        exc_text = traceback.format_exc()
        label = self._report_type_label(report_type)
        for result, _path in items:
            self._set_report_email_state(run, result, report_type, False, reason=reason)
            self._record_error(run, result, f"{label} email failed", exc_text)
            logger.error(
                "Email sending failure | caster=%s | caster_number=%s | report_type=%s | date=%s | shift=%s | error=%s",
                result.caster.id,
                result.caster.number,
                label,
                run.date_str,
                run.shift_name,
                reason,
            )

    @staticmethod
    def _attachment_note(file_label: str, count: int) -> str:
        noun = "files" if count != 1 else "file"
        return f"{file_label} {noun} attached."

    def _send_consolidated_email(
        self,
        report_type: str,
        run: ShiftRun,
        items: list[tuple[CasterRunResult, str]],
        subject: str,
        body: str,
    ) -> bool:
        label = self._report_type_label(report_type)
        if not items:
            logger.info(
                "Consolidated %s email skipped | date=%s | shift=%s | reason=no eligible attachments",
                label,
                run.date_str,
                run.shift_name,
            )
            return False

        results = [result for result, _path in items]
        mailer_caster, recipients, skip_reason = self._consolidated_send_context(report_type, results)
        if mailer_caster is None:
            reason = skip_reason or self._no_recipients_reason(report_type)
            for result in results:
                self._set_report_email_state(run, result, report_type, False, reason=reason)
            logger.info(
                "Consolidated %s email skipped | date=%s | shift=%s | reason=%s",
                label,
                run.date_str,
                run.shift_name,
                reason,
            )
            return False

        attachments = [path for _result, path in items]
        try:
            backoff_retry(
                lambda: self._mailer(mailer_caster).send(
                    self._email_subject(subject),
                    body,
                    attachments=attachments,
                    recipients=recipients,
                ),
                what=f"{label} consolidated email",
            )
        except Exception as exc:
            self._record_consolidated_email_failure(run, report_type, items, exc)
            return False

        for result in results:
            self._set_report_email_state(run, result, report_type, True, recipients=recipients)
        logger.info(
            "Consolidated %s email sent | date=%s | shift=%s | casters=%s | attachments=%s | recipients=%s | test=%s",
            label,
            run.date_str,
            run.shift_name,
            [result.caster.id for result in results],
            len(attachments),
            len(recipients),
            self.test_mode,
        )
        return True

    def _send_consolidated_raw_csv_email(self, casters: list[CasterConfig], run: ShiftRun) -> bool:
        items = self._report_attachment_items(casters, run, "raw", "csv_path", "csv_path")
        if not items:
            return self._send_consolidated_email("raw", run, items, "", "")

        shift_name = self._shift_display_name(run.shift_name)
        body_lines = [
            "Pipe Production Report",
            "",
            f"Date  : {run.date_str}",
            f"Shift : {shift_name}",
            "",
        ]
        for result, _path in items:
            count = result.pipe_count if result.pipe_count not in (None, "") else result.state.get("pipe_count")
            body_lines.append(f"{caster_label(result.caster, result.caster.cfg)} : {self._display_count(count)}")
        body_lines.extend(["", self._attachment_note("CSV", len(items))])

        subject = f"Raw Pipe Production Report - {run.date_str} - Shift {shift_name}"
        return self._send_consolidated_email("raw", run, items, subject, "\n".join(body_lines))

    def _send_consolidated_verified_pipes_email(self, casters: list[CasterConfig], run: ShiftRun) -> bool:
        items = self._report_attachment_items(casters, run, "verified", "verified_path", "verified_pipes_csv_path")
        if not items:
            return self._send_consolidated_email("verified", run, items, "", "")

        shift_name = self._shift_display_name(run.shift_name)
        body_lines = [
            "Pipe Production Report",
            "",
            f"Date  : {run.date_str}",
            f"Shift : {shift_name}",
            "",
        ]
        for result, _path in items:
            summary = result.verified_summary or result.state.get("verified_pipes_summary") or {}
            body_lines.append(
                f"{caster_label(result.caster, result.caster.cfg)} : "
                f"{self._summary_count(summary, 'verified_count', unavailable='N/A')}"
            )
        body_lines.extend(["", self._attachment_note("CSV", len(items))])

        subject = f"Verified Pipe Production Report - {run.date_str} - Shift {shift_name}"
        return self._send_consolidated_email("verified", run, items, subject, "\n".join(body_lines))

    def _missing_loadcell_video_text(self, result: CasterRunResult, kind: str) -> str:
        prefix = f"missing_loadcell_{kind}_video"
        link = result.state.get(f"{prefix}_drive_link")
        if kind == "overlay":
            link = result.missing_overlay_link or link
        else:
            link = result.missing_normal_link or link
        if link:
            return str(link)

        skip_reason = result.state.get(f"{prefix}_skip_reason")
        if skip_reason:
            return f"N/A - {skip_reason}"
        if result.state.get(f"{prefix}_upload_timed_out"):
            timeout_reason = result.state.get(f"{prefix}_upload_timeout_reason", "Upload timed out")
            return f"N/A - {timeout_reason}"
        if result.state.get(f"{prefix}_upload_error"):
            return "N/A - Upload failed"
        if result.state.get(f"{prefix}_upload_pending"):
            return "N/A - Upload pending"
        if result.state.get(f"{prefix}_path"):
            return "N/A - Generated, upload not completed"
        return "N/A"

    def _diagnosis_section_lines(self, result: CasterRunResult) -> list[str]:
        diagnosis_summary = result.diagnosis_summary or result.state.get("diagnosis_summary") or {}
        verified_summary = result.verified_summary or result.state.get("verified_pipes_summary") or {}
        min_gap_label, max_gap_label = self._diagnosis_gap_labels(diagnosis_summary)
        pipe_count = self._summary_count(diagnosis_summary, "pipe_count")
        pipe_noun = "Pipe" if pipe_count == "1" else "Pipes"
        return [
            f"### {caster_label(result.caster, result.caster.cfg)}: {pipe_count} {pipe_noun}",
            "",
            f"Abnormal Rows                       : {self._summary_count(diagnosis_summary, 'abnormal_count')}",
            f"T-Origin Gap Abnormal               : {self._summary_count(diagnosis_summary, 't_origin_gap_abnormal_count')}",
            f"T-Origin Gap Above {max_gap_label}         : {self._summary_count(diagnosis_summary, 't_origin_gap_too_slow_count')}",
            f"T-Origin Gap Below {min_gap_label}         : {self._summary_count(diagnosis_summary, 't_origin_gap_too_fast_count')}",
            f"Loadcell Missing Rows               : {self._summary_count(diagnosis_summary, 'loadcell_missing_count')}",
            f"Removed Pipe ID Count               : {self._summary_count(verified_summary, 'removed_count', unavailable='N/A')}",
            f"Removed Pipe ID Reason              : {self._display_text(self._removed_pipe_reason(verified_summary))}",
            "",
            "Missing Loadcell Videos",
            "",
            f"Overlay Video Link                  : {self._missing_loadcell_video_text(result, 'overlay')}",
            f"Normal Video Link                   : {self._missing_loadcell_video_text(result, 'normal')}",
        ]

    def _send_consolidated_diagnosis_email(self, casters: list[CasterConfig], run: ShiftRun) -> bool:
        attachment_items = self._report_attachment_items(casters, run, "diagnosis", "diagnosis_path", "diagnosis_xlsx_path")
        items: list[tuple[CasterRunResult, str]] = []
        for result, path in attachment_items:
            if not (result.diagnosis_summary or result.state.get("diagnosis_summary")):
                reason = "Diagnosis summary unavailable"
                self._set_report_email_state(run, result, "diagnosis", False, reason=reason)
                logger.info("Diagnosis email skipped | caster=%s | reason=%s", result.caster.id, reason)
                continue
            items.append((result, path))
        if not items:
            return self._send_consolidated_email("diagnosis", run, items, "", "")

        shift_name = self._shift_display_name(run.shift_name)
        body_lines = [
            "Pipe Diagnosis Report",
            "",
            f"Date  : {run.date_str}",
            f"Shift : {shift_name}",
            "",
        ]
        for idx, (result, _path) in enumerate(items):
            if idx:
                body_lines.append("")
            body_lines.extend(self._diagnosis_section_lines(result))
        body_lines.extend(["", self._attachment_note("Excel", len(items))])

        subject = f"Pipe Diagnosis Report - {run.date_str} - Shift {shift_name}"
        return self._send_consolidated_email("diagnosis", run, items, subject, "\n".join(body_lines))

    def _send_raw_csv_email(self, run: ShiftRun, result: CasterRunResult) -> bool:
        self.results[result.caster.id] = result
        return self._send_consolidated_raw_csv_email([result.caster], run)

    def _export_verified_pipes_report(self, run: ShiftRun, result: CasterRunResult):
        cfg = result.caster.cfg
        verified_exporter = VerifiedPipeExporter(cfg=cfg, caster=result.caster)
        verified_path_obj, verified_summary = backoff_retry(
            lambda: verified_exporter.export(
                run.date_str,
                run.shift_name,
                result.csv_path,
                mode=self._verified_pipes_mode(cfg),
            ),
            what=f"{result.caster.id} verified pipes CSV export",
        )
        result.verified_path = str(verified_path_obj)
        result.verified_summary = verified_summary
        result.verified_exported = True
        result.state["verified_pipes_csv_path"] = result.verified_path
        result.state["verified_pipes_summary"] = verified_summary
        self._save_state(run, result.caster, result.state)

    def _send_verified_pipes_report(self, run: ShiftRun, result: CasterRunResult):
        self.results[result.caster.id] = result
        self._export_verified_pipes_report(run, result)
        self._send_consolidated_verified_pipes_email([result.caster], run)

    def _missing_loadcell_video_lines(self, result: CasterRunResult) -> list[str]:
        return [
            "",
            "Missing Loadcell Videos",
            f"Overlay Video Link        : {self._missing_loadcell_video_text(result, 'overlay')}",
            f"Normal Video Link         : {self._missing_loadcell_video_text(result, 'normal')}",
        ]

    def _export_diagnosis_report(self, run: ShiftRun, result: CasterRunResult):
        cfg = result.caster.cfg
        diagnosis_exporter = PipeExporter(cfg=cfg, caster=result.caster)
        diagnosis_path_obj, diagnosis_summary = backoff_retry(
            lambda: diagnosis_exporter.export_diagnosis(run.date_str, run.shift_name),
            what=f"{result.caster.id} diagnosis XLSX export",
        )
        result.diagnosis_path = str(diagnosis_path_obj)
        result.diagnosis_summary = diagnosis_summary
        result.diagnosis_exported = True
        result.state["diagnosis_xlsx_path"] = result.diagnosis_path
        result.state["diagnosis_summary"] = diagnosis_summary
        self._save_state(run, result.caster, result.state)

    def _send_diagnosis_email(self, run: ShiftRun, result: CasterRunResult):
        self.results[result.caster.id] = result
        return self._send_consolidated_diagnosis_email([result.caster], run)

    def _send_diagnosis_report(self, run: ShiftRun, result: CasterRunResult):
        self.results[result.caster.id] = result
        self._export_diagnosis_report(run, result)
        self._send_consolidated_diagnosis_email([result.caster], run)

    def load_selected_enabled_casters(self) -> list[CasterConfig]:
        return self.casters

    def phase_raw_and_verified(self, casters: list[CasterConfig], run: ShiftRun, *, require_raw_email_for_verified: bool = True):
        force = os.getenv("FORCE_RERUN") == "1"
        for caster in casters:
            result = CasterRunResult(caster=caster)
            self.results[caster.id] = result
            state = self._load_state(run, caster)
            raw_verified_done = bool(state.get("csv_path") and state.get("verified_pipes_csv_path"))
            if state.get("status") == "success" and raw_verified_done and not force:
                logger.info("Already success for %s %s %s. Skipping raw/verified.", caster.id, run.date_str, run.shift_name)
                result.state = state
                result.csv_path = state.get("csv_path")
                result.verified_path = state.get("verified_pipes_csv_path")
                result.pipe_count = state.get("pipe_count", 0)
                result.csv_drive_link = state.get("csv_drive_link")
                result.verified_summary = state.get("verified_pipes_summary")
                result.raw_email_sent = bool(state.get("emailed_csv"))
                continue

            self._clear_previous_run_outputs(state)
            state.update({
                "date": run.date_str,
                "shift": run.shift_name,
                "caster_id": caster.id,
                "caster_number": caster.number,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "status": "raw_verified_running",
            })
            result.state = state
            self._save_state(run, caster, state)
            logger.info("Raw/verified phase start | caster=%s | date=%s | shift=%s", caster.id, run.date_str, run.shift_name)

            try:
                exporter = PipeExporter(cfg=caster.cfg, caster=caster)
                csv_path_obj, pipe_count = backoff_retry(
                    lambda: exporter.export(run.date_str, run.shift_name),
                    what=f"{caster.id} CSV export",
                )
                result.csv_path = str(csv_path_obj)
                result.pipe_count = pipe_count
                result.raw_exported = True
                state["csv_path"] = result.csv_path
                state["pipe_count"] = pipe_count
                self._save_state(run, caster, state)
                logger.info("CSV export success | caster=%s | pipe_count=%s | path=%s", caster.id, pipe_count, result.csv_path)
            except Exception:
                self._record_error(run, result, "CSV export failed")
                logger.exception("CSV export failed | caster=%s", caster.id)

        self._send_consolidated_raw_csv_email(casters, run)

        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            state = result.state
            if result.verified_path and not result.raw_exported and not force:
                continue
            if result.csv_path and (result.raw_email_sent or not require_raw_email_for_verified):
                try:
                    self._export_verified_pipes_report(run, result)
                except Exception:
                    self._record_error(run, result, "Verified pipes CSV export failed")
                    logger.exception("Verified pipes export failed | caster=%s", caster.id)
            elif result.csv_path and require_raw_email_for_verified:
                state["emailed_verified_pipes"] = False
                state["verified_pipes_skip_reason"] = "Raw CSV email was not sent"
                self._save_state(run, caster, state)
                logger.info("Verified pipes skipped | caster=%s | reason=%s", caster.id, state["verified_pipes_skip_reason"])
            elif not result.csv_path:
                state["emailed_verified_pipes"] = False
                state["verified_pipes_skip_reason"] = "Raw CSV export failed"
                self._save_state(run, caster, state)

        self._send_consolidated_verified_pipes_email(casters, run)

        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state["raw_verified_finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)

    def phase_csv_uploads(self, casters: list[CasterConfig], run: ShiftRun):
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            if not result.csv_path:
                continue
            try:
                uploader = GDriveUploader(cfg=caster.cfg, caster=caster)
                result.csv_drive_link = backoff_retry(
                    lambda: uploader.upload_csv(result.csv_path),
                    what=f"{caster.id} Drive upload CSV",
                )
                result.state["csv_drive_link"] = result.csv_drive_link
                self._save_state(run, caster, result.state)
                try:
                    Path(result.csv_path).unlink(missing_ok=True)
                    result.state["csv_deleted_after_upload"] = True
                except Exception as exc:
                    result.state["csv_deleted_after_upload"] = False
                    logger.warning("Failed to delete CSV | caster=%s | path=%s | error=%s", caster.id, result.csv_path, exc)
                self._save_state(run, caster, result.state)
            except Exception:
                self._record_error(run, result, "Drive upload CSV failed")
                logger.exception("Drive upload CSV failed | caster=%s", caster.id)

    def phase_diagnosis(self, casters: list[CasterConfig], run: ShiftRun, *, send_email: bool = True):
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state.setdefault("date", run.date_str)
            result.state.setdefault("shift", run.shift_name)
            result.state.setdefault("caster_id", caster.id)
            result.state.setdefault("caster_number", caster.number)
            result.state["diagnosis_started_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)
            try:
                self._export_diagnosis_report(run, result)
            except Exception:
                self._record_error(run, result, "Diagnosis XLSX export failed")
                logger.exception("Diagnosis export failed | caster=%s", caster.id)
            result.state["diagnosis_export_finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)

        if send_email:
            self.phase_diagnosis_emails(casters, run)

    def phase_missing_loadcell_videos(self, casters: list[CasterConfig], run: ShiftRun):
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            try:
                self._generate_missing_loadcell_videos(run, result)
            except Exception:
                self._record_error(run, result, "Missing-loadcell video generation/upload failed")
                result.state["missing_loadcell_video_error"] = traceback.format_exc()
                self._save_state(run, caster, result.state)
                logger.exception("Missing-loadcell video failed | caster=%s", caster.id)

    def phase_diagnosis_emails(self, casters: list[CasterConfig], run: ShiftRun):
        try:
            self._send_consolidated_diagnosis_email(casters, run)
        except Exception:
            exc_text = traceback.format_exc()
            for caster in casters:
                result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
                self._record_error(run, result, "Diagnosis XLSX email failed", exc_text)
                logger.exception("Diagnosis email failed | caster=%s", caster.id)

        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state["diagnosis_finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)

    def phase_normal_shift_videos(self, casters: list[CasterConfig], run: ShiftRun):
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            try:
                if (caster.cfg.get("video", {}) or {}).get("enabled", True) is False:
                    result.state["normal_shift_video_skipped"] = True
                    result.state["normal_shift_video_skip_reason"] = "video.enabled is false"
                    self._save_state(run, caster, result.state)
                    continue
                caster_text = caster_label(caster, caster.cfg)
                logger.info(
                    "%s video is generating | caster_id=%s | date=%s | shift=%s",
                    caster_text,
                    caster.id,
                    run.date_str,
                    run.shift_name,
                )
                video_gen = ShiftVideoGenerator(
                    run.date_str,
                    run.shift_name.split("_")[1],
                    cfg=caster.cfg,
                    caster=caster,
                )
                video_gen.verified_report_path = (
                    result.verified_path or result.state.get("verified_pipes_csv_path")
                )
                result.full_shift_video_path = backoff_retry(
                    lambda: video_gen.generate(),
                    what=f"{caster.id} normal shift video generation",
                )
                logger.info(
                    "%s video generated successfully | caster_id=%s | path=%s",
                    caster_text,
                    caster.id,
                    result.full_shift_video_path,
                )
                result.state["normal_shift_video_path"] = result.full_shift_video_path
                result.state["normal_shift_video_uploaded"] = False
                result.state["video_path"] = result.full_shift_video_path
                result.state["video_drive_link"] = None
                try:
                    history_root = getattr(video_gen, "image_root", None)
                    if history_root is None:
                        history_root = (self.root / caster.cfg["history"]["image_root"]).resolve()
                    logger.info(
                        "%s source folder cleanup starting after video success | caster_id=%s | date=%s | shift=%s",
                        caster_text,
                        caster.id,
                        run.date_str,
                        run.shift_name,
                    )
                    cleanup_summary = cleanup_shift_sources(
                        history_root,
                        run.date_str,
                        run.shift_name,
                        caster_name=caster_text,
                    )
                    result.state["normal_shift_source_cleanup"] = cleanup_summary
                    if cleanup_summary.get("failed_dirs"):
                        result.errors.append("Normal shift source cleanup failed for one or more folders")
                        result.state["errors"] = result.errors
                    logger.info(
                        "%s source folder cleanup completed | caster_id=%s | deleted_folders=%s | removed_date_folders=%s | failed_folders=%s",
                        caster_text,
                        caster.id,
                        len(cleanup_summary.get("deleted_dirs", [])),
                        len(cleanup_summary.get("removed_empty_date_dirs", [])),
                        len(cleanup_summary.get("failed_dirs", {})),
                    )
                except Exception:
                    result.state["normal_shift_source_cleanup_error"] = traceback.format_exc()
                    result.errors.append("Normal shift source cleanup failed:\n" + result.state["normal_shift_source_cleanup_error"])
                    result.state["errors"] = result.errors
                    logger.exception("Normal shift source cleanup failed | caster=%s", caster.id)
                self._save_state(run, caster, result.state)
            except Exception:
                self._record_error(run, result, "Normal shift video generation failed")
                result.state["normal_shift_video_error"] = traceback.format_exc()
                self._save_state(run, caster, result.state)
                logger.exception("Normal shift video failed | caster=%s", caster.id)

    def phase_videos(self, casters: list[CasterConfig], run: ShiftRun):
        self.phase_missing_loadcell_videos(casters, run)
        self.phase_normal_shift_videos(casters, run)
        self.wait_for_video_uploads(run)

    def _result_summary_lines(self, result: CasterRunResult) -> list[str]:
        verified_summary = result.verified_summary or result.state.get("verified_pipes_summary") or {}
        diagnosis_summary = result.diagnosis_summary or result.state.get("diagnosis_summary") or {}
        raw_count = result.pipe_count if result.pipe_count not in (None, "") else result.state.get("pipe_count", "N/A")
        verified_count = verified_summary.get("verified_count", "N/A")
        removed_count = verified_summary.get("removed_count", "N/A")
        diagnosis_status = (
            "sent" if result.state.get("emailed_diagnosis_xlsx") else
            result.state.get("diagnosis_skip_reason") or ("failed" if result.state.get("diagnosis_error") else "N/A")
        )
        video_status = result.full_shift_video_path or result.state.get("normal_shift_video_path")
        if not video_status:
            video_status = result.state.get("normal_shift_video_skip_reason") or ("failed" if result.state.get("normal_shift_video_error") else "N/A")
        return [
            f"{caster_label(result.caster, result.caster.cfg)} ({result.caster.id})",
            f"  Raw Pipe Count              : {raw_count}",
            f"  Verified Pipe Count         : {verified_count}",
            f"  Removed Pipe Count          : {removed_count}",
            f"  Removed Pipe Reason         : {self._removed_pipe_reason(verified_summary)}",
            f"  Diagnosis Status            : {diagnosis_status}",
            f"  Diagnosis Pipe Count        : {diagnosis_summary.get('pipe_count', 'N/A')}",
            f"  Raw CSV Drive Link          : {result.csv_drive_link or result.state.get('csv_drive_link') or 'N/A'}",
            f"  Missing Overlay Link        : {self._missing_loadcell_video_text(result, 'overlay')}",
            f"  Missing Normal Link         : {self._missing_loadcell_video_text(result, 'normal')}",
            f"  Full Shift Video            : {video_status}",
        ]

    def send_final_multi_caster_summary(self, casters: list[CasterConfig], run: ShiftRun):
        body_lines = [
            "Pipe Production Summary",
            "",
            f"Date  : {run.date_str}",
            f"Shift : {run.shift_name}",
            "",
        ]
        attachments = []
        all_errors = []
        multi_state = {
            "date": run.date_str,
            "shift": run.shift_name,
            "caster_ids": [caster.id for caster in casters],
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            body_lines.extend(self._result_summary_lines(result))
            body_lines.append("")
            if result.diagnosis_path and Path(result.diagnosis_path).exists():
                attachments.append(result.diagnosis_path)
            if result.errors:
                all_errors.extend(f"{caster.id}: {error}" for error in result.errors)
        if all_errors:
            body_lines.extend(["Errors", "", *all_errors])

        subject = (
            f"Final Summary - All Casters - {run.shift_name} - {run.date_str}"
            if self.multi_caster_mode
            else f"Pipe Recordings - {run.shift_name} - {run.date_str}"
        )
        multi_state["errors"] = all_errors
        try:
            base_caster = casters[0]
            recipients = self._test_recipients(base_caster.cfg) if self.test_mode else None
            if self.test_mode and not recipients:
                multi_state["final_summary_email_sent"] = False
                multi_state["final_summary_email_skip_reason"] = self._test_mode_skip_reason()
                return
            password_skip_reason = self._email_password_skip_reason(base_caster.cfg)
            if password_skip_reason:
                multi_state["final_summary_email_sent"] = False
                multi_state["final_summary_email_skip_reason"] = password_skip_reason
                return
            if self.test_mode:
                send_summary = lambda: self._mailer(base_caster).send(
                    self._email_subject(subject),
                    "\n".join(body_lines),
                    attachments=attachments,
                    recipients=recipients,
                )
            else:
                send_summary = lambda: self._mailer(base_caster).send(
                    self._email_subject(subject),
                    "\n".join(body_lines),
                    attachments=attachments,
                )
            backoff_retry(send_summary, what="Final summary email")
            multi_state["final_summary_email_sent"] = True
            multi_state["email_test_mode"] = self.test_mode
            multi_state["final_summary_recipients"] = recipients if self.test_mode else self._email_recipients(base_caster.cfg)
        except Exception:
            multi_state["final_summary_email_sent"] = False
            multi_state["final_summary_email_error"] = traceback.format_exc()
            logger.exception("Final summary email failed")
        finally:
            multi_state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_multi_state(run, multi_state)

        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state["status"] = "partial_failure" if result.errors else "success"
            result.state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)

    def finish_caster_states(self, casters: list[CasterConfig], run: ShiftRun):
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state["status"] = "partial_failure" if result.errors else "success"
            result.state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)

    def run_verified_only(self, run: ShiftRun):
        casters = self.load_selected_enabled_casters()
        logger.info("Verified-only workflow start | date=%s | shift=%s | casters=%s | test=%s", run.date_str, run.shift_name, [c.id for c in casters], self.test_mode)
        self.phase_raw_and_verified(casters, run, require_raw_email_for_verified=False)
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state["status"] = "partial_failure" if result.errors else "success"
            result.state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)
        logger.info("Verified-only workflow finished")

    def run_diagnosis_only(self, run: ShiftRun):
        casters = self.load_selected_enabled_casters()
        logger.info("Diagnosis-only workflow start | date=%s | shift=%s | casters=%s | test=%s", run.date_str, run.shift_name, [c.id for c in casters], self.test_mode)
        self.phase_diagnosis(casters, run)
        for caster in casters:
            result = self.results.setdefault(caster.id, CasterRunResult(caster=caster))
            result.state["status"] = "partial_failure" if result.errors else "success"
            result.state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._save_state(run, caster, result.state)
        logger.info("Diagnosis-only workflow finished")

    def run(self, run: ShiftRun):
        self._start_workflow_timer()
        casters = self.load_selected_enabled_casters()
        logger.info("Workflow start | date=%s | shift=%s | casters=%s | test=%s", run.date_str, run.shift_name, [c.id for c in casters], self.test_mode)
        self.phase_raw_and_verified(casters, run)
        self.phase_csv_uploads(casters, run)
        self.phase_diagnosis(casters, run, send_email=False)
        self.phase_missing_loadcell_videos(casters, run)
        self.phase_normal_shift_videos(casters, run)
        self.wait_for_video_uploads(run)
        self.phase_diagnosis_emails(casters, run)
        self.finish_caster_states(casters, run)
        logger.info("Workflow finished | date=%s | shift=%s", run.date_str, run.shift_name)

    def validate_config(self) -> str:
        lines = []
        for caster in self.casters:
            cfg = caster.cfg
            root = self.root
            db_path = (root / cfg.get("database", {}).get("path", "")).resolve()
            history_root = (root / cfg.get("history", {}).get("image_root", "")).resolve()
            rois_path = (root / cfg.get("rois", {}).get("path", "")).resolve()
            lines.extend([
                f"{caster.id}:",
                f"  enabled: {caster.enabled}",
                f"  caster_number: {caster.number}",
                f"  database.path: {db_path} (exists={db_path.exists()})",
                f"  history.image_root: {history_root} (exists={history_root.exists()})",
                f"  rois.path: {rois_path} (exists={rois_path.exists()})",
                f"  outputs.csv_dir: {cfg.get('outputs', {}).get('csv_dir')}",
                f"  video.output_dir: {cfg.get('video', {}).get('output_dir')}",
                f"  video.overlay_output_dir: {cfg.get('video', {}).get('overlay_output_dir')}",
                f"  gdrive.pipes_csv_dir: {cfg.get('gdrive', {}).get('pipes_csv_dir')}",
                f"  gdrive.videos_dir: {cfg.get('gdrive', {}).get('videos_dir')}",
            ])
        return "\n".join(lines)


def _selected_ids_from_args(args) -> list[str] | None:
    selected = []
    if args.caster:
        selected.append(args.caster)
    if args.casters:
        selected.extend(part.strip() for part in args.casters.split(",") if part.strip())
    if args.all_casters:
        return None
    return selected or None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date")
    parser.add_argument("--shift")
    parser.add_argument("--diagnosis-only", action="store_true")
    parser.add_argument("--verified-only", action="store_true", help="Run raw pipe CSV and verified pipes only")
    parser.add_argument("--test", action="store_true", help="Send every workflow email only to email.test_recipients")
    parser.add_argument("--caster", help="Single caster id, for example caster1")
    parser.add_argument("--casters", help="Comma-separated caster ids, for example caster1,caster2,caster8")
    parser.add_argument("--all-casters", action="store_true")
    parser.add_argument("--validate-config", action="store_true")
    args = parser.parse_args()

    cfg = load_runtime_config()
    setup_logging(cfg)
    wf = ShiftWorkflow(cfg=cfg, selected_ids=_selected_ids_from_args(args), test_mode=args.test)

    if args.validate_config:
        print(wf.validate_config())
        return

    if args.diagnosis_only and args.verified_only:
        parser.error("--diagnosis-only and --verified-only cannot be used together")

    if args.date and args.shift:
        run = ShiftRun(args.date, ShiftWorkflow._normalize_shift_name(args.shift))
    else:
        run = detect_shift_for_trigger(_now())
        if not run:
            logger.info("Not a scheduled shift time. Exiting.")
            return

    if args.verified_only:
        wf.run_verified_only(run)
    elif args.diagnosis_only:
        wf.run_diagnosis_only(run)
    else:
        wf.run(run)


if __name__ == "__main__":
    main()
