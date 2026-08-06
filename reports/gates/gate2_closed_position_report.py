import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from reports.common.caster_config import CasterConfig, caster_label, resolve_enabled_casters
from reports.common.config_loader import load_runtime_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

Point = tuple[float, float]
TextSample = tuple[datetime, Path]

COMMON_ROI_SOURCE_SIZES = (
    (1440, 1080),
    (2620, 1216),
    (1920, 1080),
    (1280, 720),
)

BOOL_TRUE = {"1", "true", "yes", "y", "on"}
BOOL_FALSE = {"0", "false", "no", "n", "off"}
CLOCK_FORMATS = ("%H:%M:%S", "%H:%M")
DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
)
ROI_SOURCE_KEYS = (
    "source_resolution",
    "source_size",
    "coordinate_resolution",
    "roi_source_resolution",
    "roi_coordinate_resolution",
)


@dataclass(frozen=True)
class FrameCoverage:
    timestamp: datetime
    text_path: Path
    coverage_percent: float
    gate2_detection_count: int
    centroid_inside_count: int
    centroid_x_min: float | None = None
    centroid_x_max: float | None = None
    centroid_y_min: float | None = None
    centroid_y_max: float | None = None


@dataclass(frozen=True)
class ShiftSegment:
    start: datetime
    end: datetime
    date_str: str
    shift_letter: str


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _first_dict(section: dict, keys: tuple[str, ...]) -> dict | None:
    for key in keys:
        value = section.get(key)
        if isinstance(value, dict):
            return value
    return None


class Gate2ClosedPositionReport:
    """
    Reports the percentage of detected Gate2 area inside roi_gate2_closed.

    For each sampled YOLO text frame:
      1. keep only gate2 detections,
      2. calculate each detection centroid,
      3. count centroids inside roi_gate2_closed as a diagnostic,
      4. calculate the percentage of detected Gate2 area inside roi_gate2_closed,
      5. use the best detection-inside-ROI percentage for that frame.
    """

    DEFAULT_INTERVAL_SECONDS = 10 * 60
    DEFAULT_THRESHOLD_PERCENT = 80.0
    DEFAULT_ROI_NAME = "roi_gate2_closed"
    DEFAULT_GATE2_CLASS_ID = 3
    EPSILON = 1e-9

    def __init__(
        self,
        config_path: str | Path | None = None,
        cfg: dict | None = None,
        caster: CasterConfig | None = None,
    ):
        self.root = PROJECT_ROOT
        if cfg is not None:
            self.cfg = cfg
        elif config_path:
            self.cfg = self._load_yaml(Path(config_path))
        else:
            self.cfg = load_runtime_config()
        self.caster = caster
        self.report_cfg = _as_dict(self.cfg.get("gate2_closed_position_report"))

        self.interval_seconds = self._configured_interval_seconds()
        self.threshold_percent = self._configured_threshold_percent()
        self.alert_on_no_samples = self._configured_alert_on_no_samples()
        self.roi_name = str(self.report_cfg.get("roi_name") or self.DEFAULT_ROI_NAME)
        self.roi_source_size = self._configured_roi_source_size()
        self.gate2_class_id = int(self.report_cfg.get("gate2_class_id", self.DEFAULT_GATE2_CLASS_ID))

        history_cfg = _as_dict(self.cfg.get("history"))
        image_root = history_cfg.get("image_root")
        if not image_root:
            raise ValueError("history.image_root is required in runtime.yaml")
        self.history_root = self._resolve_path(image_root)

        self.raw_rois = self._load_rois(self._resolve_roi_path())
        if self.roi_name not in self.raw_rois:
            raise ValueError(f"ROI {self.roi_name!r} not found")

        self.rois = self.raw_rois
        self.roi_scale_x = 1.0
        self.roi_scale_y = 1.0
        self.roi_points = self.raw_rois[self.roi_name]
        self.roi_area = self._polygon_area(self.roi_points)
        if self.roi_area <= 0:
            raise ValueError(f"{self.roi_name} has zero area")

        shifts_cfg = history_cfg.get("shifts", [])
        if not shifts_cfg:
            raise ValueError("history.shifts is required in runtime.yaml")
        self.shifts = {str(s["name"]).lower(): (s["start"], s["end"]) for s in shifts_cfg}

    @staticmethod
    def _load_yaml(path: Path) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}

    def _resolve_path(self, value: str | Path) -> Path:
        path = Path(str(value)).expanduser()
        return path if path.is_absolute() else (self.root / path).resolve()

    def _resolve_roi_path(self) -> Path:
        rois_cfg = self.cfg.get("rois") or {}
        path_value = rois_cfg.get("path") if isinstance(rois_cfg, dict) else rois_cfg
        if not path_value:
            raise ValueError("rois.path is required in runtime.yaml")
        return self._resolve_path(path_value)

    def _load_rois(self, path: Path) -> dict[str, list[Point]]:
        if not path.exists():
            raise FileNotFoundError(f"ROI config not found: {path}")

        data = self._load_yaml(path)
        roi_data = data.get("rois", data) if isinstance(data, dict) else {}
        rois: dict[str, list[Point]] = {}
        for name, raw_points in roi_data.items():
            points = self._normalize_points(raw_points)
            if len(points) >= 3:
                rois[str(name)] = points

        if not rois:
            raise ValueError(f"No valid ROIs found in {path}")
        return rois

    @staticmethod
    def _normalize_points(points) -> list[Point]:
        normalized: list[Point] = []
        for point in points or []:
            if isinstance(point, dict):
                x, y = point.get("x"), point.get("y")
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                x, y = point[:2]
            else:
                continue
            try:
                normalized.append((float(x), float(y)))
            except (TypeError, ValueError):
                continue
        return normalized

    @staticmethod
    def _parse_duration_seconds(value, *, default_seconds: int, name: str) -> int:
        if value is None:
            return default_seconds

        if isinstance(value, (int, float)):
            seconds = int(value)
        else:
            text = str(value).strip()
            if not text:
                return default_seconds
            if ":" in text:
                pieces = [int(piece) for piece in text.split(":")]
                if len(pieces) == 2:
                    seconds = pieces[0] * 60 + pieces[1]
                elif len(pieces) == 3:
                    seconds = pieces[0] * 3600 + pieces[1] * 60 + pieces[2]
                else:
                    raise ValueError(f"{name} must be seconds, MM:SS, or HH:MM:SS")
            else:
                seconds = int(float(text))

        if seconds <= 0:
            raise ValueError(f"{name} must be greater than 0")
        return seconds

    def _seconds_from_config(
        self,
        *,
        seconds_key: str,
        minutes_key: str,
        default_seconds: int,
        seconds_name: str,
        minutes_name: str,
    ) -> int:
        if self.report_cfg.get(seconds_key) is not None:
            return self._parse_duration_seconds(
                self.report_cfg.get(seconds_key),
                default_seconds=default_seconds,
                name=seconds_name,
            )

        minutes = self.report_cfg.get(minutes_key)
        if minutes is None:
            return default_seconds
        minutes = float(str(minutes).strip())
        if minutes <= 0:
            raise ValueError(f"{minutes_name} must be greater than 0")
        return int(minutes * 60)

    def _configured_interval_seconds(self) -> int:
        return self._seconds_from_config(
            seconds_key="interval_seconds",
            minutes_key="interval_minutes",
            default_seconds=self.DEFAULT_INTERVAL_SECONDS,
            seconds_name="gate2_closed_position_report.interval_seconds",
            minutes_name="gate2_closed_position_report.interval_minutes",
        )

    def _configured_recent_window_seconds(self) -> int:
        return self._seconds_from_config(
            seconds_key="recent_window_seconds",
            minutes_key="recent_window_minutes",
            default_seconds=self.interval_seconds,
            seconds_name="gate2_closed_position_report.recent_window_seconds",
            minutes_name="gate2_closed_position_report.recent_window_minutes",
        )

    def _configured_threshold_percent(self) -> float:
        # Backward-compatible config name; this threshold applies to the average
        # percentage of detected Gate2 area inside roi_gate2_closed.
        threshold = float(self.report_cfg.get("min_avg_coverage_percent", self.DEFAULT_THRESHOLD_PERCENT))
        if threshold < 0 or threshold > 100:
            raise ValueError("gate2_closed_position_report.min_avg_coverage_percent must be between 0 and 100")
        return threshold

    def _configured_alert_on_no_samples(self) -> bool:
        return self._parse_bool(
            self.report_cfg.get("alert_on_no_samples", True),
            name="gate2_closed_position_report.alert_on_no_samples",
        )

    @staticmethod
    def _parse_bool(value, *, name: str) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in BOOL_TRUE:
            return True
        if text in BOOL_FALSE:
            return False
        raise ValueError(f"{name} must be true or false")

    @staticmethod
    def _parse_with_formats(value: str, formats: tuple[str, ...], name: str) -> datetime:
        text = str(value).strip()
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{name} has an invalid datetime format") from exc

    @staticmethod
    def _parse_clock_time(value: str, name: str):
        for fmt in CLOCK_FORMATS:
            try:
                return datetime.strptime(str(value).strip(), fmt).time()
            except ValueError:
                continue
        raise ValueError(f"{name} must be in HH:MM or HH:MM:SS format")

    @staticmethod
    def _parse_datetime(value: str, name: str) -> datetime:
        try:
            return Gate2ClosedPositionReport._parse_with_formats(value, DATETIME_FORMATS, name)
        except ValueError as exc:
            raise ValueError(
                f"{name} must be in YYYY-MM-DD HH:MM[:SS], DD-MM-YYYY HH:MM[:SS], or ISO format"
            ) from exc

    @staticmethod
    def _normalize_shift_letter(shift: str) -> str:
        value = str(shift).strip()
        if value.lower().startswith("shift_"):
            value = value.split("_", 1)[1]
        value = value.upper()
        if value not in {"A", "B", "C"}:
            raise ValueError("Invalid shift. Use A, B, C, Shift_A, Shift_B, or Shift_C")
        return value

    @staticmethod
    def _shift_span(date_str: str, start_s: str, end_s: str) -> tuple[datetime, datetime]:
        start = datetime.strptime(f"{date_str} {start_s}", "%d-%m-%Y %H:%M")
        end = datetime.strptime(f"{date_str} {end_s}", "%d-%m-%Y %H:%M")
        return start, end + timedelta(days=1) if end <= start else end

    def _shift_window(self, date_str: str, shift: str) -> tuple[datetime, datetime, str]:
        shift_letter = self._normalize_shift_letter(shift)
        shift_key = f"shift_{shift_letter.lower()}"
        if shift_key not in self.shifts:
            raise ValueError(f"Shift {shift_letter} is not configured in runtime.yaml")
        start, end = self._shift_span(date_str, *self.shifts[shift_key])
        return start, end, shift_letter

    def _shift_segments_for_window(self, start: datetime, end: datetime) -> list[ShiftSegment]:
        if end <= start:
            raise ValueError("Recent window end must be after start")

        segments: list[ShiftSegment] = []
        day = (start - timedelta(days=1)).date()
        while day <= end.date():
            date_str = day.strftime("%d-%m-%Y")
            for shift_key, (start_s, end_s) in self.shifts.items():
                shift_start, shift_end = self._shift_span(date_str, start_s, end_s)
                segment_start, segment_end = max(start, shift_start), min(end, shift_end)
                if segment_start < segment_end:
                    segments.append(ShiftSegment(
                        segment_start,
                        segment_end,
                        date_str,
                        self._normalize_shift_letter(shift_key),
                    ))
            day += timedelta(days=1)

        return sorted(segments, key=lambda segment: (segment.start, segment.end, segment.shift_letter))

    def _resolve_custom_window(
        self,
        date_str: str,
        start_time: str,
        stop_time: str,
        shift_start: datetime,
        shift_end: datetime,
    ) -> tuple[datetime, datetime]:
        start = datetime.combine(shift_start.date(), self._parse_clock_time(start_time, "--start"))
        if start < shift_start:
            start += timedelta(days=1)

        stop = datetime.combine(shift_start.date(), self._parse_clock_time(stop_time, "--stop"))
        if stop <= start:
            stop += timedelta(days=1)

        if start < shift_start or stop > shift_end:
            raise ValueError(
                f"Window {date_str} {start_time}->{stop_time} is outside "
                f"shift window {shift_start:%Y-%m-%d %H:%M:%S}->{shift_end:%Y-%m-%d %H:%M:%S}"
            )
        return start, stop

    @staticmethod
    def _date_range(start: datetime, end: datetime):
        day = start.date()
        while day <= end.date():
            yield day
            day += timedelta(days=1)

    @staticmethod
    def _timestamp_from_path(path: Path) -> datetime | None:
        try:
            dd, mm, yyyy, hh, minute, ss, *rest = path.stem.split("_")[-1].split("-")
            microsecond = int((rest[0] if rest else "0")[:3].ljust(3, "0")) * 1000
            return datetime(int(yyyy), int(mm), int(dd), int(hh), int(minute), int(ss), microsecond)
        except (TypeError, ValueError):
            return None

    def _collect_text_files(self, start: datetime, end: datetime, shift_letter: str) -> list[TextSample]:
        files: list[TextSample] = []
        for day in self._date_range(start, end):
            text_dir = self.history_root / day.strftime("%Y_%m_%d") / f"Shift_{shift_letter}_text"
            if not text_dir.exists():
                continue
            for path in text_dir.glob("*.txt"):
                timestamp = self._timestamp_from_path(path)
                if timestamp and start <= timestamp < end:
                    files.append((timestamp, path))
        return sorted(files, key=lambda item: (item[0], str(item[1])))

    def _collect_text_files_for_segments(self, segments: list[ShiftSegment]) -> list[TextSample]:
        files: list[TextSample] = []
        seen: set[Path] = set()
        for segment in segments:
            for sample in self._collect_text_files(segment.start, segment.end, segment.shift_letter):
                if sample[1] not in seen:
                    seen.add(sample[1])
                    files.append(sample)
        return sorted(files, key=lambda item: (item[0], str(item[1])))

    @staticmethod
    def _shift_label_for_segments(segments: list[ShiftSegment]) -> str:
        letters = list(dict.fromkeys(segment.shift_letter for segment in segments))
        return "+".join(f"Shift_{letter}" for letter in letters) if letters else "Unknown"

    @staticmethod
    def _date_label_for_segments(segments: list[ShiftSegment], start: datetime, end: datetime) -> str:
        dates = list(dict.fromkeys(segment.date_str for segment in segments))
        if not dates:
            return start.strftime("%d-%m-%Y")
        if len(dates) == 1 and start.date() == (end - timedelta(microseconds=1)).date():
            return dates[0]
        return f"{start:%d-%m-%Y} to {end:%d-%m-%Y}"

    @staticmethod
    def _image_path_for_text(text_path: Path) -> Path | None:
        image_dir = text_path.parent.parent / text_path.parent.name.replace("_text", "_img")
        for extension in (".jpeg", ".jpg", ".png"):
            candidate = image_dir / f"{text_path.stem}{extension}"
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _parse_size_config(
        source_cfg,
        *,
        positive_error: str,
        axis_positive_prefix: str | None = None,
        require_both: bool = False,
    ) -> tuple[int, int] | None:
        if not isinstance(source_cfg, dict):
            return None

        width, height = source_cfg.get("width"), source_cfg.get("height")
        width_empty, height_empty = width in (None, "auto"), height in (None, "auto")
        if width_empty or height_empty:
            if require_both and width_empty != height_empty:
                raise ValueError("ROI source resolution must include both width and height")
            return None

        values = []
        for axis, value in (("width", width), ("height", height)):
            value = int(value)
            if value <= 0:
                if axis_positive_prefix:
                    raise ValueError(f"{axis_positive_prefix}.{axis} must be positive")
                raise ValueError(f"{positive_error} must be positive")
            values.append(value)
        return values[0], values[1]

    def _configured_source_size(self) -> tuple[int, int] | None:
        return self._parse_size_config(
            _first_dict(self.report_cfg, ("source_resolution", "source_size")),
            positive_error="gate2_closed_position_report.source_resolution",
        )

    def _configured_roi_source_size(self) -> tuple[int, int] | None:
        source_cfg = _first_dict(self.report_cfg, ("roi_source_resolution", "roi_coordinate_resolution"))
        if source_cfg is None:
            source_cfg = self._find_roi_source_size_cfg(_as_dict(getattr(self, "cfg", {}).get("rois")))
        return self._parse_size_config(
            source_cfg,
            positive_error="gate2_closed_position_report.roi_source_resolution",
            axis_positive_prefix="gate2_closed_position_report.roi_source_resolution",
            require_both=True,
        )

    @staticmethod
    def _find_roi_source_size_cfg(section):
        if not isinstance(section, dict):
            return None
        found = _first_dict(section, ROI_SOURCE_KEYS)
        if found is not None:
            return found
        for nested_key in ("rois", "overlay_rois"):
            found = Gate2ClosedPositionReport._find_roi_source_size_cfg(section.get(nested_key))
            if found is not None:
                return found
        return None

    def _prepare_rois_for_source_size(self, frame_width: int, frame_height: int):
        roi_source_width, roi_source_height = self._resolve_roi_source_size(
            self.raw_rois[self.roi_name],
            frame_width,
            frame_height,
        )
        self.roi_scale_x = frame_width / float(roi_source_width)
        self.roi_scale_y = frame_height / float(roi_source_height)
        self.rois = {
            name: self._scale_roi_points(points, frame_width, frame_height, roi_source_width, roi_source_height)
            for name, points in self.raw_rois.items()
        }
        self.roi_points = self.rois[self.roi_name]
        self.roi_area = self._polygon_area(self.roi_points)
        if self.roi_area <= 0:
            raise ValueError(f"{self.roi_name} has zero area after ROI scaling")

        if abs(self.roi_scale_x - 1.0) > self.EPSILON or abs(self.roi_scale_y - 1.0) > self.EPSILON:
            logger.info(
                "Scaled gate2 ROI coordinates | roi_source=%sx%s | frame=%sx%s | scale_x=%.4f | scale_y=%.4f",
                roi_source_width,
                roi_source_height,
                frame_width,
                frame_height,
                self.roi_scale_x,
                self.roi_scale_y,
            )

    def _resolve_roi_source_size(self, target_points: list[Point], frame_width: int, frame_height: int) -> tuple[int, int]:
        configured = getattr(self, "roi_source_size", None) or self._configured_roi_source_size()
        if configured:
            return configured

        inferred = self._infer_roi_source_size_from_points(target_points, frame_width, frame_height)
        if inferred != (frame_width, frame_height):
            logger.warning(
                "rois.source_resolution is not configured; inferred gate2 ROI source size %sx%s from %s and frame %sx%s",
                inferred[0],
                inferred[1],
                self.roi_name,
                frame_width,
                frame_height,
            )
            return inferred

        logger.warning(
            "rois.source_resolution is not configured; %s coordinates fit frame size %sx%s, so no ROI scaling is applied",
            self.roi_name,
            frame_width,
            frame_height,
        )
        return inferred

    def _infer_roi_source_size_from_points(self, points: list[Point], frame_width: int, frame_height: int) -> tuple[int, int]:
        max_x = max((x for x, _y in points), default=0.0)
        max_y = max((y for _x, y in points), default=0.0)
        if max_x <= frame_width and max_y <= frame_height:
            return frame_width, frame_height

        for width, height in COMMON_ROI_SOURCE_SIZES:
            if max_x <= width + 2 and max_y <= height + 2 and (width, height) != (frame_width, frame_height):
                return width, height

        return (
            self._infer_roi_axis_source_size(max_x, frame_width),
            self._infer_roi_axis_source_size(max_y, frame_height),
        )

    @staticmethod
    def _infer_roi_axis_source_size(max_coordinate: float, frame_size: int) -> int:
        if frame_size <= 0 or max_coordinate <= frame_size:
            return frame_size
        doubled = frame_size * 2
        return doubled if max_coordinate <= doubled + 2 else int(round(max_coordinate)) + 1

    @staticmethod
    def _scale_roi_points(
        points: list[Point],
        frame_width: int,
        frame_height: int,
        roi_source_width: int,
        roi_source_height: int,
    ) -> list[Point]:
        if roi_source_width <= 0 or roi_source_height <= 0:
            return []

        scale_x = frame_width / float(roi_source_width)
        scale_y = frame_height / float(roi_source_height)
        max_x, max_y = max(frame_width - 1, 0), max(frame_height - 1, 0)
        return [
            (
                float(min(max(int(round(x * scale_x)), 0), max_x)),
                float(min(max(int(round(y * scale_y)), 0), max_y)),
            )
            for x, y in points
        ]

    def _source_size_from_images(self, text_files: list[TextSample]) -> tuple[int, int] | None:
        for _timestamp, text_path in text_files:
            image_path = self._image_path_for_text(text_path)
            if image_path is None:
                continue
            try:
                import cv2

                image = cv2.imread(str(image_path))
            except Exception as exc:
                logger.warning("Unable to read image size with OpenCV: %s", exc)
                break
            if image is not None:
                height, width = image.shape[:2]
                if width > 0 and height > 0:
                    return int(width), int(height)
        return None

    def _resolve_source_size(self, text_files: list[TextSample]) -> tuple[int, int]:
        image_size = self._source_size_from_images(text_files)
        if image_size:
            return image_size

        configured_size = self._configured_source_size()
        if configured_size:
            return configured_size

        points = [point for roi_points in getattr(self, "rois", {}).values() for point in roi_points] or self.roi_points
        inferred = int(round(max(x for x, _y in points))) + 1, int(round(max(y for _x, y in points))) + 1
        logger.warning(
            "Falling back to all-ROI-derived source size %sx%s. Configure "
            "gate2_closed_position_report.source_resolution for better accuracy.",
            inferred[0],
            inferred[1],
        )
        return inferred

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return min(max(value, lower), upper)

    def _yolo_bbox_to_polygon(
        self,
        xc: float,
        yc: float,
        bw: float,
        bh: float,
        frame_width: int,
        frame_height: int,
    ) -> list[Point] | None:
        x1 = self._clamp((xc - bw / 2.0) * frame_width, 0.0, float(frame_width))
        y1 = self._clamp((yc - bh / 2.0) * frame_height, 0.0, float(frame_height))
        x2 = self._clamp((xc + bw / 2.0) * frame_width, 0.0, float(frame_width))
        y2 = self._clamp((yc + bh / 2.0) * frame_height, 0.0, float(frame_height))
        return None if x2 <= x1 or y2 <= y1 else [(x1, y1), (x2, y1), (x2, y2), (x1, y2)]

    @staticmethod
    def _yolo_centroid(xc: float, yc: float, frame_width: int, frame_height: int) -> Point:
        return xc * frame_width, yc * frame_height

    @staticmethod
    def _parse_yolo_line(line: str) -> tuple[int, float, float, float, float] | None:
        parts = line.strip().split()
        if len(parts) < 5:
            return None
        try:
            class_id = int(float(parts[0]))
            xc, yc, bw, bh = [float(value) for value in parts[1:5]]
        except ValueError:
            return None
        return class_id, xc, yc, bw, bh

    @staticmethod
    def _centroid_bounds(centroids: list[Point]) -> tuple[float | None, float | None, float | None, float | None]:
        if not centroids:
            return None, None, None, None
        xs, ys = zip(*centroids)
        return min(xs), max(xs), min(ys), max(ys)

    def _measure_frame(self, timestamp: datetime, text_path: Path, frame_width: int, frame_height: int) -> FrameCoverage:
        best_coverage = 0.0
        gate2_count = 0
        centroid_inside_count = 0
        centroids: list[Point] = []

        with open(text_path, "r") as f:
            for line in f:
                parsed = self._parse_yolo_line(line)
                if parsed is None:
                    continue

                class_id, xc, yc, bw, bh = parsed
                if class_id != self.gate2_class_id:
                    continue

                polygon = self._yolo_bbox_to_polygon(xc, yc, bw, bh, frame_width, frame_height)
                if polygon is None:
                    continue

                gate2_count += 1
                detection_area = self._polygon_area(polygon)
                if detection_area <= self.EPSILON:
                    continue

                centroid = self._yolo_centroid(xc, yc, frame_width, frame_height)
                centroids.append(centroid)
                if self._point_in_polygon(centroid, self.roi_points):
                    centroid_inside_count += 1

                intersection = self._convex_intersection(polygon, self.roi_points)
                if intersection:
                    intersection_area = self._polygon_area(intersection)
                    detection_inside_roi_percent = min((intersection_area / detection_area) * 100.0, 100.0)
                    best_coverage = max(best_coverage, detection_inside_roi_percent)

        x_min, x_max, y_min, y_max = self._centroid_bounds(centroids)
        return FrameCoverage(
            timestamp=timestamp,
            text_path=text_path,
            coverage_percent=best_coverage,
            gate2_detection_count=gate2_count,
            centroid_inside_count=centroid_inside_count,
            centroid_x_min=x_min,
            centroid_x_max=x_max,
            centroid_y_min=y_min,
            centroid_y_max=y_max,
        )

    @staticmethod
    def _none_aware_min(left, right):
        return right if left is None else left if right is None else min(left, right)

    @staticmethod
    def _none_aware_max(left, right):
        return right if left is None else left if right is None else max(left, right)

    @staticmethod
    def _round_optional(value):
        return "" if value is None else round(float(value), 2)

    def _new_interval(self, start: datetime, end: datetime) -> dict:
        return {
            "interval_start": start,
            "interval_end": end,
            "sample_count": 0,
            "gate2_detection_count": 0,
            "centroid_inside_count": 0,
            "centroid_x_min": None,
            "centroid_x_max": None,
            "centroid_y_min": None,
            "centroid_y_max": None,
            "coverage_sum": 0.0,
        }

    def _intervals(self, start: datetime, end: datetime) -> list[dict]:
        count = int((end - start).total_seconds() // self.interval_seconds)
        if start + timedelta(seconds=count * self.interval_seconds) < end:
            count += 1
        return [
            self._new_interval(
                start + timedelta(seconds=index * self.interval_seconds),
                min(start + timedelta(seconds=(index + 1) * self.interval_seconds), end),
            )
            for index in range(count)
        ]

    def _add_frame_to_interval(self, interval: dict, frame: FrameCoverage):
        interval["sample_count"] += 1
        interval["gate2_detection_count"] += frame.gate2_detection_count
        interval["centroid_inside_count"] += frame.centroid_inside_count
        interval["coverage_sum"] += frame.coverage_percent
        interval["centroid_x_min"] = self._none_aware_min(interval["centroid_x_min"], frame.centroid_x_min)
        interval["centroid_x_max"] = self._none_aware_max(interval["centroid_x_max"], frame.centroid_x_max)
        interval["centroid_y_min"] = self._none_aware_min(interval["centroid_y_min"], frame.centroid_y_min)
        interval["centroid_y_max"] = self._none_aware_max(interval["centroid_y_max"], frame.centroid_y_max)

    def _interval_row(self, interval: dict) -> dict:
        sample_count = interval["sample_count"]
        avg_detection_inside_roi = interval["coverage_sum"] / sample_count if sample_count else 0.0
        if sample_count == 0:
            status, alert = "NO_SAMPLES", bool(getattr(self, "alert_on_no_samples", True))
        elif avg_detection_inside_roi >= self.threshold_percent:
            status, alert = "VIEW_UNCHANGED", False
        else:
            status, alert = "POSSIBLE_VIEW_CHANGE", True

        return {
            "interval_start": interval["interval_start"].strftime("%Y-%m-%d %H:%M:%S"),
            "interval_end": interval["interval_end"].strftime("%Y-%m-%d %H:%M:%S"),
            "sample_count": sample_count,
            "gate2_detection_count": interval["gate2_detection_count"],
            "centroid_inside_count": interval["centroid_inside_count"],
            "centroid_x_min": self._round_optional(interval["centroid_x_min"]),
            "centroid_x_max": self._round_optional(interval["centroid_x_max"]),
            "centroid_y_min": self._round_optional(interval["centroid_y_min"]),
            "centroid_y_max": self._round_optional(interval["centroid_y_max"]),
            "avg_detection_inside_roi_percent": round(avg_detection_inside_roi, 2),
            "avg_coverage_percent": round(avg_detection_inside_roi, 2),
            "threshold_percent": round(self.threshold_percent, 2),
            "status": status,
            "alert": alert,
        }

    def _build_interval_rows(self, start: datetime, end: datetime, frame_coverages: list[FrameCoverage]) -> list[dict]:
        intervals = self._intervals(start, end)
        for frame in frame_coverages:
            if not (start <= frame.timestamp < end):
                continue
            index = int((frame.timestamp - start).total_seconds() // self.interval_seconds)
            if 0 <= index < len(intervals):
                self._add_frame_to_interval(intervals[index], frame)
        return [self._interval_row(interval) for interval in intervals]

    def _add_geometry_columns(self, rows: list[dict], frame_width: int, frame_height: int) -> list[dict]:
        roi_x_values = [x for x, _y in self.roi_points]
        roi_y_values = [y for _x, y in self.roi_points]
        geometry = {
            "source_width": int(frame_width),
            "source_height": int(frame_height),
            "roi_scale_x": round(float(self.roi_scale_x), 4),
            "roi_scale_y": round(float(self.roi_scale_y), 4),
            "roi_x_min": self._round_optional(min(roi_x_values)),
            "roi_x_max": self._round_optional(max(roi_x_values)),
            "roi_y_min": self._round_optional(min(roi_y_values)),
            "roi_y_max": self._round_optional(max(roi_y_values)),
        }
        for row in rows:
            row.update(geometry)
        return rows

    @staticmethod
    def _normalize_recipients(value) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        return [str(item).strip() for item in value if str(item).strip()]

    def _alert_recipients(self) -> list[str]:
        recipients = self._normalize_recipients(
            self.report_cfg.get("recipients") or self.report_cfg.get("email_recipients")
        )
        return recipients or self._normalize_recipients(_as_dict(self.cfg.get("email")).get("diagnosis_recipients"))

    def _email_password_skip_reason(self) -> str | None:
        email_cfg = _as_dict(self.cfg.get("email"))
        password_env = email_cfg.get("password_env")
        if password_env:
            return None if os.getenv(password_env) else f"Email password environment variable {password_env} is not set"
        return None if email_cfg.get("password") else "No email.password or email.password_env configured"

    def _caster_number(self) -> str:
        value = (
            self.cfg.get("caster_number")
            or self.cfg.get("Caster number")
            or self.cfg.get("caster number")
        )
        return str(value).strip() if value is not None and str(value).strip() else "N/A"

    @staticmethod
    def _weighted_avg_coverage(rows: list[dict]) -> float:
        total_samples = sum(int(row["sample_count"]) for row in rows)
        if not total_samples:
            return 0.0
        coverage_sum = 0.0
        for row in rows:
            row_average = (
                row["avg_detection_inside_roi_percent"]
                if "avg_detection_inside_roi_percent" in row
                else row["avg_coverage_percent"]
            )
            coverage_sum += float(row_average) * int(row["sample_count"])
        return round(coverage_sum / total_samples, 2)

    def _build_summary(
        self,
        *,
        date_label: str,
        shift_label: str,
        start: datetime,
        end: datetime,
        text_files: list[TextSample],
        rows: list[dict],
        window_mode: str,
    ) -> dict:
        avg_area = self._weighted_avg_coverage(rows)
        alert_count = sum(1 for row in rows if row["alert"])
        threshold_status = "BELOW_THRESHOLD" if alert_count else "WITHIN_LIMIT"
        threshold_value = round(float(self.threshold_percent), 2)
        caster = getattr(self, "caster", None)
        return {
            "date": date_label,
            "shift": shift_label,
            "caster_id": getattr(caster, "id", self.cfg.get("caster_id", "legacy")),
            "caster_number": self._caster_number(),
            "window_mode": window_mode,
            "window_start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "window_end": end.strftime("%Y-%m-%d %H:%M:%S"),
            "interval_seconds": self.interval_seconds,
            "threshold_percent": threshold_value,
            "threshold": threshold_value,
            "total_sample_count": len(text_files),
            "gate2_detection_count": sum(int(row["gate2_detection_count"]) for row in rows),
            "gate2_closed_count": sum(int(row["centroid_inside_count"]) for row in rows),
            "avg_detection_inside_roi_percent": avg_area,
            "avg_area_covered_percent": avg_area,
            "interval_count": len(rows),
            "alert_count": alert_count,
            "status": threshold_status,
            "threshold_status": threshold_status,
        }

    @staticmethod
    def _summary_body(summary: dict) -> str:
        avg_detection_inside_roi = summary.get(
            "avg_detection_inside_roi_percent",
            summary["avg_area_covered_percent"],
        )
        return "\n".join([
            f"Date              : {summary['date']}",
            f"Shift             : {summary['shift']}",
            f"Caster id         : {summary.get('caster_id', 'legacy')}",
            f"Caster number     : {summary['caster_number']}",
            (
                f"Interval seconds  : {summary['interval_seconds']} "
                f"({summary['window_start']} -> {summary['window_end']})"
            ),
            f"Total sample count: {summary['total_sample_count']}",
            f"Gate2 detection count: {summary['gate2_detection_count']}",
            f"Gate2 closed count   : {summary['gate2_closed_count']}",
            f"Avg detection inside ROI: {avg_detection_inside_roi:.2f}",
            f"Threshold            : {float(summary['threshold']):g}",
        ])

    def _send_alert_email(self, summary: dict) -> dict:
        recipients = self._alert_recipients()
        if not recipients:
            return {"sent": False, "skip_reason": "No recipients configured"}

        password_skip_reason = self._email_password_skip_reason()
        if password_skip_reason:
            return {"sent": False, "skip_reason": password_skip_reason}

        subject = (
            f"Gate2 Closed Position Alert - {caster_label(self.caster, self.cfg)} - "
            f"{summary['shift']} - {summary['date']}"
        )
        body = "\n".join([
            "Gate2 closed-position detection-inside-ROI percent is below the configured threshold.",
            "",
            self._summary_body(summary),
        ])

        from reports.common.email_sender import EmailSender

        EmailSender(cfg=self.cfg).send_text(subject, body, recipients=recipients)
        return {"sent": True, "recipients": recipients}

    def _export_window(
        self,
        *,
        date_label: str,
        shift_label: str,
        start: datetime,
        end: datetime,
        text_files: list[TextSample],
        window_mode: str,
        send_email: bool | None,
    ) -> dict:
        frame_width, frame_height = self._resolve_source_size(text_files)
        self._prepare_rois_for_source_size(frame_width, frame_height)

        frame_coverages = [self._measure_frame(ts, path, frame_width, frame_height) for ts, path in text_files]
        rows = self._add_geometry_columns(self._build_interval_rows(start, end, frame_coverages), frame_width, frame_height)
        summary = self._build_summary(
            date_label=date_label,
            shift_label=shift_label,
            start=start,
            end=end,
            text_files=text_files,
            rows=rows,
            window_mode=window_mode,
        )

        should_send_email = bool(self.report_cfg.get("send_email", True)) if send_email is None else send_email
        if summary["alert_count"] and should_send_email:
            try:
                summary["email"] = self._send_alert_email(summary)
            except Exception as exc:
                logger.exception("Gate2 closed-position alert email failed")
                summary["email"] = {"sent": False, "error": str(exc)}
        elif summary["alert_count"]:
            summary["email"] = {"sent": False, "skip_reason": "send_email is disabled"}
        else:
            summary["email"] = {"sent": False, "skip_reason": "No below-threshold alert intervals"}

        logger.info("GATE2 CLOSED POSITION REPORT\n%s", self._summary_body(summary))
        return summary

    def export(
        self,
        date_str: str,
        shift: str,
        *,
        send_email: bool | None = None,
        start_time: str | None = None,
        stop_time: str | None = None,
    ) -> dict:
        shift_start, shift_end, shift_letter = self._shift_window(date_str, shift)
        if bool(start_time) != bool(stop_time):
            raise ValueError("Use --start and --stop together")

        if start_time and stop_time:
            start, end = self._resolve_custom_window(date_str, start_time, stop_time, shift_start, shift_end)
            window_mode = "custom"
        else:
            start, end = shift_start, shift_end
            window_mode = "shift"

        return self._export_window(
            date_label=date_str,
            shift_label=f"Shift_{shift_letter}",
            start=start,
            end=end,
            text_files=self._collect_text_files(start, end, shift_letter),
            window_mode=window_mode,
            send_email=send_email,
        )

    def export_recent(
        self,
        *,
        minutes: float | None = None,
        end_time: datetime | None = None,
        send_email: bool | None = None,
    ) -> dict:
        if minutes is None:
            window_seconds = self._configured_recent_window_seconds()
        else:
            minutes = float(minutes)
            if minutes <= 0:
                raise ValueError("--last-minutes must be greater than 0")
            window_seconds = int(minutes * 60)

        end = end_time or datetime.now()
        start = end - timedelta(seconds=window_seconds)
        segments = self._shift_segments_for_window(start, end)
        if not segments:
            raise ValueError(f"No configured shift covers recent window {start} -> {end}")

        return self._export_window(
            date_label=self._date_label_for_segments(segments, start, end),
            shift_label=self._shift_label_for_segments(segments),
            start=start,
            end=end,
            text_files=self._collect_text_files_for_segments(segments),
            window_mode="recent",
            send_email=send_email,
        )

    @staticmethod
    def _polygon_centroid(points: list[Point]) -> Point:
        return sum(x for x, _y in points) / len(points), sum(y for _x, y in points) / len(points)

    @staticmethod
    def _signed_area(points: list[Point]) -> float:
        if len(points) < 3:
            return 0.0
        return sum(
            x1 * y2 - x2 * y1
            for (x1, y1), (x2, y2) in zip(points, points[1:] + points[:1])
        ) / 2.0

    @classmethod
    def _polygon_area(cls, points: list[Point]) -> float:
        return abs(cls._signed_area(points))

    @staticmethod
    def _cross(a: Point, b: Point, c: Point) -> float:
        return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])

    @classmethod
    def _point_on_segment(cls, point: Point, start: Point, end: Point) -> bool:
        return (
            abs(cls._cross(start, end, point)) <= cls.EPSILON
            and min(start[0], end[0]) - cls.EPSILON <= point[0] <= max(start[0], end[0]) + cls.EPSILON
            and min(start[1], end[1]) - cls.EPSILON <= point[1] <= max(start[1], end[1]) + cls.EPSILON
        )

    @classmethod
    def _point_in_polygon(cls, point: Point, polygon: list[Point]) -> bool:
        x, y = point
        inside = False
        previous = polygon[-1]
        for current in polygon:
            if cls._point_on_segment(point, previous, current):
                return True
            xi, yi = current
            xj, yj = previous
            if (yi > y) != (yj > y):
                x_intersection = (xj - xi) * (y - yi) / (yj - yi) + xi
                if x_intersection >= x - cls.EPSILON:
                    inside = not inside
            previous = current
        return inside

    @classmethod
    def _line_intersection(cls, segment_start: Point, segment_end: Point, clip_start: Point, clip_end: Point) -> Point:
        sx, sy = segment_start
        ex, ey = segment_end
        cx, cy = clip_start
        dx, dy = clip_end
        r = (ex - sx, ey - sy)
        s = (dx - cx, dy - cy)
        denominator = r[0] * s[1] - r[1] * s[0]
        if abs(denominator) <= cls.EPSILON:
            return segment_end
        q_minus_p = (cx - sx, cy - sy)
        t = (q_minus_p[0] * s[1] - q_minus_p[1] * s[0]) / denominator
        return sx + t * r[0], sy + t * r[1]

    @classmethod
    def _convex_intersection(cls, subject: list[Point], clip_polygon: list[Point]) -> list[Point]:
        output = subject[:]
        if len(output) < 3 or len(clip_polygon) < 3:
            return []

        orientation = 1.0 if cls._signed_area(clip_polygon) >= 0 else -1.0

        def is_inside(point, edge_start, edge_end):
            return cls._cross(edge_start, edge_end, point) * orientation >= -cls.EPSILON

        for index, clip_start in enumerate(clip_polygon):
            clip_end = clip_polygon[(index + 1) % len(clip_polygon)]
            input_polygon, output = output, []
            if not input_polygon:
                break

            segment_start = input_polygon[-1]
            for segment_end in input_polygon:
                end_inside = is_inside(segment_end, clip_start, clip_end)
                start_inside = is_inside(segment_start, clip_start, clip_end)
                if end_inside:
                    if not start_inside:
                        output.append(cls._line_intersection(segment_start, segment_end, clip_start, clip_end))
                    output.append(segment_end)
                elif start_inside:
                    output.append(cls._line_intersection(segment_start, segment_end, clip_start, clip_end))
                segment_start = segment_end

        return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate the gate2 closed-position coverage report")
    parser.add_argument("--date", help="DD-MM-YYYY")
    parser.add_argument("--shift", help="A/B/C or Shift_A/Shift_B/Shift_C")
    parser.add_argument("--caster", help="Single caster id, for example caster1")
    parser.add_argument("--casters", help="Comma-separated caster ids, for example caster1,caster2")
    parser.add_argument("--all-casters", action="store_true", help="Run all enabled casters from runtime.yaml")
    parser.add_argument("--start", help="Optional window start time, HH:MM or HH:MM:SS")
    parser.add_argument("--stop", help="Optional window stop time, HH:MM or HH:MM:SS")
    parser.add_argument(
        "--last-minutes",
        type=float,
        help="Recent-window size in minutes. Defaults to gate2_closed_position_report.recent_window_minutes or interval_minutes.",
    )
    parser.add_argument(
        "--end-at",
        help="Testing/backfill only: end time for recent mode, e.g. '2026-06-30 15:40:00'",
    )
    parser.add_argument("--no-email", action="store_true", help="Generate the report without sending alert email")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    selected_ids = []
    if args.caster:
        selected_ids.append(args.caster)
    if args.casters:
        selected_ids.extend(part.strip() for part in args.casters.split(",") if part.strip())
    if args.all_casters:
        selected_ids = []

    base_cfg = load_runtime_config()
    casters = resolve_enabled_casters(base_cfg, selected_ids or None)
    manual_mode = any((args.date, args.shift, args.start, args.stop))
    summaries = []

    for caster in casters:
        report = Gate2ClosedPositionReport(cfg=caster.cfg, caster=caster)
        if manual_mode:
            if args.last_minutes is not None or args.end_at:
                parser.error("--last-minutes/--end-at cannot be combined with --date/--shift/--start/--stop")
            if not args.date or not args.shift:
                parser.error("Manual mode requires both --date and --shift")
            summary = report.export(
                args.date,
                args.shift,
                send_email=not args.no_email,
                start_time=args.start,
                stop_time=args.stop,
            )
        else:
            summary = report.export_recent(
                minutes=args.last_minutes,
                end_time=report._parse_datetime(args.end_at, "--end-at") if args.end_at else None,
                send_email=not args.no_email,
            )
        summaries.append(summary)

    logger.info("Gate2 closed-position summaries: %s", summaries)


if __name__ == "__main__":
    main()
