import cv2
import glob
import logging
import numpy as np
import time
import yaml
from pathlib import Path
from datetime import datetime, timedelta

from reports.common.config_loader import load_runtime_config
from reports.common.caster_config import resolve_enabled_casters


logger = logging.getLogger(__name__)

DEFAULT_SHIFT_TIMES = {
    "A": ("06:00", "14:00"),
    "B": ("14:00", "22:00"),
    "C": ("22:00", "06:00"),
}

CLASS_ID_TO_NAME = {
    0: "pipe",
    1: "trolley",
    2: "gate1",
    3: "gate2",
    4: "human",
}

CLASS_COLORS = {
    0: (0, 255, 0),
    1: (255, 0, 0),
    2: (0, 255, 255),
    3: (0, 165, 255),
    4: (255, 0, 0),
}

ROI_COLORS = {
    "roi_loadcell": (0, 255, 255),
    "roi_caster_origin": (255, 255, 0),
    "roi_pipe_checkpoint": (0, 200, 255),
    "roi_left_origin": (0, 165, 255),
    "roi_right_origin": (255, 0, 255),
    "roi_gate1_closed": (255, 0, 0),
    "roi_gate1_open": (0, 255, 0),
    "roi_gate2_closed": (128, 0, 0),
    "roi_gate2_open": (0, 128, 0),
    "roi_safety_critical": (0, 0, 255),
}
ROI_FALLBACK_COLORS = (
    (0, 255, 255),
    (255, 0, 255),
    (0, 165, 255),
    (255, 255, 0),
    (0, 255, 0),
)
ROI_FILL_ALPHA = 0.20
ROI_LINE_THICKNESS = 2
ROI_LABEL_FONT_SCALE = 0.6
ROI_LABEL_THICKNESS = 2
COMMON_ROI_SOURCE_SIZES = (
    (1440, 1080),
    (2620, 1216),
    (1920, 1080),
    (1280, 720),
)


class ShiftVideoOverlayGenerator:
    """
    Generates overlay videos from history frames and YOLO text files.

    The workflow uses `windows` to create a compact compilation containing only
    the loadcell-missing time ranges. The CLI-compatible start/stop path remains
    available for one continuous window.
    """

    _ROI_CACHE: dict[Path, list[dict]] = {}

    def __init__(
        self,
        date_str: str,
        shift: str,
        start: str | None = None,
        stop: str | None = None,
        windows: list[dict] | None = None,
        output_name: str | None = None,
        normal_output_name: str | None = None,
        cfg: dict | None = None,
        caster=None,
    ):
        self.date_str = date_str
        self.shift = shift.upper()
        self.date_obj = datetime.strptime(date_str, "%d-%m-%Y")

        self.root = Path(__file__).resolve().parents[2]
        cfg = cfg or load_runtime_config()
        if (
            caster is None
            and not (cfg.get("history") or {}).get("image_root")
            and isinstance(cfg.get("casters"), dict)
        ):
            caster = resolve_enabled_casters(cfg)[0]
            cfg = caster.cfg
        self.caster = caster
        self.caster_file_token = getattr(caster, "file_token", None)

        self.video_cfg = cfg["video"]
        self.shift_times = self._load_shift_times(cfg)
        self.shift_start, self.shift_end = self._shift_window()

        self.image_root = (self.root / cfg["history"]["image_root"]).resolve()
        self.text_root = self.image_root
        self.res_cfg = self.video_cfg.get("resolution", {})
        self.roi_source_size = self._configured_roi_source_size(cfg)
        self.input_images_have_overlay = self._input_images_have_overlay(cfg)
        self.rois = [] if self.input_images_have_overlay else self._load_rois_once(self._resolve_roi_path(cfg))

        output_dir = self.video_cfg.get("overlay_output_dir", "outputs/videos-overlay")
        self.output_dir = self.root / output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.windows = self._resolve_windows(start, stop, windows)

        if output_name:
            self.output_path = self.output_dir / output_name
        else:
            caster_part = f"_{self.caster_file_token}" if self.caster_file_token else ""
            self.output_path = (
                self.output_dir /
                f"{date_str}{caster_part}_shift_{self.shift.lower()}{self._window_suffix()}_overlay.mp4"
            )

        self.normal_output_path = None
        if normal_output_name:
            normal_output_dir = self.root / self.video_cfg.get("output_dir", "outputs/videos")
            normal_output_dir.mkdir(parents=True, exist_ok=True)
            self.normal_output_path = normal_output_dir / normal_output_name

    def generate(self) -> str:
        frames = self._collect_frames()
        if not frames:
            ranges = ", ".join(
                f"{self._format_dt(w['start'])} -> {self._format_dt(w['end'])}"
                for w in self.windows
            )
            raise RuntimeError(
                f"No images found for shift {self.shift} in overlay window(s): {ranges}"
            )

        logger.info(
            "Generating overlay video | shift=%s | windows=%s | images=%s | range=%s -> %s",
            self.shift,
            len(self.windows),
            len(frames),
            self._timestamp_from_name(frames[0]["path"]),
            self._timestamp_from_name(frames[-1]["path"]),
        )

        first = cv2.imread(frames[0]["path"])
        if first is None:
            raise RuntimeError("Failed to read first image")

        source_h, source_w = first.shape[:2]
        w, h = self._resolve_resolution(source_w, source_h)
        rois = [] if self.input_images_have_overlay else self._scale_rois(w, h, source_w, source_h)

        writer = cv2.VideoWriter(
            str(self.output_path),
            cv2.VideoWriter_fourcc(*self.video_cfg.get("codec", "mp4v")),
            self.video_cfg.get("fps", 10),
            (w, h),
        )
        if not writer.isOpened():
            raise RuntimeError(f"Failed to open overlay video writer: {self.output_path}")

        normal_writer = None
        if self.normal_output_path:
            normal_writer = cv2.VideoWriter(
                str(self.normal_output_path),
                cv2.VideoWriter_fourcc(*self.video_cfg.get("codec", "mp4v")),
                self.video_cfg.get("fps", 10),
                (w, h),
            )
            if not normal_writer.isOpened():
                writer.release()
                raise RuntimeError(f"Failed to open normal video writer: {self.normal_output_path}")

        start_time = time.time()
        written = 0

        for frame_info in frames:
            img_path = frame_info["path"]
            frame = cv2.imread(img_path)
            if frame is None:
                logger.warning("Skipping unreadable overlay frame: %s", img_path)
                continue

            frame_source_h, frame_source_w = frame.shape[:2]
            if (frame_source_w, frame_source_h) != (w, h):
                frame = cv2.resize(frame, (w, h))

            if normal_writer:
                normal_frame = frame.copy()
                if not self.input_images_have_overlay:
                    self._draw_timestamp(normal_frame, img_path, h)
                normal_writer.write(normal_frame)

            if not self.input_images_have_overlay:
                if rois:
                    self._draw_rois(frame, rois)

                base = Path(img_path).stem
                txt_path = self._get_txt_path(base)
                if txt_path.exists():
                    self._draw_yolo(frame, txt_path, w, h)

                self._draw_timestamp(frame, img_path, h)
                self._draw_window_label(frame, frame_info, w)

            writer.write(frame)
            written += 1

        writer.release()
        if normal_writer:
            normal_writer.release()

        if written == 0:
            raise RuntimeError("No readable frames were written to overlay video")

        duration = int(time.time() - start_time)
        logger.info(
            "Overlay video created | path=%s | frames=%s | duration=%ss",
            self.output_path,
            written,
            duration,
        )
        return str(self.output_path)

    def _resolve_windows(self, start, stop, windows):
        if windows:
            normalized = []
            for idx, window in enumerate(windows, start=1):
                start_at = window["start"]
                end_at = window["end"]
                if not isinstance(start_at, datetime) or not isinstance(end_at, datetime):
                    raise TypeError("Overlay windows must use datetime start/end values")
                if end_at <= start_at:
                    continue

                label = window.get("label") or f"Window {idx}"
                normalized.append({
                    "start": start_at,
                    "end": end_at,
                    "label": label,
                })

            if not normalized:
                raise ValueError("No valid overlay windows were provided")
            return self._merge_windows(normalized)

        if bool(start) != bool(stop):
            raise ValueError("Use --start and --stop together, both in HH:MM or HH:MM:SS format")

        if not start:
            return [{
                "start": self.shift_start,
                "end": self.shift_end,
                "label": f"Shift {self.shift}",
            }]

        start_at = self._resolve_time_in_shift(start, "--start")
        stop_at = self._resolve_time_in_shift(stop, "--stop", after=start_at)
        return [{
            "start": start_at,
            "end": stop_at,
            "label": f"{start_at:%H:%M:%S} to {stop_at:%H:%M:%S}",
        }]

    def _collect_frames(self):
        frames_by_timestamp = {}
        for day in self._window_dates():
            path = self.image_root / day.strftime("%Y_%m_%d") / f"Shift_{self.shift}_img"
            for img_path in glob.glob(str(path / "*.jpeg")):
                ts = self._datetime_from_name(img_path)
                if not ts:
                    continue

                for idx, window in enumerate(self.windows):
                    if window["start"] <= ts <= window["end"]:
                        frame_info = {
                            "timestamp": ts,
                            "path": img_path,
                            "window_index": idx,
                            "window_count": len(self.windows),
                            "window_label": window["label"],
                        }
                        existing = frames_by_timestamp.get(ts)
                        if existing is None or self._prefer_frame_path(img_path, existing["path"]):
                            frames_by_timestamp[ts] = frame_info
                        break

        return sorted(frames_by_timestamp.values(), key=lambda item: item["timestamp"])

    def _merge_windows(self, windows):
        windows = sorted(windows, key=lambda item: item["start"])
        merged = [windows[0].copy()]

        for window in windows[1:]:
            current = merged[-1]
            if window["start"] <= current["end"]:
                current["end"] = max(current["end"], window["end"])
                current["label"] = self._merge_labels(current["label"], window["label"])
                continue
            merged.append(window.copy())

        return merged

    @staticmethod
    def _merge_labels(existing, new):
        parts = [p.strip() for p in f"{existing}, {new}".split(",") if p.strip()]
        return ", ".join(dict.fromkeys(parts))

    @staticmethod
    def _prefer_frame_path(candidate, existing):
        candidate_dt = ShiftVideoOverlayGenerator._date_folder_from_path(candidate)
        existing_dt = ShiftVideoOverlayGenerator._date_folder_from_path(existing)
        candidate_ts = ShiftVideoOverlayGenerator._datetime_from_name(candidate)
        existing_ts = ShiftVideoOverlayGenerator._datetime_from_name(existing)

        if candidate_dt and candidate_ts and candidate_dt == candidate_ts.date():
            if not (existing_dt and existing_ts and existing_dt == existing_ts.date()):
                return True

        return str(candidate) < str(existing)

    @staticmethod
    def _date_folder_from_path(path):
        for part in Path(path).parts:
            pieces = part.split("_")
            if len(pieces) != 3:
                continue
            try:
                year, month, day = map(int, pieces)
                return datetime(year, month, day).date()
            except ValueError:
                continue
        return None

    def _draw_yolo(self, img, txt_path, w, h):
        with open(txt_path, "r") as f:
            lines = f.readlines()

        for line in lines:
            parts = line.strip().split()
            if len(parts) != 5:
                continue

            cls_id, xc, yc, bw, bh = map(float, parts)
            cls_id = int(cls_id)

            x1 = int((xc - bw / 2) * w)
            y1 = int((yc - bh / 2) * h)
            x2 = int((xc + bw / 2) * w)
            y2 = int((yc + bh / 2) * h)

            x1, y1 = max(0, x1), max(0, y1)
            x2, y2 = min(w, x2), min(h, y2)

            label = CLASS_ID_TO_NAME.get(cls_id, str(cls_id))
            color = CLASS_COLORS.get(cls_id, (255, 255, 255))

            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            cv2.putText(
                img,
                label,
                (x1, max(20, y1 - 10)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                color,
                2,
                cv2.LINE_AA,
            )

    def _resolve_roi_path(self, cfg):
        rois_cfg = cfg.get("rois") or {}
        if isinstance(rois_cfg, dict) and rois_cfg.get("enabled") is False:
            return None

        path_value = rois_cfg.get("path") if isinstance(rois_cfg, dict) else rois_cfg
        if not path_value:
            return None

        path = Path(str(path_value)).expanduser()
        return path if path.is_absolute() else (self.root / path).resolve()

    def _load_rois_once(self, path):
        if not path:
            return []
        if path not in self._ROI_CACHE:
            self._ROI_CACHE[path] = self._load_rois(path)
        return self._ROI_CACHE[path]

    def _load_rois(self, path):
        if not path.exists():
            logger.warning("ROI overlay config not found; continuing without ROIs | path=%s", path)
            return []

        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}

        rois = []
        for name, points in (data.get("rois", data) if isinstance(data, dict) else {}).items():
            points = self._normalize_roi_points(points)
            if len(points) < 2:
                logger.warning("Skipping invalid ROI %s in %s", name, path)
                continue
            rois.append({"name": str(name), "points": points})

        logger.info("Loaded ROI overlays | path=%s | count=%s", path, len(rois))
        return rois

    def _configured_roi_source_size(self, cfg):
        source_cfg = self._find_roi_source_size_cfg(cfg.get("rois"))
        if source_cfg is None:
            source_cfg = self._find_roi_source_size_cfg(cfg.get("video"))

        if source_cfg is None:
            return None

        def parse_axis(axis):
            value = source_cfg.get(axis)
            if value in (None, "auto"):
                return None
            value = int(value)
            if value <= 0:
                raise ValueError(f"rois.source_resolution.{axis} must be positive")
            return value

        width = parse_axis("width")
        height = parse_axis("height")
        if bool(width) != bool(height):
            raise ValueError("rois.source_resolution must include both width and height")
        if width is None:
            return None

        return width, height

    def _input_images_have_overlay(self, cfg):
        configured = self.video_cfg.get("input_images_have_overlay", "auto")
        if str(configured).strip().lower() != "auto":
            return self._parse_bool(configured, "video.input_images_have_overlay")

        history_cfg = cfg.get("history") or {}
        if isinstance(history_cfg, dict) and history_cfg.get("images_have_overlay") is not None:
            return self._parse_bool(history_cfg.get("images_have_overlay"), "history.images_have_overlay")

        producer_cfg_path = self._producer_runtime_path()
        if producer_cfg_path is None:
            return False

        try:
            with open(producer_cfg_path, "r") as f:
                producer_cfg = yaml.safe_load(f) or {}
        except OSError as exc:
            logger.warning("Unable to read producer runtime config for overlay detection: %s", exc)
            return False

        if producer_cfg.get("publish_overlay") is None:
            return False

        has_overlay = self._parse_bool(producer_cfg.get("publish_overlay"), "producer.publish_overlay")
        if has_overlay:
            logger.info(
                "History images already contain overlays; skipping report-side ROI/detection redraw | producer_config=%s",
                producer_cfg_path,
            )
        return has_overlay

    def _producer_runtime_path(self):
        for parent in self.image_root.parents:
            candidate = parent / "config" / "runtime.yaml"
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _parse_bool(value, name):
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        raise ValueError(f"{name} must be true, false, or auto")

    @staticmethod
    def _find_roi_source_size_cfg(section):
        if not isinstance(section, dict):
            return None

        for key in (
            "source_resolution",
            "source_size",
            "coordinate_resolution",
            "roi_source_resolution",
            "roi_coordinate_resolution",
        ):
            candidate = section.get(key)
            if isinstance(candidate, dict):
                return candidate

        for nested_key in ("rois", "overlay_rois"):
            candidate = section.get(nested_key)
            if isinstance(candidate, dict):
                nested = ShiftVideoOverlayGenerator._find_roi_source_size_cfg(candidate)
                if nested is not None:
                    return nested

        return None

    @staticmethod
    def _normalize_roi_points(points):
        normalized = []
        for point in points or []:
            if isinstance(point, dict):
                x = point.get("x")
                y = point.get("y")
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                x, y = point[:2]
            else:
                continue

            try:
                normalized.append((float(x), float(y)))
            except (TypeError, ValueError):
                continue

        return normalized

    def _scale_rois(self, output_w, output_h, source_w, source_h):
        if not self.rois:
            return []

        roi_source_w, roi_source_h = self._resolve_roi_source_size(source_w, source_h)
        if (roi_source_w, roi_source_h) != (source_w, source_h):
            scale_x = output_w / float(roi_source_w)
            scale_y = output_h / float(roi_source_h)
            logger.info(
                "Scaling ROI overlay coordinates | roi_source=%sx%s | saved_frame=%sx%s | video=%sx%s | scale_x=%.4f | scale_y=%.4f",
                roi_source_w,
                roi_source_h,
                source_w,
                source_h,
                output_w,
                output_h,
                scale_x,
                scale_y,
            )

        scaled_rois = []
        for idx, roi in enumerate(self.rois):
            if len(roi["points"]) < 2:
                continue

            points = self._scale_roi_points(
                roi["points"],
                output_w,
                output_h,
                roi_source_w,
                roi_source_h,
            )
            scaled_rois.append({
                "name": roi["name"],
                "points": points,
                "pts": np.array(points, dtype=np.int32),
                "color": self._roi_color(roi["name"], idx),
            })

        return scaled_rois

    def _resolve_roi_source_size(self, frame_w, frame_h):
        configured = getattr(self, "roi_source_size", None)
        if configured:
            return configured

        inferred_w, inferred_h = self._infer_roi_source_size_from_points(frame_w, frame_h)
        if (inferred_w, inferred_h) != (frame_w, frame_h):
            logger.warning(
                "rois.source_resolution is not configured; inferred ROI source size %sx%s from coordinates and saved frame %sx%s",
                inferred_w,
                inferred_h,
                frame_w,
                frame_h,
            )
            return inferred_w, inferred_h

        logger.warning(
            "rois.source_resolution is not configured; ROI coordinates fit saved frame size %sx%s, so no scaling is applied",
            frame_w,
            frame_h,
        )
        return frame_w, frame_h

    def _infer_roi_source_size_from_points(self, frame_w, frame_h):
        max_x = 0.0
        max_y = 0.0
        for roi in self.rois:
            for x, y in roi["points"]:
                max_x = max(max_x, float(x))
                max_y = max(max_y, float(y))

        if max_x <= frame_w and max_y <= frame_h:
            return frame_w, frame_h

        for width, height in COMMON_ROI_SOURCE_SIZES:
            if max_x <= width + 2 and max_y <= height + 2:
                if width != frame_w or height != frame_h:
                    return width, height

        inferred_w = self._infer_roi_axis_source_size(max_x, frame_w)
        inferred_h = self._infer_roi_axis_source_size(max_y, frame_h)
        return inferred_w, inferred_h

    @staticmethod
    def _infer_roi_axis_source_size(max_coordinate, frame_size):
        if frame_size <= 0:
            return frame_size

        if max_coordinate <= frame_size:
            return frame_size

        doubled = frame_size * 2
        if max_coordinate <= doubled + 2:
            return doubled

        return int(round(max_coordinate)) + 1

    def _draw_rois(self, frame, rois):
        overlay = None
        draw_items = []

        for roi in rois:
            points = roi["points"]
            if len(points) < 2:
                continue

            color = roi["color"]
            pts = roi["pts"]
            draw_items.append((roi, color, pts))

            if len(points) >= 3:
                if overlay is None:
                    overlay = frame.copy()
                cv2.fillPoly(overlay, [pts], color)

        if overlay is not None:
            cv2.addWeighted(overlay, ROI_FILL_ALPHA, frame, 1.0 - ROI_FILL_ALPHA, 0, frame)

        for roi, color, pts in draw_items:
            is_closed = len(pts) >= 3
            cv2.polylines(
                frame,
                [pts],
                isClosed=is_closed,
                color=color,
                thickness=ROI_LINE_THICKNESS,
                lineType=cv2.LINE_AA,
            )
            self._draw_roi_label(frame, roi["name"], pts, color)

    @staticmethod
    def _roi_color(name, index):
        return ROI_COLORS.get(
            str(name),
            ROI_FALLBACK_COLORS[index % len(ROI_FALLBACK_COLORS)],
        )

    @staticmethod
    def _scale_roi_points(points, output_w, output_h, source_w, source_h):
        if source_w <= 0 or source_h <= 0:
            return []

        scale_x = output_w / source_w
        scale_y = output_h / source_h
        scaled = []

        for x, y in points:
            px = int(round(x * scale_x))
            py = int(round(y * scale_y))
            px = min(max(px, 0), max(output_w - 1, 0))
            py = min(max(py, 0), max(output_h - 1, 0))
            scaled.append((px, py))

        return scaled

    def _draw_roi_label(self, frame, name, pts, color):
        label = self._format_roi_label(name)
        if not label:
            return

        output_h, output_w = frame.shape[:2]
        x, y = pts.mean(axis=0).astype(int)
        font = cv2.FONT_HERSHEY_SIMPLEX

        (text_w, text_h), baseline = cv2.getTextSize(
            label,
            font,
            ROI_LABEL_FONT_SCALE,
            ROI_LABEL_THICKNESS,
        )

        x = max(0, min(int(x), output_w - text_w - 5))
        y = max(text_h + 5, min(int(y), output_h - 5))
        cv2.rectangle(
            frame,
            (x - 3, y - text_h - 5),
            (x + text_w + 3, y + baseline + 3),
            color,
            -1,
        )
        cv2.putText(
            frame,
            label,
            (x, y),
            font,
            ROI_LABEL_FONT_SCALE,
            (255, 255, 255),
            ROI_LABEL_THICKNESS,
            cv2.LINE_AA,
        )

    @staticmethod
    def _format_roi_label(name):
        return str(name).strip()[:32]

    def _draw_timestamp(self, frame, img_path, h):
        ts = self._timestamp_from_name(img_path)
        if not ts:
            return

        cv2.putText(
            frame,
            ts,
            (20, h - 20),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 255),
            2,
            cv2.LINE_AA,
        )

    def _draw_window_label(self, frame, frame_info, w):
        if frame_info["window_count"] <= 1:
            return

        label = (
            f"Loadcell missing window {frame_info['window_index'] + 1}/"
            f"{frame_info['window_count']}: {frame_info['window_label']}"
        )
        cv2.putText(
            frame,
            label[:90],
            (20, 35),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 255),
            2,
            cv2.LINE_AA,
        )

    def _get_txt_path(self, base):
        try:
            name = base.split("_")[-1]
            dd, mm, yyyy = name.split("-")[:3]
            date_folder = f"{yyyy}_{mm}_{dd}"
        except Exception:
            date_folder = self.date_obj.strftime("%Y_%m_%d")

        return (
            self.text_root /
            date_folder /
            f"Shift_{self.shift}_text" /
            f"{base}.txt"
        )

    def _load_shift_times(self, cfg):
        shift_times = DEFAULT_SHIFT_TIMES.copy()
        for shift_cfg in cfg.get("history", {}).get("shifts", []):
            name = str(shift_cfg.get("name", "")).split("_")[-1].upper()
            start = shift_cfg.get("start")
            end = shift_cfg.get("end")
            if name and start and end:
                shift_times[name] = (start, end)
        return shift_times

    def _shift_window(self):
        try:
            start_s, end_s = self.shift_times[self.shift]
        except KeyError as exc:
            raise ValueError(f"Unknown shift: {self.shift}") from exc

        start = datetime.strptime(f"{self.date_str} {start_s}", "%d-%m-%Y %H:%M")
        end = datetime.strptime(f"{self.date_str} {end_s}", "%d-%m-%Y %H:%M")

        if end <= start:
            end += timedelta(days=1)

        return start, end

    def _resolve_time_in_shift(self, value, arg_name, after=None):
        parsed = self._parse_time(value, arg_name)
        inside_shift = [
            candidate for candidate in self._time_candidates(parsed)
            if self.shift_start <= candidate <= self.shift_end
        ]

        if after is not None:
            inside_shift = [candidate for candidate in inside_shift if candidate > after]

        if inside_shift:
            return sorted(inside_shift)[0]

        if after is not None:
            raise ValueError(
                f"{arg_name}={value} must be after --start and inside "
                f"shift {self.shift} ({self._format_dt(self.shift_start)} -> "
                f"{self._format_dt(self.shift_end)})"
            )

        raise ValueError(
            f"{arg_name}={value} is outside "
            f"shift {self.shift} ({self._format_dt(self.shift_start)} -> "
            f"{self._format_dt(self.shift_end)})"
        )

    def _time_candidates(self, value):
        candidates = [self._time_on_shift_calendar(value)]

        if 1 <= value.hour <= 11:
            pm_value = (
                datetime.combine(self.shift_start.date(), value) + timedelta(hours=12)
            ).time()
            candidates.append(self._time_on_shift_calendar(pm_value))
        elif value.hour == 12:
            candidates.append(self._time_on_shift_calendar(value.replace(hour=0)))

        return list(dict.fromkeys(candidates))

    def _time_on_shift_calendar(self, value):
        at = datetime.combine(self.shift_start.date(), value)
        if at < self.shift_start:
            at += timedelta(days=1)
        return at

    @staticmethod
    def _parse_time(value, arg_name):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                continue
        raise ValueError(f"{arg_name} must be in HH:MM or HH:MM:SS format")

    def _window_dates(self):
        day = min(window["start"] for window in self.windows).date()
        end_day = max(window["end"] for window in self.windows).date()

        while day <= end_day:
            yield day
            day += timedelta(days=1)

    def _window_suffix(self):
        if len(self.windows) != 1:
            return f"_windows_{len(self.windows)}"

        window = self.windows[0]
        if (window["start"], window["end"]) == (self.shift_start, self.shift_end):
            return ""

        return f"_{window['start']:%H%M%S}_to_{window['end']:%H%M%S}"

    def _resolve_resolution(self, w, h):
        width = self.res_cfg.get("width", "auto")
        height = self.res_cfg.get("height", "auto")

        return (
            int(w if width == "auto" else width),
            int(h if height == "auto" else height),
        )

    @staticmethod
    def _format_dt(value):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _timestamp_from_name(path):
        ts = ShiftVideoOverlayGenerator._datetime_from_name(path)
        return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else None

    @staticmethod
    def _datetime_from_name(path):
        try:
            name = Path(path).stem.split("_")[-1]
            dd, mm, yyyy, hh, mi, ss = name.split("-")[:6]
            return datetime(int(yyyy), int(mm), int(dd), int(hh), int(mi), int(ss))
        except (IndexError, ValueError):
            return None


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Generate overlay video")
    parser.add_argument("--date", required=True)
    parser.add_argument("--shift", required=True, choices=["a", "b", "c", "A", "B", "C"])
    parser.add_argument("--start", help="Optional start time (HH:MM or HH:MM:SS)")
    parser.add_argument("--stop", help="Optional stop time (HH:MM or HH:MM:SS)")
    args = parser.parse_args()

    try:
        ShiftVideoOverlayGenerator(
            args.date,
            args.shift,
            start=args.start,
            stop=args.stop,
        ).generate()
    except ValueError as exc:
        parser.error(str(exc))
