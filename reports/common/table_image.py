from __future__ import annotations

from pathlib import Path
from typing import Sequence

import cv2
import numpy as np


_FONT = cv2.FONT_HERSHEY_SIMPLEX


def _image_text(value: object) -> str:
    """Return text supported by OpenCV's built-in Hershey font."""
    text = str(value)
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u00e2\u20ac\u201c", "-").replace("\u00e2\u20ac\u201d", "-")
    return text.encode("ascii", errors="replace").decode("ascii")


def _fit_scale(text: str, available_width: int, preferred: float) -> float:
    (text_width, _), _ = cv2.getTextSize(text, _FONT, preferred, 1)
    if text_width <= available_width or text_width == 0:
        return preferred
    return max(0.32, preferred * available_width / text_width)


def _draw_cell(
    image: np.ndarray,
    *,
    left: int,
    top: int,
    width: int,
    height: int,
    text: object,
    background: tuple[int, int, int],
    foreground: tuple[int, int, int],
    border: tuple[int, int, int],
    centered: bool,
    bold: bool = False,
):
    cv2.rectangle(image, (left, top), (left + width, top + height), background, -1)
    cv2.rectangle(image, (left, top), (left + width, top + height), border, 1)

    rendered = _image_text(text)
    thickness = 2 if bold else 1
    scale = _fit_scale(rendered, width - 24, 0.54)
    (text_width, text_height), _ = cv2.getTextSize(rendered, _FONT, scale, thickness)
    text_left = left + (width - text_width) // 2 if centered else left + 12
    baseline = top + (height + text_height) // 2
    cv2.putText(
        image,
        rendered,
        (text_left, baseline),
        _FONT,
        scale,
        foreground,
        thickness,
        cv2.LINE_AA,
    )


def save_table_image(
    output_path: str | Path,
    *,
    title: str,
    metadata: Sequence[tuple[str, str]],
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
) -> Path:
    """Render a compact report table to a PNG without a browser dependency."""
    if len(headers) < 2:
        raise ValueError("A report table image needs at least two columns")
    if any(len(row) != len(headers) for row in rows):
        raise ValueError("Every report table image row must match the header count")

    margin = 32
    first_column_width = 220
    minimum_table_width = 640
    caster_width = 150
    table_width = max(
        minimum_table_width,
        first_column_width + caster_width * (len(headers) - 1),
    )
    remaining_width = table_width - first_column_width
    caster_widths = [remaining_width // (len(headers) - 1)] * (len(headers) - 1)
    caster_widths[-1] += remaining_width - sum(caster_widths)
    column_widths = [first_column_width, *caster_widths]

    title_height = 52
    metadata_height = 42
    section_gap = 18
    row_height = 44
    table_height = row_height * (len(rows) + 1)
    image_width = table_width + margin * 2
    image_height = margin * 2 + title_height + metadata_height + section_gap + table_height

    image = np.full((image_height, image_width, 3), (248, 245, 242), dtype=np.uint8)
    card_left = 16
    card_top = 16
    card_right = image_width - 16
    card_bottom = image_height - 16
    cv2.rectangle(image, (card_left, card_top), (card_right, card_bottom), (255, 255, 255), -1)
    cv2.rectangle(image, (card_left, card_top), (card_right, card_bottom), (231, 224, 217), 1)

    title_text = _image_text(title)
    title_scale = _fit_scale(title_text, table_width, 0.72)
    cv2.putText(
        image,
        title_text,
        (margin, margin + 27),
        _FONT,
        title_scale,
        (77, 50, 23),
        2,
        cv2.LINE_AA,
    )

    metadata_top = margin + title_height
    metadata_items = list(metadata)
    metadata_width = table_width // max(1, len(metadata_items))
    for index, (label, value) in enumerate(metadata_items):
        left = margin + index * metadata_width
        width = table_width - index * metadata_width if index == len(metadata_items) - 1 else metadata_width
        _draw_cell(
            image,
            left=left,
            top=metadata_top,
            width=width,
            height=metadata_height,
            text=f"{label}: {value}",
            background=(252, 249, 247),
            foreground=(116, 100, 82),
            border=(235, 229, 224),
            centered=False,
            bold=True,
        )

    table_top = metadata_top + metadata_height + section_gap
    left = margin
    for index, (header, width) in enumerate(zip(headers, column_widths)):
        _draw_cell(
            image,
            left=left,
            top=table_top,
            width=width,
            height=row_height,
            text=header,
            background=(245, 238, 232),
            foreground=(77, 50, 23),
            border=(225, 213, 203),
            centered=index > 0,
            bold=True,
        )
        left += width

    for row_index, row in enumerate(rows):
        is_total = row_index == len(rows) - 1
        background = (
            (241, 232, 223)
            if is_total
            else ((255, 255, 255) if row_index % 2 == 0 else (252, 249, 247))
        )
        border = (209, 197, 184) if is_total else (227, 220, 214)
        foreground = (77, 50, 23) if is_total else (70, 55, 38)
        top = table_top + row_height * (row_index + 1)
        left = margin
        for column_index, (value, width) in enumerate(zip(row, column_widths)):
            _draw_cell(
                image,
                left=left,
                top=top,
                width=width,
                height=row_height,
                text=value,
                background=background,
                foreground=foreground,
                border=border,
                centered=column_index > 0,
                bold=is_total,
            )
            left += width

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(path), image):
        raise OSError(f"Could not save report table image: {path}")
    return path
