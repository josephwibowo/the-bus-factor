"""Render a structured text capture into a PNG.

Input JSON shape:
{
  "title": "Optional title",
  "subtitle": "Optional subtitle",
  "footer": "Optional footer",
  "theme": "default|terminal",
  "blocks": [
    {"label": "Prompt", "text": "Question text"},
    {"label": "Answer", "text": "Rendered answer"}
  ]
}
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

WIDTH = 1480
PADDING_X = 48
PADDING_Y = 40
BLOCK_GAP = 22
TEXT_WIDTH = WIDTH - (PADDING_X * 2) - 40


@dataclass(frozen=True)
class Theme:
    background: tuple[int, int, int]
    panel: tuple[int, int, int]
    border: tuple[int, int, int]
    title: tuple[int, int, int]
    subtitle: tuple[int, int, int]
    label_bg: tuple[int, int, int]
    label_text: tuple[int, int, int]
    text: tuple[int, int, int]
    footer: tuple[int, int, int]


THEMES = {
    "default": Theme(
        background=(10, 16, 27),
        panel=(18, 28, 43),
        border=(47, 64, 89),
        title=(242, 246, 255),
        subtitle=(164, 177, 201),
        label_bg=(34, 76, 140),
        label_text=(236, 244, 255),
        text=(232, 238, 248),
        footer=(134, 148, 176),
    ),
    "terminal": Theme(
        background=(8, 10, 12),
        panel=(17, 20, 24),
        border=(61, 68, 76),
        title=(238, 242, 244),
        subtitle=(148, 158, 170),
        label_bg=(36, 74, 60),
        label_text=(227, 245, 236),
        text=(224, 230, 237),
        footer=(135, 144, 156),
    ),
}


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    if bold:
        candidates = [
            "/System/Library/Fonts/SFNSMono.ttf",
            "/System/Library/Fonts/Supplemental/Menlo Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
        ]
    else:
        candidates = [
            "/System/Library/Fonts/SFNSMono.ttf",
            "/System/Library/Fonts/Supplemental/Menlo.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        ]
    for path in candidates:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size=size)
            except OSError:
                continue
    return ImageFont.load_default()


TITLE_FONT = _font(34, bold=True)
SUBTITLE_FONT = _font(22)
LABEL_FONT = _font(18, bold=True)
BODY_FONT = _font(21)
FOOTER_FONT = _font(18)


def _text_height(text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont) -> int:
    lines = text.count("\n") + 1
    return int(lines * (font.size + 10) if hasattr(font, "size") else lines * 28)


def _wrap_line(
    draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont
) -> list[str]:
    if not text:
        return [""]
    words = text.split(" ")
    lines: list[str] = []
    current = ""
    for word in words:
        trial = word if not current else f"{current} {word}"
        width = draw.textbbox((0, 0), trial, font=font)[2]
        if width <= TEXT_WIDTH:
            current = trial
            continue
        if current:
            lines.append(current)
        current = word
    if current:
        lines.append(current)
    return lines


def _wrap_text(
    draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont
) -> str:
    wrapped: list[str] = []
    for raw_line in text.splitlines() or [""]:
        if raw_line.startswith("```"):
            continue
        if not raw_line.strip():
            wrapped.append("")
            continue
        wrapped.extend(_wrap_line(draw, raw_line.rstrip(), font))
    return "\n".join(wrapped)


def _block_height(draw: ImageDraw.ImageDraw, label: str, text: str) -> int:
    wrapped = _wrap_text(draw, text, BODY_FONT)
    body_h = _text_height(wrapped, BODY_FONT)
    return 28 + 44 + body_h + 28


def render(payload: dict[str, Any], output_path: Path) -> None:
    theme = THEMES[payload.get("theme", "default")]
    blocks = payload.get("blocks", [])
    if not blocks:
        raise ValueError("payload.blocks must not be empty")

    scratch = Image.new("RGB", (WIDTH, 100), theme.background)
    draw = ImageDraw.Draw(scratch)

    total_height = PADDING_Y
    title = payload.get("title", "")
    subtitle = payload.get("subtitle", "")
    footer = payload.get("footer", "")

    if title:
        total_height += _text_height(_wrap_text(draw, title, TITLE_FONT), TITLE_FONT)
    if subtitle:
        total_height += _text_height(_wrap_text(draw, subtitle, SUBTITLE_FONT), SUBTITLE_FONT) + 14
    if title or subtitle:
        total_height += 20

    for block in blocks:
        total_height += _block_height(draw, block.get("label", ""), block.get("text", ""))
        total_height += BLOCK_GAP

    if footer:
        total_height += _text_height(_wrap_text(draw, footer, FOOTER_FONT), FOOTER_FONT) + 12
    total_height += PADDING_Y

    image = Image.new("RGB", (WIDTH, max(900, total_height)), theme.background)
    draw = ImageDraw.Draw(image)

    y = PADDING_Y
    if title:
        wrapped = _wrap_text(draw, title, TITLE_FONT)
        draw.text((PADDING_X, y), wrapped, font=TITLE_FONT, fill=theme.title)
        y += _text_height(wrapped, TITLE_FONT) + 10
    if subtitle:
        wrapped = _wrap_text(draw, subtitle, SUBTITLE_FONT)
        draw.text((PADDING_X, y), wrapped, font=SUBTITLE_FONT, fill=theme.subtitle)
        y += _text_height(wrapped, SUBTITLE_FONT) + 20

    for block in blocks:
        block_h = _block_height(draw, block.get("label", ""), block.get("text", ""))
        draw.rounded_rectangle(
            (PADDING_X, y, WIDTH - PADDING_X, y + block_h),
            radius=18,
            fill=theme.panel,
            outline=theme.border,
            width=2,
        )
        label = block.get("label", "")
        if label:
            label_w = draw.textbbox((0, 0), label, font=LABEL_FONT)[2] + 28
            draw.rounded_rectangle(
                (PADDING_X + 18, y + 16, PADDING_X + 18 + label_w, y + 48),
                radius=12,
                fill=theme.label_bg,
            )
            draw.text((PADDING_X + 32, y + 22), label, font=LABEL_FONT, fill=theme.label_text)

        wrapped = _wrap_text(draw, block.get("text", ""), BODY_FONT)
        draw.text((PADDING_X + 20, y + 64), wrapped, font=BODY_FONT, fill=theme.text, spacing=10)
        y += block_h + BLOCK_GAP

    if footer:
        wrapped = _wrap_text(draw, footer, FOOTER_FONT)
        draw.text((PADDING_X, y), wrapped, font=FOOTER_FONT, fill=theme.footer)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, format="PNG", optimize=True)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to JSON payload")
    parser.add_argument("--output", required=True, help="Path to PNG output")
    args = parser.parse_args()

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    render(payload, Path(args.output))


if __name__ == "__main__":
    main()
