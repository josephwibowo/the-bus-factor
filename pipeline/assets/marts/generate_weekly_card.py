"""@bruin

name: generate_weekly_card
type: python

description: |
  Render the weekly share card (1200x630) as a PNG using Pillow. The card
  reads from public-data/weekly.json so it is driven by the same data the
  site renders from. Follows the up-to-5-findings rule and swaps to a
  zero-flagged fallback layout when the headline reports zero packages.
  Writes two files:
    reports/cards/weekly-<snapshot_week>.png (archival)
    reports/cards/latest.png                  (always the most recent)

depends:
  - export_public_bundle

tags:
  - layer:export
  - surface:weekly_card

@bruin"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

AnyFont = ImageFont.FreeTypeFont | ImageFont.ImageFont

REPO_ROOT = Path(__file__).resolve().parents[3]
WEEKLY_JSON = REPO_ROOT / "public-data" / "weekly.json"
CARDS_DIR = REPO_ROOT / "reports" / "cards"

CARD_W, CARD_H = 1200, 630
BG = (12, 18, 29)
SURFACE = (22, 30, 46)
ACCENT = (255, 196, 87)
TEXT_PRIMARY = (242, 246, 255)
TEXT_MUTED = (166, 176, 196)
DIVIDER = (45, 56, 82)
TIER_COLORS = {
    "Critical": (220, 69, 92),
    "High": (244, 127, 89),
    "Elevated": (255, 196, 87),
    "Watch": (124, 188, 255),
    "Stable": (96, 224, 169),
}


def _font(size: int, weight: str = "regular") -> AnyFont:
    candidates = {
        "bold": [
            "/System/Library/Fonts/Helvetica.ttc",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ],
        "regular": [
            "/System/Library/Fonts/Helvetica.ttc",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ],
    }[weight]
    for path in candidates:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size=size)
            except OSError:
                continue
    return ImageFont.load_default()


def _text_size(draw: ImageDraw.ImageDraw, text: str, font: AnyFont) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return int(bbox[2] - bbox[0]), int(bbox[3] - bbox[1])


def _wrap(draw: ImageDraw.ImageDraw, text: str, font: AnyFont, width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        trial = f"{current} {word}".strip()
        if _text_size(draw, trial, font)[0] <= width:
            current = trial
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _draw_base(draw: ImageDraw.ImageDraw, weekly: dict[str, Any]) -> None:
    draw.rectangle([(0, 0), (CARD_W, CARD_H)], fill=BG)
    draw.rectangle([(0, 0), (CARD_W, 12)], fill=ACCENT)
    title_font = _font(52, "bold")
    label_font = _font(20, "bold")
    meta_font = _font(22)

    draw.text((60, 40), "THE BUS FACTOR", font=label_font, fill=ACCENT)
    week_label = f"Week {weekly['headline']['snapshot_week']} | methodology {weekly['headline']['methodology_version']}"
    draw.text((60, 72), week_label, font=meta_font, fill=TEXT_MUTED)

    headline_lines = _wrap(draw, weekly["headline"]["headline"], title_font, CARD_W - 120)
    y = 120
    for line in headline_lines[:2]:
        draw.text((60, y), line, font=title_font, fill=TEXT_PRIMARY)
        y += 62


def _draw_findings(draw: ImageDraw.ImageDraw, findings: list[dict[str, Any]]) -> None:
    row_font = _font(26, "bold")
    detail_font = _font(20)
    y = 260
    row_h = 62
    for idx, finding in enumerate(findings[:5], start=1):
        draw.rectangle([(60, y), (CARD_W - 60, y + row_h - 8)], fill=SURFACE)
        tier_color = TIER_COLORS.get(finding["severity_tier"], ACCENT)
        draw.rectangle([(60, y), (68, y + row_h - 8)], fill=tier_color)
        header = f"#{idx} {finding['ecosystem']} - {finding['package_name']}"
        draw.text((88, y + 8), header, font=row_font, fill=TEXT_PRIMARY)
        subtitle = (
            f"{finding['severity_tier']} | risk {finding['risk_score']:.1f} | "
            f"{finding['primary_finding']}"
        )
        draw.text((88, y + 34), subtitle[:110], font=detail_font, fill=TEXT_MUTED)
        y += row_h


def _draw_zero_state(draw: ImageDraw.ImageDraw, weekly: dict[str, Any]) -> None:
    body_font = _font(28)
    divider_y = 250
    draw.line([(60, divider_y), (CARD_W - 60, divider_y)], fill=DIVIDER, width=2)
    lines = _wrap(draw, weekly["headline"]["summary"], body_font, CARD_W - 120)
    y = divider_y + 20
    for line in lines[:4]:
        draw.text((60, y), line, font=body_font, fill=TEXT_PRIMARY)
        y += 40
    if weekly.get("zero_flagged_fallback_copy"):
        tag_font = _font(22, "bold")
        draw.text((60, CARD_H - 90), "Why no alarm", font=tag_font, fill=ACCENT)
        fallback_lines = _wrap(draw, weekly["zero_flagged_fallback_copy"], _font(20), CARD_W - 120)
        y = CARD_H - 60
        for line in fallback_lines[:2]:
            draw.text((60, y), line, font=_font(20), fill=TEXT_MUTED)
            y += 26


def _draw_footer(draw: ImageDraw.ImageDraw) -> None:
    footer_font = _font(18)
    url_font = _font(18, "bold")
    draw.text(
        (60, CARD_H - 32),
        "Scores combine importance (reach, downloads, security exposure) x fragility signals.",
        font=footer_font,
        fill=TEXT_MUTED,
    )
    draw.text(
        (CARD_W - 450, CARD_H - 32),
        "josephwibowo.github.io/the-bus-factor",
        font=url_font,
        fill=ACCENT,
    )


def render(weekly: dict[str, Any]) -> Image.Image:
    image = Image.new("RGB", (CARD_W, CARD_H), BG)
    draw = ImageDraw.Draw(image)
    _draw_base(draw, weekly)
    findings = weekly.get("findings") or []
    if findings:
        _draw_findings(draw, findings)
    else:
        _draw_zero_state(draw, weekly)
    _draw_footer(draw)
    return image


def main() -> None:
    CARDS_DIR.mkdir(parents=True, exist_ok=True)
    with WEEKLY_JSON.open("r", encoding="utf-8") as fh:
        weekly = json.load(fh)
    image = render(weekly)
    dated_path = CARDS_DIR / f"weekly-{weekly['headline']['snapshot_week']}.png"
    latest_path = CARDS_DIR / "latest.png"
    image.save(dated_path, format="PNG", optimize=True)
    image.save(latest_path, format="PNG", optimize=True)
    print(f"Wrote weekly share card to {dated_path} and {latest_path}")


main()
