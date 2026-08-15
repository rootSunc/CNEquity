#!/usr/bin/env python3
"""Render macOS-style terminal screenshots for README (docs/assets/*.png).

Requires Pillow. Content is a cleaned transcript of a short live `cml demo`
run — edit the string constants below when CLI copy changes.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "docs" / "assets"

_FONT_CANDIDATES = [
    "/System/Library/Fonts/Menlo.ttc",
    "/System/Library/Fonts/SFNSMono.ttf",
    "/Library/Fonts/SF-Mono-Regular.otf",
    "/System/Library/Fonts/Supplemental/Courier New.ttf",
    "/System/Library/Fonts/Monaco.ttf",
]

DEMO = """\
$ cml demo --symbols 600519.SH,000001.SZ --days 5

=== [1/6] Prepare demo lake at data/cn-market-lake-demo ===
data_root = …/data/cn-market-lake-demo
config    = configs/cn-market-lake.demo.toml
Note: this is a SEPARATE lake from a full `cml init` — safe to wipe.

=== [2/6] Probe TDX ===
Probing TDX hosts (first successful server wins)…
TDX connection OK (1.2s)

=== [3/6] Instruments (demo universe) ===
Fetching full instrument list, then keeping 2 demo symbols…
Wrote 2 instruments → curated/instruments/

=== [4/6] Trading calendar ===
Demo window: 2026-07-21 → 2026-07-27 (5 trading days target)

=== [5/6] daily_bars for 2 symbols ===
Bars run …: status=success rows_written≈16

=== [6/6] Sample result ===
600519.SH — latest rows:

┌───────────┬────────────┬─────────┬─────────┬─────────┬─────────┬────────┬──────────────┐
│ symbol    │ trade_date │ open    │ high    │ low     │ close   │ volume │ source       │
├───────────┼────────────┼─────────┼─────────┼─────────┼─────────┼────────┼──────────────┤
│ 600519.SH │ 2026-07-24 │ 1305.00 │ 1309.21 │ 1286.20 │ 1297.41 │  35698 │ tdx_protocol │
│ 600519.SH │ 2026-07-23 │ 1299.80 │ 1303.00 │ 1285.43 │ 1292.01 │  33917 │ tdx_protocol │
│ 600519.SH │ 2026-07-22 │ 1300.00 │ 1308.00 │ 1283.24 │ 1305.00 │  65181 │ tdx_protocol │
│ 600519.SH │ 2026-07-21 │ 1338.98 │ 1344.70 │ 1296.87 │ 1308.00 │  77147 │ tdx_protocol │
└───────────┴────────────┴─────────┴─────────┴─────────┴─────────┴────────┴──────────────┘

Demo lake ready under: data/cn-market-lake-demo
"""

QUERY = """\
$ cml query --config configs/cn-market-lake.demo.toml --sql \\
    "SELECT symbol, trade_date, close, volume, source
     FROM daily_bars
     ORDER BY trade_date DESC, symbol
     LIMIT 8"

┌───────────┬────────────┬─────────┬─────────┬──────────────┐
│ symbol    │ trade_date │ close   │ volume  │ source       │
├───────────┼────────────┼─────────┼─────────┼──────────────┤
│ 000001.SZ │ 2026-07-24 │   11.10 │ 1140933 │ tdx_protocol │
│ 600519.SH │ 2026-07-24 │ 1297.41 │   35698 │ tdx_protocol │
│ 000001.SZ │ 2026-07-23 │   11.08 │ 1095742 │ tdx_protocol │
│ 600519.SH │ 2026-07-23 │ 1292.01 │   33917 │ tdx_protocol │
│ 000001.SZ │ 2026-07-22 │   10.98 │ 1029483 │ tdx_protocol │
│ 600519.SH │ 2026-07-22 │ 1305.00 │   65181 │ tdx_protocol │
│ 000001.SZ │ 2026-07-21 │   10.84 │ 1755113 │ tdx_protocol │
│ 600519.SH │ 2026-07-21 │ 1308.00 │   77147 │ tdx_protocol │
└───────────┴────────────┴─────────┴─────────┴──────────────┘
"""

LOAD = """\
$ python
>>> from cn_market_lake.query import load
>>> bars = load(
...     "daily_bars",
...     symbols=["600519.SH"],
...     start="2026-07-21",
...     end="2026-07-24",
...     data_root="data/cn-market-lake-demo",
... )
>>> bars.select(["symbol", "trade_date", "close", "source"])
shape: (4, 4)
┌───────────┬────────────┬─────────┬──────────────┐
│ symbol    │ trade_date │ close   │ source       │
│ ---       │ ---        │ ---     │ ---          │
│ str       │ date       │ f64     │ str          │
╞═══════════╪════════════╪═════════╪══════════════╡
│ 600519.SH │ 2026-07-21 │ 1308.0  │ tdx_protocol │
│ 600519.SH │ 2026-07-22 │ 1305.0  │ tdx_protocol │
│ 600519.SH │ 2026-07-23 │ 1292.01 │ tdx_protocol │
│ 600519.SH │ 2026-07-24 │ 1297.41 │ tdx_protocol │
└───────────┴────────────┴─────────┴──────────────┘
"""


def _load_font(size: int) -> ImageFont.ImageFont:
    for path in _FONT_CANDIDATES:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size=size, index=0)
            except OSError:
                continue
    return ImageFont.load_default()


def _colorize_line(line: str) -> list[tuple[str, str]]:
    if line.startswith(("$ ", ">>> ", "... ")):
        return [(line, "#7ee787")]
    if line.startswith("==="):
        return [(line, "#79c0ff")]
    if "tdx_protocol" in line and "│" in line:
        parts = line.rsplit("tdx_protocol", 1)
        return [(parts[0], "#c9d1d9"), ("tdx_protocol", "#ffa657"), (parts[1], "#c9d1d9")]
    if line.startswith(("Demo lake", "Config →", "data_root", "config    =", "Note:")):
        return [(line, "#a5d6ff")]
    if "OK" in line or "success" in line or "ready" in line:
        return [(line, "#7ee787")]
    return [(line, "#c9d1d9")]


def render_terminal(text: str, out: Path, *, title: str) -> None:
    lines = text.rstrip("\n").splitlines()
    max_width_chars = max(len(line) for line in lines)
    font = _load_font(15)
    title_font = _load_font(13)
    sample = "M" * 10
    bbox = font.getbbox(sample)
    char_w = (bbox[2] - bbox[0]) / 10
    line_h = (bbox[3] - bbox[1]) + 6

    pad_x, pad_y = 22, 18
    title_h = 36
    content_w = int(char_w * max_width_chars) + pad_x * 2
    content_h = int(line_h * len(lines)) + pad_y * 2
    width = max(content_w, 720)
    height = title_h + content_h + 8

    img = Image.new("RGB", (width, height), "#0d1117")
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, width, title_h], fill="#161b22")
    for i, color in enumerate(("#ff5f56", "#ffbd2e", "#27c93f")):
        cx = 18 + i * 18
        draw.ellipse([cx - 5, title_h // 2 - 5, cx + 5, title_h // 2 + 5], fill=color)
    draw.text((70, title_h // 2 - 7), title, font=title_font, fill="#8b949e")

    y = title_h + pad_y
    for line in lines:
        x = pad_x
        for chunk, color in _colorize_line(line):
            draw.text((x, y), chunk, font=font, fill=color)
            x += int(char_w * len(chunk))
        y += line_h

    draw.rectangle([0, 0, width - 1, height - 1], outline="#30363d")
    out.parent.mkdir(parents=True, exist_ok=True)
    img.save(out, "PNG", optimize=True)
    print(f"wrote {out.relative_to(ROOT)} ({width}x{height})")


def main() -> None:
    render_terminal(DEMO, ASSETS / "cml-demo.png", title="cml demo")
    render_terminal(QUERY, ASSETS / "cml-query.png", title="cml query")
    render_terminal(LOAD, ASSETS / "cml-load.png", title="python — load()")


if __name__ == "__main__":
    main()
