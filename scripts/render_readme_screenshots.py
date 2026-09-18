#!/usr/bin/env python3
"""Render macOS-style terminal screenshots for README (docs/assets/*.png).

Requires Pillow. Content is a cleaned transcript of a short live `cne init
--profile demo` run — edit the string constants below when CLI copy changes.

Two fonts, because the CLI speaks Chinese and no monospace face on macOS
carries both scripts: Menlo draws the ASCII and the box-drawing characters,
a CJK face draws the rest, and each run advances by terminal cells (a CJK
glyph is two) so the tables still line up.
"""

from __future__ import annotations

import unicodedata
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

_CJK_FONT_CANDIDATES = [
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/STHeiti Light.ttc",
    "/System/Library/Fonts/Supplemental/Songti.ttc",
]

DEMO = """\
$ cne init --profile demo --symbols 600519.SH,000001.SZ --days 5

=== [1/6] 在 data/cnequity-demo 准备 demo 湖 ===
data_root = …/data/cnequity-demo
config    = configs/cnequity.demo.toml
提示：这是和 `cne init --profile quick|full` 完全分开的湖 —— 可以随时删掉。

=== [2/6] 探测 TDX ===
正在探测 TDX 主机（第一个连通的服务器胜出）…
TDX 连接正常（1.2s）

=== [3/6] Instruments（demo 标的范围） ===
拉取完整标的清单，然后只保留 2 只 demo 标的…
已写入 2 条 instruments → curated/instruments/

=== [4/6] 交易日历 ===
demo 窗口：2026-09-14 → 2026-09-18（目标 5 个交易日）

=== [5/6] daily_bars（2 只标的） ===
日线 run …：status=success rows_written≈21

=== [6/6] 结果样例 ===
000001.SZ —— 最近几行：

┌───────────┬────────────┬───────┬───────┬───────┬───────┬──────────┬──────────────┐
│ symbol    │ trade_date │ open  │ high  │ low   │ close │ volume   │ source       │
├───────────┼────────────┼───────┼───────┼───────┼───────┼──────────┼──────────────┤
│ 000001.SZ │ 2026-09-18 │ 11.59 │ 11.82 │ 11.56 │ 11.70 │ 85303700 │ tdx_protocol │
│ 000001.SZ │ 2026-09-17 │ 11.68 │ 11.74 │ 11.57 │ 11.61 │ 69192500 │ tdx_protocol │
│ 000001.SZ │ 2026-09-16 │ 11.80 │ 11.84 │ 11.57 │ 11.70 │ 94962600 │ tdx_protocol │
│ 000001.SZ │ 2026-09-15 │ 11.82 │ 11.88 │ 11.77 │ 11.82 │ 75525100 │ tdx_protocol │
└───────────┴────────────┴───────┴───────┴───────┴───────┴──────────┴──────────────┘

demo 湖已就绪：data/cnequity-demo
配置写到：     configs/cnequity.demo.toml
"""

QUERY = """\
$ cne query --config configs/cnequity.demo.toml --sql \\
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
>>> from cnequity.query import load
>>> bars = load(
...     "daily_bars",
...     symbols=["600519.SH"],
...     start="2026-07-21",
...     end="2026-07-24",
...     data_root="data/cnequity-demo",
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


def _load_cjk_font(size: int) -> ImageFont.ImageFont | None:
    """A face that actually has the glyphs. Pillow does no font fallback.

    Without this every Chinese character in the transcript renders as a blank
    box — which is how the first Chinese screenshot came out.
    """
    for path in _CJK_FONT_CANDIDATES:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size=size, index=0)
            except OSError:
                continue
    return None


def _cells(text: str) -> int:
    """Terminal columns the text occupies: wide glyphs take two."""
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def _is_wide(ch: str) -> bool:
    return unicodedata.east_asian_width(ch) in "WF"


def _runs(text: str) -> list[tuple[str, bool]]:
    """Split into (chunk, wide) runs so each is drawn with the right face."""
    out: list[tuple[str, bool]] = []
    for ch in text:
        wide = _is_wide(ch)
        if out and out[-1][1] == wide:
            out[-1] = (out[-1][0] + ch, wide)
        else:
            out.append((ch, wide))
    return out


def _colorize_line(line: str) -> list[tuple[str, str]]:
    if line.startswith(("$ ", ">>> ", "... ")):
        return [(line, "#7ee787")]
    if line.startswith("==="):
        return [(line, "#79c0ff")]
    if "tdx_protocol" in line and "│" in line:
        parts = line.rsplit("tdx_protocol", 1)
        return [(parts[0], "#c9d1d9"), ("tdx_protocol", "#ffa657"), (parts[1], "#c9d1d9")]
    if line.startswith(("demo 湖", "配置写到", "data_root", "config    =", "提示：")):
        return [(line, "#a5d6ff")]
    if "正常" in line or "success" in line or "就绪" in line:
        return [(line, "#7ee787")]
    return [(line, "#c9d1d9")]


def render_terminal(text: str, out: Path, *, title: str) -> None:
    lines = text.rstrip("\n").splitlines()
    max_width_chars = max(_cells(line) for line in lines)
    font = _load_font(15)
    title_font = _load_font(13)
    sample = "M" * 10
    bbox = font.getbbox(sample)
    char_w = (bbox[2] - bbox[0]) / 10
    # Sized so one CJK glyph is exactly two Menlo cells, which is what keeps a
    # line of Chinese prose from pushing the table borders out of column.
    cjk_size = int(round(char_w * 2))
    cjk_font = _load_cjk_font(cjk_size)
    # Tall enough for the CJK face: at Menlo's own line height the Chinese
    # collides with the row above it.
    line_h = max((bbox[3] - bbox[1]) + 6, cjk_size + 4)

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
        cell = 0
        for chunk, color in _colorize_line(line):
            for run, wide in _runs(chunk):
                face = cjk_font if (wide and cjk_font is not None) else font
                # Anchored on the shared baseline: two faces at two sizes have
                # nothing else in common, and top-anchoring them staggers every
                # mixed line.
                draw.text(
                    (pad_x + int(char_w * cell), y + line_h - 6),
                    run,
                    font=face,
                    fill=color,
                    anchor="ls",
                )
                cell += _cells(run)
        y += line_h

    draw.rectangle([0, 0, width - 1, height - 1], outline="#30363d")
    out.parent.mkdir(parents=True, exist_ok=True)
    img.save(out, "PNG", optimize=True)
    print(f"wrote {out.relative_to(ROOT)} ({width}x{height})")


def main() -> None:
    render_terminal(DEMO, ASSETS / "cne-demo.png", title="cne init --profile demo")
    render_terminal(QUERY, ASSETS / "cne-query.png", title="cne query")
    render_terminal(LOAD, ASSETS / "cne-load.png", title="python — load()")


if __name__ == "__main__":
    main()
