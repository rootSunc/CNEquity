#!/usr/bin/env python3
"""Measure survivorship bias in this lake, and draw it.

The question: what does a backtest report when its universe is "the stocks that
exist today" instead of "the stocks that existed then"? Every vendor that serves
a current roster answers the first question while looking like it answered the
second, because the names that went to zero are simply absent — not zero,
absent, which is why nothing in the output ever looks wrong.

This measures it on real bars rather than arguing about it. For each window it
buys every stock trading on the start date, equal-weighted, and holds:

* **complete** — every name, with the ones that stopped trading valued at their
  final bar.
* **survivors only** — the same basket minus every name that has since
  delisted, which is the basket a current-roster vendor can build.

The gap between the two is the bias, and it is a floor rather than an estimate,
for three reasons that all point the same way:

1. A delisted name is carried to its last printed bar. That bar is usually
   before a long suspension and well above what a holder actually recovered, so
   the complete basket's loss is understated.
2. Only names with an exact adjustment factor are counted (``adj_is_exact``).
   Factor coverage is thinner for delisted names, so some of the worst outcomes
   are excluded from both baskets.
3. This lake's own delisted coverage may be incomplete. Anything still missing
   sits in neither basket, which shrinks the measured gap rather than widening
   it.

Usage::

    python scripts/survivorship_gap.py                    # table only
    python scripts/survivorship_gap.py --svg docs/assets/survivorship-gap.svg
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import duckdb  # noqa: E402

from ashare_lake.config import load_config  # noqa: E402
from ashare_lake.query.views import ensure_duckdb_views  # noqa: E402

DEFAULT_CONFIG = ROOT / "configs/ashare-lake.toml"

# Mid-year anchors so a window never starts on a New Year gap, and five-year
# holds because that is the horizon a backtest is usually defending.
DEFAULT_WINDOWS: tuple[tuple[str, str], ...] = (
    ("2016-06-30", "2021-06-30"),
    ("2018-06-29", "2023-06-30"),
    ("2020-06-30", "2025-06-30"),
)

_MEASURE_SQL = """
WITH universe AS (
    SELECT b.symbol, i.delist_date
    FROM daily_bars b
    JOIN instruments i USING (symbol)
    WHERE b.trade_date = ? AND i.asset_type = 'stock'
),
held AS (
    SELECT
        u.symbol,
        u.delist_date,
        first(a.hfq_close ORDER BY a.trade_date) AS entry,
        last(a.hfq_close ORDER BY a.trade_date)  AS exit,
        max(a.trade_date)                        AS last_bar,
        bool_and(a.adj_is_exact)                 AS exact
    FROM universe u
    JOIN daily_bars_adj a ON a.symbol = u.symbol
    WHERE a.trade_date BETWEEN ? AND ?
    GROUP BY u.symbol, u.delist_date
)
SELECT
    count(*) FILTER (WHERE exact)                                        AS names,
    count(*) FILTER (WHERE exact AND delist_date IS NOT NULL)            AS delisted,
    avg(exit / entry) FILTER (WHERE exact)                               AS complete,
    avg(exit / entry) FILTER (WHERE exact AND delist_date IS NULL)       AS survivors
FROM held
WHERE entry > 0
"""


@dataclass(frozen=True)
class Window:
    start: date
    end: date
    names: int
    delisted: int
    complete: float
    survivors: float

    @property
    def gap_pp(self) -> float:
        """Percentage points of return the missing names would have cost."""
        return (self.survivors - self.complete) * 100

    @property
    def inflation_pct(self) -> float:
        """How much higher the biased return reads, relative to the true one.

        Undefined against a basket that lost money or broke even, which is not
        a case to rank — return 0 so it never wins ``max``.
        """
        if self.complete <= 0:
            return 0.0
        return (self.survivors / self.complete - 1) * 100

    @property
    def label(self) -> str:
        return f"{self.start.year}–{self.end.year}"


def measure(con: duckdb.DuckDBPyConnection, start: date, end: date) -> Window:
    names, delisted, complete, survivors = con.execute(_MEASURE_SQL, [start, start, end]).fetchone()
    if not names:
        raise SystemExit(
            f"No stocks with bars on {start}. Is the lake backfilled that far? "
            "Check `asl status` / coverage_start for daily_bars."
        )
    return Window(
        start=start,
        end=end,
        names=names,
        delisted=delisted,
        complete=complete - 1,
        survivors=survivors - 1,
    )


def print_table(windows: list[Window]) -> None:
    print(f"{'window':<12}{'names':>7}{'delisted':>10}{'complete':>11}{'survivors':>11}{'gap':>9}")
    print("-" * 60)
    for w in windows:
        print(
            f"{w.label:<12}{w.names:>7}{w.delisted:>10}"
            f"{w.complete * 100:>10.1f}%{w.survivors * 100:>10.1f}%{w.gap_pp:>8.1f}pp"
        )
    print()
    # Ranked by relative inflation, not by percentage points. The same 6pp is a
    # rounding error on a 40% decade and a doubling on a 6% one, and it is the
    # ratio that says how wrong a backtest's conclusion could be.
    worst = max(windows, key=lambda w: w.inflation_pct)
    print(
        f"Worst case {worst.label}: dropping {worst.delisted} delisted names lifts the "
        f"equal-weight return from {worst.complete * 100:.1f}% to "
        f"{worst.survivors * 100:.1f}% — {worst.gap_pp:.1f}pp, or "
        f"{worst.inflation_pct:.0f}% higher than it actually was."
    )
    print("This is a floor, not an estimate: see the module docstring for why.")


# --- chart -----------------------------------------------------------------
#
# Hand-written SVG rather than a plotting library. The lake has no chart
# dependency and this figure does not justify adding one; the same reasoning
# that keeps the dashboard's assets self-contained applies to a README image.
# Colours are mid-tone and the text is a neutral grey, so the file reads on
# GitHub's light and dark themes without needing either to be detected.

_W, _H = 760, 380
_PAD_L, _PAD_R, _PAD_T, _PAD_B = 56, 20, 64, 92
_COMPLETE = "#2563eb"
_SURVIVORS = "#f59e0b"
_TEXT = "#6b7280"
_AXIS = "#9ca3af"


def _svg(windows: list[Window]) -> str:
    plot_w = _W - _PAD_L - _PAD_R
    plot_h = _H - _PAD_T - _PAD_B
    top = max(max(w.complete, w.survivors) for w in windows) * 100
    # Round the axis up to a clean 10% so the tick labels are readable numbers.
    top = max(10.0, 10 * ((top // 10) + 1))

    def y_of(pct: float) -> float:
        return _PAD_T + plot_h - (pct / top) * plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {_W} {_H}" '
        f'width="{_W}" height="{_H}" font-family="-apple-system, BlinkMacSystemFont, '
        f"'Segoe UI', Helvetica, Arial, sans-serif\">",
        f'<text x="{_PAD_L}" y="26" font-size="16" font-weight="600" fill="{_TEXT}">'
        "Survivorship bias in an equal-weight A-share hold</text>",
        f'<text x="{_PAD_L}" y="46" font-size="12" fill="{_TEXT}">'
        "Same basket, same dates — the only difference is whether the names that "
        "delisted are still in it.</text>",
    ]

    for tick in range(0, int(top) + 1, 10):
        y = y_of(tick)
        parts.append(
            f'<line x1="{_PAD_L}" y1="{y:.1f}" x2="{_W - _PAD_R}" y2="{y:.1f}" '
            f'stroke="{_AXIS}" stroke-width="0.5" stroke-opacity="0.35"/>'
        )
        parts.append(
            f'<text x="{_PAD_L - 8}" y="{y + 4:.1f}" font-size="11" fill="{_TEXT}" '
            f'text-anchor="end">{tick}%</text>'
        )

    slot = plot_w / len(windows)
    bar_w = min(70.0, slot / 3)
    for i, w in enumerate(windows):
        centre = _PAD_L + slot * (i + 0.5)
        for offset, value, colour in (
            (-bar_w * 0.55, w.complete * 100, _COMPLETE),
            (bar_w * 0.55, w.survivors * 100, _SURVIVORS),
        ):
            x = centre + offset - bar_w / 2
            y = y_of(max(value, 0))
            height = abs(y_of(0) - y)
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
                f'height="{height:.1f}" rx="2" fill="{colour}"/>'
            )
            parts.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{y - 6:.1f}" font-size="12" '
                f'font-weight="600" fill="{colour}" text-anchor="middle">'
                f"{value:.1f}%</text>"
            )
        parts.append(
            f'<text x="{centre:.1f}" y="{_PAD_T + plot_h + 20:.1f}" font-size="13" '
            f'fill="{_TEXT}" text-anchor="middle">{w.label}</text>'
        )
        parts.append(
            f'<text x="{centre:.1f}" y="{_PAD_T + plot_h + 37:.1f}" font-size="11" '
            f'fill="{_TEXT}" text-anchor="middle" opacity="0.8">'
            f"{w.names} names, {w.delisted} later delisted</text>"
        )

    legend_y = _H - 30
    for x, colour, text in (
        (_PAD_L, _COMPLETE, "complete universe (delisted names kept)"),
        (_PAD_L + 320, _SURVIVORS, "survivors only (what a current roster gives you)"),
    ):
        parts.append(
            f'<rect x="{x}" y="{legend_y - 9}" width="11" height="11" rx="2" fill="{colour}"/>'
        )
        parts.append(
            f'<text x="{x + 18}" y="{legend_y}" font-size="12" fill="{_TEXT}">{text}</text>'
        )

    parts.append(
        f'<text x="{_PAD_L}" y="{_H - 8}" font-size="10.5" fill="{_TEXT}" opacity="0.75">'
        "Delisted names carried to their last printed bar, so the real gap is wider. "
        "Generated by scripts/survivorship_gap.py</text>"
    )
    parts.append("</svg>")
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--window",
        action="append",
        metavar="START:END",
        help="Holding window as YYYY-MM-DD:YYYY-MM-DD; repeatable. "
        "Defaults to three overlapping five-year holds.",
    )
    parser.add_argument("--svg", default=None, help="Write the chart here.")
    args = parser.parse_args()

    pairs = [tuple(w.split(":", 1)) for w in args.window] if args.window else list(DEFAULT_WINDOWS)
    con = duckdb.connect(str(ensure_duckdb_views(load_config(args.config))), read_only=True)
    try:
        windows = [
            measure(con, date.fromisoformat(start), date.fromisoformat(end)) for start, end in pairs
        ]
    finally:
        con.close()

    print_table(windows)
    if args.svg:
        out = Path(args.svg)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_svg(windows), encoding="utf-8")
        print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
