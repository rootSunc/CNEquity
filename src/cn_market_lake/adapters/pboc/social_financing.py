"""中国人民银行 调查统计司 — 社会融资规模增量统计表.

https://www.pbc.gov.cn/diaochatongjisi/116219/116319/index.html

社会融资规模 is a PBOC statistic. This reads it from the PBOC's own published
statistical table rather than from a republisher, for a reason measured on
2026-08-01 rather than assumed: MOFCOM, which this replaces, was serving
2026-04 as 6245 after the PBOC had revised it to 6238, and its newest month was
2026-04 while the PBOC had published through 2026-06. Summing the PBOC series
for 2026-01..04 gives 154,500 — exactly the 15.45万亿 the PBOC states in prose —
while the republisher's copy summed to 154,507. The intermediary was propagating
a superseded vintage, so it is not a safe backup either (ADR-0003: a backup must
not silently write canonical).

The table is an Excel attachment, not prose: bilingual headers, an explicit
``单位：亿元人民币``, one row per month. Older years ship ``.xls``, recent ones
``.xlsx``; the layout is the same and this project already carries pandas,
openpyxl and xlrd to parse the Shenwan / CNI constituent workbooks.

Discovery and workbook parsing are shared with the 货币供应量 table — see
``_tables``.
"""

from __future__ import annotations

from cn_market_lake.adapters.pboc._tables import fetch_yearly_series

TOPIC = "社会融资规模"
TABLE_LABEL = "社会融资规模增量统计表"
# Column 0 is the month, column 1 the headline 增量; the rest are its components
# (人民币贷款, 委托贷款, …) and are not read today.
_VALUE_COLUMN = 1


def fetch_social_financing(*, config=None, start_year: int = 2015) -> list[dict]:
    """社融增量 as ``[{"obs_date", "value"}, ...]``, newest year first."""
    return fetch_yearly_series(
        topic=TOPIC,
        table_label=TABLE_LABEL,
        value_column=_VALUE_COLUMN,
        config=config,
        start_year=start_year,
    )
