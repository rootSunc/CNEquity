"""Issuer-reviewed holder-specific cash rights from exact original notices.

Only known PDFs can enter this registry. These facts separate actual pretax
receipts from the share-weighted ex-price reference amount in the same PDF.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

_REVIEWED = {
    ("600025.SH", date(2019, 7, 5)): {
        "announcement_id": "1206408689",
        "pdf_sha256": "804c45fc429a75413979b70c82451e065b82cb3e9ce54ff26f9705df45ba4b31",
        "old_cash": Decimal("0.178"),
        "tradable_a": Decimal("0.178"),
        "other_class": "founder",
        "other_cash": Decimal("0.14713"),
        "payment_date": date(2019, 7, 5),
    },
    ("600025.SH", date(2020, 6, 19)): {
        "announcement_id": "1207919396",
        "pdf_sha256": "54d9f9ade9baabe182ac47634cf269033c9b010928d0b6aeafb135346a57b884",
        "old_cash": Decimal("0.15"),  # ex-price average, not any holder's actual cash
        "tradable_a": Decimal("0.18"),
        "other_class": "founder",
        "other_cash": Decimal("0.14913"),
        "payment_date": date(2020, 6, 19),
    },
    ("600900.SH", date(2017, 7, 14)): {
        "announcement_id": "1203690729",
        "pdf_sha256": "a48f933327deb9993bb7a3ddcb41ddbacd32cbda16b20623aa97a8bf8384fc41",
        "old_cash": Decimal("0.725"),
        "tradable_a": Decimal("0.725"),
        "other_class": "strategic_holder",
        "other_cash": Decimal("0.65"),
        "payment_date": date(2017, 7, 14),
    },
    ("600989.SH", date(2019, 9, 27)): {
        "announcement_id": "1206941900",
        "pdf_sha256": "bbcb897f303105ec71ed3cc539c94eab457b13a6eb204c26faeb4759a4e977e3",
        "old_cash": Decimal("0.28"),
        "tradable_a": Decimal("0.32091"),
        "other_class": "restricted_a",
        "other_cash": Decimal("0.27545"),
        "payment_date": date(2019, 9, 27),
    },
    ("600989.SH", date(2020, 6, 4)): {
        "announcement_id": "1207872552",
        "pdf_sha256": "ee98586ab004942e8b03a7a9ed78bbd33455d7a2fd4366f3fa40f3c31ef0f118",
        "old_cash": Decimal("0.28"),
        "tradable_a": Decimal("0.32091"),
        "other_class": "restricted_a",
        "other_cash": Decimal("0.27545"),
        "payment_date": date(2020, 6, 4),
    },
    ("600989.SH", date(2021, 5, 20)): {
        "announcement_id": "1209953094",
        "pdf_sha256": "18570ede629e1cadef96c4ce83a8a44b8121a5b01f0c09b9679c10540718d40b",
        "old_cash": Decimal("0.28"),
        "tradable_a": Decimal("0.32091"),
        "other_class": "restricted_a",
        "other_cash": Decimal("0.26472"),
        "payment_date": date(2021, 5, 20),
    },
    ("600989.SH", date(2022, 5, 12)): {
        "announcement_id": "1213274374",
        "pdf_sha256": "77c7945236346f652800a271f942a9610c3960538e011146aedd9860afb39bc8",
        "old_cash": Decimal("0.28"),
        "tradable_a": Decimal("0.3210"),
        "other_class": "restricted_a",
        "other_cash": Decimal("0.2648"),
        "payment_date": date(2022, 5, 12),
    },
    ("600989.SH", date(2022, 12, 27)): {
        "announcement_id": "1215408119",
        "pdf_sha256": "d0ce17862db32ba2dde975b62dcc2286d16188ee59e287589f1969edaad909d0",
        "old_cash": Decimal("0.14"),
        "tradable_a": Decimal("0.1841"),
        "other_class": "restricted_a",
        "other_cash": Decimal("0.1216"),
        "payment_date": date(2022, 12, 27),
    },
}


def _amounts(text: str, symbol: str) -> tuple[Decimal, Decimal] | None:
    from cnequity.adapters.cninfo.payment_notices import _compact_text

    compact = _compact_text(text)
    if symbol == "600025.SH":
        match = re.search(
            r"社会公众股每股实际分派现金红利([0-9.]+)元；"
            r"上市前原三家大股东.{0,100}?每股实际分派现金红利([0-9.]+)元",
            compact,
        )
        return (Decimal(match[1]), Decimal(match[2])) if match else None
    if symbol == "600900.SH":
        match = re.search(
            r"股，每股派现([0-9.]+)元；其他股东持有的[0-9,]+股，每股派现([0-9.]+)元",
            compact,
        )
        return (Decimal(match[2]), Decimal(match[1])) if match else None
    if symbol == "600989.SH":
        header = compact.split("相关日期", 1)[0]
        public = re.search(
            r"(?:无限售股股东|流通股股东).*?每股(?:分配)?现金红利([0-9.]+)元\(含税\)",
            header,
        )
        restricted = re.search(
            r"(?<!无)限售股股东.*?每股(?:分配)?现金红利([0-9.]+)元\(含税\)",
            header,
        )
        if public is not None and restricted is not None:
            return Decimal(public[1]), Decimal(restricted[1])
    return None


def _a_share_record_date(text: str, ex_date: date, payment_date: date) -> date | None:
    """Accept only a consistent, explicitly labelled A-share date table."""
    from cnequity.adapters.cninfo.payment_notices import _compact_text

    compact = _compact_text(text)
    table = re.compile(
        r"股权登记日最后交易日除权\(息\)日(?:.{0,40}?)现金红利发放日"
        r"(?:Ａ|A)股(.{0,85})"
    )
    rows: set[date] = set()
    for match in table.finditer(compact):
        segment = re.split(r"差异化|[一二三四五六]、|(?:H|Ｈ)股", match.group(1), maxsplit=1)[0]
        values = re.findall(
            r"20\d{2}/(?:1[0-2]|0?[1-9])/(?:3[01]|[12]\d|0?[1-9])"
            r"(?=20\d{2}/|[^0-9]|$)",
            segment,
        )
        if len(values) != 3:
            continue
        try:
            record, ex, payment = (
                date(*(int(part) for part in value.split("/"))) for value in values
            )
        except ValueError:
            continue
        if record < ex and ex == ex_date and payment == payment_date:
            rows.add(record)
    return next(iter(rows)) if len(rows) == 1 else None


def reviewed_holder_notice(old: dict, text: str, announcement_id: str, digest: str) -> dict | None:
    """Return exact reviewed classes or keep a conflict blocked."""
    from cnequity.adapters.cninfo.payment_notices import _compact_text, _sh_a_share_table_dates

    key = (str(old["symbol"]), old["ex_date"])
    expected = _REVIEWED.get(key)
    if expected is None or expected["announcement_id"] != announcement_id:
        return None
    if expected["pdf_sha256"] != digest:
        return None
    if abs(Decimal(str(old["cash_dividend"])) - expected["old_cash"]) > Decimal("0.00000001"):
        return None
    compact = _compact_text(text)
    if not re.search(r"(?:股票|证券)代码[：:]?" + key[0].split(".", 1)[0], compact):
        return None
    dates = _sh_a_share_table_dates(compact)
    if dates != (key[1], expected["payment_date"]):
        return None
    record_date = _a_share_record_date(text, key[1], expected["payment_date"])
    if record_date is None:
        return None
    amounts = _amounts(text, key[0])
    if amounts != (expected["tradable_a"], expected["other_cash"]):
        return None
    return {
        "symbol": key[0],
        "ex_date": key[1],
        "record_date": record_date,
        "payment_date": expected["payment_date"],
        "cash_dividend": float(expected["tradable_a"]),
        "holder_classes": {
            "tradable_a": str(expected["tradable_a"]),
            expected["other_class"]: str(expected["other_cash"]),
        },
    }


def save_holder_evidence(meta_root: Path, run_id: str, symbol: str, entries: list[dict]) -> Path:
    """Keep reviewed class rights as immutable source evidence for the later table migration."""
    payload = {
        "schema": "cnequity.holder_cash_evidence.v1",
        "run_id": run_id,
        "symbol": symbol,
        "entries": sorted(entries, key=lambda item: item["ex_date"]),
    }
    raw = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    digest = hashlib.sha256(raw).hexdigest()
    root = Path(meta_root) / "decision_cash_entitlements"
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{digest}.json"
    fd, temp_name = tempfile.mkstemp(dir=root, prefix=".holder-cash-", suffix=".tmp")
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, target)
        except FileExistsError:
            pass
        except OSError:
            # exFAT/FAT and some network shares refuse hard links. The path is
            # content addressed, so a racing writer lands the same bytes, and a
            # real failure resurfaces from the rename. (Adapters sit below
            # storage, so storage.atomic.publish_once is not importable here.)
            if not os.path.lexists(target):
                os.replace(temp, target)
        if target.read_bytes() != raw:
            raise ValueError(f"holder cash evidence hash collision: {target}")
    finally:
        temp.unlink(missing_ok=True)
    return target
