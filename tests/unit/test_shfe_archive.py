"""Annual workbook prices must retain the same daily-bar semantics."""

import io
import json
from zipfile import ZipFile

import openpyxl
import pytest
from tests.catalog_seed import compact_into_lake

from cnequity.adapters.futures_exchange import shfe_archive
from cnequity.adapters.futures_exchange.common import FuturesPayloadError
from cnequity.adapters.futures_exchange.shfe_archive import HEADER, parse_workbook
from cnequity.cli.backfill_cmds import _run_shfe_annual_archive
from cnequity.config import Config
from cnequity.query.parquet_scan import root_parquet_files
from cnequity.steps.derivative_archive import import_shfe_annual
from cnequity.storage.parquet import StagingWriter


def workbook(rows, header=HEADER):
    book = openpyxl.Workbook()
    sheet = book.active
    for row in [("所内合约行情报表",), ("Daily Data",), header, ("Contract",), *rows]:
        sheet.append(row)
    out = io.BytesIO()
    book.save(out)
    return out.getvalue()


def bar(code="cu2608", day="20260803", volume=10):
    return [code, day, 100, 100, 100, 110, 90, 105, 102, 5, 2, volume, 1.234, 20]


def test_mixed_workbook_keeps_kind_and_missing_fields():
    option = bar("bc2608C100000", volume=0)
    option[8] = 0
    result = parse_workbook(workbook([bar(), option]), year=2026)
    future = result["futures_bars"].row(0, named=True)
    assert future["amount"] == 12340
    assert future["oi_change"] is None
    option = result["option_bars"].row(0, named=True)
    assert option["symbol"] == "BC2608C100000.INE"
    assert option["settle"] == 0
    assert option["close"] is None
    assert option["exercise_volume"] is None
    assert option["delta"] is None


@pytest.mark.parametrize("rows", [[bar(day="20250803")], [bar(volume=1.5)], [bar(), bar()]])
def test_bad_year_count_or_duplicate_is_rejected(rows):
    with pytest.raises((FuturesPayloadError, ValueError)):
        parse_workbook(workbook(rows), year=2026)


def test_old_counting_basis_and_unknown_header_are_rejected():
    with pytest.raises(FuturesPayloadError):
        parse_workbook(workbook([bar()]), year=2019)
    with pytest.raises(FuturesPayloadError):
        parse_workbook(workbook([bar()], header=["changed"]), year=2026)


def test_verified_2004_layout_keeps_published_one_sided_count():
    row = bar(code="cu0401", day="20040102", volume=405)
    unopened = bar(code="al0412", day="20040102", volume=0)
    unopened[8] = None
    unopened[13] = 0
    parsed = parse_workbook(workbook([row, unopened]), year=2004)["futures_bars"]
    assert parsed["volume"].to_list() == [405]


@pytest.mark.parametrize("year", [2002, 2003, 2005, 2006, 2007, 2008])
def test_verified_early_layout_uses_archive_amount_scale(year):
    parsed = parse_workbook(
        workbook([bar(code=f"cu{year % 100:02d}01", day=f"{year}0104")]), year=year
    )["futures_bars"]
    assert parsed["amount"].to_list() == [12340]


def test_archive_quarantine_requires_explicit_rejection_ledger():
    bad = bar("cu2608")
    bad[6] = 108  # Low exceeds close while the source prints all OHLC fields.
    good = bar("al2608")
    payload = workbook([good, bad])
    with pytest.raises(FuturesPayloadError, match="invalid futures_bars"):
        parse_workbook(payload, year=2026)
    rejected = []
    frame = parse_workbook(payload, year=2026, rejected=rejected)["futures_bars"]
    assert frame["symbol"].to_list() == ["AL2608.SHF"]
    assert len(rejected) == 1 and rejected[0]["symbol"] == "CU2608.SHF"


@pytest.mark.parametrize("year", [2020, 2021, 2022, 2023, 2024])
def test_grouped_xls_code_and_note_separator(monkeypatch, year):
    class Sheet:
        name = "2024 sample"

        def __init__(self):
            self.rows = [
                [],
                [],
                list(shfe_archive.GROUPED_XLS_HEADER),
                [""] * 14,
                bar(f"cu{year % 100:02d}01", f"{year}0102"),
                bar("", f"{year}0103"),
                [""] * 14,
                ["成交量、持仓量 双边计算" if year == 2019 else "1、 notes"] + [""] * 13,
            ]
            self.nrows = len(self.rows)

        def row_values(self, index):
            return self.rows[index].copy()

    class Book:
        def sheets(self):
            return [Sheet()]

        def release_resources(self):
            pass

    monkeypatch.setattr(shfe_archive.xlrd, "open_workbook", lambda **_: Book())
    frame = shfe_archive.parse_grouped_xls(b"sample", year=year)["futures_bars"]
    assert frame.height == 2
    assert frame["symbol"].to_list() == [f"CU{year % 100:02d}01.SHF"] * 2
    assert frame["trade_date"].to_list()[1].isoformat() == f"{year}-01-03"


def test_2021_april_grouped_xls_has_an_extra_product_column(monkeypatch):
    class Sheet:
        name = "April 2021"

        def __init__(self):
            self.rows = [
                [],
                [],
                ["品种", *shfe_archive.GROUPED_XLS_HEADER],
                ["cu_f", *bar("cu2104", "20210401")],
                ["", *bar("", "20210402")],
                [""] * 15,
            ]
            self.nrows = len(self.rows)

        def row_values(self, index):
            return self.rows[index].copy()

    class Book:
        def sheets(self):
            return [Sheet()]

        def release_resources(self):
            pass

    monkeypatch.setattr(shfe_archive.xlrd, "open_workbook", lambda **_: Book())
    frame = shfe_archive.parse_grouped_xls(b"sample", year=2021)["futures_bars"]
    assert frame.height == 2
    assert frame["symbol"].to_list() == ["CU2104.SHF"] * 2


@pytest.mark.parametrize("year", [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
def test_two_sided_grouped_xls_starts_data_immediately_after_header(monkeypatch, year):
    class Sheet:
        name = "early two-sided sample"

        def __init__(self):
            self.rows = [
                [],
                [],
                list(shfe_archive.GROUPED_XLS_HEADER),
                bar(f"cu{year % 100:02d}03", f"{year}0301"),
                bar("", f"{year}0304"),
                [""] * 14,
                ["成交量、持仓量 双边计算"] + [""] * 13,
                ["", "成交量、持仓量单位为手，双边计算"] + [""] * 12,
            ]
            self.nrows = len(self.rows)

        def row_values(self, index):
            return self.rows[index].copy()

    class Book:
        def sheets(self):
            return [Sheet()]

        def release_resources(self):
            pass

    monkeypatch.setattr(shfe_archive.xlrd, "open_workbook", lambda **_: Book())
    frame = shfe_archive.parse_grouped_xls(b"sample", year=year)["futures_bars"]
    assert frame["symbol"].to_list() == [f"CU{year % 100:02d}03.SHF"] * 2
    assert frame["volume"].to_list() == [5, 5]
    assert frame["open_interest"].to_list() == [10, 10]
    assert frame["amount"].to_list() == [6170, 6170]


def test_annual_import_retains_zip_and_protects_full_daily_rows(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    dataset = "futures_bars"
    full_daily = parse_workbook(workbook([bar()]), year=2026)[dataset].with_columns(
        shfe_archive.pl.lit(7).alias("oi_change"),
        shfe_archive.pl.lit("v1").alias("data_version"),
    )
    StagingWriter(config.staging_root).write_batch(dataset, "daily", "one", full_daily)
    compact_into_lake(config, dataset, "daily")

    path = tmp_path / "2026.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("august.xlsx", workbook([bar(), bar("al2608", "20260804")]))
    result = import_shfe_annual(config, "annual", dataset, path, year=2026)
    assert result["status"] == "success"
    assert result["coverage_status"] == "partial_fields"
    assert result["protected_daily_rows"] == 1
    assert result["rows_written"] == 1
    assert result["retained_path"].endswith(".zip")
    assert (config.meta_root / "derivatives" / "annual_archives").exists()
    staged = StagingWriter(config.staging_root).list_run_files(dataset, "annual")
    assert len(staged) == 1
    assert shfe_archive.pl.read_parquet(staged[0])["symbol"].to_list() == ["AL2608.SHF"]
    compact_into_lake(config, dataset, "annual")
    rows = shfe_archive.pl.concat(
        [shfe_archive.pl.read_parquet(p) for p in root_parquet_files(config.curated_root / dataset)]
    )
    assert rows.height == 2
    assert rows.filter(shfe_archive.pl.col("symbol") == "CU2608.SHF")["oi_change"][0] == 7
    assert rows.filter(shfe_archive.pl.col("symbol") == "AL2608.SHF")["oi_change"][0] is None

    revised = tmp_path / "2026-revised.zip"
    changed = bar("al2608", "20260804")
    changed[7] = 106
    with ZipFile(revised, "w") as archive:
        archive.writestr("august.xlsx", workbook([changed]))
    correction = import_shfe_annual(config, "revision", dataset, revised, year=2026)
    assert correction["rows_written"] == 1
    compact_into_lake(config, dataset, "revision")
    rows = shfe_archive.pl.concat(
        [shfe_archive.pl.read_parquet(p) for p in root_parquet_files(config.curated_root / dataset)]
    )
    assert rows.filter(shfe_archive.pl.col("symbol") == "AL2608.SHF")["close"][0] == 106


def test_bad_late_member_keeps_valid_import_slice_with_warning(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    path = tmp_path / "2026.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("a.xlsx", workbook([bar()]))
        archive.writestr("b.xlsx", workbook([bar("al2608")], header=["changed"]))
    result = import_shfe_annual(config, "annual", "futures_bars", path, year=2026)
    assert result["status"] == "warning"
    assert result["rows_written"] == 1
    assert "b.xlsx" in result["errors"][0]
    assert len(StagingWriter(config.staging_root).list_run_files("futures_bars", "annual")) == 1
    receipt = json.loads(
        (config.meta_root / "derivatives" / "annual_archive_imports" / "annual.json").read_text()
    )
    assert receipt["coverage_status"] == "partial_fields"
    assert receipt["errors"]


def test_annual_import_uses_manifest_and_compact_without_network(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    config.futures_enabled = True
    path = tmp_path / "2026.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("a.xlsx", workbook([bar()]))
    result = _run_shfe_annual_archive(
        config, "futures_bars", path, 2026, start=None, end=None, source_url=None
    )
    assert result["status"] == "success"
    assert result["compact"]["status"] == "success"
    assert root_parquet_files(config.curated_root / "futures_bars")
