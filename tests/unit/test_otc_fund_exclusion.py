"""519xxx is the open-end fund code space, not an exchange-traded product.

A "51" prefix swept all 188 of them into the tradable universe and TDX answered
with a NAV series: 436,533 rows in `daily_bars` carrying a close but zero volume
and zero turnover on every one, against 95-99% non-zero volume for every genuine
prefix beside it.
"""

from datetime import date

import polars as pl
import pytest

from cnequity.domain.symbols import is_etf_symbol


@pytest.mark.parametrize("code", ["519622", "519017", "519093", "519110", "519683"])
def test_open_end_fund_codes_are_not_exchange_traded(code):
    assert is_etf_symbol(code, "SH") is False


@pytest.mark.parametrize(
    ("code", "exchange"),
    [
        ("510300", "SH"),  # 沪深300 ETF
        ("511990", "SH"),  # 华宝添益
        ("518880", "SH"),  # 黄金 ETF
        ("520820", "SH"),  # 恒指通
        ("563510", "SH"),
        ("588000", "SH"),  # 科创 50 ETF
        ("159915", "SZ"),  # 创业板 ETF
        ("160516", "SZ"),  # LOF — quotes on-exchange, so it belongs
    ],
)
def test_every_genuine_exchange_traded_prefix_still_classifies(code, exchange):
    assert is_etf_symbol(code, exchange) is True


def test_the_check_finds_a_price_with_no_trade_behind_it(tmp_path):
    """A price with no volume and no turnover is a NAV, not a quote."""
    from cnequity.config import Config
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.quality.cross_checks import untraded_instrument_findings

    config = Config(data_root=tmp_path)
    sessions = [date(2026, 6, 1) + timedelta_days for timedelta_days in _weekdays(25)]
    for day in sessions:
        part = config.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        frame = pl.DataFrame(
            {
                "symbol": ["519622.SH", "510300.SH"],
                "trade_date": [day, day],
                "open": [1.0, 4.0],
                "high": [1.0, 4.0],
                "low": [1.0, 4.0],
                "close": [1.0, 4.0],
                # The fund never prints; the ETF does.
                "volume": [0, 1_000_000],
                "amount": [0.0, 4_000_000.0],
            }
        )
        with_provenance(
            frame, source="tdx_protocol", data_version=data_version_for("daily_bars")
        ).write_parquet(part / "part-merged.parquet")

    finding = untraded_instrument_findings(config, sessions[-1])[0]
    assert finding["symbols"] == 1
    assert finding["sample"][0]["symbol"] == "519622.SH"


def _weekdays(count: int):
    from datetime import timedelta

    out, day = [], 0
    while len(out) < count:
        candidate = date(2026, 6, 1) + timedelta(days=day)
        if candidate.weekday() < 5:
            out.append(timedelta(days=day))
        day += 1
    return out


def _lake_with_source(tmp_path, dataset, partition, source):
    from cnequity.domain.schemas import data_version_for, with_provenance

    part = tmp_path / "curated" / dataset / f"{partition}=2026-09-04"
    part.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2026, 9, 4)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
            "amount": [1.0],
        }
    )
    with_provenance(frame, source=source, data_version=data_version_for(dataset)).write_parquet(
        part / "part-merged.parquet"
    )


def test_a_source_with_no_policy_entry_is_an_error(tmp_path):
    """Terms that cannot be looked up are worse than terms that are restrictive.

    `bse` was the real instance: 654 rows in daily_bars and two sub-labels in
    trading_status, with no entry in SOURCES.yml, so `cne sources policy bse`
    answered nothing at all. It is registered now, hence the invented label here.
    """
    from cnequity.config import Config
    from cnequity.quality.cross_checks import undeclared_source_findings

    _lake_with_source(tmp_path, "daily_bars", "trade_date", "not_a_registered_vendor")
    findings = undeclared_source_findings(Config(data_root=tmp_path))
    unregistered = [f for f in findings if f["check"] == "unregistered_source"]
    assert unregistered and unregistered[0]["severity"] == "error"
    assert "not_a_registered_vendor" in unregistered[0]["sources"]


def test_a_provenance_sublabel_inherits_its_base_policy(tmp_path):
    """`eastmoney_cached` is EastMoney's terms, not an unknown source."""
    from cnequity.config import Config
    from cnequity.quality.cross_checks import undeclared_source_findings

    _lake_with_source(tmp_path, "daily_bars", "trade_date", "eastmoney_cached")
    findings = undeclared_source_findings(Config(data_root=tmp_path))
    assert not [f for f in findings if f["check"] == "unregistered_source"]


def test_a_registered_source_off_its_route_is_reported_separately(tmp_path):
    """policies_for_dataset omits terms that do apply, which is the whole risk.

    `cninfo` is a registered vendor with its own terms, and it has no business
    writing daily bars.
    """
    from cnequity.config import Config
    from cnequity.quality.cross_checks import undeclared_source_findings

    _lake_with_source(tmp_path, "daily_bars", "trade_date", "cninfo")
    findings = undeclared_source_findings(Config(data_root=tmp_path))
    unrouted = [f for f in findings if f["check"] == "unrouted_source"]
    assert unrouted and unrouted[0]["severity"] == "warning"
    assert "cninfo" in unrouted[0]["sources"]


def test_a_declared_recovery_chain_member_is_on_its_route(tmp_path):
    """Three slots could not describe an eight-source chain.

    A tip key TDX misses is chased through the exchange board files, BSE,
    EastMoney, Sina and THS in turn, and each stamps its own `source`. They are
    real dependencies with real terms, so they are declared
    (`supplementary_sources`) rather than left to read as strays.
    """
    from cnequity.config import Config
    from cnequity.domain.datasets import DATASETS
    from cnequity.quality.cross_checks import undeclared_source_findings

    assert "exchange" in DATASETS["daily_bars"].supplementary_sources

    for source in ("exchange", "bse", "ths_official"):
        _lake_with_source(tmp_path / source, "daily_bars", "trade_date", source)
        assert undeclared_source_findings(Config(data_root=tmp_path / source)) == [], source


def test_a_dataset_read_only_from_its_declared_route_is_quiet(tmp_path):
    from cnequity.config import Config
    from cnequity.quality.cross_checks import undeclared_source_findings

    _lake_with_source(tmp_path, "daily_bars", "trade_date", "tdx_protocol")
    assert undeclared_source_findings(Config(data_root=tmp_path)) == []


def test_every_source_present_in_this_lake_can_be_looked_up():
    """A compliance matrix that cannot speak for a stored source has a blind spot.

    Guards the registry itself rather than one label: any future adapter whose
    provenance string resolves to nothing fails here.
    """
    from cnequity.compliance.source_policy import load_source_policies
    from cnequity.quality.cross_checks import _policy_base

    registered = set(load_source_policies())
    for label in ("bse", "bse_public_announcement", "bse_listed_company_snapshot"):
        assert _policy_base(label, registered) == "bse", label
