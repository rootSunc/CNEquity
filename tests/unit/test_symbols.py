from cn_market_lake.domain.symbols import (
    format_symbol,
    is_all_a_symbol,
    is_cdr_symbol,
    is_etf_symbol,
    parse_symbol,
)


def test_format_symbol():
    assert format_symbol("600519", "SH") == "600519.SH"


def test_parse_symbol():
    info = parse_symbol("000001.SZ")
    assert info.code == "000001"
    assert info.exchange == "SZ"


def test_universe_filter():
    assert is_all_a_symbol("600519", "SH")
    assert is_all_a_symbol("300750", "SZ")
    assert not is_all_a_symbol("810001", "SH")
    # ETFs are not all_a (PREFIX_WHITELIST omits 51/15/…)
    assert not is_all_a_symbol("510300", "SH")
    assert not is_all_a_symbol("159915", "SZ")
    # BSE current + legacy NEEQ numbering (83xxxx must not hit the SH/SZ 81–89 ban).
    assert is_all_a_symbol("920001", "BJ")
    assert is_all_a_symbol("430001", "BJ")
    assert is_all_a_symbol("830001", "BJ")
    assert is_all_a_symbol("870001", "BJ")


def test_cdr_symbol():
    assert is_cdr_symbol("689009", "SH")
    assert not is_cdr_symbol("688981", "SH")
    assert not is_cdr_symbol("600519", "SH")
    # CDR segment exists only on SH
    assert not is_cdr_symbol("689009", "SZ")
    # CDRs stay inside the fetch scope; only the all_a universe excludes them
    assert is_all_a_symbol("689009", "SH")


def test_etf_symbol():
    assert is_etf_symbol("510300", "SH")
    assert is_etf_symbol("588000", "SH")
    assert is_etf_symbol("159915", "SZ")
    assert is_etf_symbol("160706", "SZ")
    assert not is_etf_symbol("600519", "SH")
    assert not is_etf_symbol("510300", "SZ")  # SH prefix on SZ
    assert not is_etf_symbol("159915", "SH")
