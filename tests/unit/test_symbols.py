from cnequity.domain.symbols import (
    format_symbol,
    infer_exchange_from_code,
    is_all_a_symbol,
    is_cdr_symbol,
    is_etf_symbol,
    parse_symbol,
)


def test_format_symbol():
    assert format_symbol("600519", "SH") == "600519.SH"


def test_infer_exchange_from_code_keeps_legacy_beijing_ranges():
    assert infer_exchange_from_code("600519") == "SH"
    assert infer_exchange_from_code("000001") == "SZ"
    for code in ("430001", "830001", "870001", "920001"):
        assert infer_exchange_from_code(code) == "BJ"


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
    assert not is_all_a_symbol("abc", "SZ")
    assert not is_all_a_symbol("0000001", "SZ")


def test_cdr_symbol():
    assert is_cdr_symbol("689009", "SH")
    assert not is_cdr_symbol("688981", "SH")
    assert not is_cdr_symbol("600519", "SH")
    # CDR segment exists only on SH
    assert not is_cdr_symbol("689009", "SZ")
    # CDRs stay inside the fetch scope; only the all_a universe excludes them
    assert is_all_a_symbol("689009", "SH")
    assert not is_cdr_symbol("68900x", "SH")


def test_etf_symbol():
    assert is_etf_symbol("510300", "SH")
    assert is_etf_symbol("588000", "SH")
    assert is_etf_symbol("530050", "SH")
    assert is_etf_symbol("530060", "SH")
    assert is_etf_symbol("501001", "SH")
    assert is_etf_symbol("502003", "SH")
    assert not is_etf_symbol("515473", "SH")
    assert not is_etf_symbol("589493", "SH")
    assert not is_etf_symbol("510301", "SH")
    assert is_etf_symbol("159915", "SZ")
    assert is_etf_symbol("160706", "SZ")
    assert not is_etf_symbol("600519", "SH")
    assert not is_etf_symbol("510300", "SZ")  # SH prefix on SZ
    assert not is_etf_symbol("159915", "SH")
    assert not is_etf_symbol("15991x", "SZ")


def test_shanghai_lof_is_not_an_etf_subscription_code():
    from cnequity.domain.symbols import is_subscription_placeholder

    assert not is_subscription_placeholder("财通精选", "501001.SH")
    assert not is_subscription_placeholder("证券分级", "502003.SH")
    assert is_subscription_placeholder("某 ETF", "515473.SH")
