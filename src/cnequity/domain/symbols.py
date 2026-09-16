from __future__ import annotations

from dataclasses import dataclass

PREFIX_WHITELIST = {
    "SH": ("60", "68"),
    "SZ": ("00", "30"),
    # 92 = BSE today; 43/83/87 = pre-transfer NEEQ codes that delisted (or still
    # quote) under the old numbering — needed so recovered names enter all_a.
    "BJ": ("92", "43", "83", "87"),
}

EXCLUDED_PREFIXES = tuple(f"{p}" for p in range(81, 90))

# SSE reserves 689xxx for CDRs (存托凭证). They trade on SH and stay in fetch
# scope (is_all_a_symbol), but are not common stock: primary sources (sina adj
# factors, tdx xdxr) have no coverage and the all_a selection universe excludes
# them (see query/universe.py).
CDR_PREFIXES = ("689",)

# Exchange-traded funds / LOFs. Kept in instruments + daily_bars for UI/quotes,
# but NOT in PREFIX_WHITELIST — all_a research universe excludes them.
# Exchange-traded fund products: ETFs and LOFs, both of which quote on-exchange
# with real volume.
#
# SH is enumerated rather than written as "51" because 519xxx is *not* one of
# these — it is the open-end fund code space, sold and redeemed at NAV away from
# the exchange. A "51" prefix swept all 188 of them into the tradable universe,
# and TDX answered with a NAV series: 436,533 rows in `daily_bars` carrying a
# close but zero volume and zero turnover on every single one, against 95-99%
# non-zero volume for every genuine prefix beside it.
SH_LOF_PREFIXES = ("501", "502")

ETF_PREFIXES = {
    "SH": (
        *SH_LOF_PREFIXES,
        "510",
        "511",
        "512",
        "513",
        "514",
        "515",
        "516",
        "517",
        "518",
        "52",
        "53",
        "56",
        "58",
    ),
    "SZ": ("15", "16"),
}

# Temporary ETF subscription / allotment placeholders that TDX lists as if they
# were securities. They are not tradable equities: a daily instruments compact
# that sees them vanish invents a delist_date, and they inflate the "delisted"
# count with noise like ``认购款``. Drop at ingest; purge from curated.
SUBSCRIPTION_PLACEHOLDER_NAMES = frozenset({"认购款", "申购款"})

# Numeric bands the exchanges have actually issued equity codes from, as
# ``(exchange, first, last_exclusive)``. Narrower than PREFIX_WHITELIST, which
# admits e.g. all of 60xxxx — enumerating every prefix would mean 50,000 codes
# where ~14,000 covers the issued space.
#
# This exists because no free source will hand over a list of *delisted* codes.
# Sweeping the space and asking a vendor "did this code ever trade" reconstructs
# the delisted set from the outside, which is the only route to a
# survivorship-free universe once the vendor lists are unavailable. Widen a band
# if a probe finds live codes at its edge; the sweep is cheap enough that erring
# wide costs minutes, while erring narrow silently loses delisted names.
ISSUED_CODE_BANDS: tuple[tuple[str, int, int], ...] = (
    ("SH", 600000, 606000),  # main board: 600/601/603/605
    ("SH", 688000, 689000),  # STAR (689xxx CDRs excluded from all_a)
    ("SZ", 1, 5000),  # main board 000/001, SME 002, 003
    ("SZ", 300000, 302000),  # ChiNext (300) + registration-based (301)
    ("BJ", 920000, 921000),  # BSE current numbering
    # Legacy NEEQ → BSE numbering. Most survivors were renumbered into 92xxxx;
    # the codes that *didn't* transfer are exactly the delisted set the SH/SZ
    # bands miss. ~21k probes, one-time, resumable.
    ("BJ", 430000, 431000),
    ("BJ", 830000, 840000),
    ("BJ", 870000, 880000),
)


# Exchanges the TDX protocol serves. It rejects anything else outright
# ("市场代码错误, 目前只支持沪深市场"), so the Beijing exchange has no TDX route
# at all — which is why the lake carried zero BJ instruments despite
# PREFIX_WHITELIST admitting the prefix, and why `universe="all_a"` silently
# meant "Shanghai and Shenzhen only". BJ bars come from Sina instead.
TDX_EXCHANGES = frozenset({"SH", "SZ"})


def is_tdx_servable(symbol: str) -> bool:
    """Whether the TDX protocol can serve this symbol's quotes."""
    try:
        return parse_symbol(symbol).exchange in TDX_EXCHANGES
    except ValueError:
        return False


def split_by_quote_source(symbols: list[str]) -> tuple[list[str], list[str]]:
    """Partition into ``(tdx_servable, needs_fallback)`` preserving order."""
    tdx, fallback = [], []
    for symbol in symbols:
        (tdx if is_tdx_servable(symbol) else fallback).append(symbol)
    return tdx, fallback


def issued_code_space() -> list[str]:
    """Every equity symbol the exchanges could plausibly have issued, ascending."""
    out: list[str] = []
    seen: set[str] = set()
    for exchange, first, last in ISSUED_CODE_BANDS:
        for num in range(first, last):
            symbol = format_symbol(f"{num:06d}", exchange)
            if symbol not in seen:
                seen.add(symbol)
                out.append(symbol)
    return out


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    code: str
    exchange: str


def parse_symbol(symbol: str) -> SymbolInfo:
    if "." not in symbol:
        raise ValueError(f"Invalid symbol format: {symbol}")
    code, exchange = symbol.rsplit(".", 1)
    exchange = exchange.upper()
    if exchange not in ("SH", "SZ", "BJ"):
        raise ValueError(f"Unknown exchange: {exchange}")
    return SymbolInfo(symbol=symbol, code=code, exchange=exchange)


def format_symbol(code: str, exchange: str) -> str:
    return f"{code}.{exchange.upper()}"


def infer_exchange_from_code(code: str) -> str:
    """Infer an exchange from a six-digit equity code when the feed omits it.

    Legacy Beijing/NEEQ codes remain valid universe members, so recognizing
    only today's ``92xxxx`` BSE range silently drops or mislabels ``43/83/87``
    codes in source adapters that do not provide a market field.
    """
    normalized = str(code).strip().zfill(6)
    if normalized.startswith(("60", "68")):
        return "SH"
    if normalized.startswith(PREFIX_WHITELIST["BJ"]):
        return "BJ"
    return "SZ"


def is_all_a_symbol(code: str, exchange: str) -> bool:
    if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
        return False
    exchange = exchange.upper()
    # 81–89 is a SH/SZ reservation (bonds etc.); BJ's legacy 83xxxx NEEQ
    # band must not be caught by the same digit check.
    if exchange in ("SH", "SZ") and any(code.startswith(p) for p in EXCLUDED_PREFIXES):
        return False
    prefixes = PREFIX_WHITELIST.get(exchange, ())
    return any(code.startswith(p) for p in prefixes)


def is_cdr_symbol(code: str, exchange: str) -> bool:
    """Whether *code* is a CDR (Chinese Depositary Receipt, SH 689xxx segment)."""
    if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
        return False
    return exchange.upper() == "SH" and any(code.startswith(p) for p in CDR_PREFIXES)


def is_etf_symbol(code: str, exchange: str) -> bool:
    """Whether *code* is an exchange-traded fund / LOF on SH/SZ."""
    if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
        return False
    prefixes = ETF_PREFIXES.get(exchange.upper(), ())
    if not any(code.startswith(p) for p in prefixes):
        return False
    # SSE ETF secondary-market codes end in 0. Codes ending in 1 and 3
    # identify creation/redemption and subscription respectively, even when
    # TDX gives them the fund's normal name and a padded quote series.
    # This rule does not apply to Shanghai LOFs or Shenzhen ETF/LOF codes.
    return exchange.upper() != "SH" or code.startswith(SH_LOF_PREFIXES) or code.endswith("0")


def is_subscription_placeholder(name: str | None, symbol: str | None = None) -> bool:
    """TDX allotment / subscription stubs (``认购款``), not tradable securities."""
    if symbol:
        try:
            info = parse_symbol(symbol)
        except ValueError:
            pass
        else:
            if (
                info.exchange == "SH"
                and len(info.code) == 6
                and info.code.isdigit()
                and info.code.startswith(ETF_PREFIXES["SH"])
                and not info.code.startswith(SH_LOF_PREFIXES)
                and info.code.endswith(("1", "3"))
            ):
                return True
    if not name:
        return False
    # Some TDX stock-list responses use C-style NUL padding for fixed-width
    # names (for example ``认购款\x00\x00``). Treat that padding as transport
    # noise before matching; otherwise the stub survives compaction and can be
    # mistaken for a real instrument that was delisted on the snapshot date.
    stripped = name.strip().rstrip("\x00").strip()
    if stripped in SUBSCRIPTION_PLACEHOLDER_NAMES:
        return True
    return any(stripped.endswith(marker) for marker in SUBSCRIPTION_PLACEHOLDER_NAMES)


def normalize_market_code(code: str, market: str) -> tuple[str, str]:
    market = market.lower()
    if market in ("sh", "1"):
        exchange = "SH"
    elif market in ("sz", "0"):
        exchange = "SZ"
    elif market in ("bj", "2"):
        exchange = "BJ"
    else:
        exchange = market.upper()
    return code.zfill(6), exchange


# Ingest scope policies for `[universe].ingest`.  These bound what the daily
# fetch asks the vendors for; they are NOT the research selection universe
# (see `domain/universe_profiles.py`), and they deliberately keep ST,
# suspended, CDR and delisted names — dropping those is the survivorship bias
# the lake exists to avoid.
INGEST_UNIVERSES = frozenset({"all_a", "all_a_sh_sz", "all_instruments"})


def in_ingest_universe(code: str, exchange: str, universe: str = "all_a") -> bool:
    """Whether *code* belongs to the configured ingest scope.

    ``all_instruments`` restores the historical behaviour of fetching every
    code ``instruments`` lists, including the ETF/LOF quote codes.
    """
    if universe == "all_instruments":
        return True
    if not is_all_a_symbol(code, exchange):
        return False
    if universe == "all_a_sh_sz":
        return exchange.upper() != "BJ"
    return True


def filter_ingest_universe(symbols, universe: str = "all_a") -> list[str]:
    """Return *symbols* restricted to the ingest scope, order preserved.

    An unparseable symbol is dropped by every scope except
    ``all_instruments``: the fetch layer cannot route it either way, and
    keeping it only feeds an unfillable key to the coverage gate.
    """
    if universe == "all_instruments":
        return list(symbols)
    kept: list[str] = []
    for symbol in symbols:
        try:
            info = parse_symbol(symbol)
        except ValueError:
            continue
        if in_ingest_universe(info.code, info.exchange, universe):
            kept.append(symbol)
    return kept
