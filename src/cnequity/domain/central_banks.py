"""Central bank policy-rate series inside the Jin10 economic calendar.

A rate decision is an ordinary calendar release; its Jin10 ``indicator_id``
(stored as ``economic_calendar.indicator_code``) names the series across years
while the indicator text changes. Measured 2026-10-30 over the 2008- backfill.
``policy`` marks the one rate a bank steers by; the others are its corridor or
a retired benchmark. A bank that changed its framework has more than one
policy series over time (BoJ: 285 to 2024-01, 8060 from 2024-03).
"""

from __future__ import annotations

from typing import NamedTuple


class RateSeries(NamedTuple):
    indicator_code: str
    bank: str
    rate: str
    policy: bool


CENTRAL_BANK_RATES: tuple[RateSeries, ...] = (
    RateSeries("jin10:77", "Fed", "federal funds target, upper bound", True),
    RateSeries("jin10:990", "Fed", "federal funds target, lower bound", False),
    RateSeries("jin10:753", "ECB", "main refinancing rate", True),
    RateSeries("jin10:395", "ECB", "deposit facility rate", False),
    RateSeries("jin10:419", "ECB", "marginal lending rate", False),
    RateSeries("jin10:638", "BoE", "bank rate", True),
    RateSeries("jin10:285", "BoJ", "policy-rate balance rate", True),
    RateSeries("jin10:8060", "BoJ", "target rate, upper bound", True),
    RateSeries("jin10:8061", "BoJ", "target rate, lower bound", False),
    RateSeries("jin10:1417", "PBoC", "1-year loan prime rate", True),
    RateSeries("jin10:1416", "PBoC", "5-year loan prime rate", False),
    RateSeries("jin10:960", "PBoC", "1-year lending benchmark", True),
    RateSeries("jin10:961", "PBoC", "1-year deposit benchmark", False),
    RateSeries("jin10:557", "HKMA", "base rate", True),
    RateSeries("jin10:800", "BoC", "overnight rate target", True),
    RateSeries("jin10:862", "RBA", "cash rate target", True),
    RateSeries("jin10:591", "RBNZ", "official cash rate", True),
    RateSeries("jin10:1420", "SNB", "policy rate", True),
    RateSeries("jin10:943", "SNB", "deposit rate", False),
    RateSeries("jin10:1511", "Riksbank", "policy rate", True),
    RateSeries("jin10:1456", "Norges Bank", "policy rate", True),
    RateSeries("jin10:615", "BoK", "base rate", True),
    RateSeries("jin10:897", "RBI", "repo rate", True),
    RateSeries("jin10:898", "RBI", "reverse repo rate", False),
    RateSeries("jin10:1017", "CBR", "key rate", True),
    RateSeries("jin10:1015", "BCB", "Selic rate", True),
    RateSeries("jin10:1019", "Banxico", "overnight interbank target", True),
    RateSeries("jin10:8098", "BCCh", "monetary policy rate", True),
    RateSeries("jin10:1020", "BI", "7-day reverse repo rate", True),
    RateSeries("jin10:1461", "BoT", "policy rate", True),
    RateSeries("jin10:1457", "BSP", "policy rate", True),
    RateSeries("jin10:1016", "CBRT", "one-week repo rate", True),
    RateSeries("jin10:1018", "SARB", "repo rate", True),
)
