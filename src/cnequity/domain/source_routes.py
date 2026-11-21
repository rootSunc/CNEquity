"""Shared naming and enablement for provider routes."""


def source_family(source: str) -> str:
    text = str(source).strip().lower()
    if text == "ths_official":
        return text  # independent keyed service, not the public THS website
    if text.startswith("tdx_fin"):
        return "tdx_fin"  # HTTP package server, not the TDX quote protocol
    for prefix in ("ths", "cninfo", "eastmoney_push2", "eastmoney_dc", "sina"):
        if text.startswith(prefix):
            return prefix
    if text.startswith("tdx"):
        return "tdx_protocol"
    if text.startswith("eastmoney") or text in {"em", "datacenter"}:
        return "eastmoney"
    return text


def source_route_enabled(config, source: str) -> bool:
    family = source_family(source)
    switches = config.sources
    return (
        switches.get(source, True)
        and switches.get(family, True)
        and (not family.startswith("eastmoney") or switches.get("eastmoney", True))
        and (not family.startswith("futures_exchange_") or switches.get("futures_exchange", True))
        and (family != "tdx_protocol" or config.tdx_enabled)
    )
