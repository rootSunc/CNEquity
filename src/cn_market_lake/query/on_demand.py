from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.adapters.eastmoney.stock_news import fetch_stock_news
from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

# Datasets with a real fetch path. Stubs stay callable only so old configs get a
# clear NotImplementedError instead of an empty JSON that poisons the cache.
_IMPLEMENTED = frozenset({"stock_news", "research_reports"})


class OnDemandService:
    """Fetch high-churn per-symbol data on first query and cache locally."""

    def __init__(self, config: Config):
        self.config = config
        self.cache_root = config.meta_root / "on_demand"
        self.cache_root.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, dataset: str, symbol: str) -> Path:
        safe = symbol.replace(".", "_")
        return self.cache_root / dataset / f"{safe}.json"

    def fetch(self, dataset: str, symbol: str, **kwargs) -> dict:
        if dataset not in self.config.on_demand_datasets and self.config.on_demand_datasets:
            raise ValueError(f"Dataset {dataset} not enabled for on-demand")

        path = self._cache_path(dataset, symbol)
        if path.exists() and not kwargs.get("refresh"):
            with open(path, encoding="utf-8") as f:
                return json.load(f)

        payload = self._fetch_remote(dataset, symbol, **kwargs)
        if self._should_cache(payload):
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        return payload

    @staticmethod
    def _should_cache(payload: dict) -> bool:
        if payload.get("status") == "not_implemented":
            return False
        if "error" in payload:
            return False
        return True

    def _fetch_remote(self, dataset: str, symbol: str, **kwargs) -> dict:
        if dataset == "stock_news":
            return self._fetch_stock_news(symbol, **kwargs)
        if dataset == "research_reports":
            return self._fetch_research_reports(symbol)
        if dataset in {"announcement_body", "financial_reports"}:
            raise NotImplementedError(
                f"{dataset} is not implemented yet; remove it from "
                "[on_demand].datasets (implemented: " + ", ".join(sorted(_IMPLEMENTED)) + ")."
            )
        raise NotImplementedError(
            f"on-demand dataset {dataset!r} is not implemented "
            f"(implemented: {', '.join(sorted(_IMPLEMENTED))})."
        )

    def _fetch_stock_news(self, symbol: str, **kwargs) -> dict:
        if not self.config.sources.get("eastmoney", True):
            raise RuntimeError("stock_news: eastmoney source disabled in config")
        on_date = kwargs.get("on_date")
        if isinstance(on_date, str):
            from datetime import date

            on_date = date.fromisoformat(on_date)
        limit = int(kwargs.get("limit", 30))
        use_snownlp = bool(kwargs.get("use_snownlp", self.config.sentiment_use_snownlp))
        self.config.rate_limit("eastmoney")
        payload = fetch_stock_news(
            symbol,
            on_date=on_date,
            limit=limit,
            use_snownlp=use_snownlp,
        )
        payload["data_version"] = "v1"
        payload["fetched_at"] = datetime.now(timezone.utc).isoformat()
        return payload

    def _fetch_research_reports(self, symbol: str) -> dict:
        code = symbol.split(".")[0]
        url = f"https://reportapi.eastmoney.com/report/list?code={code}&pageSize=10"
        try:
            self.config.rate_limit("eastmoney")
            with EastMoneyClient(
                min_interval=self.config.source_intervals.get("eastmoney", 1.0)
            ) as client:
                resp = client.get(url)
                data = resp.json()
                return {"symbol": symbol, "items": data, "source": "eastmoney"}
        except Exception as exc:
            logger.warning("research_reports fetch failed: %s", exc)
            return {"symbol": symbol, "items": [], "error": str(exc), "source": "eastmoney"}
