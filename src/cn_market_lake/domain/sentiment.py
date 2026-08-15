"""Chinese financial headline sentiment — keyword lexicon + optional SnowNLP."""

from __future__ import annotations

_POSITIVE_WEIGHTS: dict[str, float] = {
    "增长": 1.0,
    "盈利": 1.2,
    "超预期": 1.3,
    "中标": 1.0,
    "回购": 1.1,
    "增持": 1.2,
    "突破": 0.9,
    "创新高": 1.2,
    "分红": 1.0,
    "利好": 1.1,
    "签约": 0.8,
    "获批": 1.0,
    "大涨": 1.0,
    "涨停": 0.8,
}

_NEGATIVE_WEIGHTS: dict[str, float] = {
    "亏损": 1.2,
    "下降": 0.9,
    "减持": 1.1,
    "立案": 1.3,
    "处罚": 1.3,
    "警示": 1.0,
    "诉讼": 1.1,
    "违约": 1.2,
    "退市": 1.4,
    "风险": 0.7,
    "下滑": 1.0,
    "暴雷": 1.4,
    "调查": 1.1,
    "大跌": 1.0,
    "跌停": 0.8,
}


def keyword_score(text: str) -> float:
    """Weighted keyword balance in [-1, 1]; 0 when no lexicon hit."""
    if not text:
        return 0.0
    pos = sum(w for kw, w in _POSITIVE_WEIGHTS.items() if kw in text)
    neg = sum(w for kw, w in _NEGATIVE_WEIGHTS.items() if kw in text)
    total = pos + neg
    if total == 0:
        return 0.0
    return (pos - neg) / total


def snownlp_score(text: str) -> float | None:
    """Map SnowNLP [0, 1] probability to [-1, 1], or None if unavailable."""
    if not text:
        return None
    try:
        from snownlp import SnowNLP  # type: ignore[import-not-found]
    except ImportError:
        return None
    try:
        prob = float(SnowNLP(text).sentiments)
    except Exception:
        return None
    return prob * 2.0 - 1.0


def score_text(text: str, *, use_snownlp: bool = True) -> tuple[float, str]:
    """Return (score, method) where method describes signals used."""
    kw = keyword_score(text)
    if not use_snownlp:
        return kw, "keyword"
    sn = snownlp_score(text)
    if sn is None:
        return kw, "keyword"
    if kw == 0.0:
        return sn, "snownlp"
    return (kw + sn) / 2.0, "keyword+snownlp"


def aggregate_scores(scores: list[float]) -> float:
    if not scores:
        return 0.0
    return sum(scores) / len(scores)
