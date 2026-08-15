"""Keyword-lexicon sentiment + snownlp fallback wiring (domain/sentiment.py)."""

from __future__ import annotations

import sys
import types

import pytest

from cn_market_lake.domain.sentiment import (
    aggregate_scores,
    keyword_score,
    score_text,
    snownlp_score,
)


def test_keyword_score_empty_text_is_zero():
    assert keyword_score("") == 0.0


def test_keyword_score_no_lexicon_hit_is_zero():
    assert keyword_score("今日天气晴朗") == 0.0


def test_keyword_score_positive_and_negative_balance():
    assert keyword_score("公司业绩增长超预期") > 0
    assert keyword_score("公司被立案调查，业绩亏损") < 0


def test_snownlp_score_empty_text_returns_none():
    assert snownlp_score("") is None


def test_snownlp_score_returns_none_when_package_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "snownlp", None)
    assert snownlp_score("公司业绩增长") is None


def test_snownlp_score_returns_none_on_scoring_exception(monkeypatch):
    fake = types.ModuleType("snownlp")

    class _Boom:
        def __init__(self, text):
            raise RuntimeError("model unavailable")

    fake.SnowNLP = _Boom
    monkeypatch.setitem(sys.modules, "snownlp", fake)
    assert snownlp_score("公司业绩增长") is None


def test_snownlp_score_maps_probability_to_signed_range(monkeypatch):
    fake = types.ModuleType("snownlp")

    class _Fake:
        def __init__(self, text):
            self.sentiments = 0.9

        pass

    fake.SnowNLP = _Fake
    monkeypatch.setitem(sys.modules, "snownlp", fake)
    assert snownlp_score("好消息") == 0.8


def test_score_text_without_snownlp_returns_keyword_only():
    score, method = score_text("公司业绩增长超预期", use_snownlp=False)
    assert method == "keyword"
    assert score == keyword_score("公司业绩增长超预期")


def test_score_text_falls_back_to_keyword_when_snownlp_unavailable(monkeypatch):
    monkeypatch.setitem(sys.modules, "snownlp", None)
    score, method = score_text("公司业绩增长超预期", use_snownlp=True)
    assert method == "keyword"
    assert score == keyword_score("公司业绩增长超预期")


def test_score_text_uses_snownlp_alone_when_keyword_is_zero(monkeypatch):
    fake = types.ModuleType("snownlp")

    class _Fake:
        def __init__(self, text):
            self.sentiments = 0.7

    fake.SnowNLP = _Fake
    monkeypatch.setitem(sys.modules, "snownlp", fake)
    score, method = score_text("今日天气晴朗", use_snownlp=True)
    assert method == "snownlp"
    assert score == pytest.approx(0.4)


def test_score_text_averages_keyword_and_snownlp_when_both_present(monkeypatch):
    fake = types.ModuleType("snownlp")

    class _Fake:
        def __init__(self, text):
            self.sentiments = 0.9

    fake.SnowNLP = _Fake
    monkeypatch.setitem(sys.modules, "snownlp", fake)
    score, method = score_text("公司业绩增长超预期", use_snownlp=True)
    kw = keyword_score("公司业绩增长超预期")
    assert method == "keyword+snownlp"
    assert score == (kw + 0.8) / 2.0


def test_aggregate_scores_empty_is_zero():
    assert aggregate_scores([]) == 0.0


def test_aggregate_scores_averages():
    assert aggregate_scores([0.5, -0.5, 1.0]) == (0.5 - 0.5 + 1.0) / 3
