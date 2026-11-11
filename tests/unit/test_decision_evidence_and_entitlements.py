from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from cnequity.adapters.cninfo.payment_notices import parse_payment_notice_text
from cnequity.adapters.cninfo.reviewed_cash_corrections import reviewed_cash_correction
from cnequity.domain.cash_entitlements import CashEntitlement
from cnequity.domain.decision_evidence import DecisionEvidence


def evidence(*, status="verified_repaired", published=None):
    return DecisionEvidence(
        source_document_id="issuer-123",
        source_sha256="a" * 64,
        source_published_at=published,
        effective_at=None,
        observed_at=datetime(2026, 9, 25, tzinfo=timezone.utc),
        evidence_status=status,
    )


def test_observed_time_cannot_replace_unknown_publication():
    item = evidence(published=None)
    assert not item.published_by(datetime(2026, 9, 26, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="document id"):
        DecisionEvidence(None, None, None, None, None, evidence_status="verified_repaired")


def test_zero_entitlement_for_excluded_repurchase_account():
    with pytest.raises(ValueError, match="cannot receive cash"):
        CashEntitlement(
            symbol="600001.SH",
            ex_date=date(2020, 6, 10),
            record_date=date(2020, 6, 9),
            payment_date=None,
            holder_class="repurchase_account",
            pretax_cash_per_share=Decimal("0.1"),
            evidence=evidence(),
        )


def test_single_class_sh_table_does_not_read_b_or_h_class_dates():
    text = (
        "证券代码：688320 每股派发现金红利0.11元（含税）"
        "股权登记日 除权（息）日 现金红利发放日 "
        "2024/6/13 2024/6/14 2024/6/14"
    )
    assert parse_payment_notice_text(text) == {
        "code": "688320",
        "ex_date": date(2024, 6, 14),
        "payment_date": date(2024, 6, 14),
        "cash_dividend": 0.11,
    }
    assert parse_payment_notice_text(text + "H股2024/7/1") is None


def test_reviewed_cash_correction_is_bound_to_exact_original_document():
    old = {
        "symbol": "688320.SH",
        "ex_date": date(2024, 6, 14),
        "cash_dividend": 0.11038000583648681,
    }
    facts = {
        "code": "688320",
        "ex_date": date(2024, 6, 14),
        "payment_date": date(2024, 6, 14),
        "cash_dividend": 0.11,
    }
    digest = "06fa22c5ffc2fee854cb2b88fc827395f6834a15055a1d43857b3293d0b86bc0"
    assert reviewed_cash_correction(old, facts, "1220289873", digest)
    assert not reviewed_cash_correction(old, facts, "1220289874", digest)
    assert not reviewed_cash_correction(old, facts, "1220289873", "b" * 64)
    assert not reviewed_cash_correction({**old, "cash_dividend": 0.12}, facts, "1220289873", digest)
    assert not reviewed_cash_correction(old, {**facts, "cash_dividend": 0.12}, "1220289873", digest)
