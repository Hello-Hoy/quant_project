from __future__ import annotations

from quant.services.corporate_action.event_type_policy import (
    canonicalize_event_type,
    is_factor_affecting_event_type,
)


def test_event_type_policy_canonicalizes_aliases() -> None:
    assert canonicalize_event_type("cash_dividend") == "DIVIDEND_CASH"
    assert canonicalize_event_type("stock_split") == "SPLIT"
    assert canonicalize_event_type("무상증자") == "BONUS_ISSUE"


def test_event_type_policy_affecting_flags() -> None:
    assert is_factor_affecting_event_type("SPLIT") is True
    assert is_factor_affecting_event_type("유상증자") is True
    assert is_factor_affecting_event_type("DIVIDEND_CASH") is False
    assert is_factor_affecting_event_type("STOCK_DIVIDEND") is False
