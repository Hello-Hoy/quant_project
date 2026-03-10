from __future__ import annotations

from quant.services.ingestion.corporate_action_event_mapper import canonicalize_event_type


def test_event_mapper_maps_known_aliases() -> None:
    assert canonicalize_event_type("cash_dividend") == "DIVIDEND_CASH"
    assert canonicalize_event_type("stock_split") == "SPLIT"
    assert canonicalize_event_type("무상증자") == "BONUS_ISSUE"


def test_event_mapper_normalizes_unknown_values() -> None:
    assert canonicalize_event_type("weird event") == "WEIRD_EVENT"
    assert canonicalize_event_type("   ") == "UNKNOWN"
