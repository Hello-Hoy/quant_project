from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from quant.services.adjustment.factor_rules import compute_cumulative_factor


@dataclass
class EventStub:
    event_type: str
    effective_date: date | None
    ex_date: date | None
    ratio_value: float | None



def test_compute_factor_no_events_returns_identity() -> None:
    result = compute_cumulative_factor(events=[], up_to_date=date(2026, 3, 9))
    assert result.cumulative_factor == 1.0
    assert result.derived_event_count == 0



def test_compute_factor_uses_supported_ratio_events() -> None:
    events = [
        EventStub("SPLIT", date(2026, 3, 1), None, 2.0),
        EventStub("BONUS_ISSUE", date(2026, 3, 5), None, 1.1),
    ]
    result = compute_cumulative_factor(events=events, up_to_date=date(2026, 3, 9))
    assert result.cumulative_factor == 2.2
    assert result.derived_event_count == 2



def test_compute_factor_ignores_unsupported_or_invalid_events() -> None:
    events = [
        EventStub("DIVIDEND_CASH", date(2026, 3, 1), None, 1.2),
        EventStub("DIVIDEND_STOCK", date(2026, 3, 1), None, 1.2),
        EventStub("SPLIT", date(2026, 3, 2), None, None),
        EventStub("SPLIT", date(2026, 3, 3), None, -1.0),
        EventStub("SPLIT", date(2026, 4, 1), None, 2.0),
    ]
    result = compute_cumulative_factor(events=events, up_to_date=date(2026, 3, 9))
    assert result.cumulative_factor == 1.0
    assert result.derived_event_count == 0


def test_compute_factor_accepts_alias_event_type_after_canonicalization() -> None:
    events = [
        EventStub("stock_split", date(2026, 3, 1), None, 2.0),
        EventStub("유상증자", date(2026, 3, 5), None, 1.1),
    ]
    result = compute_cumulative_factor(events=events, up_to_date=date(2026, 3, 9))
    assert result.cumulative_factor == 2.2
    assert result.derived_event_count == 2
