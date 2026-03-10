from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

from quant.services.corporate_action.event_type_policy import (
    FACTOR_AFFECTING_EVENT_TYPES,
    canonicalize_event_type,
)


class CorporateActionLike(Protocol):
    event_type: str
    effective_date: date | None
    ex_date: date | None
    ratio_value: float | None


@dataclass(frozen=True)
class FactorComputationResult:
    cumulative_factor: float
    derived_event_count: int


def _event_date(event: CorporateActionLike) -> date | None:
    return event.effective_date or event.ex_date


def compute_cumulative_factor(events: list[CorporateActionLike], up_to_date: date) -> FactorComputationResult:
    cumulative_factor = 1.0
    derived_event_count = 0

    for event in events:
        event_date = _event_date(event)
        if event_date is None or event_date > up_to_date:
            continue
        canonical_event_type = canonicalize_event_type(event.event_type)
        if canonical_event_type not in FACTOR_AFFECTING_EVENT_TYPES:
            continue
        if event.ratio_value is None:
            continue
        ratio = float(event.ratio_value)
        if ratio <= 0:
            continue

        cumulative_factor *= ratio
        derived_event_count += 1

    return FactorComputationResult(
        cumulative_factor=cumulative_factor,
        derived_event_count=derived_event_count,
    )
