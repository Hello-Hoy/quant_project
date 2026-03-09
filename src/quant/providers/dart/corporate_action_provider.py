from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class CorporateActionDisclosureRow:
    symbol: str
    event_type: str
    announce_date: date | None
    ex_date: date | None
    effective_date: date | None
    ratio_value: float | None = None
    cash_value: float | None = None
    raw_payload: dict | None = None


class DartCorporateActionProvider:
    def fetch_corporate_actions(self, start_date: str, end_date: str) -> list[CorporateActionDisclosureRow]:
        # TODO(provider): Wire DART disclosures to normalized corporate action rows.
        return []
