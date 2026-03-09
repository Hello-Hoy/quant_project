from __future__ import annotations
from dataclasses import dataclass
from datetime import date


@dataclass
class IndexDailyRow:
    trade_date: date
    index_code: str
    index_name: str
    index_family: str
    open: float
    high: float
    low: float
    close: float
    volume: int | None = None
    turnover: float | None = None

class KrxIndexProvider:
    def fetch_index_daily(self, target_date: str) -> list[IndexDailyRow]:
        # TODO(provider): Integrate with verified KRX index endpoint(s).
        # Returning an empty list is intentional scaffold behavior.
        return []
