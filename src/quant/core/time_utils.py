from __future__ import annotations
from datetime import date, datetime



def to_trade_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(value).date()
