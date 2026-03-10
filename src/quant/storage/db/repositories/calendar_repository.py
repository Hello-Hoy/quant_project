from __future__ import annotations
from collections.abc import Iterable
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant.storage.db.models.calendar import MarketCalendar


class CalendarRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_many(self, rows: Iterable[MarketCalendar]) -> int:
        count = 0
        for row in rows:
            existing = self.session.get(MarketCalendar, row.trade_date)
            if existing is None:
                self.session.add(row)
            else:
                existing.is_open = row.is_open
                existing.market_scope = row.market_scope
                existing.open_time = row.open_time
                existing.close_time = row.close_time
                existing.is_half_day = row.is_half_day
                existing.prev_trade_date = row.prev_trade_date
                existing.next_trade_date = row.next_trade_date
                existing.updated_at = row.updated_at
                self.session.add(existing)
            count += 1
        self.session.flush()
        return count

    def get_by_date(self, trade_date: date) -> MarketCalendar | None:
        return self.session.get(MarketCalendar, trade_date)

    def get_open_dates_in_range(self, start_date: date, end_date: date) -> list[date]:
        stmt = (
            select(MarketCalendar.trade_date)
            .where(
                MarketCalendar.trade_date >= start_date,
                MarketCalendar.trade_date <= end_date,
                MarketCalendar.is_open.is_(True),
            )
            .order_by(MarketCalendar.trade_date)
        )
        return [value for value in self.session.scalars(stmt)]
