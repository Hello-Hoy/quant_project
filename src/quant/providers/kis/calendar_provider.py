from __future__ import annotations
from dataclasses import dataclass
from datetime import date, time, timedelta


@dataclass
class CalendarRow:
    trade_date: date
    is_open: bool
    market_scope: str
    open_time: time | None
    close_time: time | None
    is_half_day: bool
    prev_trade_date: date | None
    next_trade_date: date | None

class KisCalendarProvider:
    def fetch_calendar_range(self, start_date: date, end_date: date) -> list[CalendarRow]:
        # TODO(provider): Replace weekday-based approximation with authoritative KRX/KIS calendar source.
        rows: list[CalendarRow] = []
        current = start_date
        prev_open: date | None = None
        open_dates: list[date] = []
        while current <= end_date:
            if current.weekday() < 5:
                open_dates.append(current)
            current += timedelta(days=1)
        current = start_date
        idx = 0
        while current <= end_date:
            is_open = current.weekday() < 5
            if is_open:
                nxt = open_dates[idx+1] if idx+1 < len(open_dates) else None
                rows.append(CalendarRow(current, True, "KRX", time(9,0), time(15,30), False, prev_open, nxt))
                prev_open = current
                idx += 1
            else:
                rows.append(CalendarRow(current, False, "KRX", None, None, False, prev_open, None))
            current += timedelta(days=1)
        return rows
