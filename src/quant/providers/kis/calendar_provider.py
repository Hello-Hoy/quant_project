from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time, timedelta
from typing import cast

from quant.providers.base import ProviderAdapter, ProviderMode
from quant.providers.kis.client import KisClient, KisRequest
from quant.providers.normalize import parse_bool, parse_date, parse_str, parse_time, pick_value


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


class KisCalendarProvider(ProviderAdapter):
    provider_name = "KIS"

    def __init__(
        self,
        enabled: bool = True,
        mode: str | ProviderMode = ProviderMode.PLACEHOLDER,
        note: str | None = None,
        base_url: str | None = None,
        timeout_sec: int = 10,
        endpoints: dict[str, str] | None = None,
        client: KisClient | None = None,
    ) -> None:
        super().__init__(enabled=enabled, mode=mode, note=note)
        self.client = client or KisClient(base_url=base_url, endpoints=endpoints, timeout_sec=timeout_sec)

    def _placeholder_calendar(self, start_date: date, end_date: date) -> list[CalendarRow]:
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
                nxt = open_dates[idx + 1] if idx + 1 < len(open_dates) else None
                rows.append(CalendarRow(current, True, "KRX", time(9, 0), time(15, 30), False, prev_open, nxt))
                prev_open = current
                idx += 1
            else:
                rows.append(CalendarRow(current, False, "KRX", None, None, False, prev_open, None))
            current += timedelta(days=1)
        return rows

    def _normalize_row(self, item: dict) -> CalendarRow:
        trade_date = parse_date(
            pick_value(item, ["trade_date", "date", "trd_dd"], "trade_date"),
            "trade_date",
        )
        is_open = parse_bool(
            pick_value(item, ["is_open", "open_flag"], "is_open"),
            "is_open",
        )
        market_scope = parse_str(
            pick_value(item, ["market_scope", "market"], "market_scope", required=False),
            "market_scope",
            required=False,
        )

        return CalendarRow(
            trade_date=cast(date, trade_date),
            is_open=cast(bool, is_open),
            market_scope=market_scope or "KRX",
            open_time=parse_time(
                pick_value(item, ["open_time", "market_open"], "open_time", required=False),
                "open_time",
                required=False,
            ),
            close_time=parse_time(
                pick_value(item, ["close_time", "market_close"], "close_time", required=False),
                "close_time",
                required=False,
            ),
            is_half_day=bool(
                parse_bool(
                    pick_value(item, ["is_half_day", "half_day"], "is_half_day", required=False),
                    "is_half_day",
                    required=False,
                )
                or False
            ),
            prev_trade_date=parse_date(
                pick_value(item, ["prev_trade_date", "prev_open_date"], "prev_trade_date", required=False),
                "prev_trade_date",
                required=False,
            ),
            next_trade_date=parse_date(
                pick_value(item, ["next_trade_date", "next_open_date"], "next_trade_date", required=False),
                "next_trade_date",
                required=False,
            ),
        )

    def fetch_calendar_range(self, start_date: date, end_date: date) -> list[CalendarRow]:
        # TODO(provider): Replace weekday-based approximation with authoritative KRX/KIS calendar source.
        if not self.enabled or self.mode == ProviderMode.DISABLED:
            return []
        if self.mode == ProviderMode.PLACEHOLDER:
            return self._placeholder_calendar(start_date=start_date, end_date=end_date)

        payload = self.client.call(
            KisRequest(
                api_name="calendar_range",
                params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )
        )
        return [self._normalize_row(item) for item in payload]
