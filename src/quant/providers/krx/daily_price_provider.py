from __future__ import annotations
from dataclasses import dataclass
from datetime import date


@dataclass
class DailyPriceRow:
    trade_date: date
    symbol: str
    market_code: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    turnover: float | None = None
    market_cap: float | None = None
    shares_outstanding: int | None = None
    asset_type: str = "COMMON"

class KrxDailyPriceProvider:
    def fetch_daily_prices(self, target_date: str) -> list[DailyPriceRow]:
        # TODO(provider): Integrate with verified KRX EOD endpoint(s) for KOSPI/KOSDAQ/ETF.
        # Returning an empty list is intentional scaffold behavior.
        return []
