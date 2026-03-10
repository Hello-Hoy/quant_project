from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import cast

from quant.providers.base import ProviderAdapter, ProviderMode
from quant.providers.krx.client import KrxClient, KrxRequest
from quant.providers.normalize import parse_date, parse_float, parse_int, parse_str, pick_value


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


class KrxDailyPriceProvider(ProviderAdapter):
    provider_name = "KRX"

    def __init__(
        self,
        enabled: bool = True,
        mode: str | ProviderMode = ProviderMode.PLACEHOLDER,
        note: str | None = None,
        base_url: str | None = None,
        timeout_sec: int = 10,
        endpoints: dict[str, str] | None = None,
        client: KrxClient | None = None,
    ) -> None:
        super().__init__(enabled=enabled, mode=mode, note=note)
        self.client = client or KrxClient(base_url=base_url, endpoints=endpoints, timeout_sec=timeout_sec)

    def _normalize_row(self, item: dict) -> DailyPriceRow:
        trade_date = parse_date(
            pick_value(item, ["trade_date", "date", "trd_dd"], "trade_date"),
            "trade_date",
        )
        symbol = parse_str(
            pick_value(item, ["symbol", "ticker", "isu_cd"], "symbol"),
            "symbol",
        )
        market_code = parse_str(
            pick_value(item, ["market_code", "market", "mrkt"], "market_code"),
            "market_code",
        )
        open_price = parse_float(pick_value(item, ["open", "open_price"], "open"), "open")
        high_price = parse_float(pick_value(item, ["high", "high_price"], "high"), "high")
        low_price = parse_float(pick_value(item, ["low", "low_price"], "low"), "low")
        close_price = parse_float(pick_value(item, ["close", "close_price"], "close"), "close")
        volume = parse_int(pick_value(item, ["volume", "vol"], "volume"), "volume")

        return DailyPriceRow(
            trade_date=cast(date, trade_date),
            symbol=cast(str, symbol),
            market_code=cast(str, market_code),
            open=cast(float, open_price),
            high=cast(float, high_price),
            low=cast(float, low_price),
            close=cast(float, close_price),
            volume=cast(int, volume),
            turnover=parse_float(
                pick_value(item, ["turnover", "trd_value"], "turnover", required=False),
                "turnover",
                required=False,
            ),
            market_cap=parse_float(
                pick_value(item, ["market_cap", "mkt_cap"], "market_cap", required=False),
                "market_cap",
                required=False,
            ),
            shares_outstanding=parse_int(
                pick_value(item, ["shares_outstanding", "listed_shares"], "shares_outstanding", required=False),
                "shares_outstanding",
                required=False,
            ),
            asset_type=parse_str(
                pick_value(item, ["asset_type", "asset"], "asset_type", required=False),
                "asset_type",
                required=False,
            )
            or "COMMON",
        )

    def fetch_daily_prices(self, target_date: str) -> list[DailyPriceRow]:
        if not self.enabled or self.mode == ProviderMode.DISABLED:
            return []
        if self.mode == ProviderMode.PLACEHOLDER:
            return []

        payload = self.client.call(
            KrxRequest(api_name="daily_price", params={"target_date": target_date})
        )
        return [self._normalize_row(item) for item in payload]
