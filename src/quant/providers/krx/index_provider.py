from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import cast

from quant.providers.base import ProviderAdapter, ProviderMode
from quant.providers.krx.client import KrxClient, KrxRequest
from quant.providers.normalize import parse_date, parse_float, parse_int, parse_str, pick_value


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


class KrxIndexProvider(ProviderAdapter):
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

    def _normalize_row(self, item: dict) -> IndexDailyRow:
        trade_date = parse_date(
            pick_value(item, ["trade_date", "date", "trd_dd"], "trade_date"),
            "trade_date",
        )
        index_code = parse_str(
            pick_value(item, ["index_code", "idx_cd", "code"], "index_code"),
            "index_code",
        )
        index_name = parse_str(
            pick_value(item, ["index_name", "name", "idx_nm"], "index_name"),
            "index_name",
        )
        index_family = parse_str(
            pick_value(item, ["index_family", "family", "idx_family"], "index_family"),
            "index_family",
        )
        open_price = parse_float(pick_value(item, ["open", "open_price"], "open"), "open")
        high_price = parse_float(pick_value(item, ["high", "high_price"], "high"), "high")
        low_price = parse_float(pick_value(item, ["low", "low_price"], "low"), "low")
        close_price = parse_float(pick_value(item, ["close", "close_price"], "close"), "close")

        return IndexDailyRow(
            trade_date=cast(date, trade_date),
            index_code=cast(str, index_code),
            index_name=cast(str, index_name),
            index_family=cast(str, index_family),
            open=cast(float, open_price),
            high=cast(float, high_price),
            low=cast(float, low_price),
            close=cast(float, close_price),
            volume=parse_int(
                pick_value(item, ["volume", "vol"], "volume", required=False),
                "volume",
                required=False,
            ),
            turnover=parse_float(
                pick_value(item, ["turnover", "trd_value"], "turnover", required=False),
                "turnover",
                required=False,
            ),
        )

    def fetch_index_daily(self, target_date: str) -> list[IndexDailyRow]:
        if not self.enabled or self.mode == ProviderMode.DISABLED:
            return []
        if self.mode == ProviderMode.PLACEHOLDER:
            return []

        payload = self.client.call(
            KrxRequest(api_name="index_daily", params={"target_date": target_date})
        )
        return [self._normalize_row(item) for item in payload]
