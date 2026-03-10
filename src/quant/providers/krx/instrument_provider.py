from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import cast

from quant.providers.base import ProviderAdapter, ProviderMode
from quant.providers.krx.client import KrxClient, KrxRequest
from quant.providers.normalize import parse_bool, parse_date, parse_float, parse_str, pick_value


@dataclass
class InstrumentRow:
    symbol: str
    name_kr: str
    market_code: str
    asset_type: str
    listing_status: str
    is_tradable: bool
    listing_date: date | None = None
    delisting_date: date | None = None
    is_etf: bool = False
    underlying_index: str | None = None
    etf_category: str | None = None
    leverage_type: str | None = None
    management_company: str | None = None
    expense_ratio: float | None = None


class KrxInstrumentProvider(ProviderAdapter):
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

    def _normalize_row(self, item: dict) -> InstrumentRow:
        symbol = parse_str(
            pick_value(item, ["symbol", "ticker", "isu_cd", "short_code"], "symbol"),
            "symbol",
        )
        name_kr = parse_str(
            pick_value(item, ["name_kr", "name", "kor_name", "itms_nm"], "name_kr"),
            "name_kr",
        )
        market_code = parse_str(
            pick_value(item, ["market_code", "market", "mrkt"], "market_code"),
            "market_code",
        )
        asset_type = parse_str(
            pick_value(item, ["asset_type", "asset", "instrument_type"], "asset_type"),
            "asset_type",
        )
        listing_status = parse_str(
            pick_value(item, ["listing_status", "status"], "listing_status"),
            "listing_status",
        )
        is_tradable = parse_bool(
            pick_value(item, ["is_tradable", "tradable", "trade_flag"], "is_tradable"),
            "is_tradable",
        )

        return InstrumentRow(
            symbol=cast(str, symbol),
            name_kr=cast(str, name_kr),
            market_code=cast(str, market_code),
            asset_type=cast(str, asset_type),
            listing_status=cast(str, listing_status),
            is_tradable=cast(bool, is_tradable),
            listing_date=parse_date(
                pick_value(item, ["listing_date", "list_date"], "listing_date", required=False),
                "listing_date",
                required=False,
            ),
            delisting_date=parse_date(
                pick_value(item, ["delisting_date", "delist_date"], "delisting_date", required=False),
                "delisting_date",
                required=False,
            ),
            is_etf=bool(
                parse_bool(
                    pick_value(item, ["is_etf", "etf_flag"], "is_etf", required=False),
                    "is_etf",
                    required=False,
                )
                or False
            ),
            underlying_index=parse_str(
                pick_value(item, ["underlying_index"], "underlying_index", required=False),
                "underlying_index",
                required=False,
            ),
            etf_category=parse_str(
                pick_value(item, ["etf_category", "category"], "etf_category", required=False),
                "etf_category",
                required=False,
            ),
            leverage_type=parse_str(
                pick_value(item, ["leverage_type"], "leverage_type", required=False),
                "leverage_type",
                required=False,
            ),
            management_company=parse_str(
                pick_value(item, ["management_company", "manager"], "management_company", required=False),
                "management_company",
                required=False,
            ),
            expense_ratio=parse_float(
                pick_value(item, ["expense_ratio"], "expense_ratio", required=False),
                "expense_ratio",
                required=False,
            ),
        )

    def fetch_instruments(self, target_date: str | None = None) -> list[InstrumentRow]:
        if not self.enabled or self.mode == ProviderMode.DISABLED:
            return []
        if self.mode == ProviderMode.PLACEHOLDER:
            return []

        payload = self.client.call(
            KrxRequest(
                api_name="instrument_master",
                params={"target_date": target_date} if target_date else {},
            )
        )
        return [self._normalize_row(item) for item in payload]
