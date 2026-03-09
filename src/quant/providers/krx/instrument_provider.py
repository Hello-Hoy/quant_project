from __future__ import annotations
from dataclasses import dataclass
from datetime import date


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

class KrxInstrumentProvider:
    def fetch_instruments(self, target_date: str | None = None) -> list[InstrumentRow]:
        # TODO(provider): Map official KRX listing/ETF endpoints to InstrumentRow.
        # Keep empty return for scaffold to avoid fake external behavior.
        return []
