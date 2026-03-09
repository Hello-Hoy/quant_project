from __future__ import annotations
from dataclasses import dataclass
from sqlalchemy.orm import Session
from quant.providers.krx.instrument_provider import KrxInstrumentProvider
from quant.storage.db.repositories.instrument_repository import InstrumentRepository

@dataclass
class InstrumentMasterSyncResult:
    row_count: int
    etf_count: int
    message: str | None = None

class InstrumentMasterService:
    def __init__(self, session: Session, provider: KrxInstrumentProvider | None = None, repository: InstrumentRepository | None = None) -> None:
        self.provider = provider or KrxInstrumentProvider()
        self.repository = repository or InstrumentRepository(session)

    def sync(self, target_date: str | None = None, force: bool = False) -> InstrumentMasterSyncResult:
        rows = self.provider.fetch_instruments(target_date=target_date)
        if not rows:
            return InstrumentMasterSyncResult(0, 0, f"No instrument rows returned for target_date={target_date}")

        total_count = 0
        etf_count = 0
        for row in rows:
            instrument = self.repository.upsert_instrument(
                symbol=row.symbol, name_kr=row.name_kr, market_code=row.market_code,
                asset_type=row.asset_type, listing_status=row.listing_status,
                is_tradable=row.is_tradable, listing_date=row.listing_date, delisting_date=row.delisting_date
            )
            self.repository.add_listing_history(
                instrument_id=instrument.instrument_id, market_code=row.market_code, listing_status=row.listing_status,
                effective_from=row.listing_date or instrument.created_at.date(), effective_to=row.delisting_date,
                note="synced_from_krx_provider"
            )
            if row.is_etf:
                etf_count += 1
                self.repository.upsert_etf_metadata(
                    instrument_id=instrument.instrument_id, underlying_index=row.underlying_index,
                    category=row.etf_category, leverage_type=row.leverage_type or "NORMAL",
                    management_company=row.management_company, expense_ratio=row.expense_ratio
                )
            total_count += 1
        return InstrumentMasterSyncResult(total_count, etf_count, f"Instrument master synced for target_date={target_date}")
