from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant.storage.db.models.instrument import EtfMetadata, InstrumentListingHistory, InstrumentMaster


class InstrumentRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_symbol_market(self, symbol: str, market_code: str, asset_type: str) -> InstrumentMaster | None:
        return self.session.scalar(
            select(InstrumentMaster).where(
                InstrumentMaster.symbol == symbol,
                InstrumentMaster.market_code == market_code,
                InstrumentMaster.asset_type == asset_type,
            )
        )

    def upsert_instrument(
        self,
        symbol: str,
        name_kr: str,
        market_code: str,
        asset_type: str,
        listing_status: str,
        is_tradable: bool,
        listing_date: date | None,
        delisting_date: date | None,
    ) -> InstrumentMaster:
        now = datetime.now(timezone.utc)
        existing = self.get_by_symbol_market(symbol=symbol, market_code=market_code, asset_type=asset_type)
        if existing is None:
            existing = InstrumentMaster(
                symbol=symbol,
                name_kr=name_kr,
                market_code=market_code,
                asset_type=asset_type,
                listing_status=listing_status,
                is_tradable=is_tradable,
                base_currency="KRW",
                listing_date=listing_date,
                delisting_date=delisting_date,
                created_at=now,
                updated_at=now,
            )
            self.session.add(existing)
            self.session.flush()
            return existing
        existing.name_kr = name_kr
        existing.listing_status = listing_status
        existing.is_tradable = is_tradable
        existing.listing_date = listing_date
        existing.delisting_date = delisting_date
        existing.updated_at = now
        self.session.add(existing)
        self.session.flush()
        return existing

    def add_listing_history(
        self,
        instrument_id: int,
        market_code: str,
        listing_status: str,
        effective_from: date,
        effective_to: date | None = None,
        note: str | None = None,
    ) -> InstrumentListingHistory:
        existing = self.session.scalar(
            select(InstrumentListingHistory).where(
                InstrumentListingHistory.instrument_id == instrument_id,
                InstrumentListingHistory.effective_from == effective_from,
            )
        )
        if existing is not None:
            existing.market_code = market_code
            existing.listing_status = listing_status
            existing.effective_to = effective_to
            existing.note = note
            self.session.add(existing)
            self.session.flush()
            return existing
        row = InstrumentListingHistory(
            instrument_id=instrument_id,
            market_code=market_code,
            listing_status=listing_status,
            effective_from=effective_from,
            effective_to=effective_to,
            note=note,
            created_at=datetime.now(timezone.utc),
        )
        self.session.add(row)
        self.session.flush()
        return row

    def upsert_etf_metadata(
        self,
        instrument_id: int,
        underlying_index: str | None = None,
        category: str | None = None,
        leverage_type: str = "NORMAL",
        management_company: str | None = None,
        expense_ratio: float | None = None,
    ) -> EtfMetadata:
        now = datetime.now(timezone.utc)
        existing = self.session.get(EtfMetadata, instrument_id)
        if existing is None:
            existing = EtfMetadata(
                instrument_id=instrument_id,
                underlying_index=underlying_index,
                category=category,
                leverage_type=leverage_type,
                management_company=management_company,
                expense_ratio=expense_ratio,
                created_at=now,
                updated_at=now,
            )
            self.session.add(existing)
            self.session.flush()
            return existing
        existing.underlying_index = underlying_index
        existing.category = category
        existing.leverage_type = leverage_type
        existing.management_company = management_company
        existing.expense_ratio = expense_ratio
        existing.updated_at = now
        self.session.add(existing)
        self.session.flush()
        return existing

    def get_active_tradable_instruments(self) -> list[InstrumentMaster]:
        return list(
            self.session.scalars(
                select(InstrumentMaster).where(
                    InstrumentMaster.is_tradable.is_(True),
                    InstrumentMaster.listing_status == "ACTIVE",
                )
            )
        )
