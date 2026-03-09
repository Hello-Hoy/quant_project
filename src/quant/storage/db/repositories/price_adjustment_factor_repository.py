from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant.storage.db.models.corporate_action import PriceAdjustmentFactor


class PriceAdjustmentFactorRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_factor(
        self,
        instrument_id: int,
        trade_date: date,
        cumulative_factor: float,
        factor_version: str,
        derived_from_event_count: int = 0,
    ) -> PriceAdjustmentFactor:
        existing = self.session.scalar(
            select(PriceAdjustmentFactor).where(
                PriceAdjustmentFactor.instrument_id == instrument_id,
                PriceAdjustmentFactor.trade_date == trade_date,
                PriceAdjustmentFactor.factor_version == factor_version,
            )
        )
        if existing is None:
            existing = PriceAdjustmentFactor(
                instrument_id=instrument_id,
                trade_date=trade_date,
                cumulative_factor=cumulative_factor,
                factor_version=factor_version,
                derived_from_event_count=derived_from_event_count,
                created_at=datetime.now(timezone.utc),
            )
            self.session.add(existing)
            self.session.flush()
            return existing
        existing.cumulative_factor = cumulative_factor
        existing.derived_from_event_count = derived_from_event_count
        self.session.add(existing)
        self.session.flush()
        return existing

    def get_factors_by_date(self, trade_date: date, factor_version: str) -> list[PriceAdjustmentFactor]:
        return list(
            self.session.scalars(
                select(PriceAdjustmentFactor).where(
                    PriceAdjustmentFactor.trade_date == trade_date,
                    PriceAdjustmentFactor.factor_version == factor_version,
                )
            )
        )

    def get_factor_map_by_date(self, trade_date: date, factor_version: str) -> dict[int, float]:
        rows = self.get_factors_by_date(trade_date=trade_date, factor_version=factor_version)
        return {int(row.instrument_id): float(row.cumulative_factor) for row in rows}
