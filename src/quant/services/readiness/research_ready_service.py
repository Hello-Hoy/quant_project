from __future__ import annotations
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.orm import Session

from quant.bootstrap.settings import settings
from quant.core.enums import ValidationResult
from quant.storage.db.repositories.calendar_repository import CalendarRepository
from quant.storage.db.repositories.instrument_repository import InstrumentRepository
from quant.storage.db.repositories.research_ready_repository import ResearchReadyRepository
from quant.storage.db.repositories.validation_repository import ValidationRepository
from quant.storage.parquet.adjusted_price_store import AdjustedPriceStore
from quant.storage.parquet.daily_price_store import DailyPriceStore
from quant.storage.parquet.feature_store import FeatureStore

@dataclass
class ResearchReadyResult:
    reference_ready: bool
    raw_ready: bool
    validated: bool
    adjusted_ready: bool
    feature_ready: bool
    research_ready: bool
    message: str | None = None

class ResearchReadyService:
    def __init__(self, session: Session, calendar_repository: CalendarRepository | None = None, instrument_repository: InstrumentRepository | None = None, validation_repository: ValidationRepository | None = None, research_ready_repository: ResearchReadyRepository | None = None, daily_price_store: DailyPriceStore | None = None, adjusted_price_store: AdjustedPriceStore | None = None, feature_store: FeatureStore | None = None) -> None:
        self.calendar_repository = calendar_repository or CalendarRepository(session)
        self.instrument_repository = instrument_repository or InstrumentRepository(session)
        self.validation_repository = validation_repository or ValidationRepository(session)
        self.research_ready_repository = research_ready_repository or ResearchReadyRepository(session)
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.adjusted_price_store = adjusted_price_store or AdjustedPriceStore()
        self.feature_store = feature_store or FeatureStore()

    def _has_raw_data(self, target_date: str) -> bool:
        ts = pd.to_datetime(target_date); year, month = int(ts.year), int(ts.month)
        frames = []
        for market_code in ["KOSPI","KOSDAQ","ETF"]:
            try: frames.append(self.daily_price_store.read_partition(market_code=market_code, year=year, month=month))
            except FileNotFoundError: continue
        if not frames: return False
        merged = pd.concat(frames, ignore_index=True); merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return not merged[merged["trade_date"] == ts].empty

    def _has_adjusted_data(self, target_date: str) -> bool:
        ts = pd.to_datetime(target_date); year, month = int(ts.year), int(ts.month)
        frames = []
        for asset_type in ["COMMON","ETF"]:
            try: frames.append(self.adjusted_price_store.read_partition(asset_type=asset_type, year=year, month=month))
            except FileNotFoundError: continue
        if not frames: return False
        merged = pd.concat(frames, ignore_index=True); merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return not merged[merged["trade_date"] == ts].empty

    def _has_feature_data(self, target_date: str) -> bool:
        try:
            df = self.feature_store.read_snapshot(feature_set_name=settings.default_feature_set_name, snapshot_date=target_date)
            return not df.empty
        except FileNotFoundError:
            return False

    def update(self, target_date: str) -> ResearchReadyResult:
        trade_date = pd.to_datetime(target_date).date()
        calendar_row = self.calendar_repository.get_by_date(trade_date)
        instrument_count = len(self.instrument_repository.get_active_tradable_instruments())
        validation_rows = self.validation_repository.get_results_by_date_domain(target_date=trade_date, data_domain="DAILY_PRICE")
        reference_ready = calendar_row is not None and instrument_count > 0
        raw_ready = self._has_raw_data(target_date)
        adjusted_ready = self._has_adjusted_data(target_date)
        feature_ready = self._has_feature_data(target_date)
        has_fail = any(str(row.result) == ValidationResult.FAIL.value for row in validation_rows)
        validated = len(validation_rows) > 0 and not has_fail
        research_ready = reference_ready and raw_ready and validated and adjusted_ready and feature_ready
        notes = []
        if not reference_ready: notes.append("reference_not_ready")
        if not raw_ready: notes.append("raw_not_ready")
        if not validated: notes.append("validation_not_passed")
        if not adjusted_ready: notes.append("adjusted_not_ready")
        if not feature_ready: notes.append("feature_not_ready")
        status_note = ",".join(notes) if notes else "ready"
        self.research_ready_repository.upsert_status(trade_date, reference_ready, raw_ready, validated, adjusted_ready, feature_ready, research_ready, status_note)
        return ResearchReadyResult(reference_ready, raw_ready, validated, adjusted_ready, feature_ready, research_ready, f"Research readiness updated for {target_date}: {status_note}")
