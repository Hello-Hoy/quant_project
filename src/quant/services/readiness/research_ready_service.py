from __future__ import annotations
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.orm import Session

from quant.bootstrap.config_loader import ConfigLoader
from quant.bootstrap.settings import settings
from quant.core.enums import ValidationResult
from quant.storage.db.repositories.calendar_repository import CalendarRepository
from quant.storage.db.repositories.ingestion_run_repository import IngestionRunRepository
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
    corporate_action_sync_ready: bool
    corporate_action_sync_status: str | None
    require_corporate_action_sync_preflight: bool
    research_ready: bool
    message: str | None = None

class ResearchReadyService:
    def __init__(
        self,
        session: Session,
        calendar_repository: CalendarRepository | None = None,
        instrument_repository: InstrumentRepository | None = None,
        validation_repository: ValidationRepository | None = None,
        research_ready_repository: ResearchReadyRepository | None = None,
        ingestion_run_repository: IngestionRunRepository | None = None,
        daily_price_store: DailyPriceStore | None = None,
        adjusted_price_store: AdjustedPriceStore | None = None,
        feature_store: FeatureStore | None = None,
        config_loader: ConfigLoader | None = None,
        require_corporate_action_sync_preflight: bool | None = None,
    ) -> None:
        self.calendar_repository = calendar_repository or CalendarRepository(session)
        self.instrument_repository = instrument_repository or InstrumentRepository(session)
        self.validation_repository = validation_repository or ValidationRepository(session)
        self.research_ready_repository = research_ready_repository or ResearchReadyRepository(session)
        self.ingestion_run_repository = ingestion_run_repository or IngestionRunRepository(session)
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.adjusted_price_store = adjusted_price_store or AdjustedPriceStore()
        self.feature_store = feature_store or FeatureStore()
        self.config_loader = config_loader or ConfigLoader()
        if require_corporate_action_sync_preflight is None:
            app_cfg = self.config_loader.load_app_config()
            readiness_cfg = app_cfg.get("readiness", {})
            if not isinstance(readiness_cfg, dict):
                readiness_cfg = {}
            self.require_corporate_action_sync_preflight = bool(
                readiness_cfg.get("require_corporate_action_sync_preflight", True)
            )
        else:
            self.require_corporate_action_sync_preflight = require_corporate_action_sync_preflight

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

    def _is_corporate_action_sync_ready(self, trade_date) -> tuple[bool, str | None]:  # noqa: ANN001
        latest_run = self.ingestion_run_repository.get_latest_run_by_job_and_target_date(
            job_name="sync_corporate_action_events",
            target_date=trade_date,
        )
        if latest_run is None:
            return False, None
        status = str(latest_run.status)
        return status in {"SUCCESS", "WARNING"}, status

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
        corporate_action_sync_ready = True
        sync_status: str | None = "NOT_REQUIRED"
        if self.require_corporate_action_sync_preflight:
            corporate_action_sync_ready, sync_status = self._is_corporate_action_sync_ready(trade_date)

        research_ready = (
            reference_ready
            and raw_ready
            and validated
            and adjusted_ready
            and feature_ready
            and corporate_action_sync_ready
        )
        notes = []
        if not reference_ready: notes.append("reference_not_ready")
        if not raw_ready: notes.append("raw_not_ready")
        if not validated: notes.append("validation_not_passed")
        if not adjusted_ready: notes.append("adjusted_not_ready")
        if not feature_ready: notes.append("feature_not_ready")
        if not corporate_action_sync_ready:
            notes.append(f"corporate_action_sync_not_ready({sync_status})")
        status_note = ",".join(notes) if notes else "ready"
        self.research_ready_repository.upsert_status(trade_date, reference_ready, raw_ready, validated, adjusted_ready, feature_ready, research_ready, status_note)
        return ResearchReadyResult(
            reference_ready,
            raw_ready,
            validated,
            adjusted_ready,
            feature_ready,
            corporate_action_sync_ready,
            sync_status,
            self.require_corporate_action_sync_preflight,
            research_ready,
            f"Research readiness updated for {target_date}: {status_note}",
        )
