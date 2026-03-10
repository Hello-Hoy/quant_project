from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy.orm import Session

from quant.services.adjustment.factor_rules import compute_cumulative_factor
from quant.storage.db.repositories.corporate_action_event_repository import CorporateActionEventRepository
from quant.storage.db.repositories.ingestion_run_repository import IngestionRunRepository
from quant.storage.db.repositories.price_adjustment_factor_repository import PriceAdjustmentFactorRepository
from quant.storage.parquet.daily_price_store import DailyPriceStore


@dataclass
class FactorBuildResult:
    row_count: int
    factor_version: str
    derived_event_count_total: int
    preflight_corporate_action_synced: bool = False
    preflight_corporate_action_status: str | None = None
    message: str | None = None


class FactorBuilderService:
    def __init__(
        self,
        session: Session,
        daily_price_store: DailyPriceStore | None = None,
        factor_repository: PriceAdjustmentFactorRepository | None = None,
        corporate_action_repository: CorporateActionEventRepository | None = None,
        ingestion_run_repository: IngestionRunRepository | None = None,
    ) -> None:
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.factor_repository = factor_repository or PriceAdjustmentFactorRepository(session)
        self.corporate_action_repository = corporate_action_repository or CorporateActionEventRepository(session)
        self.ingestion_run_repository = ingestion_run_repository or IngestionRunRepository(session)

    def _load_daily_price_df(self, target_date: str) -> pd.DataFrame:
        ts = pd.to_datetime(target_date)
        year, month = int(ts.year), int(ts.month)
        frames = []
        for market_code in ["KOSPI", "KOSDAQ", "ETF"]:
            try:
                frames.append(self.daily_price_store.read_partition(market_code=market_code, year=year, month=month))
            except FileNotFoundError:
                continue
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return merged[merged["trade_date"] == ts].copy()

    def build(self, target_date: str, factor_version: str = "v1_corporate_action") -> FactorBuildResult:
        df = self._load_daily_price_df(target_date)
        if df.empty:
            return FactorBuildResult(
                row_count=0,
                factor_version=factor_version,
                derived_event_count_total=0,
                preflight_corporate_action_synced=False,
                preflight_corporate_action_status=None,
                message=f"No raw daily price rows found for {target_date}",
            )

        trade_date = pd.to_datetime(target_date).date()
        latest_sync_run = self.ingestion_run_repository.get_latest_run_by_job_and_target_date(
            job_name="sync_corporate_action_events",
            target_date=trade_date,
        )
        preflight_synced = (
            latest_sync_run is not None and latest_sync_run.status in {"SUCCESS", "WARNING"}
        )
        preflight_status = latest_sync_run.status if latest_sync_run is not None else None

        unique_instruments = (
            df[["instrument_id"]]
            .drop_duplicates()
            .sort_values("instrument_id")
            .reset_index(drop=True)
        )

        instrument_ids = [int(value) for value in unique_instruments["instrument_id"].tolist()]
        event_map = self.corporate_action_repository.get_events_for_instruments_up_to_date(
            instrument_ids=instrument_ids,
            target_date=trade_date,
        )

        derived_event_count_total = 0
        for instrument_id in instrument_ids:
            computation = compute_cumulative_factor(
                events=event_map.get(instrument_id, []),
                up_to_date=trade_date,
            )
            self.factor_repository.upsert_factor(
                instrument_id=instrument_id,
                trade_date=trade_date,
                cumulative_factor=computation.cumulative_factor,
                factor_version=factor_version,
                derived_from_event_count=computation.derived_event_count,
            )
            derived_event_count_total += computation.derived_event_count

        message = (
            f"Built corporate-action adjustment factors for {target_date} "
            f"(instruments={len(instrument_ids)}, derived_events={derived_event_count_total})"
        )
        if not preflight_synced:
            message = (
                f"{message}; preflight: sync_corporate_action_events not found for {target_date}"
            )
        return FactorBuildResult(
            row_count=len(instrument_ids),
            factor_version=factor_version,
            derived_event_count_total=derived_event_count_total,
            preflight_corporate_action_synced=preflight_synced,
            preflight_corporate_action_status=preflight_status,
            message=message,
        )
