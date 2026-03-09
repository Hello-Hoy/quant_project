from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from sqlalchemy.orm import Session
from quant.storage.db.repositories.price_adjustment_factor_repository import PriceAdjustmentFactorRepository
from quant.storage.parquet.daily_price_store import DailyPriceStore

@dataclass
class FactorBuildResult:
    row_count: int
    factor_version: str
    message: str | None = None

class FactorBuilderService:
    def __init__(self, session: Session, daily_price_store: DailyPriceStore | None = None, factor_repository: PriceAdjustmentFactorRepository | None = None) -> None:
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.factor_repository = factor_repository or PriceAdjustmentFactorRepository(session)

    def _load_daily_price_df(self, target_date: str) -> pd.DataFrame:
        ts = pd.to_datetime(target_date); year, month = int(ts.year), int(ts.month)
        frames = []
        for market_code in ["KOSPI","KOSDAQ","ETF"]:
            try:
                frames.append(self.daily_price_store.read_partition(market_code=market_code, year=year, month=month))
            except FileNotFoundError:
                continue
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return merged[merged["trade_date"] == ts].copy()

    def build(self, target_date: str, factor_version: str = "v0_identity") -> FactorBuildResult:
        df = self._load_daily_price_df(target_date)
        if df.empty:
            return FactorBuildResult(0, factor_version, f"No raw daily price rows found for {target_date}")
        trade_date = pd.to_datetime(target_date).date()
        unique_instruments = df[["instrument_id"]].drop_duplicates().sort_values("instrument_id").reset_index(drop=True)
        # TODO(adjustment): Replace identity factor generation with corporate-action-aware factor chain.
        for instrument_id in unique_instruments["instrument_id"].tolist():
            self.factor_repository.upsert_factor(
                instrument_id=int(instrument_id), trade_date=trade_date, cumulative_factor=1.0,
                factor_version=factor_version, derived_from_event_count=0
            )
        return FactorBuildResult(len(unique_instruments), factor_version, f"Built placeholder identity adjustment factors for {target_date}")
