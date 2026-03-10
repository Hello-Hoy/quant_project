from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from quant.storage.db.repositories.price_adjustment_factor_repository import PriceAdjustmentFactorRepository
from quant.storage.parquet.adjusted_price_store import AdjustedPriceStore
from quant.storage.parquet.daily_price_store import DailyPriceStore

@dataclass
class AdjustedPriceBuildResult:
    row_count: int
    adjustment_version: str
    artifacts: list[str]
    message: str | None = None

class AdjustedPriceBuilderService:
    def __init__(self, session: Session, daily_price_store: DailyPriceStore | None = None, adjusted_price_store: AdjustedPriceStore | None = None, factor_repository: PriceAdjustmentFactorRepository | None = None) -> None:
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.adjusted_price_store = adjusted_price_store or AdjustedPriceStore()
        self.factor_repository = factor_repository or PriceAdjustmentFactorRepository(session)

    def _load_raw_daily_price_df(self, target_date: str) -> pd.DataFrame:
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

    def build(self, target_date: str, adjustment_version: str = "v1_corporate_action") -> AdjustedPriceBuildResult:
        raw_df = self._load_raw_daily_price_df(target_date)
        if raw_df.empty:
            return AdjustedPriceBuildResult(0, adjustment_version, [], f"No raw daily price rows found for {target_date}")
        trade_date = pd.to_datetime(target_date).date()
        factor_map = self.factor_repository.get_factor_map_by_date(trade_date=trade_date, factor_version=adjustment_version)
        if not factor_map:
            return AdjustedPriceBuildResult(0, adjustment_version, [], f"No adjustment factors found for {target_date} and factor_version={adjustment_version}")
        df = raw_df.copy()
        df["adjustment_factor"] = df["instrument_id"].map(factor_map).fillna(1.0)
        for c in ["open","high","low","close"]:
            df[f"adj_{c}"] = df[c] * df["adjustment_factor"]
        df["adjustment_version"] = adjustment_version
        df["source_price_type"] = "RAW"
        df["built_at"] = datetime.now().isoformat()
        df["asset_type"] = df["market_code"].map({"ETF":"ETF","KOSPI":"COMMON","KOSDAQ":"COMMON"}).fillna("COMMON")
        out_df = df[["trade_date","instrument_id","asset_type","adj_open","adj_high","adj_low","adj_close","adjustment_factor","adjustment_version","source_price_type","built_at"]].copy()
        year, month = int(pd.to_datetime(target_date).year), int(pd.to_datetime(target_date).month)
        artifacts = []
        for asset_type, sub_df in out_df.groupby("asset_type"):
            artifacts.append(str(self.adjusted_price_store.write_partition(sub_df.reset_index(drop=True), asset_type=asset_type, year=year, month=month)))
        return AdjustedPriceBuildResult(len(out_df), adjustment_version, artifacts, f"Adjusted daily prices built for {target_date}")
