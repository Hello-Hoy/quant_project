from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from sqlalchemy.orm import Session
from quant.bootstrap.config_loader import ConfigLoader
from quant.storage.db.repositories.instrument_repository import InstrumentRepository
from quant.storage.parquet.adjusted_price_store import AdjustedPriceStore
from quant.storage.parquet.daily_price_store import DailyPriceStore
from quant.storage.parquet.universe_store import UniverseStore

@dataclass
class UniverseBuildResult:
    row_count: int
    universe_name: str
    artifacts: list[str]
    message: str | None = None

class UniverseBuilderService:
    def __init__(self, session: Session, config_loader: ConfigLoader | None = None, instrument_repository: InstrumentRepository | None = None, adjusted_price_store: AdjustedPriceStore | None = None, daily_price_store: DailyPriceStore | None = None, universe_store: UniverseStore | None = None) -> None:
        self.config_loader = config_loader or ConfigLoader()
        self.instrument_repository = instrument_repository or InstrumentRepository(session)
        self.adjusted_price_store = adjusted_price_store or AdjustedPriceStore()
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.universe_store = universe_store or UniverseStore()

    def _load_adjusted_df(self, target_date: str) -> pd.DataFrame:
        ts = pd.to_datetime(target_date); year, month = int(ts.year), int(ts.month)
        frames = []
        for asset_type in ["COMMON","ETF"]:
            try:
                frames.append(self.adjusted_price_store.read_partition(asset_type=asset_type, year=year, month=month))
            except FileNotFoundError:
                continue
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return merged[merged["trade_date"] == ts].copy()

    def _load_raw_df(self, target_date: str) -> pd.DataFrame:
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

    def build(self, target_date: str, universe_name: str) -> UniverseBuildResult:
        config = self.config_loader.load_universe_config(universe_name)
        min_turnover = float(config.get("min_turnover", 0))
        min_price = float(config.get("min_price", 0))
        include_etf = bool(config.get("include_etf", True))
        adjusted_df = self._load_adjusted_df(target_date)
        raw_df = self._load_raw_df(target_date)
        if adjusted_df.empty or raw_df.empty:
            return UniverseBuildResult(0, universe_name, [], f"Adjusted/raw data missing for {target_date}")
        tradable = self.instrument_repository.get_active_tradable_instruments()
        instrument_df = pd.DataFrame([{
            "instrument_id": row.instrument_id, "symbol": row.symbol, "name_kr": row.name_kr,
            "market_code": row.market_code, "asset_type": row.asset_type,
            "listing_status": row.listing_status, "is_tradable": row.is_tradable
        } for row in tradable])
        if instrument_df.empty:
            return UniverseBuildResult(0, universe_name, [], "No active tradable instruments found")
        merged = raw_df.merge(adjusted_df[["trade_date","instrument_id","adj_close","asset_type"]], on=["trade_date","instrument_id"], how="inner").merge(instrument_df, on="instrument_id", how="inner", suffixes=("", "_inst"))
        merged["included_flag"] = True
        merged["exclusion_reason"] = None
        merged.loc[merged["is_tradable"] != True, "included_flag"] = False
        merged.loc[merged["is_tradable"] != True, "exclusion_reason"] = "not_tradable"
        merged.loc[merged["listing_status"] != "ACTIVE", "included_flag"] = False
        merged.loc[merged["listing_status"] != "ACTIVE", "exclusion_reason"] = "listing_status_not_active"
        merged.loc[merged["turnover"].fillna(0) < min_turnover, "included_flag"] = False
        merged.loc[merged["turnover"].fillna(0) < min_turnover, "exclusion_reason"] = "below_min_turnover"
        merged.loc[merged["adj_close"].fillna(0) < min_price, "included_flag"] = False
        merged.loc[merged["adj_close"].fillna(0) < min_price, "exclusion_reason"] = "below_min_price"
        if not include_etf:
            merged.loc[merged["asset_type"] == "ETF", "included_flag"] = False
            merged.loc[merged["asset_type"] == "ETF", "exclusion_reason"] = "etf_excluded"
        out = merged[["instrument_id","symbol","name_kr","market_code","asset_type","included_flag","exclusion_reason"]].copy()
        out["snapshot_date"] = pd.to_datetime(target_date).date().isoformat()
        out["universe_name"] = universe_name
        out = out[["snapshot_date","universe_name","instrument_id","symbol","name_kr","market_code","asset_type","included_flag","exclusion_reason"]]
        file_path = self.universe_store.write_snapshot(out.reset_index(drop=True), universe_name=universe_name, snapshot_date=target_date)
        return UniverseBuildResult(len(out), universe_name, [str(file_path)], f"Universe snapshot built for {target_date}")
