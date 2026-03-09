from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from pandas.tseries.offsets import DateOffset
from sqlalchemy.orm import Session
from quant.bootstrap.config_loader import ConfigLoader
from quant.storage.parquet.adjusted_price_store import AdjustedPriceStore
from quant.storage.parquet.daily_price_store import DailyPriceStore
from quant.storage.parquet.feature_store import FeatureStore
from quant.storage.parquet.index_store import IndexStore
from quant.storage.parquet.universe_store import UniverseStore
from quant.services.feature.feature_library import FeatureLibrary

@dataclass
class FeatureBuildResult:
    row_count: int
    feature_set_name: str
    artifacts: list[str]
    message: str | None = None

class FeatureBuilderService:
    def __init__(self, session: Session, config_loader: ConfigLoader | None = None, adjusted_price_store: AdjustedPriceStore | None = None, daily_price_store: DailyPriceStore | None = None, index_store: IndexStore | None = None, universe_store: UniverseStore | None = None, feature_store: FeatureStore | None = None) -> None:
        self.config_loader = config_loader or ConfigLoader()
        self.adjusted_price_store = adjusted_price_store or AdjustedPriceStore()
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.index_store = index_store or IndexStore()
        self.universe_store = universe_store or UniverseStore()
        self.feature_store = feature_store or FeatureStore()

    def _month_windows(self, target_date: str, months_back: int = 6):
        ts = pd.to_datetime(target_date)
        start = ts - DateOffset(months=months_back)
        months = pd.period_range(start=start, end=ts, freq="M")
        return [(int(m.year), int(m.month)) for m in months]

    def _read_months(self, store, keys, target_date):
        frames = []
        for year, month in self._month_windows(target_date, months_back=6):
            for key in keys:
                try:
                    if store.__class__.__name__ == "AdjustedPriceStore":
                        frames.append(store.read_partition(asset_type=key, year=year, month=month))
                    elif store.__class__.__name__ == "DailyPriceStore":
                        frames.append(store.read_partition(market_code=key, year=year, month=month))
                    else:
                        frames.append(store.read_partition(index_family=key, year=year, month=month))
                except FileNotFoundError:
                    continue
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        out["trade_date"] = pd.to_datetime(out["trade_date"])
        return out.drop_duplicates()

    def build(self, target_date: str, feature_set_name: str, universe_name: str) -> FeatureBuildResult:
        cfg = self.config_loader.load_feature_set_config(feature_set_name)
        benchmark_family = cfg.get("benchmark_family", "KOSPI")
        try:
            universe_df = self.universe_store.read_snapshot(universe_name=universe_name, snapshot_date=target_date)
        except FileNotFoundError:
            return FeatureBuildResult(0, feature_set_name, [], f"Universe snapshot not found for {target_date}, universe={universe_name}")
        universe_df = universe_df[universe_df["included_flag"] == True].copy()
        if universe_df.empty:
            return FeatureBuildResult(0, feature_set_name, [], f"No included universe members for {target_date}")

        adjusted_df = self._read_months(self.adjusted_price_store, ["COMMON","ETF"], target_date)
        raw_df = self._read_months(self.daily_price_store, ["KOSPI","KOSDAQ","ETF"], target_date)
        index_df = self._read_months(self.index_store, ["KOSPI","KOSDAQ","KOSPI200","SECTOR"], target_date)
        if adjusted_df.empty or raw_df.empty:
            return FeatureBuildResult(0, feature_set_name, [], f"Adjusted/raw history missing for {target_date}")

        instrument_ids = set(universe_df["instrument_id"].tolist())
        merged = adjusted_df.merge(raw_df[["trade_date","instrument_id","turnover"]], on=["trade_date","instrument_id"], how="left")
        merged = merged[merged["instrument_id"].isin(instrument_ids)].copy().sort_values(["instrument_id","trade_date"]).reset_index(drop=True)

        merged = FeatureLibrary.add_returns(merged, price_col="adj_close")
        merged = FeatureLibrary.add_volatility(merged, price_col="adj_close")
        merged = FeatureLibrary.add_adv(merged, turnover_col="turnover")
        merged = FeatureLibrary.add_breakout(merged, price_col="adj_close")

        benchmark_df = index_df[index_df["index_family"] == benchmark_family].copy().sort_values("trade_date").drop_duplicates(subset=["trade_date"])
        if not benchmark_df.empty:
            merged = FeatureLibrary.add_relative_strength_vs_benchmark(merged, benchmark_df=benchmark_df)
        else:
            merged["relative_strength_vs_benchmark"] = None

        target_ts = pd.to_datetime(target_date)
        snapshot_df = merged[merged["trade_date"] == target_ts].copy()
        if snapshot_df.empty:
            return FeatureBuildResult(0, feature_set_name, [], f"No feature snapshot rows available for {target_date}")

        snapshot_df["snapshot_date"] = target_ts.date().isoformat()
        snapshot_df["feature_set_name"] = feature_set_name
        out = snapshot_df[["snapshot_date","feature_set_name","instrument_id","ret_5d","ret_20d","ret_60d","vol_20d","adv_20d","breakout_20d","relative_strength_vs_benchmark"]].copy()
        file_path = self.feature_store.write_snapshot(out.reset_index(drop=True), feature_set_name=feature_set_name, snapshot_date=target_date)
        return FeatureBuildResult(len(out), feature_set_name, [str(file_path)], f"Feature snapshot built for {target_date}")
