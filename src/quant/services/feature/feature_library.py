from __future__ import annotations
import pandas as pd

class FeatureLibrary:
    @staticmethod
    def add_returns(df: pd.DataFrame, price_col: str = "adj_close") -> pd.DataFrame:
        out = df.copy().sort_values(["instrument_id","trade_date"])
        grouped = out.groupby("instrument_id")[price_col]
        out["ret_5d"] = grouped.pct_change(5)
        out["ret_20d"] = grouped.pct_change(20)
        out["ret_60d"] = grouped.pct_change(60)
        return out

    @staticmethod
    def add_volatility(df: pd.DataFrame, price_col: str = "adj_close") -> pd.DataFrame:
        out = df.copy().sort_values(["instrument_id","trade_date"])
        grouped = out.groupby("instrument_id")[price_col]
        daily_ret = grouped.pct_change(1)
        out["vol_20d"] = daily_ret.groupby(out["instrument_id"]).rolling(20).std().reset_index(level=0, drop=True)
        return out

    @staticmethod
    def add_adv(df: pd.DataFrame, turnover_col: str = "turnover") -> pd.DataFrame:
        out = df.copy().sort_values(["instrument_id","trade_date"])
        grouped = out.groupby("instrument_id")[turnover_col]
        out["adv_20d"] = grouped.rolling(20).mean().reset_index(level=0, drop=True)
        return out

    @staticmethod
    def add_breakout(df: pd.DataFrame, price_col: str = "adj_close") -> pd.DataFrame:
        out = df.copy().sort_values(["instrument_id","trade_date"])
        rolling_max_20 = out.groupby("instrument_id")[price_col].rolling(20).max().reset_index(level=0, drop=True).shift(1)
        out["breakout_20d"] = out[price_col] >= rolling_max_20
        return out

    @staticmethod
    def add_relative_strength_vs_benchmark(df: pd.DataFrame, benchmark_df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        bench = benchmark_df.copy().sort_values("trade_date")
        bench["bench_ret_20d"] = bench["close"].pct_change(20)
        out = out.merge(bench[["trade_date","bench_ret_20d"]], on="trade_date", how="left")
        out["relative_strength_vs_benchmark"] = out["ret_20d"] - out["bench_ret_20d"] if "ret_20d" in out.columns else None
        return out
