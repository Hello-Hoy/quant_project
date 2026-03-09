from __future__ import annotations
from pathlib import Path

import pandas as pd

from quant.storage.parquet.paths import ParquetPaths


class DailyPriceStore:
    def __init__(self, paths: ParquetPaths | None = None) -> None:
        self.paths = paths or ParquetPaths()

    def write_partition(
        self,
        df: pd.DataFrame,
        market_code: str,
        year: int,
        month: int,
        filename: str = "part-000.parquet",
    ) -> Path:
        path = self.paths.daily_price_raw(market_code=market_code, year=year, month=month)
        self.paths.ensure_parent(path)
        file_path = path / filename
        df.to_parquet(file_path, index=False)
        return file_path

    def read_partition(self, market_code: str, year: int, month: int, filename: str = "part-000.parquet") -> pd.DataFrame:
        path = self.paths.daily_price_raw(market_code=market_code, year=year, month=month)
        file_path = path / filename
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        return pd.read_parquet(file_path)
