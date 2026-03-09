from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant.storage.parquet.paths import ParquetPaths


class IndexStore:
    def __init__(self, paths: ParquetPaths | None = None) -> None:
        self.paths = paths or ParquetPaths()

    def write_partition(
        self,
        df: pd.DataFrame,
        index_family: str,
        year: int,
        month: int,
        filename: str = "part-000.parquet",
    ) -> Path:
        path = self.paths.index_daily(index_family=index_family, year=year, month=month)
        self.paths.ensure_parent(path)
        file_path = path / filename
        df.to_parquet(file_path, index=False)
        return file_path

    def read_partition(
        self,
        index_family: str,
        year: int,
        month: int,
        filename: str = "part-000.parquet",
    ) -> pd.DataFrame:
        path = self.paths.index_daily(index_family=index_family, year=year, month=month)
        file_path = path / filename
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        return pd.read_parquet(file_path)
