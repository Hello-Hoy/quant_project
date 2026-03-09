from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant.storage.parquet.paths import ParquetPaths


class FeatureStore:
    def __init__(self, paths: ParquetPaths | None = None) -> None:
        self.paths = paths or ParquetPaths()

    def write_snapshot(
        self,
        df: pd.DataFrame,
        feature_set_name: str,
        snapshot_date: str,
        filename: str = "part-000.parquet",
    ) -> Path:
        path = self.paths.feature_snapshot(feature_set_name=feature_set_name, snapshot_date=snapshot_date)
        self.paths.ensure_parent(path)
        file_path = path / filename
        df.to_parquet(file_path, index=False)
        return file_path

    def read_snapshot(
        self,
        feature_set_name: str,
        snapshot_date: str,
        filename: str = "part-000.parquet",
    ) -> pd.DataFrame:
        path = self.paths.feature_snapshot(feature_set_name=feature_set_name, snapshot_date=snapshot_date)
        file_path = path / filename
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        return pd.read_parquet(file_path)
