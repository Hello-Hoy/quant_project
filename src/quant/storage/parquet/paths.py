from __future__ import annotations

from pathlib import Path
from re import sub

from quant.bootstrap.config_loader import ConfigLoader
from quant.bootstrap.settings import settings


def _slug(value: str) -> str:
    return sub(r"[^A-Za-z0-9_\-]", "_", value.strip())


class ParquetPaths:
    def __init__(self, raw_root: Path | None = None, curated_root: Path | None = None, feature_root: Path | None = None) -> None:
        storage_cfg = ConfigLoader().load_storage_config()
        parquet_cfg = storage_cfg.get("parquet", {})

        configured_raw = parquet_cfg.get("raw_root", settings.raw_data_root)
        configured_curated = parquet_cfg.get("curated_root", settings.curated_data_root)
        configured_feature = parquet_cfg.get("features_root", settings.feature_data_root)

        self.raw_root = settings.resolve_path(raw_root or configured_raw)
        self.curated_root = settings.resolve_path(curated_root or configured_curated)
        self.feature_root = settings.resolve_path(feature_root or configured_feature)
        for root in [self.raw_root, self.curated_root, self.feature_root]:
            root.mkdir(parents=True, exist_ok=True)

    def ensure_parent(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def daily_price_raw(self, market_code: str, year: int, month: int) -> Path:
        return (
            self.raw_root
            / "daily_price_raw"
            / f"market_code={_slug(market_code)}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )

    def daily_price_adjusted(self, asset_type: str, year: int, month: int) -> Path:
        return (
            self.curated_root
            / "daily_price_adjusted"
            / f"asset_type={_slug(asset_type)}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )

    def index_daily(self, index_family: str, year: int, month: int) -> Path:
        return (
            self.raw_root
            / "index_daily"
            / f"index_family={_slug(index_family)}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )

    def universe_snapshot(self, universe_name: str, snapshot_date: str) -> Path:
        return (
            self.curated_root
            / "universe_snapshot"
            / f"universe_name={_slug(universe_name)}"
            / f"snapshot_date={_slug(snapshot_date)}"
        )

    def feature_snapshot(self, feature_set_name: str, snapshot_date: str) -> Path:
        return (
            self.feature_root
            / "feature_snapshot"
            / f"feature_set_name={_slug(feature_set_name)}"
            / f"snapshot_date={_slug(snapshot_date)}"
        )
