from __future__ import annotations

from pathlib import Path

from quant.storage.parquet.paths import ParquetPaths


def test_partition_path_shapes(tmp_path: Path) -> None:
    paths = ParquetPaths(
        raw_root=tmp_path / "raw",
        curated_root=tmp_path / "curated",
        feature_root=tmp_path / "features",
    )

    assert paths.daily_price_raw("KOSPI", 2026, 3) == (
        tmp_path / "raw" / "daily_price_raw" / "market_code=KOSPI" / "year=2026" / "month=03"
    )
    assert paths.daily_price_adjusted("COMMON", 2026, 3) == (
        tmp_path / "curated"
        / "daily_price_adjusted"
        / "asset_type=COMMON"
        / "year=2026"
        / "month=03"
    )
    assert paths.index_daily("KOSPI200", 2026, 3) == (
        tmp_path / "raw" / "index_daily" / "index_family=KOSPI200" / "year=2026" / "month=03"
    )
    assert paths.universe_snapshot("core_equity_etf", "2026-03-09") == (
        tmp_path
        / "curated"
        / "universe_snapshot"
        / "universe_name=core_equity_etf"
        / "snapshot_date=2026-03-09"
    )
    assert paths.feature_snapshot("core_v1", "2026-03-09") == (
        tmp_path
        / "features"
        / "feature_snapshot"
        / "feature_set_name=core_v1"
        / "snapshot_date=2026-03-09"
    )
