from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from quant.bootstrap.config_loader import ConfigLoader
from quant.providers.krx.index_provider import KrxIndexProvider
from quant.storage.parquet.index_store import IndexStore


@dataclass
class IndexIngestionResult:
    row_count: int
    index_family_counts: dict[str, int]
    artifacts: list[str]
    message: str | None = None

class IndexIngestionService:
    def __init__(
        self,
        session: Session,
        provider: KrxIndexProvider | None = None,
        store: IndexStore | None = None,
        config_loader: ConfigLoader | None = None,
    ) -> None:
        _ = session
        self.config_loader = config_loader or ConfigLoader()
        if provider is None:
            runtime = self.config_loader.get_provider_runtime_config("krx")
            provider = KrxIndexProvider(**runtime)
        self.provider = provider
        self.store = store or IndexStore()

    def ingest(self, target_date: str, force: bool = False) -> IndexIngestionResult:
        _ = force
        rows = self.provider.fetch_index_daily(target_date=target_date)
        if not rows:
            return IndexIngestionResult(
                0,
                {},
                [],
                self.provider.unavailable_message(
                    capability="index daily ingestion",
                    context=f"target_date={target_date}",
                ),
            )
        normalized_rows = [{
            "trade_date": row.trade_date.isoformat(),
            "index_code": row.index_code, "index_name": row.index_name, "index_family": row.index_family,
            "open": row.open, "high": row.high, "low": row.low, "close": row.close,
            "volume": row.volume, "turnover": row.turnover, "data_source": "KRX",
            "ingested_at": datetime.now().isoformat(),
        } for row in rows]
        df = pd.DataFrame(normalized_rows)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        year, month = int(df["trade_date"].dt.year.iloc[0]), int(df["trade_date"].dt.month.iloc[0])
        artifacts, index_family_counts = [], {}
        for index_family, sub_df in df.groupby("index_family"):
            file_path = self.store.write_partition(sub_df.reset_index(drop=True), index_family=index_family, year=year, month=month)
            artifacts.append(str(file_path))
            index_family_counts[index_family] = len(sub_df)
        return IndexIngestionResult(len(df), index_family_counts, artifacts, f"Index daily data ingested for {target_date}")
