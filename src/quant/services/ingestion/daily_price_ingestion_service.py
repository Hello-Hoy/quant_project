from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from quant.providers.krx.daily_price_provider import KrxDailyPriceProvider
from quant.storage.db.repositories.instrument_repository import InstrumentRepository
from quant.storage.parquet.daily_price_store import DailyPriceStore

@dataclass
class DailyPriceIngestionResult:
    row_count: int
    market_counts: dict[str, int]
    artifacts: list[str]
    message: str | None = None

class DailyPriceIngestionService:
    def __init__(self, session: Session, provider: KrxDailyPriceProvider | None = None, instrument_repository: InstrumentRepository | None = None, store: DailyPriceStore | None = None) -> None:
        self.provider = provider or KrxDailyPriceProvider()
        self.instrument_repository = instrument_repository or InstrumentRepository(session)
        self.store = store or DailyPriceStore()

    def ingest(self, target_date: str, force: bool = False) -> DailyPriceIngestionResult:
        rows = self.provider.fetch_daily_prices(target_date=target_date)
        if not rows:
            return DailyPriceIngestionResult(0, {}, [], f"No daily price rows returned for {target_date}")

        active_instruments = self.instrument_repository.get_active_tradable_instruments()
        symbol_map = {(row.symbol, row.market_code, row.asset_type): row.instrument_id for row in active_instruments}

        normalized_rows: list[dict] = []
        missing_symbols: list[tuple[str, str, str]] = []

        for row in rows:
            key = (row.symbol, row.market_code, row.asset_type)
            instrument_id = symbol_map.get(key)
            if instrument_id is None:
                missing_symbols.append(key)
                continue
            normalized_rows.append(
                {
                    "trade_date": row.trade_date.isoformat(),
                    "instrument_id": instrument_id,
                    "market_code": row.market_code,
                    "open": row.open, "high": row.high, "low": row.low, "close": row.close,
                    "volume": row.volume, "turnover": row.turnover, "market_cap": row.market_cap,
                    "shares_outstanding": row.shares_outstanding, "data_source": "KRX",
                    "ingested_at": datetime.now().isoformat(),
                }
            )
        if not normalized_rows:
            return DailyPriceIngestionResult(0, {}, [], f"All rows filtered out because instrument mapping failed: {missing_symbols[:5]}")

        df = pd.DataFrame(normalized_rows)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        year, month = int(df["trade_date"].dt.year.iloc[0]), int(df["trade_date"].dt.month.iloc[0])
        artifacts: list[str] = []
        market_counts: dict[str, int] = {}
        for market_code, sub_df in df.groupby("market_code"):
            file_path = self.store.write_partition(sub_df.reset_index(drop=True), market_code=market_code, year=year, month=month)
            artifacts.append(str(file_path))
            market_counts[market_code] = len(sub_df)
        message = f"Some symbols were not mapped to instrument_id: {missing_symbols[:10]}" if missing_symbols else None
        return DailyPriceIngestionResult(len(df), market_counts, artifacts, message)
