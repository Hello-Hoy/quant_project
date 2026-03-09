from __future__ import annotations
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.orm import Session

from quant.core.enums import RunStatus, ValidationResult
from quant.storage.db.repositories.instrument_repository import InstrumentRepository
from quant.storage.db.repositories.validation_repository import ValidationRepository
from quant.storage.parquet.daily_price_store import DailyPriceStore
from quant.storage.parquet.index_store import IndexStore
from quant.services.validation.validation_rules import (
    ValidationCheck,
    check_expected_coverage,
    check_minimum_row_count,
    check_no_duplicate_keys,
    check_non_negative_columns,
    check_ohlc_relationship,
)


@dataclass
class DailyMarketValidationResult:
    row_count: int
    validation_count: int
    status: str | RunStatus
    message: str | None = None


class DailyMarketValidationService:
    def __init__(self, session: Session, daily_price_store: DailyPriceStore | None = None, index_store: IndexStore | None = None, instrument_repository: InstrumentRepository | None = None, validation_repository: ValidationRepository | None = None) -> None:
        self.daily_price_store = daily_price_store or DailyPriceStore()
        self.index_store = index_store or IndexStore()
        self.instrument_repository = instrument_repository or InstrumentRepository(session)
        self.validation_repository = validation_repository or ValidationRepository(session)

    def _load_daily_price_df(self, target_date: str) -> pd.DataFrame:
        ts = pd.to_datetime(target_date)
        year, month = int(ts.year), int(ts.month)
        frames = []
        for market_code in ["KOSPI", "KOSDAQ", "ETF"]:
            try:
                frames.append(self.daily_price_store.read_partition(market_code=market_code, year=year, month=month))
            except FileNotFoundError:
                continue
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return merged[merged["trade_date"] == ts].copy()

    def _load_index_df(self, target_date: str) -> pd.DataFrame:
        ts = pd.to_datetime(target_date)
        year, month = int(ts.year), int(ts.month)
        frames = []
        for index_family in ["KOSPI", "KOSDAQ", "KOSPI200", "SECTOR"]:
            try:
                frames.append(self.index_store.read_partition(index_family=index_family, year=year, month=month))
            except FileNotFoundError:
                continue
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        merged["trade_date"] = pd.to_datetime(merged["trade_date"])
        return merged[merged["trade_date"] == ts].copy()

    def validate(self, target_date: str) -> DailyMarketValidationResult:
        trade_date = pd.to_datetime(target_date).date()
        price_df = self._load_daily_price_df(target_date)
        index_df = self._load_index_df(target_date)
        checks: list[ValidationCheck] = []
        checks.append(check_minimum_row_count(price_df, minimum_rows=1))
        if not price_df.empty:
            checks.append(check_no_duplicate_keys(price_df, ["trade_date", "instrument_id"]))
            checks.append(check_non_negative_columns(price_df, ["open", "high", "low", "close", "volume"]))
            checks.append(check_ohlc_relationship(price_df))
        active_count = len(self.instrument_repository.get_active_tradable_instruments())
        checks.append(check_expected_coverage(actual_count=len(price_df), expected_count=active_count, min_ratio=0.90))
        checks.append(check_minimum_row_count(index_df, minimum_rows=1))
        validation_count = self.validation_repository.add_many(
            target_date=trade_date,
            data_domain="DAILY_PRICE",
            results=[{"check_name": c.check_name, "result": str(c.result), "detail": c.detail} for c in checks],
        )
        statuses = {ValidationResult(str(c.result)) for c in checks}
        status = (
            RunStatus.FAILED
            if ValidationResult.FAIL in statuses
            else (RunStatus.WARNING if ValidationResult.WARN in statuses else RunStatus.SUCCESS)
        )
        return DailyMarketValidationResult(
            len(price_df),
            validation_count,
            status,
            f"Validated daily market data for {target_date}",
        )
