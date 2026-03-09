from __future__ import annotations
from enum import StrEnum


class MarketCode(StrEnum):
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"
    KONEX = "KONEX"
    ETF = "ETF"


class AssetType(StrEnum):
    COMMON = "COMMON"
    ETF = "ETF"


class ListingStatus(StrEnum):
    ACTIVE = "ACTIVE"
    HALTED = "HALTED"
    DELISTED = "DELISTED"


class RunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    WARNING = "WARNING"

    @classmethod
    def terminal_statuses(cls) -> set["RunStatus"]:
        return {cls.SUCCESS, cls.WARNING, cls.FAILED, cls.PARTIAL}


class ValidationResult(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
