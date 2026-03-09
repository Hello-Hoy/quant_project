from __future__ import annotations

import pandas as pd

from quant.core.enums import ValidationResult
from quant.services.validation.validation_rules import (
    check_expected_coverage,
    check_no_duplicate_keys,
    check_non_negative_columns,
    check_ohlc_relationship,
)


def test_check_duplicate_keys_fails() -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2026-03-09", "instrument_id": 1},
            {"trade_date": "2026-03-09", "instrument_id": 1},
        ]
    )
    result = check_no_duplicate_keys(df, ["trade_date", "instrument_id"])
    assert str(result.result) == ValidationResult.FAIL.value


def test_check_non_negative_columns_fails() -> None:
    df = pd.DataFrame([{"open": -1, "high": 1, "low": 0, "close": 1, "volume": 1}])
    result = check_non_negative_columns(df, ["open", "high", "low", "close", "volume"])
    assert str(result.result) == ValidationResult.FAIL.value


def test_check_ohlc_relationship_fails() -> None:
    df = pd.DataFrame([{"open": 100, "high": 99, "low": 80, "close": 95}])
    result = check_ohlc_relationship(df)
    assert str(result.result) == ValidationResult.FAIL.value


def test_check_expected_coverage_warns() -> None:
    result = check_expected_coverage(actual_count=80, expected_count=100, min_ratio=0.9)
    assert str(result.result) == ValidationResult.WARN.value
