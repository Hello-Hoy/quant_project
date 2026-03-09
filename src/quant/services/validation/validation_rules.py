from __future__ import annotations
from dataclasses import dataclass

import pandas as pd

from quant.core.enums import ValidationResult


@dataclass
class ValidationCheck:
    check_name: str
    result: str | ValidationResult
    detail: str | None = None


def check_no_duplicate_keys(df: pd.DataFrame, key_columns: list[str]) -> ValidationCheck:
    duplicated = int(df.duplicated(subset=key_columns).sum())
    if duplicated > 0:
        return ValidationCheck(
            "no_duplicate_keys",
            ValidationResult.FAIL,
            f"Found {duplicated} duplicated rows on keys={key_columns}",
        )
    return ValidationCheck(
        "no_duplicate_keys",
        ValidationResult.PASS,
        f"No duplicated rows on keys={key_columns}",
    )


def check_non_negative_columns(df: pd.DataFrame, columns: list[str]) -> ValidationCheck:
    violations = 0
    for col in columns:
        if col in df.columns:
            violations += int((df[col].fillna(0) < 0).sum())
    if violations > 0:
        return ValidationCheck(
            "non_negative_columns",
            ValidationResult.FAIL,
            f"Found {violations} negative values across columns={columns}",
        )
    return ValidationCheck(
        "non_negative_columns",
        ValidationResult.PASS,
        f"No negative values across columns={columns}",
    )


def check_ohlc_relationship(df: pd.DataFrame) -> ValidationCheck:
    if df.empty:
        return ValidationCheck("ohlc_relationship", ValidationResult.WARN, "Input dataframe is empty")
    invalid = df[
        (df["high"] < df[["open", "close", "low"]].max(axis=1))
        | (df["low"] > df[["open", "close", "high"]].min(axis=1))
        | (df["high"] < df["low"])
    ]
    if len(invalid) > 0:
        return ValidationCheck(
            "ohlc_relationship",
            ValidationResult.FAIL,
            f"Found {len(invalid)} rows with invalid OHLC relationship",
        )
    return ValidationCheck("ohlc_relationship", ValidationResult.PASS, "OHLC relationship is valid")


def check_minimum_row_count(df: pd.DataFrame, minimum_rows: int) -> ValidationCheck:
    actual = len(df)
    if actual < minimum_rows:
        return ValidationCheck(
            "minimum_row_count",
            ValidationResult.WARN,
            f"Row count {actual} is below minimum threshold {minimum_rows}",
        )
    return ValidationCheck(
        "minimum_row_count",
        ValidationResult.PASS,
        f"Row count {actual} satisfies minimum threshold {minimum_rows}",
    )


def check_expected_coverage(actual_count: int, expected_count: int, min_ratio: float = 0.90) -> ValidationCheck:
    if expected_count <= 0:
        return ValidationCheck("expected_coverage", ValidationResult.WARN, "Expected count is zero or undefined")
    ratio = actual_count / expected_count
    if ratio < min_ratio:
        return ValidationCheck(
            "expected_coverage",
            ValidationResult.WARN,
            f"Coverage ratio {ratio:.4f} below threshold {min_ratio:.4f} "
            f"(actual={actual_count}, expected={expected_count})",
        )
    return ValidationCheck(
        "expected_coverage",
        ValidationResult.PASS,
        f"Coverage ratio {ratio:.4f} (actual={actual_count}, expected={expected_count})",
    )
