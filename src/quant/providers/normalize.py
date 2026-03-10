from __future__ import annotations

from datetime import date, datetime, time
from typing import Any

from quant.core.exceptions import ProviderNotImplementedError


def pick_value(payload: dict[str, Any], keys: list[str], field_name: str, required: bool = True) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    if required:
        raise ProviderNotImplementedError(
            f"Missing required field '{field_name}'. accepted_keys={keys}"
        )
    return None


def parse_str(value: Any, field_name: str, required: bool = True) -> str | None:
    if value in (None, ""):
        if required:
            raise ProviderNotImplementedError(f"Field '{field_name}' is required")
        return None
    return str(value)


def parse_bool(value: Any, field_name: str, required: bool = True) -> bool | None:
    if value in (None, ""):
        if required:
            raise ProviderNotImplementedError(f"Field '{field_name}' is required")
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "y", "yes", "t"}:
        return True
    if normalized in {"0", "false", "n", "no", "f"}:
        return False
    raise ProviderNotImplementedError(f"Field '{field_name}' has invalid bool value: {value}")


def parse_int(value: Any, field_name: str, required: bool = True) -> int | None:
    if value in (None, ""):
        if required:
            raise ProviderNotImplementedError(f"Field '{field_name}' is required")
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError) as exc:
        raise ProviderNotImplementedError(
            f"Field '{field_name}' has invalid int value: {value}"
        ) from exc


def parse_float(value: Any, field_name: str, required: bool = True) -> float | None:
    if value in (None, ""):
        if required:
            raise ProviderNotImplementedError(f"Field '{field_name}' is required")
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ProviderNotImplementedError(
            f"Field '{field_name}' has invalid float value: {value}"
        ) from exc


def parse_date(value: Any, field_name: str, required: bool = True) -> date | None:
    if value in (None, ""):
        if required:
            raise ProviderNotImplementedError(f"Field '{field_name}' is required")
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    text = str(value).strip()
    formats = ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d")
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ProviderNotImplementedError(
        f"Field '{field_name}' has unsupported date format: {value}. "
        "Supported formats: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD"
    )


def parse_time(value: Any, field_name: str, required: bool = True) -> time | None:
    if value in (None, ""):
        if required:
            raise ProviderNotImplementedError(f"Field '{field_name}' is required")
        return None

    if isinstance(value, time):
        return value

    text = str(value).strip()
    formats = ("%H:%M:%S", "%H:%M")
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise ProviderNotImplementedError(
        f"Field '{field_name}' has unsupported time format: {value}. "
        "Supported formats: HH:MM[:SS]"
    )
