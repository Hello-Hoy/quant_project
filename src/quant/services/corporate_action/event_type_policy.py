from __future__ import annotations

from re import sub

_EVENT_TYPE_ALIASES = {
    "SPLIT": "SPLIT",
    "STOCK_SPLIT": "SPLIT",
    "액면분할": "SPLIT",
    "REVERSE_SPLIT": "REVERSE_SPLIT",
    "STOCK_CONSOLIDATION": "REVERSE_SPLIT",
    "액면병합": "REVERSE_SPLIT",
    "BONUS_ISSUE": "BONUS_ISSUE",
    "무상증자": "BONUS_ISSUE",
    "RIGHTS_ISSUE": "RIGHTS_ISSUE",
    "유상증자": "RIGHTS_ISSUE",
    "CASH_DIVIDEND": "DIVIDEND_CASH",
    "DIVIDEND": "DIVIDEND_CASH",
    "현금배당": "DIVIDEND_CASH",
    "STOCK_DIVIDEND": "DIVIDEND_STOCK",
    "주식배당": "DIVIDEND_STOCK",
}

FACTOR_AFFECTING_EVENT_TYPES = {
    "SPLIT",
    "REVERSE_SPLIT",
    "BONUS_ISSUE",
    "RIGHTS_ISSUE",
}


def canonicalize_event_type(raw_event_type: str) -> str:
    normalized = raw_event_type.strip()
    if not normalized:
        return "UNKNOWN"

    alias_key = normalized.upper()
    if alias_key in _EVENT_TYPE_ALIASES:
        return _EVENT_TYPE_ALIASES[alias_key]
    return sub(r"[^A-Za-z0-9_]+", "_", alias_key).strip("_") or "UNKNOWN"


def is_factor_affecting_event_type(event_type: str) -> bool:
    return canonicalize_event_type(event_type) in FACTOR_AFFECTING_EVENT_TYPES
