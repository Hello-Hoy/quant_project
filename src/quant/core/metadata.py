from __future__ import annotations

from typing import Any


PREFLIGHT_STATUS_MISSING = "MISSING"


def build_preflight_metadata(
    check_name: str,
    is_ready: bool,
    status: str | None,
    target_date: str | None,
) -> dict[str, Any]:
    normalized = (status or "").strip().upper() or PREFLIGHT_STATUS_MISSING
    return {
        "preflight_check_name": check_name,
        "preflight_ready": bool(is_ready),
        "preflight_status": normalized,
        "preflight_target_date": target_date,
    }

