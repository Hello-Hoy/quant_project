from __future__ import annotations

from quant.core.metadata import PREFLIGHT_STATUS_MISSING, build_preflight_metadata


def test_build_preflight_metadata_normalizes_values() -> None:
    result = build_preflight_metadata(
        check_name="sync_corporate_action_events",
        is_ready=False,
        status=" warning ",
        target_date="2026-03-09",
    )

    assert result == {
        "preflight_check_name": "sync_corporate_action_events",
        "preflight_ready": False,
        "preflight_status": "WARNING",
        "preflight_target_date": "2026-03-09",
    }


def test_build_preflight_metadata_uses_missing_status_when_empty() -> None:
    result = build_preflight_metadata(
        check_name="sync_corporate_action_events",
        is_ready=True,
        status=None,
        target_date=None,
    )

    assert result["preflight_status"] == PREFLIGHT_STATUS_MISSING
    assert result["preflight_ready"] is True
