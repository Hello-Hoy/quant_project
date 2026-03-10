from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from quant.core.enums import ValidationResult
from quant.services.readiness.research_ready_service import ResearchReadyService


class _FakeCalendarRepository:
    def get_by_date(self, trade_date: date):  # noqa: ANN201
        _ = trade_date
        return object()


class _FakeInstrumentRepository:
    def get_active_tradable_instruments(self) -> list[object]:
        return [object()]


@dataclass
class _ValidationRow:
    result: str


class _FakeValidationRepository:
    def get_results_by_date_domain(self, target_date: date, data_domain: str) -> list[_ValidationRow]:
        _ = target_date
        _ = data_domain
        return [_ValidationRow(result=ValidationResult.PASS.value)]


class _FakeResearchReadyRepository:
    def __init__(self) -> None:
        self.last_upsert: dict[str, object] | None = None

    def upsert_status(
        self,
        trade_date: date,
        reference_ready: bool,
        raw_ready: bool,
        validated: bool,
        adjusted_ready: bool,
        feature_ready: bool,
        research_ready: bool,
        status_note: str | None = None,
    ) -> object:
        self.last_upsert = {
            "trade_date": trade_date,
            "reference_ready": reference_ready,
            "raw_ready": raw_ready,
            "validated": validated,
            "adjusted_ready": adjusted_ready,
            "feature_ready": feature_ready,
            "research_ready": research_ready,
            "status_note": status_note,
        }
        return object()


class _FakeIngestionRun:
    def __init__(self, status: str) -> None:
        self.status = status


class _FakeIngestionRunRepository:
    def __init__(self, status: str | None) -> None:
        self.status = status

    def get_latest_run_by_job_and_target_date(self, job_name: str, target_date: date):  # noqa: ANN201
        _ = job_name
        _ = target_date
        if self.status is None:
            return None
        return _FakeIngestionRun(self.status)


class _FakeDailyPriceStore:
    def read_partition(self, market_code: str, year: int, month: int) -> pd.DataFrame:
        _ = year
        _ = month
        if market_code != "KOSPI":
            raise FileNotFoundError(market_code)
        return pd.DataFrame([{"trade_date": "2026-03-09", "instrument_id": 1}])


class _FakeAdjustedPriceStore:
    def read_partition(self, asset_type: str, year: int, month: int) -> pd.DataFrame:
        _ = year
        _ = month
        if asset_type != "COMMON":
            raise FileNotFoundError(asset_type)
        return pd.DataFrame([{"trade_date": "2026-03-09", "instrument_id": 1}])


class _FakeFeatureStore:
    def read_snapshot(self, feature_set_name: str, snapshot_date: str) -> pd.DataFrame:
        _ = feature_set_name
        _ = snapshot_date
        return pd.DataFrame([{"instrument_id": 1}])


def _build_service(
    *,
    sync_status: str | None,
    require_preflight: bool,
    ready_repo: _FakeResearchReadyRepository,
) -> ResearchReadyService:
    return ResearchReadyService(
        session=object(),
        calendar_repository=_FakeCalendarRepository(),
        instrument_repository=_FakeInstrumentRepository(),
        validation_repository=_FakeValidationRepository(),
        research_ready_repository=ready_repo,
        ingestion_run_repository=_FakeIngestionRunRepository(sync_status),
        daily_price_store=_FakeDailyPriceStore(),
        adjusted_price_store=_FakeAdjustedPriceStore(),
        feature_store=_FakeFeatureStore(),
        require_corporate_action_sync_preflight=require_preflight,
    )


def test_research_ready_requires_corporate_action_sync_when_policy_enabled() -> None:
    ready_repo = _FakeResearchReadyRepository()
    service = _build_service(
        sync_status=None,
        require_preflight=True,
        ready_repo=ready_repo,
    )

    result = service.update("2026-03-09")

    assert result.reference_ready is True
    assert result.raw_ready is True
    assert result.validated is True
    assert result.adjusted_ready is True
    assert result.feature_ready is True
    assert result.corporate_action_sync_ready is False
    assert result.corporate_action_sync_status is None
    assert result.research_ready is False
    assert ready_repo.last_upsert is not None
    assert ready_repo.last_upsert["research_ready"] is False
    assert "corporate_action_sync_not_ready" in str(ready_repo.last_upsert["status_note"])


def test_research_ready_can_skip_corporate_action_sync_when_policy_disabled() -> None:
    ready_repo = _FakeResearchReadyRepository()
    service = _build_service(
        sync_status=None,
        require_preflight=False,
        ready_repo=ready_repo,
    )

    result = service.update("2026-03-09")

    assert result.corporate_action_sync_ready is True
    assert result.corporate_action_sync_status == "NOT_REQUIRED"
    assert result.research_ready is True
    assert ready_repo.last_upsert is not None
    assert ready_repo.last_upsert["research_ready"] is True
