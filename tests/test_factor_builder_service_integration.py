from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from quant.services.adjustment.factor_builder_service import FactorBuilderService


class FakeDailyPriceStore:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def read_partition(self, market_code: str, year: int, month: int) -> pd.DataFrame:
        _ = year
        _ = month
        if market_code != "KOSPI":
            raise FileNotFoundError(market_code)
        return self.frame.copy()


@dataclass
class EventStub:
    event_type: str
    effective_date: date | None
    ex_date: date | None
    ratio_value: float | None


class FakeCorporateActionRepository:
    def __init__(self, event_map: dict[int, list[EventStub]]) -> None:
        self.event_map = event_map
        self.last_instrument_ids: list[int] | None = None
        self.last_target_date: date | None = None

    def get_events_for_instruments_up_to_date(
        self,
        instrument_ids: list[int],
        target_date: date,
    ) -> dict[int, list[EventStub]]:
        self.last_instrument_ids = instrument_ids
        self.last_target_date = target_date
        return self.event_map


class FakeFactorRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def upsert_factor(
        self,
        instrument_id: int,
        trade_date: date,
        cumulative_factor: float,
        factor_version: str,
        derived_from_event_count: int = 0,
    ) -> None:
        self.calls.append(
            {
                "instrument_id": instrument_id,
                "trade_date": trade_date,
                "cumulative_factor": cumulative_factor,
                "factor_version": factor_version,
                "derived_from_event_count": derived_from_event_count,
            }
        )


class FakeIngestionRun:
    def __init__(self, status: str) -> None:
        self.status = status


class FakeIngestionRunRepository:
    def __init__(self, status: str | None = None) -> None:
        self.status = status

    def get_latest_run_by_job_and_target_date(self, job_name: str, target_date: date):  # noqa: ANN201
        _ = job_name
        _ = target_date
        if self.status is None:
            return None
        return FakeIngestionRun(status=self.status)


def test_factor_builder_service_applies_event_driven_factors() -> None:
    frame = pd.DataFrame(
        [
            {"trade_date": "2026-03-09", "instrument_id": 1},
            {"trade_date": "2026-03-09", "instrument_id": 2},
            {"trade_date": "2026-03-08", "instrument_id": 1},
        ]
    )
    event_repo = FakeCorporateActionRepository(
        event_map={
            1: [EventStub("SPLIT", date(2026, 3, 1), None, 2.0)],
            2: [EventStub("DIVIDEND", date(2026, 3, 1), None, 1.5)],
        }
    )
    factor_repo = FakeFactorRepository()
    service = FactorBuilderService(
        session=object(),
        daily_price_store=FakeDailyPriceStore(frame),
        factor_repository=factor_repo,
        corporate_action_repository=event_repo,
        ingestion_run_repository=FakeIngestionRunRepository(status="SUCCESS"),
    )

    result = service.build(target_date="2026-03-09", factor_version="v1_corporate_action")

    assert result.row_count == 2
    assert result.derived_event_count_total == 1
    assert result.preflight_corporate_action_synced is True
    assert result.preflight_corporate_action_status == "SUCCESS"
    assert event_repo.last_instrument_ids == [1, 2]
    assert event_repo.last_target_date == date(2026, 3, 9)
    assert len(factor_repo.calls) == 2

    call_by_instrument = {int(item["instrument_id"]): item for item in factor_repo.calls}
    assert call_by_instrument[1]["cumulative_factor"] == 2.0
    assert call_by_instrument[1]["derived_from_event_count"] == 1
    assert call_by_instrument[2]["cumulative_factor"] == 1.0
    assert call_by_instrument[2]["derived_from_event_count"] == 0


def test_factor_builder_service_returns_empty_when_no_raw_rows() -> None:
    frame = pd.DataFrame([{"trade_date": "2026-03-08", "instrument_id": 1}])
    factor_repo = FakeFactorRepository()
    service = FactorBuilderService(
        session=object(),
        daily_price_store=FakeDailyPriceStore(frame),
        factor_repository=factor_repo,
        corporate_action_repository=FakeCorporateActionRepository(event_map={}),
        ingestion_run_repository=FakeIngestionRunRepository(status=None),
    )

    result = service.build(target_date="2026-03-09", factor_version="v1_corporate_action")

    assert result.row_count == 0
    assert result.derived_event_count_total == 0
    assert result.preflight_corporate_action_synced is False
    assert not factor_repo.calls


def test_factor_builder_service_marks_preflight_false_when_sync_missing() -> None:
    frame = pd.DataFrame([{"trade_date": "2026-03-09", "instrument_id": 1}])
    factor_repo = FakeFactorRepository()
    service = FactorBuilderService(
        session=object(),
        daily_price_store=FakeDailyPriceStore(frame),
        factor_repository=factor_repo,
        corporate_action_repository=FakeCorporateActionRepository(event_map={}),
        ingestion_run_repository=FakeIngestionRunRepository(status=None),
    )

    result = service.build(target_date="2026-03-09", factor_version="v1_corporate_action")

    assert result.row_count == 1
    assert result.preflight_corporate_action_synced is False
    assert result.preflight_corporate_action_status is None
    assert "preflight" in (result.message or "")
