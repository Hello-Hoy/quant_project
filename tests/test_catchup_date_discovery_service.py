from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from quant.services.ops.catchup_date_discovery_service import CatchupDateDiscoveryService


@dataclass
class ReadyStub:
    research_ready: bool


class FakeCalendarRepository:
    def __init__(self, open_dates: list[date]) -> None:
        self.open_dates = open_dates

    def get_open_dates_in_range(self, start_date: date, end_date: date) -> list[date]:
        return [d for d in self.open_dates if start_date <= d <= end_date]


class FakeResearchReadyRepository:
    def __init__(self, status_map: dict[date, ReadyStub]) -> None:
        self.status_map = status_map

    def get_status_map_in_range(self, start_date: date, end_date: date) -> dict[date, ReadyStub]:
        return {
            d: s
            for d, s in self.status_map.items()
            if start_date <= d <= end_date
        }


@dataclass
class RunStub:
    status: str


class FakeIngestionRunRepository:
    def __init__(self, run_map: dict[date, RunStub]) -> None:
        self.run_map = run_map

    def get_latest_runs_by_job_in_range(
        self,
        job_name: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, RunStub]:
        _ = job_name
        return {
            d: s
            for d, s in self.run_map.items()
            if start_date <= d <= end_date
        }



def test_discover_excludes_research_ready_dates() -> None:
    service = CatchupDateDiscoveryService(
        calendar_repository=FakeCalendarRepository(
            [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)]
        ),
        research_ready_repository=FakeResearchReadyRepository(
            {
                date(2026, 3, 2): ReadyStub(research_ready=True),
                date(2026, 3, 3): ReadyStub(research_ready=False),
            }
        ),
        ingestion_run_repository=FakeIngestionRunRepository({}),
    )

    result = service.discover(start_date=date(2026, 3, 2), end_date=date(2026, 3, 4))

    assert result.target_dates == ["2026-03-03", "2026-03-04"]
    assert result.used_fallback_window is False



def test_discover_can_include_research_ready_dates() -> None:
    service = CatchupDateDiscoveryService(
        calendar_repository=FakeCalendarRepository([date(2026, 3, 2), date(2026, 3, 3)]),
        research_ready_repository=FakeResearchReadyRepository(
            {
                date(2026, 3, 2): ReadyStub(research_ready=True),
                date(2026, 3, 3): ReadyStub(research_ready=True),
            }
        ),
        ingestion_run_repository=FakeIngestionRunRepository({}),
    )

    result = service.discover(
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 3),
        include_research_ready=True,
    )

    assert result.target_dates == ["2026-03-02", "2026-03-03"]



def test_discover_uses_window_when_start_date_is_none() -> None:
    service = CatchupDateDiscoveryService(
        calendar_repository=FakeCalendarRepository([date(2026, 3, 8), date(2026, 3, 9)]),
        research_ready_repository=FakeResearchReadyRepository({}),
        ingestion_run_repository=FakeIngestionRunRepository({}),
    )

    result = service.discover(
        start_date=None,
        end_date=date(2026, 3, 9),
        fallback_window_days=5,
    )

    assert result.used_fallback_window is True
    assert result.target_dates == ["2026-03-08", "2026-03-09"]


def test_discover_can_include_unsynced_corporate_action_dates() -> None:
    service = CatchupDateDiscoveryService(
        calendar_repository=FakeCalendarRepository([date(2026, 3, 2), date(2026, 3, 3)]),
        research_ready_repository=FakeResearchReadyRepository(
            {
                date(2026, 3, 2): ReadyStub(research_ready=True),
                date(2026, 3, 3): ReadyStub(research_ready=True),
            }
        ),
        ingestion_run_repository=FakeIngestionRunRepository(
            {date(2026, 3, 3): RunStub(status="SUCCESS")}
        ),
    )

    result = service.discover(
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 3),
        include_unsynced_corporate_action_dates=True,
    )

    assert result.target_dates == ["2026-03-02"]
