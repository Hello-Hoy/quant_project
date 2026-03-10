from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

from quant.services.ops.catchup_inclusion_policy import CatchupInclusionPolicy


class CalendarRepositoryLike(Protocol):
    def get_open_dates_in_range(self, start_date: date, end_date: date) -> list[date]:
        ...


class ResearchReadyRepositoryLike(Protocol):
    def get_status_map_in_range(self, start_date: date, end_date: date) -> dict[date, object]:
        ...


class IngestionRunRepositoryLike(Protocol):
    def get_latest_runs_by_job_in_range(
        self,
        job_name: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, object]:
        ...


@dataclass
class CatchupDateDiscoveryResult:
    target_dates: list[str]
    used_fallback_window: bool
    message: str


class CatchupDateDiscoveryService:
    def __init__(
        self,
        session: object | None = None,
        calendar_repository: CalendarRepositoryLike | None = None,
        research_ready_repository: ResearchReadyRepositoryLike | None = None,
        ingestion_run_repository: IngestionRunRepositoryLike | None = None,
    ) -> None:
        if (
            calendar_repository is None
            or research_ready_repository is None
            or ingestion_run_repository is None
        ):
            if session is None:
                raise ValueError("session is required when repositories are not provided")

            from quant.storage.db.repositories.calendar_repository import CalendarRepository
            from quant.storage.db.repositories.ingestion_run_repository import IngestionRunRepository
            from quant.storage.db.repositories.research_ready_repository import ResearchReadyRepository

            calendar_repository = calendar_repository or CalendarRepository(session)  # type: ignore[arg-type]
            research_ready_repository = research_ready_repository or ResearchReadyRepository(session)  # type: ignore[arg-type]
            ingestion_run_repository = ingestion_run_repository or IngestionRunRepository(session)  # type: ignore[arg-type]

        self.calendar_repository = calendar_repository
        self.research_ready_repository = research_ready_repository
        self.ingestion_run_repository = ingestion_run_repository

    def discover(
        self,
        start_date: date | None,
        end_date: date | None,
        include_research_ready: bool = False,
        include_unsynced_corporate_action_dates: bool = False,
        fallback_window_days: int = 30,
    ) -> CatchupDateDiscoveryResult:
        resolved_end = end_date or date.today()
        used_fallback_window = start_date is None
        resolved_start = start_date or (resolved_end - timedelta(days=fallback_window_days))

        if resolved_start > resolved_end:
            raise ValueError("start_date must be on or before end_date")

        open_dates = self.calendar_repository.get_open_dates_in_range(
            start_date=resolved_start,
            end_date=resolved_end,
        )
        readiness_map = self.research_ready_repository.get_status_map_in_range(
            start_date=resolved_start,
            end_date=resolved_end,
        )
        policy = CatchupInclusionPolicy(
            include_research_ready=include_research_ready,
            include_unsynced_corporate_action_dates=include_unsynced_corporate_action_dates,
        )
        sync_map: dict[date, object] = {}
        if policy.requires_sync_map:
            sync_map = self.ingestion_run_repository.get_latest_runs_by_job_in_range(
                job_name="sync_corporate_action_events",
                start_date=resolved_start,
                end_date=resolved_end,
            )

        target_dates: list[str] = []
        for trade_date in open_dates:
            ready_row = readiness_map.get(trade_date)
            sync_row = sync_map.get(trade_date) if policy.requires_sync_map else None
            if policy.should_include(ready_row=ready_row, sync_row=sync_row):
                target_dates.append(trade_date.isoformat())

        message = (
            f"Discovered {len(target_dates)} catch-up date(s) in range "
            f"{resolved_start}~{resolved_end} "
            f"(include_unsynced_corporate_action_dates={include_unsynced_corporate_action_dates})"
        )
        return CatchupDateDiscoveryResult(
            target_dates=target_dates,
            used_fallback_window=used_fallback_window,
            message=message,
        )
