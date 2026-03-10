from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from quant.bootstrap.config_loader import ConfigLoader
from quant.providers.kis.calendar_provider import KisCalendarProvider
from quant.storage.db.models.calendar import MarketCalendar
from quant.storage.db.repositories.calendar_repository import CalendarRepository


@dataclass
class MarketCalendarSyncResult:
    row_count: int
    message: str | None = None


class MarketCalendarService:
    def __init__(
        self,
        session: Session,
        provider: KisCalendarProvider | None = None,
        repository: CalendarRepository | None = None,
        config_loader: ConfigLoader | None = None,
    ) -> None:
        self.config_loader = config_loader or ConfigLoader()
        if provider is None:
            runtime = self.config_loader.get_provider_runtime_config("kis")
            provider = KisCalendarProvider(**runtime)
        self.provider = provider
        self.repository = repository or CalendarRepository(session)

    def sync(self, target_date: str | None = None, force: bool = False) -> MarketCalendarSyncResult:
        if target_date is not None:
            end_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            start_date = end_date - timedelta(days=10)
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=10)

        rows = self.provider.fetch_calendar_range(start_date=start_date, end_date=end_date)
        if not rows:
            return MarketCalendarSyncResult(
                row_count=0,
                message=self.provider.unavailable_message(
                    capability="market calendar sync",
                    context=f"range={start_date}~{end_date}",
                ),
            )
        now = datetime.now(timezone.utc)
        model_rows = [
            MarketCalendar(
                trade_date=row.trade_date,
                is_open=row.is_open,
                market_scope=row.market_scope,
                open_time=row.open_time,
                close_time=row.close_time,
                is_half_day=row.is_half_day,
                prev_trade_date=row.prev_trade_date,
                next_trade_date=row.next_trade_date,
                created_at=now,
                updated_at=now,
            )
            for row in rows
        ]
        row_count = self.repository.upsert_many(model_rows)
        return MarketCalendarSyncResult(
            row_count=row_count,
            message=f"Market calendar synced for range {start_date} ~ {end_date}",
        )
