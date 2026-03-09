from __future__ import annotations
from datetime import date, datetime, time, timezone

from sqlalchemy import Boolean, Date, DateTime, String, Time
from sqlalchemy.orm import Mapped, mapped_column

from quant.storage.db.base import Base


class MarketCalendar(Base):
    __tablename__ = "market_calendar"

    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False)
    market_scope: Mapped[str] = mapped_column(String(20), nullable=False, default="KRX")
    open_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    close_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    is_half_day: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    prev_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    next_trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
