from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import BigInteger, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from quant.storage.db.base import Base


class CorporateActionEvent(Base):
    __tablename__ = "corporate_action_event"

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument_master.instrument_id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    announce_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ex_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ratio_value: Mapped[float | None] = mapped_column(Numeric(20, 10), nullable=True)
    cash_value: Mapped[float | None] = mapped_column(Numeric(20, 4), nullable=True)
    source: Mapped[str] = mapped_column(String(30), nullable=False)
    raw_payload: Mapped[str | None] = mapped_column(Text, nullable=True)
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


class PriceAdjustmentFactor(Base):
    __tablename__ = "price_adjustment_factor"
    __table_args__ = (
        UniqueConstraint("instrument_id", "trade_date", "factor_version"),
        Index("ix_price_adjustment_factor_trade_date_version", "trade_date", "factor_version"),
    )

    adjustment_factor_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument_master.instrument_id"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    cumulative_factor: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    factor_version: Mapped[str] = mapped_column(String(30), nullable=False)
    derived_from_event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
