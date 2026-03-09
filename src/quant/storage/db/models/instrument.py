from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import BigInteger, Boolean, Date, DateTime, ForeignKey, Index, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from quant.storage.db.base import Base


class InstrumentMaster(Base):
    __tablename__ = "instrument_master"
    __table_args__ = (
        UniqueConstraint("symbol", "market_code", "asset_type"),
        Index("ix_instrument_master_market_listing_status", "market_code", "listing_status"),
        Index("ix_instrument_master_is_tradable", "is_tradable"),
    )

    instrument_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    name_kr: Mapped[str] = mapped_column(String(200), nullable=False)
    market_code: Mapped[str] = mapped_column(String(20), nullable=False)
    asset_type: Mapped[str] = mapped_column(String(20), nullable=False)
    listing_status: Mapped[str] = mapped_column(String(20), nullable=False)
    is_tradable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    base_currency: Mapped[str] = mapped_column(String(10), nullable=False, default="KRW")
    listing_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    delisting_date: Mapped[date | None] = mapped_column(Date, nullable=True)
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


class InstrumentListingHistory(Base):
    __tablename__ = "instrument_listing_history"
    __table_args__ = (
        UniqueConstraint("instrument_id", "effective_from"),
        Index("ix_instrument_listing_history_instrument_id", "instrument_id"),
    )

    listing_history_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument_master.instrument_id"), nullable=False)
    market_code: Mapped[str] = mapped_column(String(20), nullable=False)
    listing_status: Mapped[str] = mapped_column(String(20), nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class EtfMetadata(Base):
    __tablename__ = "etf_metadata"

    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instrument_master.instrument_id"), primary_key=True)
    underlying_index: Mapped[str | None] = mapped_column(String(200), nullable=True)
    category: Mapped[str | None] = mapped_column(String(50), nullable=True)
    leverage_type: Mapped[str] = mapped_column(String(20), nullable=False, default="NORMAL")
    management_company: Mapped[str | None] = mapped_column(String(100), nullable=True)
    expense_ratio: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
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
