from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from quant.storage.db.base import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_run"
    __table_args__ = (Index("ix_ingestion_run_job_target_date", "job_name", "target_date"),)

    ingest_run_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(100), nullable=False)
    data_domain: Mapped[str] = mapped_column(String(50), nullable=False)
    target_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class DataValidationResult(Base):
    __tablename__ = "data_validation_result"
    __table_args__ = (
        UniqueConstraint("target_date", "data_domain", "check_name", "run_id"),
        Index("ix_data_validation_result_date_domain", "target_date", "data_domain"),
    )

    validation_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    data_domain: Mapped[str] = mapped_column(String(50), nullable=False)
    check_name: Mapped[str] = mapped_column(String(100), nullable=False)
    result: Mapped[str] = mapped_column(String(20), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class ResearchReadyStatus(Base):
    __tablename__ = "research_ready_status"

    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    reference_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    raw_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    validated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    adjusted_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    feature_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    research_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
