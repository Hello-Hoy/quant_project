from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

from quant.core.enums import RunStatus


@dataclass
class JobResult:
    job_name: str
    status: str | RunStatus
    target_date: str | None = None
    row_count: int = 0
    warning_count: int = 0
    message: str | None = None
    artifacts: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.status = RunStatus(self.status)

    @property
    def is_success(self) -> bool:
        return self.status in {RunStatus.SUCCESS, RunStatus.WARNING}

    @property
    def is_failure(self) -> bool:
        return self.status == RunStatus.FAILED

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_name": self.job_name,
            "status": self.status.value,
            "target_date": self.target_date,
            "row_count": self.row_count,
            "warning_count": self.warning_count,
            "message": self.message,
            "artifacts": self.artifacts,
            "metadata": self.metadata,
        }

@dataclass
class PipelineResult:
    pipeline_name: str
    status: str | RunStatus
    results: list[JobResult] = field(default_factory=list)
    message: str | None = None

    def __post_init__(self) -> None:
        self.status = RunStatus(self.status)

    @property
    def has_failure(self) -> bool:
        return any(r.is_failure for r in self.results)

    @property
    def total_rows(self) -> int:
        return sum(r.row_count for r in self.results)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "message": self.message,
            "total_rows": self.total_rows,
            "results": [result.to_dict() for result in self.results],
        }
