from __future__ import annotations

from typer.testing import CliRunner

import quant.bootstrap.container as container_module
import quant.cli.eod as eod_cli
import quant.pipelines.eod_catchup_pipeline as catchup_pipeline_module
import quant.pipelines.single_date_eod_pipeline as single_date_pipeline_module
from quant.core.enums import RunStatus
from quant.core.result import JobResult, PipelineResult


class _FakeContainer:
    def bootstrap(self) -> None:
        return None


class _FakeSingleDatePipeline:
    def run(self, target_date: str, force: bool = False, run_mode: str = "manual") -> PipelineResult:
        _ = force
        _ = run_mode
        return PipelineResult(
            pipeline_name="single_date_eod_pipeline",
            status=RunStatus.WARNING,
            results=[
                JobResult(
                    job_name="build_price_adjustment_factor",
                    status=RunStatus.WARNING,
                    target_date=target_date,
                    metadata={
                        "preflight_check_name": "sync_corporate_action_events",
                        "preflight_ready": False,
                        "preflight_status": "MISSING",
                    },
                )
            ],
        )


class _FakeCatchupPipeline:
    def run_for_dates(self, target_dates: list[str], force: bool = False) -> PipelineResult:
        _ = force
        results: list[JobResult] = []
        for target_date in target_dates:
            results.append(
                JobResult(
                    job_name="update_research_ready_status",
                    status=RunStatus.WARNING,
                    target_date=target_date,
                    metadata={
                        "preflight_check_name": "sync_corporate_action_events",
                        "preflight_ready": True,
                        "preflight_status": "NOT_REQUIRED",
                    },
                )
            )
        return PipelineResult(
            pipeline_name="eod_catchup_pipeline",
            status=RunStatus.WARNING,
            results=results,
        )


def _setup(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(container_module, "Container", _FakeContainer)


def test_collect_preflight_rows_sorts_and_filters() -> None:
    rows = eod_cli._collect_preflight_rows(
        [
            JobResult(
                job_name="z_job",
                status=RunStatus.SUCCESS,
                target_date="2026-03-09",
                metadata={
                    "preflight_check_name": "check_b",
                    "preflight_ready": True,
                    "preflight_status": "success",
                },
            ),
            JobResult(
                job_name="a_job",
                status=RunStatus.SUCCESS,
                target_date="2026-03-08",
                metadata={
                    "preflight_check_name": "check_a",
                    "preflight_ready": False,
                    "preflight_status": "missing",
                },
            ),
            JobResult(
                job_name="no_preflight",
                status=RunStatus.SUCCESS,
                target_date="2026-03-08",
                metadata={},
            ),
        ]
    )

    assert rows == [
        {
            "target_date": "2026-03-08",
            "job_name": "a_job",
            "check_name": "check_a",
            "status": "MISSING",
            "ready": "false",
        },
        {
            "target_date": "2026-03-09",
            "job_name": "z_job",
            "check_name": "check_b",
            "status": "SUCCESS",
            "ready": "true",
        },
    ]
    assert eod_cli._format_preflight_counts(rows) == (
        "checks=2 success=1 warning=0 missing=1 not_required=0"
    )


def test_run_date_verbose_preflight_prints_summary(monkeypatch) -> None:  # noqa: ANN001
    _setup(monkeypatch)
    monkeypatch.setattr(single_date_pipeline_module, "SingleDateEodPipeline", _FakeSingleDatePipeline)

    runner = CliRunner()
    result = runner.invoke(
        eod_cli.app,
        ["run-date", "--date", "2026-03-09", "--verbose-preflight"],
    )

    assert result.exit_code == 0
    assert "Preflight summary:" in result.stdout
    assert "aggregate checks=1 success=0 warning=0 missing=1 not_required=0" in result.stdout
    assert "date=2026-03-09 checks=1 success=0 warning=0 missing=1 not_required=0" in result.stdout
    assert "Preflight details:" in result.stdout
    assert "build_price_adjustment_factor" in result.stdout


def test_catchup_verbose_preflight_prints_summary(monkeypatch) -> None:  # noqa: ANN001
    _setup(monkeypatch)
    monkeypatch.setattr(catchup_pipeline_module, "EodCatchupPipeline", _FakeCatchupPipeline)

    runner = CliRunner()
    result = runner.invoke(
        eod_cli.app,
        [
            "catchup",
            "--date",
            "2026-03-08",
            "--date",
            "2026-03-09",
            "--verbose-preflight",
        ],
    )

    assert result.exit_code == 0
    assert "Preflight summary:" in result.stdout
    assert "aggregate checks=2 success=0 warning=0 missing=0 not_required=2" in result.stdout
    assert "date=2026-03-08 checks=1 success=0 warning=0 missing=0 not_required=1" in result.stdout
    assert "date=2026-03-09 checks=1 success=0 warning=0 missing=0 not_required=1" in result.stdout
    assert "Preflight details:" in result.stdout
    assert "update_research_ready_status" in result.stdout
    assert "2026-03-08" in result.stdout
    assert "2026-03-09" in result.stdout
