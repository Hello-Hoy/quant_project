from quant.core.enums import RunStatus
from quant.core.result import JobResult, PipelineResult


def test_job_result_normalizes_status() -> None:
    result = JobResult(job_name="example", status="SUCCESS", row_count=10)
    assert result.status == RunStatus.SUCCESS
    assert result.is_success is True
    assert result.is_failure is False


def test_pipeline_result_total_rows() -> None:
    results = [
        JobResult(job_name="a", status=RunStatus.SUCCESS, row_count=2),
        JobResult(job_name="b", status=RunStatus.WARNING, row_count=3),
    ]
    pipeline = PipelineResult(pipeline_name="p", status="WARNING", results=results)
    assert pipeline.status == RunStatus.WARNING
    assert pipeline.total_rows == 5
    assert pipeline.has_failure is False
