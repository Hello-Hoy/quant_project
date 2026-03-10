from __future__ import annotations

from typer.testing import CliRunner

import quant.bootstrap.container as container_module
import quant.cli.eod as eod_cli
import quant.services.ops.platform_preflight_service as preflight_module
from quant.core.enums import RunStatus


class _FakeContainer:
    def bootstrap(self) -> None:
        return None


class _FakeReport:
    def __init__(self, status: RunStatus) -> None:
        self.status = status

    def to_dict(self) -> dict[str, object]:
        return {"status": self.status.value, "checks": []}


class _SuccessService:
    def run(self, require_db_schema: bool = True, check_database: bool = True) -> _FakeReport:
        _ = require_db_schema
        _ = check_database
        return _FakeReport(RunStatus.SUCCESS)


class _FailedService:
    def run(self, require_db_schema: bool = True, check_database: bool = True) -> _FakeReport:
        _ = require_db_schema
        _ = check_database
        return _FakeReport(RunStatus.FAILED)


def test_preflight_command_returns_zero_on_success(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(container_module, "Container", _FakeContainer)
    monkeypatch.setattr(preflight_module, "PlatformPreflightService", _SuccessService)

    runner = CliRunner()
    result = runner.invoke(eod_cli.app, ["preflight"])

    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout


def test_preflight_command_returns_nonzero_on_failure(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(container_module, "Container", _FakeContainer)
    monkeypatch.setattr(preflight_module, "PlatformPreflightService", _FailedService)

    runner = CliRunner()
    result = runner.invoke(eod_cli.app, ["preflight"])

    assert result.exit_code == 1
    assert "FAILED" in result.stdout


def test_preflight_command_can_skip_db_checks(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(container_module, "Container", _FakeContainer)
    monkeypatch.setattr(preflight_module, "PlatformPreflightService", _SuccessService)

    runner = CliRunner()
    result = runner.invoke(eod_cli.app, ["preflight", "--skip-db"])

    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout
