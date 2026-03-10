from __future__ import annotations

from typer.testing import CliRunner

import quant.bootstrap.config_loader as config_loader
import quant.bootstrap.container as container_module
import quant.cli.eod as eod_cli
import quant.pipelines.eod_catchup_pipeline as catchup_pipeline_module


class _LoaderTrue:
    def __init__(self, *args: object, **kwargs: object) -> None:
        _ = args
        _ = kwargs

    def load_app_config(self) -> dict[str, object]:
        return {
            "catchup": {
                "include_unsynced_corporate_action_dates_default": True,
            }
        }


class _LoaderBadCatchup:
    def __init__(self, *args: object, **kwargs: object) -> None:
        _ = args
        _ = kwargs

    def load_app_config(self) -> dict[str, object]:
        return {"catchup": "not-a-mapping"}


class _LoaderRaises:
    def __init__(self, *args: object, **kwargs: object) -> None:
        _ = args
        _ = kwargs

    def load_app_config(self) -> dict[str, object]:
        raise RuntimeError("boom")


def test_default_include_unsynced_reads_app_config(monkeypatch) -> None:
    monkeypatch.setattr(config_loader, "ConfigLoader", _LoaderTrue)

    assert eod_cli._default_include_unsynced_corporate_action_dates() is True


def test_default_include_unsynced_returns_false_on_bad_catchup(monkeypatch) -> None:
    monkeypatch.setattr(config_loader, "ConfigLoader", _LoaderBadCatchup)

    assert eod_cli._default_include_unsynced_corporate_action_dates() is False


def test_default_include_unsynced_returns_false_on_loader_error(monkeypatch) -> None:
    monkeypatch.setattr(config_loader, "ConfigLoader", _LoaderRaises)

    assert eod_cli._default_include_unsynced_corporate_action_dates() is False


def test_resolve_include_unsynced_prefers_cli_option(monkeypatch) -> None:
    monkeypatch.setattr(
        eod_cli,
        "_default_include_unsynced_corporate_action_dates",
        lambda: True,
    )

    assert eod_cli._resolve_include_unsynced_corporate_action_dates(True) is True
    assert eod_cli._resolve_include_unsynced_corporate_action_dates(False) is False


def test_resolve_include_unsynced_uses_default_when_option_is_none(monkeypatch) -> None:
    monkeypatch.setattr(
        eod_cli,
        "_default_include_unsynced_corporate_action_dates",
        lambda: True,
    )

    assert eod_cli._resolve_include_unsynced_corporate_action_dates(None) is True


class _CliFakeContainer:
    def bootstrap(self) -> None:
        return None


class _CliResult:
    def to_dict(self) -> dict[str, object]:
        return {"ok": True}


class _CliFakePipeline:
    def run_for_dates(self, target_dates: list[str], force: bool = False) -> _CliResult:
        _ = target_dates
        _ = force
        return _CliResult()


def _setup_cli_fakes(monkeypatch) -> None:
    monkeypatch.setattr(container_module, "Container", _CliFakeContainer)
    monkeypatch.setattr(catchup_pipeline_module, "EodCatchupPipeline", _CliFakePipeline)


def test_catchup_cli_uses_config_default_when_flag_omitted(monkeypatch) -> None:
    _setup_cli_fakes(monkeypatch)
    captured: dict[str, bool] = {}

    def _fake_discover(
        start_date: str | None,
        end_date: str | None,
        include_research_ready: bool = False,
        include_unsynced_corporate_action_dates: bool = False,
    ) -> list[str]:
        _ = start_date
        _ = end_date
        _ = include_research_ready
        captured["include_unsynced"] = include_unsynced_corporate_action_dates
        return ["2026-03-03"]

    monkeypatch.setattr(
        eod_cli,
        "_default_include_unsynced_corporate_action_dates",
        lambda: True,
    )
    monkeypatch.setattr(eod_cli, "_discover_catchup_dates", _fake_discover)

    runner = CliRunner()
    result = runner.invoke(
        eod_cli.app,
        ["catchup", "--start-date", "2026-03-01", "--end-date", "2026-03-03"],
    )

    assert result.exit_code == 0
    assert captured["include_unsynced"] is True


def test_catchup_cli_exclude_flag_overrides_default(monkeypatch) -> None:
    _setup_cli_fakes(monkeypatch)
    captured: dict[str, bool] = {}

    def _fake_discover(
        start_date: str | None,
        end_date: str | None,
        include_research_ready: bool = False,
        include_unsynced_corporate_action_dates: bool = False,
    ) -> list[str]:
        _ = start_date
        _ = end_date
        _ = include_research_ready
        captured["include_unsynced"] = include_unsynced_corporate_action_dates
        return ["2026-03-03"]

    monkeypatch.setattr(
        eod_cli,
        "_default_include_unsynced_corporate_action_dates",
        lambda: True,
    )
    monkeypatch.setattr(eod_cli, "_discover_catchup_dates", _fake_discover)

    runner = CliRunner()
    result = runner.invoke(
        eod_cli.app,
        [
            "catchup",
            "--start-date",
            "2026-03-01",
            "--end-date",
            "2026-03-03",
            "--exclude-unsynced-corp-actions",
        ],
    )

    assert result.exit_code == 0
    assert captured["include_unsynced"] is False


def test_catchup_cli_include_flag_overrides_default(monkeypatch) -> None:
    _setup_cli_fakes(monkeypatch)
    captured: dict[str, bool] = {}

    def _fake_discover(
        start_date: str | None,
        end_date: str | None,
        include_research_ready: bool = False,
        include_unsynced_corporate_action_dates: bool = False,
    ) -> list[str]:
        _ = start_date
        _ = end_date
        _ = include_research_ready
        captured["include_unsynced"] = include_unsynced_corporate_action_dates
        return ["2026-03-03"]

    monkeypatch.setattr(
        eod_cli,
        "_default_include_unsynced_corporate_action_dates",
        lambda: False,
    )
    monkeypatch.setattr(eod_cli, "_discover_catchup_dates", _fake_discover)

    runner = CliRunner()
    result = runner.invoke(
        eod_cli.app,
        [
            "catchup",
            "--start-date",
            "2026-03-01",
            "--end-date",
            "2026-03-03",
            "--include-unsynced-corp-actions",
        ],
    )

    assert result.exit_code == 0
    assert captured["include_unsynced"] is True
