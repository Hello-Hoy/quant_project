from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

import typer
from rich import print

from quant.core.result import JobResult
from quant.core.time_utils import to_trade_date

app = typer.Typer(help="EOD pipeline commands")


def _discover_weekday_dates(start_date: date, end_date: date) -> list[str]:
    dates: list[str] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _discover_catchup_dates(
    start_date: str | None,
    end_date: str | None,
    include_research_ready: bool = False,
    include_unsynced_corporate_action_dates: bool = False,
) -> list[str]:
    if not start_date and not end_date:
        return []

    resolved_end = to_trade_date(end_date or date.today().isoformat())
    resolved_start = to_trade_date(start_date or resolved_end.isoformat())
    if resolved_start > resolved_end:
        raise typer.BadParameter("--start-date must be on or before --end-date")

    try:
        from quant.services.ops.catchup_date_discovery_service import CatchupDateDiscoveryService
        from quant.storage.db.session import SessionLocal

        with SessionLocal() as session:
            discovery_service = CatchupDateDiscoveryService(session=session)
            discovery_result = discovery_service.discover(
                start_date=resolved_start,
                end_date=resolved_end,
                include_research_ready=include_research_ready,
                include_unsynced_corporate_action_dates=include_unsynced_corporate_action_dates,
            )
        return discovery_result.target_dates
    except Exception as exc:
        print(
            f"[yellow]DB catch-up discovery unavailable ({exc}). "
            "Fallback to weekday-based discovery.[/yellow]"
        )
        return _discover_weekday_dates(start_date=resolved_start, end_date=resolved_end)


def _default_include_unsynced_corporate_action_dates() -> bool:
    try:
        from quant.bootstrap.config_loader import ConfigLoader

        app_cfg = ConfigLoader().load_app_config()
        catchup_cfg = app_cfg.get("catchup", {})
        if not isinstance(catchup_cfg, dict):
            return False
        return bool(catchup_cfg.get("include_unsynced_corporate_action_dates_default", False))
    except Exception:
        return False


def _resolve_include_unsynced_corporate_action_dates(option: bool | None) -> bool:
    if option is not None:
        return option
    return _default_include_unsynced_corporate_action_dates()


def _expand_calendar_dates(start_date: str, end_date: str) -> list[str]:
    resolved_start = to_trade_date(start_date)
    resolved_end = to_trade_date(end_date)
    if resolved_start > resolved_end:
        raise typer.BadParameter("--start-date must be on or before --end-date")
    dates: list[str] = []
    current = resolved_start
    while current <= resolved_end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _collect_preflight_rows(results: list[JobResult]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for result in results:
        check_name = str(result.metadata.get("preflight_check_name", "")).strip()
        if not check_name:
            continue
        target = result.target_date or "-"
        status = str(result.metadata.get("preflight_status", "MISSING")).upper()
        ready = bool(result.metadata.get("preflight_ready", False))
        rows.append(
            {
                "target_date": target,
                "job_name": result.job_name,
                "check_name": check_name,
                "status": status,
                "ready": "true" if ready else "false",
            }
        )
    rows.sort(key=lambda item: (item["target_date"], item["job_name"], item["check_name"]))
    return rows


def _format_preflight_counts(rows: list[dict[str, str]]) -> str:
    total = len(rows)
    status_counter = Counter(row["status"] for row in rows)
    return (
        f"checks={total} "
        f"success={status_counter.get('SUCCESS', 0)} "
        f"warning={status_counter.get('WARNING', 0)} "
        f"missing={status_counter.get('MISSING', 0)} "
        f"not_required={status_counter.get('NOT_REQUIRED', 0)}"
    )


def _print_preflight_summary(results: list[JobResult]) -> None:
    rows = _collect_preflight_rows(results)
    if not rows:
        typer.echo("Preflight summary: none")
        return

    typer.echo("Preflight summary:")
    typer.echo(f"- aggregate {_format_preflight_counts(rows)}")

    by_date: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_date.setdefault(row["target_date"], []).append(row)
    for target_date in sorted(by_date.keys()):
        typer.echo(f"- date={target_date} {_format_preflight_counts(by_date[target_date])}")

    typer.echo("Preflight details:")
    for row in rows:
        typer.echo(
            f"- {row['target_date']} {row['job_name']} {row['check_name']} "
            f"ready={row['ready']} status={row['status']}"
        )


@app.command("run-date")
def run_date(
    date: str = typer.Option(..., "--date", help="Target trade date in YYYY-MM-DD"),
    force: bool = typer.Option(False, "--force", help="Force rerun"),
    verbose_preflight: bool = typer.Option(
        False,
        "--verbose-preflight",
        help="Print preflight checks from job metadata.",
    ),
) -> None:
    from quant.bootstrap.container import Container
    try:
        from quant.pipelines.single_date_eod_pipeline import SingleDateEodPipeline
        pipeline = SingleDateEodPipeline()
    except ModuleNotFoundError as exc:
        typer.echo(
            f"Missing dependency: {exc}. Run `pip install -e '.[dev]'` first.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    Container().bootstrap()
    try:
        result = pipeline.run(target_date=date, force=force)
    except Exception as exc:
        typer.echo(f"EOD run-date failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    print(result.to_dict())
    if verbose_preflight:
        _print_preflight_summary(result.results)


@app.command("catchup")
def catchup(
    dates: list[str] = typer.Option(
        [],
        "--date",
        help="Explicit dates to run. Can be provided multiple times.",
    ),
    start_date: str | None = typer.Option(None, "--start-date", help="Inclusive start date (YYYY-MM-DD)."),
    end_date: str | None = typer.Option(None, "--end-date", help="Inclusive end date (YYYY-MM-DD)."),
    include_research_ready: bool = typer.Option(
        False,
        "--include-ready",
        help="Include dates already marked as research_ready in DB discovery mode.",
    ),
    include_unsynced_corporate_action_dates: bool | None = typer.Option(
        None,
        "--include-unsynced-corp-actions/--exclude-unsynced-corp-actions",
        help=(
            "Include dates where sync_corporate_action_events preflight is missing/failed. "
            "If omitted, uses configs/app.yaml catchup default."
        ),
    ),
    force: bool = typer.Option(False, "--force", help="Force rerun"),
    verbose_preflight: bool = typer.Option(
        False,
        "--verbose-preflight",
        help="Print preflight checks from job metadata.",
    ),
) -> None:
    from quant.bootstrap.container import Container
    try:
        from quant.pipelines.eod_catchup_pipeline import EodCatchupPipeline
        pipeline = EodCatchupPipeline()
    except ModuleNotFoundError as exc:
        typer.echo(
            f"Missing dependency: {exc}. Run `pip install -e '.[dev]'` first.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    Container().bootstrap()
    resolved_include_unsynced = _resolve_include_unsynced_corporate_action_dates(
        include_unsynced_corporate_action_dates
    )
    resolved_dates = dates or _discover_catchup_dates(
        start_date=start_date,
        end_date=end_date,
        include_research_ready=include_research_ready,
        include_unsynced_corporate_action_dates=resolved_include_unsynced,
    )
    try:
        result = pipeline.run_for_dates(target_dates=resolved_dates, force=force)
    except Exception as exc:
        typer.echo(f"EOD catchup failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    print(result.to_dict())
    if verbose_preflight:
        _print_preflight_summary(result.results)


@app.command("sync-corporate-actions")
def sync_corporate_actions(
    start_date: str = typer.Option(..., "--start-date", help="Inclusive start date (YYYY-MM-DD)."),
    end_date: str | None = typer.Option(None, "--end-date", help="Inclusive end date (YYYY-MM-DD)."),
    force: bool = typer.Option(False, "--force", help="Force update existing events"),
) -> None:
    from quant.bootstrap.container import Container

    resolved_end = end_date or start_date
    target_dates = _expand_calendar_dates(start_date=start_date, end_date=resolved_end)

    try:
        from quant.pipelines.corporate_action_catchup_pipeline import (
            CorporateActionCatchupPipeline,
        )
        pipeline = CorporateActionCatchupPipeline()
    except ModuleNotFoundError as exc:
        typer.echo(
            f"Missing dependency: {exc}. Run `pip install -e '.[dev]'` first.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    Container().bootstrap()
    try:
        pipeline_result = pipeline.run_for_dates(target_dates=target_dates, force=force)
    except Exception as exc:
        typer.echo(f"Corporate action sync failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    print(pipeline_result.to_dict())


@app.command("preflight")
def preflight(
    check_db: bool = typer.Option(
        True,
        "--check-db/--skip-db",
        help="Enable or skip database checks.",
    ),
    require_db_schema: bool = typer.Option(
        True,
        "--require-db-schema/--skip-db-schema",
        help="Require DB schema to be present (otherwise only DB connectivity is checked).",
    ),
) -> None:
    from quant.bootstrap.container import Container
    from quant.core.enums import RunStatus
    from quant.services.ops.platform_preflight_service import PlatformPreflightService

    try:
        Container().bootstrap()
    except Exception as exc:
        typer.echo(f"Bootstrap failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    report = PlatformPreflightService().run(
        require_db_schema=require_db_schema,
        check_database=check_db,
    )
    print(report.to_dict())
    if report.status == RunStatus.FAILED:
        raise typer.Exit(code=1)
