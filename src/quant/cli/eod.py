from __future__ import annotations

from datetime import date, timedelta

import typer
from rich import print

from quant.core.time_utils import to_trade_date

app = typer.Typer(help="EOD pipeline commands")


def _discover_catchup_dates(start_date: str | None, end_date: str | None) -> list[str]:
    if not start_date and not end_date:
        return []

    resolved_end = to_trade_date(end_date or date.today().isoformat())
    resolved_start = to_trade_date(start_date or resolved_end.isoformat())
    if resolved_start > resolved_end:
        raise typer.BadParameter("--start-date must be on or before --end-date")

    dates: list[str] = []
    current = resolved_start
    while current <= resolved_end:
        if current.weekday() < 5:
            dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


@app.command("run-date")
def run_date(
    date: str = typer.Option(..., "--date", help="Target trade date in YYYY-MM-DD"),
    force: bool = typer.Option(False, "--force", help="Force rerun"),
) -> None:
    from quant.bootstrap.container import Container
    try:
        from quant.pipelines.single_date_eod_pipeline import SingleDateEodPipeline
    except ModuleNotFoundError as exc:
        typer.echo(
            f"Missing dependency: {exc}. Run `pip install -e '.[dev]'` first.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    Container().bootstrap()
    pipeline = SingleDateEodPipeline()
    result = pipeline.run(target_date=date, force=force)
    print(result.to_dict())


@app.command("catchup")
def catchup(
    dates: list[str] = typer.Option(
        [],
        "--date",
        help="Explicit dates to run. Can be provided multiple times.",
    ),
    start_date: str | None = typer.Option(None, "--start-date", help="Inclusive start date (YYYY-MM-DD)."),
    end_date: str | None = typer.Option(None, "--end-date", help="Inclusive end date (YYYY-MM-DD)."),
    force: bool = typer.Option(False, "--force", help="Force rerun"),
) -> None:
    from quant.bootstrap.container import Container
    try:
        from quant.pipelines.eod_catchup_pipeline import EodCatchupPipeline
    except ModuleNotFoundError as exc:
        typer.echo(
            f"Missing dependency: {exc}. Run `pip install -e '.[dev]'` first.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    Container().bootstrap()
    pipeline = EodCatchupPipeline()
    resolved_dates = dates or _discover_catchup_dates(start_date=start_date, end_date=end_date)
    result = pipeline.run_for_dates(target_dates=resolved_dates, force=force)
    print(result.to_dict())
