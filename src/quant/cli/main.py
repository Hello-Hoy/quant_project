from __future__ import annotations

import typer

from quant.cli.eod import app as eod_app

app = typer.Typer(help="Quant project CLI")
app.add_typer(eod_app, name="eod")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
