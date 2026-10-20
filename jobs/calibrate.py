"""Batch job: fit candidate calibration files from recorded or downloaded history (no HTTP surface).

uv run python jobs/calibrate.py prices tests/fixtures/pse/rce-pln_*.json uv run python
jobs/calibrate.py temperature tests/fixtures/openmeteo/archive_era5_2024_temperature.json
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from ems.ingest.calibration import fit_prices, fit_temperature, write_candidate
from ems.settings import load_settings

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
OUT = Path("config/calibration/candidates")


@app.command()
def prices(
    files: Annotated[list[Path], typer.Argument(help="PSE rce-pln JSON responses")],
    out: Annotated[Path, typer.Option(help="Candidate folder")] = OUT,
) -> None:
    result = fit_prices([json.loads(f.read_text()) for f in files], load_settings().mappings_dir)
    typer.echo(f"wrote {write_candidate(result, out, ', '.join(f.name for f in files))}")


@app.command()
def temperature(
    file: Annotated[
        Path, typer.Argument(help="Open-Meteo archive JSON with hourly temperature_2m")
    ],
    out: Annotated[Path, typer.Option(help="Candidate folder")] = OUT,
) -> None:
    result = fit_temperature(json.loads(file.read_text()))
    typer.echo(f"wrote {write_candidate(result, out, file.name)}")


if __name__ == "__main__":
    app()
