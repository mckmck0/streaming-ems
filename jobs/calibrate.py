"""Batch job: fit candidate calibration files from recorded or downloaded history (no HTTP surface).

uv run python jobs/calibrate.py prices tests/fixtures/pse/rce-pln_*.json uv run python
jobs/calibrate.py temperature tests/fixtures/openmeteo/archive_era5_2024_temperature.json uv run
python -I jobs/calibrate.py load data/external/wpuq/2019_data_15min.hdf5 --format wpuq
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from ems.ingest.calibration import (
    WPUQ_DATASET,
    fit_load,
    fit_prices,
    fit_temperature,
    read_load_csv,
    read_wpuq,
    write_candidate,
)
from ems.ingest.synthetic.load import load_load_params
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


@app.command()
def load(
    files: Annotated[
        list[Path],
        typer.Argument(help="Base-load files: CSV (timestamp_utc,household_id,kwh) or WPuQ HDF5"),
    ],
    fmt: Annotated[str, typer.Option("--format", help="csv or wpuq")] = "csv",
    out: Annotated[Path, typer.Option(help="Candidate folder")] = OUT,
) -> None:
    """Fit base-load noise (tau, interval and daily sd) from per-household quarter-hour energy."""
    if fmt not in ("csv", "wpuq"):
        raise typer.BadParameter("--format must be csv or wpuq")
    import pandas as pd

    reader = read_wpuq if fmt == "wpuq" else read_load_csv
    frame = pd.concat([reader(f) for f in files], ignore_index=True)
    dataset = WPUQ_DATASET if fmt == "wpuq" else {"name": "user-supplied CSV", "files": len(files)}
    params = load_load_params(load_settings().config_dir / "calibration" / "load.yaml")
    result = fit_load(frame, dataset=dataset, model_params=params)
    typer.echo(f"wrote {write_candidate(result, out, ', '.join(f.name for f in files))}")


if __name__ == "__main__":
    app()
