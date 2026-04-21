from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table

from htcondor_accounting.extract.htcondor import (
    HistoryQuery,
    extract_many_canonical_records,
)
from htcondor_accounting.store.jsonl import write_jsonl_zst
from htcondor_accounting.store.layout import RunStamp, canonical_run_file

app = typer.Typer(help="HTCondor accounting extraction, normalization, and APEL export utilities.")
console = Console()


@app.command()
def hello() -> None:
    """Sanity check that the package and CLI are working."""
    console.print("[green]htcondor-accounting is alive[/green]")


def _parse_day_or_timestamp(value: str, end_of_day: bool = False) -> datetime:
    if "T" in value:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        if end_of_day:
            dt = datetime.fromisoformat(f"{value}T23:59:59+00:00")
        else:
            dt = datetime.fromisoformat(f"{value}T00:00:00+00:00")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _source_name(schedd_name: str) -> str:
    if schedd_name == "local":
        return "local"
    return schedd_name.split(".")[0]


@app.command()
def extract(
    start: str = typer.Option(..., help="Start date/time, e.g. 2026-04-17 or 2026-04-17T00:00:00"),
    end: str = typer.Option(..., help="End date/time, e.g. 2026-04-17 or 2026-04-17T23:59:59"),
    schedd: Optional[List[str]] = typer.Option(
        None,
        "--schedd",
        help="Schedd hostname to query; may be given multiple times",
    ),
    output_root: Path = typer.Option(
        Path("./archive"),
        help="Root directory for canonical output",
    ),
    site_name: str = typer.Option(
        "UKI-SOUTHGRID-BRIS-HEP",
        help="Site name to embed in canonical records",
    ),
    match: int = typer.Option(100, help="Maximum number of history ads to fetch per schedd"),
) -> None:
    """Extract HTCondor history into canonical records."""
    start_dt = _parse_day_or_timestamp(start, end_of_day=False)
    end_dt = _parse_day_or_timestamp(end, end_of_day=True)

    constraint = (
        f"JobStatus == 4 && "
        f"EnteredCurrentStatus >= {int(start_dt.timestamp())} && "
        f"EnteredCurrentStatus <= {int(end_dt.timestamp())}"
    )

    base_query = HistoryQuery(
        schedd_name=None,
        match=match,
        constraint=constraint,
    )

    run_stamp = RunStamp.now()
    records_by_schedd = extract_many_canonical_records(
        site_name=site_name,
        schedd_names=list(schedd) if schedd else None,
        base_query=base_query,
    )

    summary = Table(title="Extraction summary")
    summary.add_column("Schedd")
    summary.add_column("Output file")
    summary.add_column("Records", justify="right")

    total_records = 0

    for schedd_name, records in records_by_schedd.items():
        source_name = _source_name(schedd_name)
        output_path = canonical_run_file(
            output_root,
            when=start_dt,
            source=source_name,
            run_stamp=run_stamp,
        )
        written = write_jsonl_zst(output_path, records)
        total_records += written

        summary.add_row(
            schedd_name,
            str(output_path),
            str(written),
        )

    console.print("[bold]Extract[/bold]")
    console.print(f"  start      = {start_dt.isoformat()}")
    console.print(f"  end        = {end_dt.isoformat()}")
    console.print(f"  constraint = {constraint}")
    console.print(f"  site       = {site_name}")
    console.print(f"  total      = {total_records}")
    console.print(summary)


@app.command()
def inspect(
    path: Path = typer.Argument(..., help="Path to a canonical record file or directory"),
) -> None:
    """Inspect canonical accounting data."""
    console.print("[bold]Inspect[/bold]")
    console.print(f"  path = {path}")


@app.command("export-apel")
def export_apel(
    start: str = typer.Option(..., help="Start date/time"),
    end: str = typer.Option(..., help="End date/time"),
    input_root: Path = typer.Option(Path("./archive"), help="Canonical archive root"),
    output_dir: Path = typer.Option(
        Path("/var/spool/apel/outgoing"),
        help="Directory to write APEL outgoing messages",
    ),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
) -> None:
    """Export canonical records into APEL-compatible output."""
    console.print("[bold]Export APEL[/bold]")
    console.print(f"  start      = {start}")
    console.print(f"  end        = {end}")
    console.print(f"  input      = {input_root}")
    console.print(f"  output     = {output_dir}")
    console.print(f"  config     = {config}")


def main() -> None:
    app()
