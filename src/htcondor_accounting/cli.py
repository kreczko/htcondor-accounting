from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

app = typer.Typer(help="HTCondor accounting extraction, normalization, and APEL export utilities.")
console = Console()


@app.command()
def hello() -> None:
    """Sanity check that the package and CLI are working."""
    console.print("[green]htcondor-accounting is alive[/green]")


@app.command()
def extract(
    start: str = typer.Option(..., help="Start date/time, e.g. 2026-04-17 or 2026-04-17T00:00:00"),
    end: str = typer.Option(..., help="End date/time, e.g. 2026-04-17 or 2026-04-17T23:59:59"),
    schedd: Optional[str] = typer.Option(None, help="Schedd hostname to query"),
    output_root: Path = typer.Option(
        Path("./archive"),
        help="Root directory for canonical output",
    ),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
) -> None:
    """Extract HTCondor history into canonical records."""
    console.print("[bold]Extract[/bold]")
    console.print(f"  start      = {start}")
    console.print(f"  end        = {end}")
    console.print(f"  schedd     = {schedd}")
    console.print(f"  output     = {output_root}")
    console.print(f"  config     = {config}")


@app.command()
def inspect(
    path: Path = typer.Argument(..., help="Path to a canonical record file or directory"),
) -> None:
    """Inspect canonical accounting data."""
    console.print("[bold]Inspect[/bold]")
    console.print(f"  path = {path}")


@app.command()
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
