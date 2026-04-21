from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, List, Optional

import typer
from rich.console import Console
from rich.table import Table

from htcondor_accounting.config.load import load_config, resolve_config_path
from htcondor_accounting.models.canonical import CanonicalJobRecord
from htcondor_accounting.report.daily import canonical_day_paths, derive_daily
from htcondor_accounting.store.jsonl import read_jsonl_zst, write_jsonl_zst
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


def _parse_day(value: str) -> datetime:
    return _parse_day_or_timestamp(value, end_of_day=False)


def _source_name(schedd_name: str) -> str:
    if schedd_name == "local":
        return "local"
    return schedd_name.split(".")[0]


def record_bucket_datetime(record: CanonicalJobRecord) -> datetime:
    for value in (
        record.timing.end_time,
        record.timing.status_time,
        record.timing.start_time,
    ):
        if value is not None:
            return datetime.fromtimestamp(value, tz=timezone.utc)

    raise ValueError(f"Record has no usable timing bucket: {record.job.global_job_id}")


def bucket_records_by_day(records: Iterable[CanonicalJobRecord]) -> dict[datetime, list[CanonicalJobRecord]]:
    bucketed: dict[datetime, list[CanonicalJobRecord]] = defaultdict(list)
    for record in records:
        bucket_dt = record_bucket_datetime(record)
        day = datetime(bucket_dt.year, bucket_dt.month, bucket_dt.day, tzinfo=timezone.utc)
        bucketed[day].append(record)
    return dict(sorted(bucketed.items()))


def _iter_inspect_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(child for child in path.rglob("*.jsonl.zst") if child.is_file())
    raise typer.BadParameter(f"Path does not exist: {path}")


def _field(record: dict[str, Any], *keys: str) -> Any:
    current: Any = record
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


class InspectVerbosity(str, Enum):
    least = "least"
    medium = "medium"
    full = "full"


def _format_unix_timestamp(value: Any) -> str:
    if value is None:
        return "-"
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return str(value)
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _format_wallclock(value: Any) -> str:
    if value is None:
        return "-"
    try:
        total_seconds = int(value)
    except (TypeError, ValueError):
        return str(value)

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_scale_factor(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)


def _parse_global_job_id(value: Any) -> dict[str, Any]:
    text = str(value or "")
    parts = text.split("#")

    parsed: dict[str, Any] = {
        "raw": text or "-",
        "schedd": "-",
        "job_id": "-",
        "timestamp": None,
        "date": "-",
    }

    if len(parts) >= 1 and parts[0]:
        parsed["schedd"] = parts[0]
    if len(parts) >= 2 and parts[1]:
        parsed["job_id"] = parts[1]
    if len(parts) >= 3 and parts[2]:
        try:
            timestamp = int(parts[2])
        except ValueError:
            parsed["timestamp"] = parts[2]
            parsed["date"] = parts[2]
        else:
            parsed["timestamp"] = timestamp
            parsed["date"] = _format_unix_timestamp(timestamp)

    return parsed


def _identity_display(record: dict[str, Any]) -> str:
    dn = _field(record, "identity", "dn")
    if dn:
        return str(dn)

    issuer = _field(record, "identity", "token_issuer")
    subject = _field(record, "identity", "token_subject")
    if issuer or subject:
        return f"issuer={issuer or '-'} subject={subject or '-'}"

    return "-"


def _full_record(record: dict[str, Any]) -> dict[str, Any]:
    enriched = json.loads(json.dumps(record))
    enriched["inspect"] = {
        "global_job_id": _parse_global_job_id(_field(record, "job", "global_job_id")),
        "identity_display": _identity_display(record),
    }
    return enriched


def _inspect_table(verbosity: InspectVerbosity) -> Table:
    table = Table(title="Canonical jobs")
    table.add_column("Schedd#jobID")
    table.add_column("Start Date (UTC)")
    table.add_column("End Date (UTC)")
    table.add_column("User")
    table.add_column("VO")
    table.add_column("Scale Factor", justify="right")

    if verbosity == InspectVerbosity.medium:
        table.add_column("Wallclock", justify="right")
        table.add_column("Identity")

    return table


def _inspect_row(record: dict[str, Any], verbosity: InspectVerbosity) -> list[str]:
    parsed_job = _parse_global_job_id(_field(record, "job", "global_job_id"))
    schedd = str(_field(record, "source", "schedd") or parsed_job["schedd"])
    if parsed_job["job_id"]:
        schedd += '#' + str(parsed_job["job_id"])
    row = [
        schedd,
        _format_unix_timestamp(_field(record, "timing", "start_time") or parsed_job["timestamp"]),
        _format_unix_timestamp(_field(record, "timing", "end_time")),
        str(_field(record, "job", "local_user") or _field(record, "job", "owner") or "-"),
        str(_field(record, "identity", "vo") or "-"),
        _format_scale_factor(_field(record, "benchmark", "scale_factor")),
    ]

    if verbosity == InspectVerbosity.medium:
        row.extend(
            [
                _format_wallclock(_field(record, "usage", "wall_seconds")),
                _identity_display(record),
            ]
        )

    return row


@app.command()
def extract(
    start: str = typer.Option(..., help="Start date/time, e.g. 2026-04-17 or 2026-04-17T00:00:00"),
    end: str = typer.Option(..., help="End date/time, e.g. 2026-04-17 or 2026-04-17T23:59:59"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    schedd: Optional[List[str]] = typer.Option(
        None,
        "--schedd",
        help="Schedd hostname to query; may be given multiple times",
    ),
    output_root: Optional[Path] = typer.Option(
        None,
        help="Root directory for canonical output",
    ),
    site_name: Optional[str] = typer.Option(
        None,
        help="Site name to embed in canonical records",
    ),
    match: Optional[int] = typer.Option(None, help="Maximum number of history ads to fetch per schedd"),
) -> None:
    """Extract HTCondor history into canonical records."""
    from htcondor_accounting.extract.htcondor import (
        HistoryQuery,
        extract_many_canonical_records,
    )

    app_config = load_config(config)
    resolved_schedds = list(schedd) if schedd is not None else list(app_config.extract.default_schedds)
    resolved_output_root = output_root or app_config.storage.root
    resolved_site_name = site_name or app_config.site.name
    resolved_match = match if match is not None else app_config.extract.default_match

    start_dt = _parse_day_or_timestamp(start, end_of_day=False)
    end_dt = _parse_day_or_timestamp(end, end_of_day=True)

    constraint = (
        f"JobStatus == 4 && "
        f"EnteredCurrentStatus >= {int(start_dt.timestamp())} && "
        f"EnteredCurrentStatus <= {int(end_dt.timestamp())}"
    )

    base_query = HistoryQuery(
        schedd_name=None,
        match=resolved_match,
        constraint=constraint,
    )

    run_stamp = RunStamp.now()
    records_by_schedd = extract_many_canonical_records(
        site_name=resolved_site_name,
        schedd_names=resolved_schedds or None,
        base_query=base_query,
    )

    summary = Table(title="Extraction summary")
    summary.add_column("Schedd")
    summary.add_column("Day")
    summary.add_column("Output file")
    summary.add_column("Records", justify="right")

    total_records = 0
    total_files = 0

    for schedd_name, records in records_by_schedd.items():
        source_name = _source_name(schedd_name)
        for bucket_day, day_records in bucket_records_by_day(records).items():
            output_path = canonical_run_file(
                resolved_output_root,
                when=bucket_day,
                source=source_name,
                run_stamp=run_stamp,
            )
            written = write_jsonl_zst(output_path, day_records)
            total_records += written
            total_files += 1

            summary.add_row(
                schedd_name,
                bucket_day.strftime("%Y-%m-%d"),
                str(output_path),
                str(written),
            )

    console.print("[bold]Extract[/bold]")
    console.print(f"  start      = {start_dt.isoformat()}")
    console.print(f"  end        = {end_dt.isoformat()}")
    console.print(f"  constraint = {constraint}")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  site       = {resolved_site_name}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(f"  schedds    = {resolved_schedds or ['local']}")
    console.print(f"  match      = {resolved_match}")
    console.print(f"  files      = {total_files}")
    console.print(f"  total      = {total_records}")
    console.print(summary)


@app.command()
def inspect(
    path: Path = typer.Argument(..., help="Path to a canonical record file or directory"),
    limit: int = typer.Option(20, min=1, help="Maximum number of jobs to show"),
    verbosity: InspectVerbosity = typer.Option(
        InspectVerbosity.least,
        "--verbosity",
        "-v",
        case_sensitive=False,
        help="Output detail level: least, medium, or full",
    ),
) -> None:
    """Inspect canonical accounting data."""
    paths = _iter_inspect_paths(path)
    if not paths:
        console.print(f"[yellow]No .jsonl.zst files found under {path}[/yellow]")
        raise typer.Exit(code=1)

    rows_shown = 0
    total_records = 0
    table = _inspect_table(verbosity) if verbosity != InspectVerbosity.full else None
    full_records: list[dict[str, Any]] = []

    for record in _iter_records(paths):
        total_records += 1
        if rows_shown >= limit:
            continue

        if verbosity == InspectVerbosity.full:
            full_records.append(_full_record(record))
        else:
            assert table is not None
            table.add_row(*_inspect_row(record, verbosity))
        rows_shown += 1

    console.print("[bold]Inspect[/bold]")
    console.print(f"  path         = {path}")
    console.print(f"  files        = {len(paths)}")
    console.print(f"  total jobs   = {total_records}")
    console.print(f"  showing jobs = {min(total_records, limit)}")
    console.print(f"  verbosity    = {verbosity.value}")

    if verbosity == InspectVerbosity.full:
        for index, record in enumerate(full_records, start=1):
            console.print(f"[bold]Job {index}[/bold]")
            console.print_json(json.dumps(record, sort_keys=True))
    else:
        assert table is not None
        console.print(table)


@app.command("derive-daily")
def derive_daily_command(
    day: str = typer.Option(..., help="Day to derive, e.g. 2026-04-17"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for canonical and derived data"),
) -> None:
    """Derive deduplicated daily reporting data from canonical records."""
    app_config = load_config(config)
    resolved_output_root = output_root or app_config.storage.root
    when = _parse_day(day)

    input_paths = canonical_day_paths(resolved_output_root, when)
    if not input_paths:
        console.print(f"[yellow]No canonical files found for {day} under {resolved_output_root}[/yellow]")
        raise typer.Exit(code=1)

    result = derive_daily(resolved_output_root, when)

    summary = Table(title="Daily derivation")
    summary.add_column("Field")
    summary.add_column("Value")
    summary.add_row("Day", result.day)
    summary.add_row("Input files", str(len(result.input_paths)))
    summary.add_row("Input records", str(result.input_records))
    summary.add_row("Unique records", str(result.unique_records))
    summary.add_row("Duplicates", str(result.duplicate_records))
    summary.add_row("Jobs output", str(result.jobs_path))
    summary.add_row("Summary output", str(result.summary_path))
    summary.add_row("Duplicates output", str(result.duplicates_path))

    console.print("[bold]Derive Daily[/bold]")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(summary)


@app.command("show-config")
def show_config(
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
) -> None:
    """Show the resolved application configuration."""
    resolved_path = resolve_config_path(config)
    app_config = load_config(config)

    console.print("[bold]Show Config[/bold]")
    console.print(f"  source = {resolved_path if resolved_path is not None else '<defaults>'}")
    console.print_json(json.dumps(app_config.model_dump(mode='json'), sort_keys=True))


def _iter_records(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for entry in paths:
        yield from read_jsonl_zst(entry)


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
