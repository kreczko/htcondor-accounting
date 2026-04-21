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
from htcondor_accounting.models.manifest import ExtractManifest, ExtractManifestFileEntry
from htcondor_accounting.export.apel_messages import export_apel_daily
from htcondor_accounting.report.daily import canonical_day_paths, derive_daily
from htcondor_accounting.report.rollup import (
    RollupResult,
    derive_all_rollups,
    derive_all_time,
    derive_monthly,
    derive_weekly,
    derive_yearly,
)
from htcondor_accounting.store.jsonl import read_jsonl_zst, write_jsonl_zst
from htcondor_accounting.store.layout import RunStamp, canonical_run_file, ensure_parent_dir, manifest_file
from htcondor_accounting.version import __version__

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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolved_output_root(config: Optional[Path], output_root: Optional[Path]) -> tuple[Path, Optional[Path]]:
    app_config = load_config(config)
    return output_root or app_config.storage.root, resolve_config_path(config)


def _rollup_table(title: str, result: RollupResult) -> Table:
    table = Table(title=title)
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Period", result.period)
    table.add_row("Daily summaries", str(result.summary["days_included"]))
    table.add_row("Output", str(result.output_path))
    table.add_row("Unique records", str(result.summary["unique_records"]))
    table.add_row("Wall seconds", str(result.summary["wall_seconds"]))
    table.add_row("Scaled CPU seconds", str(result.summary["scaled_cpu_seconds"]))
    return table


def _rollup_results_table(title: str, results: list[RollupResult]) -> Table:
    table = Table(title=title)
    table.add_column("Period Type")
    table.add_column("Period")
    table.add_column("Daily summaries", justify="right")
    table.add_column("Output")
    table.add_column("Unique records", justify="right")
    table.add_column("Wall seconds", justify="right")
    table.add_column("Scaled CPU seconds", justify="right")
    for result in results:
        table.add_row(
            result.period_type,
            result.period,
            str(result.summary["days_included"]),
            str(result.output_path),
            str(result.summary["unique_records"]),
            str(result.summary["wall_seconds"]),
            str(result.summary["scaled_cpu_seconds"]),
        )
    return table


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
    manifest_schedds = sorted(records_by_schedd.keys()) if records_by_schedd else (resolved_schedds or ["local"])

    summary = Table(title="Extraction summary")
    summary.add_column("Schedd")
    summary.add_column("Day")
    summary.add_column("Output file")
    summary.add_column("Records", justify="right")

    total_records = 0
    total_files = 0
    manifest_entries: list[ExtractManifestFileEntry] = []
    resolved_config_path = resolve_config_path(config)

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
            manifest_entries.append(
                ExtractManifestFileEntry(
                    schedd=schedd_name,
                    source_name=source_name,
                    day=bucket_day.strftime("%Y-%m-%d"),
                    path=str(output_path),
                    records=written,
                )
            )

            summary.add_row(
                schedd_name,
                bucket_day.strftime("%Y-%m-%d"),
                str(output_path),
                str(written),
            )

    manifest_entries.sort(key=lambda entry: (entry.schedd, entry.day, entry.path))
    manifest_path = manifest_file(resolved_output_root, run_stamp)
    manifest = ExtractManifest(
        tool_version=__version__,
        run_stamp=run_stamp.as_filename_component(),
        site_name=resolved_site_name,
        start=start_dt.isoformat(),
        end=end_dt.isoformat(),
        constraint=constraint,
        match=resolved_match,
        schedds=manifest_schedds,
        output_root=str(resolved_output_root),
        files_written=manifest_entries,
        total_records=total_records,
        files_written_count=len(manifest_entries),
        source_config_path=str(resolved_config_path) if resolved_config_path is not None else None,
    )
    _write_json(manifest_path, manifest.model_dump(mode="json"))

    console.print("[bold]Extract[/bold]")
    console.print(f"  start      = {start_dt.isoformat()}")
    console.print(f"  end        = {end_dt.isoformat()}")
    console.print(f"  constraint = {constraint}")
    console.print(f"  config     = {resolved_config_path or '<defaults>'}")
    console.print(f"  site       = {resolved_site_name}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(f"  schedds    = {resolved_schedds or ['local']}")
    console.print(f"  match      = {resolved_match}")
    console.print(f"  files      = {total_files}")
    console.print(f"  total      = {total_records}")
    console.print(f"  manifest   = {manifest_path}")
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


@app.command("derive-weekly")
def derive_weekly_command(
    year: int = typer.Option(..., help="ISO year, e.g. 2026"),
    week: int = typer.Option(..., help="ISO week number, e.g. 16"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived data"),
) -> None:
    """Derive one weekly rollup from daily summaries."""
    resolved_output_root, resolved_config_path = _resolved_output_root(config, output_root)
    result = derive_weekly(resolved_output_root, year=year, week=week)
    console.print("[bold]Derive Weekly[/bold]")
    console.print(f"  config     = {resolved_config_path or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(_rollup_table("Weekly rollup", result))


@app.command("derive-monthly")
def derive_monthly_command(
    year: int = typer.Option(..., help="Year, e.g. 2026"),
    month: int = typer.Option(..., help="Month number, e.g. 4"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived data"),
) -> None:
    """Derive one monthly rollup from daily summaries."""
    resolved_output_root, resolved_config_path = _resolved_output_root(config, output_root)
    result = derive_monthly(resolved_output_root, year=year, month=month)
    console.print("[bold]Derive Monthly[/bold]")
    console.print(f"  config     = {resolved_config_path or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(_rollup_table("Monthly rollup", result))


@app.command("derive-yearly")
def derive_yearly_command(
    year: int = typer.Option(..., help="Year, e.g. 2026"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived data"),
) -> None:
    """Derive one yearly rollup from daily summaries."""
    resolved_output_root, resolved_config_path = _resolved_output_root(config, output_root)
    result = derive_yearly(resolved_output_root, year=year)
    console.print("[bold]Derive Yearly[/bold]")
    console.print(f"  config     = {resolved_config_path or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(_rollup_table("Yearly rollup", result))


@app.command("derive-all-time")
def derive_all_time_command(
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived data"),
) -> None:
    """Derive the all-time rollup from daily summaries."""
    resolved_output_root, resolved_config_path = _resolved_output_root(config, output_root)
    result = derive_all_time(resolved_output_root)
    console.print("[bold]Derive All-Time[/bold]")
    console.print(f"  config     = {resolved_config_path or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(_rollup_table("All-time rollup", result))


@app.command("derive-rollups")
def derive_rollups_command(
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived data"),
) -> None:
    """Regenerate all weekly, monthly, yearly, and all-time rollups from daily summaries."""
    resolved_output_root, resolved_config_path = _resolved_output_root(config, output_root)
    results = derive_all_rollups(resolved_output_root)
    console.print("[bold]Derive Rollups[/bold]")
    console.print(f"  config     = {resolved_config_path or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(_rollup_results_table("Rollup derivations", results))


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


@app.command("export-apel-daily")
def export_apel_daily_command(
    day: str = typer.Option(..., help="Day to export, e.g. 2026-04-17"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived and APEL data"),
) -> None:
    """Export one day of derived jobs into staged APEL message files."""
    app_config = load_config(config)
    resolved_output_root = output_root or app_config.storage.root
    when = _parse_day(day)
    run_stamp = RunStamp.now()

    result = export_apel_daily(resolved_output_root, when, app_config.apel, run_stamp)

    summary = Table(title="APEL daily export")
    summary.add_column("Field")
    summary.add_column("Value")
    summary.add_row("Day", result.day)
    summary.add_row("Jobs seen", str(result.jobs_seen))
    summary.add_row("Messages written", str(result.messages_written))
    summary.add_row("Total bytes", str(result.total_bytes))
    summary.add_row("Manifest", str(result.manifest_path))

    console.print("[bold]Export APEL Daily[/bold]")
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


def main() -> None:
    app()
