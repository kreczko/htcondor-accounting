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
from htcondor_accounting.export.csv import write_csv_rows
from htcondor_accounting.export.apel_messages import export_apel_daily, staged_apel_files
from htcondor_accounting.export.dirq import promote_staged_message, read_staged_message_info
from htcondor_accounting.export.ledger import (
    load_ledger_entries,
    parse_run_stamp_from_staged_path,
    sent_marker_exists,
    write_resend_marker,
    write_sent_marker,
)
from htcondor_accounting.models.canonical import CanonicalJobRecord
from htcondor_accounting.models.manifest import ExtractManifest, ExtractManifestFileEntry
from htcondor_accounting.render.html import build_monthly_report_context, render_monthly_report_html
from htcondor_accounting.report.daily import canonical_day_paths, derive_daily
from htcondor_accounting.report.jobs import (
    filter_jobs_by_schedd,
    group_jobs_by_accounting_group,
    group_jobs_by_schedd,
    group_jobs_by_user,
    group_jobs_by_vo,
    load_monthly_jobs,
    monthly_schedd_names,
)
from htcondor_accounting.report.rollup import (
    RollupResult,
    derive_all_rollups,
    derive_all_time,
    derive_monthly,
    derive_weekly,
    derive_yearly,
)
from htcondor_accounting.report.summary import build_monthly_report_summary, summary_json_payload
from htcondor_accounting.report.validate import validate_day
from htcondor_accounting.store.jsonl import read_jsonl_zst, write_jsonl_zst
from htcondor_accounting.store.layout import (
    RunStamp,
    apel_ledger_resends_dir,
    apel_ledger_sent_dir,
    canonical_run_file,
    ensure_parent_dir,
    manifest_file,
    raw_history_run_file,
    reports_monthly_accounting_groups_csv_path,
    reports_monthly_index_path,
    reports_monthly_schedd_accounting_groups_csv_path,
    reports_monthly_schedd_index_path,
    reports_monthly_schedd_summary_path,
    reports_monthly_schedd_users_csv_path,
    reports_monthly_schedd_vos_csv_path,
    reports_monthly_schedds_csv_path,
    reports_monthly_summary_path,
    reports_monthly_users_csv_path,
    reports_monthly_vos_csv_path,
)
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


def _resolved_outgoing_root(output_root: Path, outgoing_dir: Path) -> Path:
    if outgoing_dir.is_absolute():
        return outgoing_dir
    return output_root / outgoing_dir


def _resolved_reporting_root(output_root: Path, reporting_dir: Path) -> Path:
    if reporting_dir.is_absolute():
        return reporting_dir
    return output_root / reporting_dir


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


def _validation_table(title: str, rows: list[tuple[str, Any]]) -> Table:
    table = Table(title=title)
    table.add_column("Field")
    table.add_column("Value")
    for field, value in rows:
        table.add_row(field, str(value))
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


def _first_present(record: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _field(record, *path)
        if value not in (None, ""):
            return value
    return None


class InspectVerbosity(str, Enum):
    least = "least"
    medium = "medium"
    full = "full"


class InspectFormat(str, Enum):
    table = "table"
    json = "json"
    ndjson = "ndjson"


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
    dn = _first_present(
        record,
        ("identity", "dn"),
        ("identity", "orig_dn"),
        ("dn",),
        ("orig_dn",),
    )
    if dn:
        return str(dn)

    issuer = _first_present(record, ("identity", "token_issuer"), ("token_issuer",))
    subject = _first_present(record, ("identity", "token_subject"), ("token_subject",))
    if issuer or subject:
        return f"issuer={issuer or '-'} subject={subject or '-'}"

    return "-"


def _inspect_global_job_id(record: dict[str, Any]) -> Any:
    return _first_present(record, ("job", "global_job_id"), ("global_job_id",))


def _inspect_source_schedd(record: dict[str, Any]) -> str:
    value = _first_present(record, ("source", "schedd"), ("source_schedd",))
    return str(value) if value not in (None, "") else "-"


def _inspect_user(record: dict[str, Any]) -> str:
    value = _first_present(
        record,
        ("job", "local_user"),
        ("local_user",),
        ("owner",),
        ("job", "owner"),
    )
    return str(value) if value not in (None, "") else "-"


def _inspect_vo(record: dict[str, Any]) -> str:
    value = _first_present(
        record,
        ("resolved_identity", "vo"),
        ("identity", "vo"),
        ("vo",),
    )
    return str(value) if value not in (None, "") else "-"


def _inspect_start_time(record: dict[str, Any], parsed_job: dict[str, Any]) -> Any:
    value = _first_present(record, ("timing", "start_time"), ("start_time",))
    if value in (None, ""):
        return parsed_job["timestamp"]
    return value


def _inspect_end_time(record: dict[str, Any]) -> Any:
    return _first_present(record, ("timing", "end_time"), ("end_time",))


def _inspect_scale_factor(record: dict[str, Any]) -> Any:
    return _first_present(record, ("benchmark", "scale_factor"), ("scale_factor",))


def _inspect_wall_seconds(record: dict[str, Any]) -> Any:
    return _first_present(record, ("usage", "wall_seconds"), ("wall_seconds",))


def _inspect_schedd_job_id(record: dict[str, Any]) -> str:
    parsed_job = _parse_global_job_id(_inspect_global_job_id(record))
    schedd = _inspect_source_schedd(record)
    if schedd == "-" and parsed_job["schedd"] != "-":
        schedd = str(parsed_job["schedd"])
    job_id = parsed_job["job_id"]
    if job_id not in (None, "", "-"):
        return f"{schedd}#{job_id}"
    return schedd


def _full_record(record: dict[str, Any]) -> dict[str, Any]:
    enriched = json.loads(json.dumps(record))
    enriched["inspect"] = {
        "global_job_id": _parse_global_job_id(_inspect_global_job_id(record)),
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
    parsed_job = _parse_global_job_id(_inspect_global_job_id(record))
    row = [
        _inspect_schedd_job_id(record),
        _format_unix_timestamp(_inspect_start_time(record, parsed_job)),
        _format_unix_timestamp(_inspect_end_time(record)),
        _inspect_user(record),
        _inspect_vo(record),
        _format_scale_factor(_inspect_scale_factor(record)),
    ]

    if verbosity == InspectVerbosity.medium:
        row.extend(
            [
                _format_wallclock(_inspect_wall_seconds(record)),
                _identity_display(record),
            ]
        )

    return row


def _inspect_object(record: dict[str, Any], verbosity: InspectVerbosity) -> dict[str, Any]:
    parsed_job = _parse_global_job_id(_field(record, "job", "global_job_id"))
    resolved_vo = _field(record, "resolved_identity", "vo") or _field(record, "identity", "vo")
    base = {
        "schedd_job_id": f"{_field(record, 'source', 'schedd') or parsed_job['schedd']}#{parsed_job['job_id']}",
        "start_time": _field(record, "timing", "start_time") or parsed_job["timestamp"],
        "end_time": _field(record, "timing", "end_time"),
        "user": _field(record, "job", "local_user") or _field(record, "job", "owner"),
        "vo": resolved_vo,
        "scale_factor": _field(record, "benchmark", "scale_factor"),
    }

    if verbosity == InspectVerbosity.medium:
        base["wall_seconds"] = _field(record, "usage", "wall_seconds")
        base["identity"] = _identity_display(record)

    if verbosity == InspectVerbosity.full:
        return _full_record(record)

    return base


def _ledger_event_time(entry: dict[str, Any]) -> str | None:
    return str(entry.get("first_pushed_at") or entry.get("resent_at") or "") or None


def _filtered_ledger_entries(
    output_root: Path,
    *,
    day: str | None,
    message_md5: str | None,
    include_resends: bool,
) -> list[dict[str, Any]]:
    entries = load_ledger_entries(output_root, include_resends=include_resends)
    filtered: list[dict[str, Any]] = []
    for entry in entries:
        if day is not None and entry.get("day") != day:
            continue
        if message_md5 is not None and entry.get("message_md5") != message_md5:
            continue
        filtered.append(entry)
    return sorted(
        filtered,
        key=lambda entry: (
            str(entry.get("day") or ""),
            str(entry.get("message_md5") or ""),
            str(_ledger_event_time(entry) or ""),
            str(entry.get("_ledger_path") or ""),
        ),
    )


def _raw_ad_bucket_datetime(ad: dict[str, Any]) -> datetime:
    for key in ("CompletionDate", "EnteredCurrentStatus", "JobStartDate"):
        value = ad.get(key)
        if value is None:
            continue
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        except (TypeError, ValueError):
            continue
    global_job_id = ad.get("GlobalJobId", "<missing-global-job-id>")
    raise ValueError(f"Raw ad has no usable timing bucket: {global_job_id}")


def bucket_raw_ads_by_day(records: Iterable[dict[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    bucketed: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        bucket_dt = _raw_ad_bucket_datetime(record)
        day = datetime(bucket_dt.year, bucket_dt.month, bucket_dt.day, tzinfo=timezone.utc)
        bucketed[day].append(record)
    return dict(sorted(bucketed.items()))


def _write_monthly_report_set(
    *,
    output_root: Path,
    year: int,
    month: int,
    jobs: list[dict[str, Any]],
    benchmark_type: str,
    benchmark_baseline: float,
    users_csv_path: Path,
    vos_csv_path: Path,
    accounting_groups_csv_path: Path,
    summary_path: Path,
    index_path: Path,
    schedd_name: str | None = None,
    parent_index_link: str | None = None,
    schedd_links: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    user_rows = group_jobs_by_user(jobs)
    vo_rows = group_jobs_by_vo(jobs)
    accounting_group_rows = group_jobs_by_accounting_group(jobs)
    summary = build_monthly_report_summary(year, month, jobs, schedd=schedd_name)

    write_csv_rows(
        users_csv_path,
        [{**row.model_dump(mode="json"), "user": row.group_key} for row in user_rows],
        [
            "user",
            "vo",
            "jobs",
            "wall_seconds",
            "cpu_user_seconds",
            "cpu_sys_seconds",
            "cpu_total_seconds",
            "scaled_wall_seconds",
            "scaled_cpu_seconds",
            "avg_processors",
            "max_processors",
            "memory_real_kb_max",
            "memory_virtual_kb_max",
        ],
    )
    write_csv_rows(
        vos_csv_path,
        [{**row.model_dump(mode="json"), "vo": row.group_key} for row in vo_rows],
        [
            "vo",
            "users",
            "jobs",
            "wall_seconds",
            "cpu_user_seconds",
            "cpu_sys_seconds",
            "cpu_total_seconds",
            "scaled_wall_seconds",
            "scaled_cpu_seconds",
            "avg_processors",
            "max_processors",
            "memory_real_kb_max",
            "memory_virtual_kb_max",
        ],
    )
    write_csv_rows(
        accounting_groups_csv_path,
        [{**row.model_dump(mode="json"), "accounting_group": row.group_key} for row in accounting_group_rows],
        [
            "accounting_group",
            "vo",
            "users",
            "jobs",
            "wall_seconds",
            "cpu_user_seconds",
            "cpu_sys_seconds",
            "cpu_total_seconds",
            "scaled_wall_seconds",
            "scaled_cpu_seconds",
            "avg_processors",
            "max_processors",
            "memory_real_kb_max",
            "memory_virtual_kb_max",
        ],
    )
    _write_json(summary_path, summary_json_payload(summary))
    ensure_parent_dir(index_path)
    report_context = build_monthly_report_context(
        summary,
        user_rows,
        vo_rows,
        accounting_group_rows,
        benchmark_type=benchmark_type,
        benchmark_baseline=benchmark_baseline,
        schedd_name=schedd_name,
        parent_index_link=parent_index_link,
        schedd_links=schedd_links,
    )
    index_path.write_text(render_monthly_report_html(report_context), encoding="utf-8")
    return {"summary": summary, "index_path": index_path}


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
    match: Optional[int] = typer.Option(None, help="Maximum number of history ads to fetch per schedd; omit for unlimited"),
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
    output_format: InspectFormat = typer.Option(
        InspectFormat.table,
        "--format",
        case_sensitive=False,
        help="Output format: table, json, or ndjson",
    ),
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
        if output_format == InspectFormat.table:
            console.print(f"[yellow]No .jsonl.zst files found under {path}[/yellow]")
        else:
            console.print("[]")
        raise typer.Exit(code=1)

    rows_shown = 0
    total_records = 0
    table = _inspect_table(verbosity) if output_format == InspectFormat.table and verbosity != InspectVerbosity.full else None
    rendered_records: list[dict[str, Any]] = []

    for record in _iter_records(paths):
        total_records += 1
        if rows_shown >= limit:
            continue

        rendered = _inspect_object(record, verbosity)
        rendered_records.append(rendered)

        if output_format == InspectFormat.table:
            if verbosity == InspectVerbosity.full:
                pass
            else:
                assert table is not None
                table.add_row(*_inspect_row(record, verbosity))
        rows_shown += 1

    if output_format == InspectFormat.json:
        typer.echo(json.dumps(rendered_records, sort_keys=True))
        return

    if output_format == InspectFormat.ndjson:
        for record in rendered_records:
            typer.echo(json.dumps(record, sort_keys=True))
        return

    console.print("[bold]Inspect[/bold]")
    console.print(f"  path         = {path}")
    console.print(f"  files        = {len(paths)}")
    console.print(f"  total jobs   = {total_records}")
    console.print(f"  showing jobs = {min(total_records, limit)}")
    console.print(f"  verbosity    = {verbosity.value}")

    if verbosity == InspectVerbosity.full:
        for index, record in enumerate(rendered_records, start=1):
            console.print(f"[bold]Job {index}[/bold]")
            console.print_json(json.dumps(record, sort_keys=True))
    else:
        assert table is not None
        console.print(table)


@app.command("snapshot-history")
def snapshot_history(
    start: str = typer.Option(..., help="Start date/time, e.g. 2026-04-17 or 2026-04-17T00:00:00"),
    end: str = typer.Option(..., help="End date/time, e.g. 2026-04-17 or 2026-04-17T23:59:59"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    schedd: Optional[List[str]] = typer.Option(
        None,
        "--schedd",
        help="Schedd hostname to query; may be given multiple times",
    ),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for raw-history output"),
    match: Optional[int] = typer.Option(None, help="Maximum number of history ads to fetch per schedd"),
) -> None:
    """Snapshot raw HTCondor history ads into compressed JSONL files."""
    from htcondor_accounting.extract.htcondor import HistoryQuery, fetch_history_ads

    app_config = load_config(config)
    resolved_schedds = list(schedd) if schedd is not None else list(app_config.extract.default_schedds)
    resolved_output_root = output_root or app_config.storage.root
    resolved_match = match if match is not None else app_config.extract.default_match

    start_dt = _parse_day_or_timestamp(start, end_of_day=False)
    end_dt = _parse_day_or_timestamp(end, end_of_day=True)
    constraint = (
        f"JobStatus == 4 && "
        f"EnteredCurrentStatus >= {int(start_dt.timestamp())} && "
        f"EnteredCurrentStatus <= {int(end_dt.timestamp())}"
    )

    run_stamp = RunStamp.now()
    summary = Table(title="Raw history snapshot")
    summary.add_column("Schedd")
    summary.add_column("Day")
    summary.add_column("Output file")
    summary.add_column("Records", justify="right")

    schedd_targets = resolved_schedds or [None]
    total_records = 0
    total_files = 0
    for schedd_name in schedd_targets:
        query = HistoryQuery(
            schedd_name=schedd_name,
            match=resolved_match,
            constraint=constraint,
        )
        ads = fetch_history_ads(query)
        resolved_schedd_name = str(schedd_name or "local")
        source_name = _source_name(resolved_schedd_name)

        for bucket_day, day_ads in bucket_raw_ads_by_day(ads).items():
            output_path = raw_history_run_file(
                resolved_output_root,
                when=bucket_day,
                source=source_name,
                run_stamp=run_stamp,
            )
            written = write_jsonl_zst(output_path, day_ads)
            total_records += written
            total_files += 1
            summary.add_row(
                resolved_schedd_name,
                bucket_day.strftime("%Y-%m-%d"),
                str(output_path),
                str(written),
            )

    console.print("[bold]Snapshot History[/bold]")
    console.print(f"  start      = {start_dt.isoformat()}")
    console.print(f"  end        = {end_dt.isoformat()}")
    console.print(f"  constraint = {constraint}")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(f"  schedds    = {resolved_schedds or ['local']}")
    console.print(f"  match      = {resolved_match}")
    console.print(f"  files      = {total_files}")
    console.print(f"  total      = {total_records}")
    console.print(summary)


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


@app.command("render-monthly")
def render_monthly_command(
    year: int = typer.Option(..., help="Year, e.g. 2026"),
    month: int = typer.Option(..., help="Month number, e.g. 4"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for derived and report data"),
    include_schedds: bool = typer.Option(False, help="Also generate a schedd-grouped CSV"),
    schedd: Optional[str] = typer.Option(None, "--schedd", help="Render only one schedd report for debugging"),
) -> None:
    """Render one monthly internal report from derived daily jobs."""
    app_config = load_config(config)
    resolved_output_root = output_root or app_config.storage.root

    jobs = load_monthly_jobs(resolved_output_root, year, month)
    schedd_rows = group_jobs_by_schedd(jobs) if include_schedds else None
    available_schedds = monthly_schedd_names(jobs)
    target_schedds = [schedd] if schedd is not None else available_schedds

    if include_schedds:
        write_csv_rows(
            reports_monthly_schedds_csv_path(resolved_output_root, year, month),
            [{**row.model_dump(mode="json"), "schedd": row.group_key} for row in (schedd_rows or [])],
            [
                "schedd",
                "jobs",
                "wall_seconds",
                "cpu_user_seconds",
                "cpu_sys_seconds",
                "cpu_total_seconds",
                "scaled_wall_seconds",
                "scaled_cpu_seconds",
                "avg_processors",
                "max_processors",
                "memory_real_kb_max",
                "memory_virtual_kb_max",
            ],
        )

    schedd_links: list[dict[str, str]] = []
    for schedd_name in available_schedds:
        schedd_jobs = filter_jobs_by_schedd(jobs, schedd_name)
        _write_monthly_report_set(
            output_root=resolved_output_root,
            year=year,
            month=month,
            jobs=schedd_jobs,
            benchmark_type=app_config.benchmark.type,
            benchmark_baseline=app_config.benchmark.baseline_per_core,
            users_csv_path=reports_monthly_schedd_users_csv_path(resolved_output_root, year, month, schedd_name),
            vos_csv_path=reports_monthly_schedd_vos_csv_path(resolved_output_root, year, month, schedd_name),
            accounting_groups_csv_path=reports_monthly_schedd_accounting_groups_csv_path(resolved_output_root, year, month, schedd_name),
            summary_path=reports_monthly_schedd_summary_path(resolved_output_root, year, month, schedd_name),
            index_path=reports_monthly_schedd_index_path(resolved_output_root, year, month, schedd_name),
            schedd_name=schedd_name,
            parent_index_link="../../index.html",
        )
        schedd_links.append(
            {
                "label": schedd_name,
                "href": f"schedds/{schedd_name}/index.html",
                "jobs": str(len(schedd_jobs)),
            }
        )

    top_level_result = _write_monthly_report_set(
        output_root=resolved_output_root,
        year=year,
        month=month,
        jobs=jobs,
        benchmark_type=app_config.benchmark.type,
        benchmark_baseline=app_config.benchmark.baseline_per_core,
        users_csv_path=reports_monthly_users_csv_path(resolved_output_root, year, month),
        vos_csv_path=reports_monthly_vos_csv_path(resolved_output_root, year, month),
        accounting_groups_csv_path=reports_monthly_accounting_groups_csv_path(resolved_output_root, year, month),
        summary_path=reports_monthly_summary_path(resolved_output_root, year, month),
        index_path=reports_monthly_index_path(resolved_output_root, year, month),
        schedd_links=schedd_links,
    )
    summary = top_level_result["summary"]
    index_path = top_level_result["index_path"]

    summary_table = Table(title="Monthly report")
    summary_table.add_column("Field")
    summary_table.add_column("Value")
    summary_table.add_row("Period", f"{year:04d}-{month:02d}")
    summary_table.add_row("Jobs", str(summary.jobs_total))
    summary_table.add_row("Days included", str(summary.days_included))
    summary_table.add_row("Schedd reports", str(len(target_schedds)))
    summary_table.add_row("Output directory", str(index_path.parent))

    console.print("[bold]Render Monthly[/bold]")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(summary_table)


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


@app.command("push-apel-daily")
def push_apel_daily_command(
    day: str = typer.Option(..., help="Day to push, e.g. 2026-04-17"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for APEL data"),
    force_resend: bool = typer.Option(False, "--force-resend", help="Push even if a sent marker already exists"),
    reason: Optional[str] = typer.Option(None, help="Optional resend reason to record in the ledger"),
) -> None:
    """Promote staged APEL message files into the live dirq-compatible outgoing queue."""
    app_config = load_config(config)
    resolved_output_root = output_root or app_config.storage.root
    when = _parse_day(day)
    staged_files = staged_apel_files(resolved_output_root, when, app_config.apel)
    outgoing_root = _resolved_outgoing_root(resolved_output_root, app_config.apel.outgoing_dir)
    pushed = 0
    skipped = 0
    resent = 0

    for path in staged_files:
        info = read_staged_message_info(path)
        already_sent = sent_marker_exists(resolved_output_root, info.message_md5)

        if already_sent and not force_resend:
            skipped += 1
            continue

        result = promote_staged_message(path, outgoing_root)
        run_stamp = parse_run_stamp_from_staged_path(path)

        if force_resend and already_sent:
            write_resend_marker(
                resolved_output_root,
                day=day,
                info=info,
                outgoing_path=result.queue_path,
                run_stamp=run_stamp,
                reason=reason,
            )
            resent += 1
            continue

        write_sent_marker(
            resolved_output_root,
            day=day,
            info=info,
            outgoing_path=result.queue_path,
            run_stamp=run_stamp,
        )
        pushed += 1

    summary = Table(title="APEL queue promotion")
    summary.add_column("Field")
    summary.add_column("Value")
    summary.add_row("Day", day)
    summary.add_row("Staged files", str(len(staged_files)))
    summary.add_row("Already-sent skipped", str(skipped))
    summary.add_row("Newly pushed", str(pushed))
    summary.add_row("Resent", str(resent))
    summary.add_row("Outgoing root", str(outgoing_root))
    summary.add_row("Ledger root", str(resolved_output_root / "apel" / "ledger"))

    console.print("[bold]Push APEL Daily[/bold]")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(summary)


@app.command("inspect-apel-ledger")
def inspect_apel_ledger(
    day: Optional[str] = typer.Option(None, help="Filter to a specific day, e.g. 2026-04-17"),
    hash: Optional[str] = typer.Option(None, "--hash", help="Filter to a specific message MD5"),
    include_resends: bool = typer.Option(False, help="Include resend events in addition to sent markers"),
    format: InspectFormat = typer.Option(InspectFormat.table, "--format", help="Output format"),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for APEL data"),
) -> None:
    """Inspect the APEL push ledger."""
    app_config = load_config(config)
    resolved_output_root = output_root or app_config.storage.root
    entries = _filtered_ledger_entries(
        resolved_output_root,
        day=day,
        message_md5=hash,
        include_resends=include_resends,
    )

    if format == InspectFormat.json:
        console.print_json(json.dumps(entries, sort_keys=True))
        return
    if format == InspectFormat.ndjson:
        for entry in entries:
            console.print(json.dumps(entry, sort_keys=True))
        return

    table = Table(title="APEL ledger")
    table.add_column("Type")
    table.add_column("Day")
    table.add_column("MD5")
    table.add_column("Pushed At")
    table.add_column("Records", justify="right")
    table.add_column("Bytes", justify="right")
    table.add_column("Outgoing Path")
    for entry in entries:
        table.add_row(
            str(entry.get("record_type") or "-"),
            str(entry.get("day") or "-"),
            str(entry.get("message_md5") or "-"),
            str(_ledger_event_time(entry) or "-"),
            str(entry.get("records") or "-"),
            str(entry.get("bytes") or "-"),
            str(entry.get("outgoing_path") or "-"),
        )

    console.print("[bold]Inspect APEL Ledger[/bold]")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(f"  sent       = {apel_ledger_sent_dir(resolved_output_root)}")
    if include_resends:
        console.print(f"  resends    = {apel_ledger_resends_dir(resolved_output_root)}")
    console.print(table)


@app.command("validate-day")
def validate_day_command(
    day: str = typer.Option(..., help="Day to validate, e.g. 2026-04-21"),
    schedd: Optional[str] = typer.Option(None, "--schedd", help="Limit canonical and derived checks to one schedd"),
    output_format: InspectFormat = typer.Option(
        InspectFormat.table,
        "--format",
        case_sensitive=False,
        help="Output format: table, json, or ndjson",
    ),
    config: Optional[Path] = typer.Option(None, help="Path to site config file"),
    output_root: Optional[Path] = typer.Option(None, help="Root directory for pipeline outputs"),
) -> None:
    """Validate one day's pipeline outputs and surface discrepancies."""
    app_config = load_config(config)
    resolved_output_root = output_root or app_config.storage.root
    when = _parse_day(day)
    result = validate_day(resolved_output_root, when, schedd_name=schedd)
    payload = result.payload

    if output_format == InspectFormat.json:
        typer.echo(json.dumps(payload, sort_keys=True))
        return
    if output_format == InspectFormat.ndjson:
        typer.echo(json.dumps(payload, sort_keys=True))
        return

    files = payload["files"]
    counts = payload["counts"]
    identity_quality = payload["identity_quality"]
    apel = payload["apel"]
    warnings = payload["warnings"]
    errors = payload["errors"]

    console.print("[bold]Validate Day[/bold]")
    console.print(f"  config     = {resolve_config_path(config) or '<defaults>'}")
    console.print(f"  output     = {resolved_output_root}")
    console.print(f"  day        = {payload['day']}")
    if schedd is not None:
        console.print(f"  schedd     = {schedd}")
        console.print("  note       = schedd filtering applies to canonical/derived job data; APEL checks remain day-scoped")

    console.print(
        _validation_table(
            "Files and Records",
            [
                ("Raw history files", files["raw_history_files"]),
                ("Raw history records", counts["raw_history_records"]),
                ("Canonical files", files["canonical_files"]),
                ("Canonical records", counts["canonical_records"]),
                ("Derived unique jobs", counts["derived_unique_jobs"]),
                ("Duplicate jobs", counts["duplicate_jobs"]),
                ("APEL staged files", files["apel_staged_files"]),
                ("APEL staged records", counts["apel_staged_records"]),
                ("APEL pushed messages", counts["apel_pushed_messages"]),
                ("APEL sent ledger entries", counts["apel_sent_ledger_entries"]),
            ],
        )
    )
    console.print(
        _validation_table(
            "Identity Quality",
            [
                ("Missing resolved VO", identity_quality["missing_resolved_vo"]),
                ("Missing resolved FQAN", identity_quality["missing_resolved_fqan"]),
                ("Missing accounting group", identity_quality["missing_accounting_group"]),
                ("Auth method: scitoken", identity_quality["auth_method_counts"]["scitoken"]),
                ("Auth method: x509", identity_quality["auth_method_counts"]["x509"]),
                ("Auth method: local", identity_quality["auth_method_counts"]["local"]),
                ("Unresolved jobs", identity_quality["unresolved_jobs"]),
            ],
        )
    )
    console.print(
        _validation_table(
            "APEL State",
            [
                ("Manifest exists", apel["manifest_exists"]),
                ("Staged files present", apel["staged_files_present"]),
                ("Sent entries", apel["sent_entries"]),
                ("Resend events", apel["resend_events"]),
                ("Manifest messages", counts["apel_manifest_messages_written"]),
                ("Manifest bytes", counts["apel_manifest_total_bytes"]),
            ],
        )
    )

    issues = Table(title="Warnings and Errors")
    issues.add_column("Level")
    issues.add_column("Message")
    if not warnings and not errors:
        issues.add_row("OK", "No validation issues detected.")
    else:
        for message in errors:
            issues.add_row("ERROR", message)
        for message in warnings:
            issues.add_row("WARN", message)
    console.print(issues)


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
