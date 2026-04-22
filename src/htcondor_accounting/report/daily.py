from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from htcondor_accounting.report.dedup import (
    DeduplicationResult,
    deduplicate_canonical_records,
    read_canonical_records,
)
from htcondor_accounting.store.jsonl import write_jsonl_zst
from htcondor_accounting.store.layout import (
    canonical_day_dir,
    derived_daily_duplicates_path,
    derived_daily_jobs_file,
    derived_daily_summary_path,
    ensure_parent_dir,
)


@dataclass(frozen=True)
class DailyDerivationResult:
    day: str
    input_paths: list[Path]
    input_records: int
    unique_records: int
    duplicate_records: int
    jobs_path: Path
    summary_path: Path
    duplicates_path: Path
    summary: dict[str, Any]
    duplicates: dict[str, Any]


def canonical_day_paths(root: Path, when: datetime) -> list[Path]:
    day_dir = canonical_day_dir(root, when)
    return sorted(day_dir.glob("*.jsonl.zst"))


def day_string(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%d")


def sanitize_reporting_record(record: dict[str, Any], day: str) -> dict[str, Any]:
    usage = record.get("usage", {})
    identity = record.get("identity", {})
    resolved_identity = record.get("resolved_identity", {})
    benchmark = record.get("benchmark", {})
    timing = record.get("timing", {})
    job = record.get("job", {})
    source = record.get("source", {})
    accounting = record.get("accounting", {})

    cpu_user_seconds = int(usage.get("cpu_user_seconds") or 0)
    cpu_sys_seconds = int(usage.get("cpu_sys_seconds") or 0)

    return {
        "schema_version": 1,
        "record_type": "report_job",
        "site_name": record.get("site_name"),
        "global_job_id": job.get("global_job_id"),
        "owner": job.get("owner"),
        "local_user": job.get("local_user"),
        "vo": resolved_identity.get("vo") or identity.get("vo"),
        "vo_group": resolved_identity.get("vo_group") or identity.get("vo_group"),
        "vo_role": resolved_identity.get("vo_role") or identity.get("vo_role"),
        "fqan": resolved_identity.get("fqan") or identity.get("fqan"),
        "resolution_method": resolved_identity.get("resolution_method"),
        "auth_method": identity.get("auth_method"),
        "start_time": timing.get("start_time"),
        "end_time": timing.get("end_time"),
        "wall_seconds": int(usage.get("wall_seconds") or 0),
        "cpu_user_seconds": cpu_user_seconds,
        "cpu_sys_seconds": cpu_sys_seconds,
        "cpu_total_seconds": cpu_user_seconds + cpu_sys_seconds,
        "processors": int(usage.get("processors") or 1),
        "memory_real_kb": usage.get("memory_real_kb"),
        "memory_virtual_kb": usage.get("memory_virtual_kb"),
        "scale_factor": benchmark.get("scale_factor"),
        "benchmark_type": benchmark.get("benchmark_type"),
        "source_schedd": source.get("schedd"),
        "acct_group": accounting.get("acct_group"),
        "acct_group_user": accounting.get("acct_group_user"),
        "accounting_group": accounting.get("accounting_group"),
        "route_name": accounting.get("route_name"),
        "day": day,
    }


def summarize_reporting_records(
    records: list[dict[str, Any]],
    *,
    day: str,
    input_files: int,
    input_records: int,
    duplicate_records: int,
) -> dict[str, Any]:
    users = {str(record.get("local_user") or record.get("owner") or "") for record in records}
    users.discard("")
    vos = {str(record.get("vo") or "") for record in records}
    vos.discard("")

    wall_seconds = sum(int(record.get("wall_seconds") or 0) for record in records)
    cpu_user_seconds = sum(int(record.get("cpu_user_seconds") or 0) for record in records)
    cpu_sys_seconds = sum(int(record.get("cpu_sys_seconds") or 0) for record in records)
    cpu_total_seconds = sum(int(record.get("cpu_total_seconds") or 0) for record in records)

    scaled_wall_seconds = 0.0
    scaled_cpu_seconds = 0.0
    for record in records:
        scale_factor = record.get("scale_factor")
        scale = float(scale_factor) if scale_factor is not None else 1.0
        scaled_wall_seconds += int(record.get("wall_seconds") or 0) * scale
        scaled_cpu_seconds += int(record.get("cpu_total_seconds") or 0) * scale

    return {
        "day": day,
        "input_files": input_files,
        "input_records": input_records,
        "unique_records": len(records),
        "duplicate_records": duplicate_records,
        "users": len(users),
        "vos": len(vos),
        "wall_seconds": wall_seconds,
        "cpu_user_seconds": cpu_user_seconds,
        "cpu_sys_seconds": cpu_sys_seconds,
        "cpu_total_seconds": cpu_total_seconds,
        "scaled_wall_seconds": scaled_wall_seconds,
        "scaled_cpu_seconds": scaled_cpu_seconds,
    }


def duplicates_report(
    *,
    day: str,
    input_files: int,
    input_records: int,
    unique_records: int,
    duplicate_records: int,
    duplicate_sample: list[str],
) -> dict[str, Any]:
    return {
        "day": day,
        "input_files": input_files,
        "input_records": input_records,
        "unique_records": unique_records,
        "duplicate_records": duplicate_records,
        "duplicate_sample": duplicate_sample,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def derive_daily(root: Path, when: datetime) -> DailyDerivationResult:
    input_paths = canonical_day_paths(root, when)
    records = read_canonical_records(input_paths)
    deduped: DeduplicationResult = deduplicate_canonical_records(records)
    day = day_string(when)

    reporting_records = [sanitize_reporting_record(record, day) for record in deduped.unique_records]

    summary = summarize_reporting_records(
        reporting_records,
        day=day,
        input_files=len(input_paths),
        input_records=deduped.input_records,
        duplicate_records=deduped.duplicate_records,
    )
    duplicates = duplicates_report(
        day=day,
        input_files=len(input_paths),
        input_records=deduped.input_records,
        unique_records=len(reporting_records),
        duplicate_records=deduped.duplicate_records,
        duplicate_sample=deduped.duplicate_sample,
    )

    jobs_path = derived_daily_jobs_file(root, when)
    summary_path = derived_daily_summary_path(root, when)
    duplicates_path = derived_daily_duplicates_path(root, when)

    write_jsonl_zst(jobs_path, reporting_records)
    write_json(summary_path, summary)
    write_json(duplicates_path, duplicates)

    return DailyDerivationResult(
        day=day,
        input_paths=input_paths,
        input_records=deduped.input_records,
        unique_records=len(reporting_records),
        duplicate_records=deduped.duplicate_records,
        jobs_path=jobs_path,
        summary_path=summary_path,
        duplicates_path=duplicates_path,
        summary=summary,
        duplicates=duplicates,
    )
