from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from htcondor_accounting.config.models import ApelConfig
from htcondor_accounting.export.apel_records import apel_record_text
from htcondor_accounting.report.daily import write_json
from htcondor_accounting.store.jsonl import read_jsonl_zst
from htcondor_accounting.store.layout import (
    RunStamp,
    apel_manifest_path,
    apel_staging_day_dir,
    apel_staging_message_path,
    derived_daily_jobs_file,
    ensure_parent_dir,
)


APEL_INDIVIDUAL_JOB_MESSAGE_HEADER = "APEL-individual-job-message: v0.3"
APEL_RECORD_SEPARATOR = "\n%%\n"


@dataclass(frozen=True)
class ApelMessageChunk:
    body: str
    records: int
    bytes: int


@dataclass(frozen=True)
class ApelDailyExportResult:
    day: str
    input_jobs_file: Path
    jobs_seen: int
    jobs_exported: int
    jobs_skipped: int
    jobs_skipped_missing_schedd: int
    skipped_by_schedd: dict[str, int]
    allowed_schedds: list[str]
    messages_written: int
    total_bytes: int
    files_written: list[dict[str, Any]]
    manifest_path: Path


def build_apel_message_body(records: list[str]) -> str:
    return f"{APEL_INDIVIDUAL_JOB_MESSAGE_HEADER}\n{APEL_RECORD_SEPARATOR.join(records)}{APEL_RECORD_SEPARATOR}"


def load_daily_jobs(root: Path, when: datetime) -> list[dict[str, Any]]:
    path = derived_daily_jobs_file(root, when)
    return list(read_jsonl_zst(path))


def _job_source_schedd(job: dict[str, Any]) -> str | None:
    value = job.get("source_schedd")
    if value in (None, ""):
        value = job.get("source", {}).get("schedd")
    if value in (None, ""):
        return None
    return str(value)


def filter_jobs_for_apel_export(
    jobs: list[dict[str, Any]],
    allowed_schedds: list[str],
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    allowed = set(allowed_schedds)
    exported: list[dict[str, Any]] = []
    skipped_missing_schedd = 0
    skipped_by_schedd: dict[str, int] = {}

    for job in jobs:
        source_schedd = _job_source_schedd(job)
        if source_schedd is None:
            skipped_missing_schedd += 1
            continue
        if source_schedd not in allowed:
            skipped_by_schedd[source_schedd] = skipped_by_schedd.get(source_schedd, 0) + 1
            continue
        exported.append(job)

    return exported, skipped_missing_schedd, dict(sorted(skipped_by_schedd.items()))


def pack_apel_messages(records: list[str], soft_limit_bytes: int, hard_limit_bytes: int) -> list[ApelMessageChunk]:
    chunks: list[ApelMessageChunk] = []
    current_records: list[str] = []

    for record in records:
        single_record_body = build_apel_message_body([record])
        single_record_bytes = len(single_record_body.encode("utf-8"))
        if single_record_bytes > hard_limit_bytes:
            raise ValueError(
                f"Single APEL message record is {single_record_bytes} bytes with framing, "
                f"exceeding hard limit {hard_limit_bytes} bytes"
            )

        candidate_records = [*current_records, record]
        candidate_body = build_apel_message_body(candidate_records)
        candidate_bytes = len(candidate_body.encode("utf-8"))

        if current_records and candidate_bytes > soft_limit_bytes:
            body = build_apel_message_body(current_records)
            chunks.append(ApelMessageChunk(body=body, records=len(current_records), bytes=len(body.encode("utf-8"))))
            candidate_records = [record]
            candidate_body = single_record_body
            candidate_bytes = single_record_bytes

        if candidate_bytes > hard_limit_bytes:
            raise ValueError(
                f"APEL message would exceed hard limit {hard_limit_bytes} bytes while adding next record"
            )

        current_records = candidate_records

    if current_records:
        body = build_apel_message_body(current_records)
        chunks.append(ApelMessageChunk(body=body, records=len(current_records), bytes=len(body.encode("utf-8"))))

    return chunks


def _staged_message_path(output_root: Path, when: datetime, run_stamp: RunStamp, index: int, config: ApelConfig) -> Path:
    if config.staging_dir.is_absolute():
        return (
            config.staging_dir
            / when.strftime("%Y")
            / when.strftime("%m")
            / when.strftime("%d")
            / f"{run_stamp.as_filename_component()}-{index:04d}.msg"
        )
    return apel_staging_message_path(output_root, when, run_stamp, index)


def export_apel_daily(output_root: Path, when: datetime, config: ApelConfig, run_stamp: RunStamp) -> ApelDailyExportResult:
    if not config.allowed_schedds:
        raise ValueError("APEL export requires apel.allowed_schedds to be configured and non-empty")

    input_jobs_file = derived_daily_jobs_file(output_root, when)
    jobs = load_daily_jobs(output_root, when)
    export_jobs, jobs_skipped_missing_schedd, skipped_by_schedd = filter_jobs_for_apel_export(
        jobs,
        config.allowed_schedds,
    )
    record_texts = [apel_record_text(job, config) for job in export_jobs]
    chunks = pack_apel_messages(
        record_texts,
        soft_limit_bytes=config.message_soft_limit_bytes,
        hard_limit_bytes=config.message_hard_limit_bytes,
    )

    files_written: list[dict[str, Any]] = []
    total_bytes = 0
    for index, chunk in enumerate(chunks, start=1):
        path = _staged_message_path(output_root, when, run_stamp, index, config)
        ensure_parent_dir(path)
        path.write_text(chunk.body, encoding="utf-8")
        files_written.append(
            {
                "path": str(path),
                "records": chunk.records,
                "bytes": chunk.bytes,
            }
        )
        total_bytes += chunk.bytes

    manifest = {
        "schema_version": 1,
        "record_type": "apel_export_manifest",
        "day": when.strftime("%Y-%m-%d"),
        "run_stamp": run_stamp.as_filename_component(),
        "allowed_schedds": list(config.allowed_schedds),
        "input_jobs_file": str(input_jobs_file),
        "jobs_seen": len(jobs),
        "jobs_exported": len(export_jobs),
        "jobs_skipped": len(jobs) - len(export_jobs),
        "jobs_skipped_missing_schedd": jobs_skipped_missing_schedd,
        "skipped_by_schedd": skipped_by_schedd,
        "messages_written": len(files_written),
        "total_bytes": total_bytes,
        "soft_limit_bytes": config.message_soft_limit_bytes,
        "hard_limit_bytes": config.message_hard_limit_bytes,
        "files_written": files_written,
    }
    manifest_path = apel_manifest_path(output_root, when, run_stamp)
    write_json(manifest_path, manifest)

    return ApelDailyExportResult(
        day=when.strftime("%Y-%m-%d"),
        input_jobs_file=input_jobs_file,
        jobs_seen=len(jobs),
        jobs_exported=len(export_jobs),
        jobs_skipped=len(jobs) - len(export_jobs),
        jobs_skipped_missing_schedd=jobs_skipped_missing_schedd,
        skipped_by_schedd=skipped_by_schedd,
        allowed_schedds=list(config.allowed_schedds),
        messages_written=len(files_written),
        total_bytes=total_bytes,
        files_written=files_written,
        manifest_path=manifest_path,
    )


def staged_apel_day_dir(output_root: Path, when: datetime, config: ApelConfig) -> Path:
    if config.staging_dir.is_absolute():
        return config.staging_dir / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")
    return apel_staging_day_dir(output_root, when)


def staged_apel_files(output_root: Path, when: datetime, config: ApelConfig) -> list[Path]:
    return sorted(staged_apel_day_dir(output_root, when, config).glob("*.msg"))
