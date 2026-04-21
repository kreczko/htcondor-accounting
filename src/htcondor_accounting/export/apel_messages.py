from __future__ import annotations

import json
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
    apel_staging_message_path,
    derived_daily_jobs_file,
    ensure_parent_dir,
)


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
    messages_written: int
    total_bytes: int
    files_written: list[dict[str, Any]]
    manifest_path: Path


def load_daily_jobs(root: Path, when: datetime) -> list[dict[str, Any]]:
    path = derived_daily_jobs_file(root, when)
    return list(read_jsonl_zst(path))


def pack_apel_messages(records: list[str], soft_limit_bytes: int, hard_limit_bytes: int) -> list[ApelMessageChunk]:
    chunks: list[ApelMessageChunk] = []
    current_records: list[str] = []
    current_bytes = 0

    for record in records:
        record_bytes = len(record.encode("utf-8"))
        if record_bytes > hard_limit_bytes:
            raise ValueError(
                f"Single APEL record is {record_bytes} bytes, exceeding hard limit {hard_limit_bytes} bytes"
            )

        if current_records and current_bytes + record_bytes > soft_limit_bytes:
            body = "".join(current_records)
            chunks.append(ApelMessageChunk(body=body, records=len(current_records), bytes=len(body.encode("utf-8"))))
            current_records = []
            current_bytes = 0

        if current_bytes + record_bytes > hard_limit_bytes:
            raise ValueError(
                f"APEL message would exceed hard limit {hard_limit_bytes} bytes while adding next record"
            )

        current_records.append(record)
        current_bytes += record_bytes

    if current_records:
        body = "".join(current_records)
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
    input_jobs_file = derived_daily_jobs_file(output_root, when)
    jobs = load_daily_jobs(output_root, when)
    record_texts = [apel_record_text(job, config) for job in jobs]
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
        "input_jobs_file": str(input_jobs_file),
        "jobs_seen": len(jobs),
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
        messages_written=len(files_written),
        total_bytes=total_bytes,
        files_written=files_written,
        manifest_path=manifest_path,
    )
