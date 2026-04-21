from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from htcondor_accounting.store.jsonl import read_jsonl_zst


@dataclass(frozen=True)
class DeduplicationResult:
    input_files: int
    input_records: int
    unique_records: list[dict[str, Any]]
    duplicate_records: int
    duplicate_sample: list[str]


def read_canonical_records(paths: Iterable[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        records.extend(read_jsonl_zst(path))
    return records


def deduplicate_canonical_records(
    records: Iterable[dict[str, Any]],
    sample_limit: int = 20,
) -> DeduplicationResult:
    seen_job_ids: set[str] = set()
    unique_records: list[dict[str, Any]] = []
    duplicate_sample: list[str] = []
    input_records = 0
    duplicate_records = 0

    for record in records:
        input_records += 1
        global_job_id = str(record.get("job", {}).get("global_job_id") or "<missing-global-job-id>")
        if global_job_id in seen_job_ids:
            duplicate_records += 1
            if len(duplicate_sample) < sample_limit and global_job_id not in duplicate_sample:
                duplicate_sample.append(global_job_id)
            continue

        seen_job_ids.add(global_job_id)
        unique_records.append(record)

    return DeduplicationResult(
        input_files=0,
        input_records=input_records,
        unique_records=unique_records,
        duplicate_records=duplicate_records,
        duplicate_sample=duplicate_sample,
    )
