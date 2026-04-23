from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from htcondor_accounting.export.ledger import load_ledger_entries
from htcondor_accounting.report.jobs import filter_jobs_by_schedd
from htcondor_accounting.store.jsonl import read_jsonl_zst
from htcondor_accounting.store.layout import (
    apel_manifest_day_dir,
    apel_staging_day_dir,
    canonical_day_dir,
    derived_daily_duplicates_path,
    derived_daily_jobs_file,
    derived_daily_summary_path,
    raw_history_day_dir,
)


def _json_load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _count_jsonl_records(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        total += sum(1 for _ in read_jsonl_zst(path))
    return total


def _collect_jsonl_records(paths: list[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        records.extend(read_jsonl_zst(path))
    return records


def _best_accounting_group(job: dict[str, Any]) -> str:
    for key in ("acct_group", "accounting_group", "acct_group_user", "route_name"):
        value = job.get(key)
        if value not in (None, "", "-"):
            return str(value)
    return "-"


@dataclass(frozen=True)
class ValidationResult:
    payload: dict[str, Any]


def validate_day(output_root: Path, day: datetime, schedd_name: str | None = None) -> ValidationResult:
    day_str = day.strftime("%Y-%m-%d")
    raw_paths = sorted(raw_history_day_dir(output_root, day).glob("*.jsonl.zst"))
    canonical_paths = sorted(canonical_day_dir(output_root, day).glob("*.jsonl.zst"))
    raw_records = _count_jsonl_records(raw_paths)
    canonical_records = _collect_jsonl_records(canonical_paths)

    derived_jobs_path = derived_daily_jobs_file(output_root, day)
    derived_jobs = list(read_jsonl_zst(derived_jobs_path)) if derived_jobs_path.exists() else []
    if schedd_name is not None:
        canonical_records = [record for record in canonical_records if record.get("source", {}).get("schedd") == schedd_name]
        derived_jobs = filter_jobs_by_schedd(derived_jobs, schedd_name)

    derived_summary_path = derived_daily_summary_path(output_root, day)
    derived_duplicates_path = derived_daily_duplicates_path(output_root, day)
    derived_summary = _json_load(derived_summary_path) if derived_summary_path.exists() else None
    derived_duplicates = _json_load(derived_duplicates_path) if derived_duplicates_path.exists() else None

    apel_manifest_paths = sorted(apel_manifest_day_dir(output_root, day).glob("*.json"))
    apel_manifests = [_json_load(path) for path in apel_manifest_paths]
    staged_paths = sorted(apel_staging_day_dir(output_root, day).glob("*.msg"))

    sent_entries = [
        entry
        for entry in load_ledger_entries(output_root, include_resends=False)
        if entry.get("day") == day_str
    ]
    resend_entries = [
        entry
        for entry in load_ledger_entries(output_root, include_resends=True)
        if entry.get("day") == day_str and entry.get("record_type") == "apel_resend_event"
    ]

    missing_vo = sum(1 for job in derived_jobs if job.get("vo") in (None, "", "-"))
    missing_fqan = sum(1 for job in derived_jobs if job.get("fqan") in (None, "", "-"))
    missing_accounting_group = sum(1 for job in derived_jobs if _best_accounting_group(job) == "-")
    auth_method_counts = {
        "scitoken": sum(1 for job in derived_jobs if job.get("auth_method") == "scitoken"),
        "x509": sum(1 for job in derived_jobs if job.get("auth_method") == "x509"),
        "local": sum(1 for job in derived_jobs if job.get("auth_method") == "local"),
    }
    unresolved = sum(1 for job in derived_jobs if job.get("resolution_method") == "unresolved")

    staged_manifest_records = sum(
        int(file_entry.get("records") or 0)
        for manifest in apel_manifests
        for file_entry in manifest.get("files_written", [])
    )
    staged_manifest_messages = sum(int(manifest.get("messages_written") or 0) for manifest in apel_manifests)
    staged_manifest_bytes = sum(int(manifest.get("total_bytes") or 0) for manifest in apel_manifests)

    warnings: list[str] = []
    errors: list[str] = []

    derived_unique = len(derived_jobs)
    duplicate_records = int((derived_duplicates or {}).get("duplicate_records") or 0)

    if derived_unique > len(canonical_records):
        errors.append("Derived unique jobs exceed canonical records.")

    if schedd_name is None and derived_duplicates is not None:
        if len(canonical_records) - derived_unique != duplicate_records:
            warnings.append("Canonical-to-derived difference does not match duplicates.json.")

    if apel_manifests and staged_manifest_records != derived_unique:
        warnings.append("APEL staged record count does not match derived unique jobs.")

    if len(sent_entries) > len(staged_paths):
        errors.append("Sent ledger count exceeds staged message files present.")
    elif len(sent_entries) < len(staged_paths):
        warnings.append("Not all staged APEL messages have been pushed.")

    if apel_manifests and not sent_entries:
        warnings.append("APEL export manifest exists but no push is recorded in the sent ledger.")

    missing_staged_for_sent = [
        entry["message_md5"]
        for entry in sent_entries
        if not Path(str(entry.get("staged_path") or "")).exists()
    ]
    if missing_staged_for_sent:
        warnings.append("Some sent ledger entries reference missing staged files.")

    payload = {
        "day": day_str,
        "schedd": schedd_name,
        "files": {
            "raw_history_files": len(raw_paths),
            "canonical_files": len(canonical_paths),
            "derived_jobs_file": str(derived_jobs_path) if derived_jobs_path.exists() else None,
            "derived_summary_path": str(derived_summary_path) if derived_summary_path.exists() else None,
            "derived_duplicates_path": str(derived_duplicates_path) if derived_duplicates_path.exists() else None,
            "apel_manifest_files": len(apel_manifest_paths),
            "apel_staged_files": len(staged_paths),
        },
        "counts": {
            "raw_history_records": raw_records,
            "canonical_records": len(canonical_records),
            "derived_unique_jobs": derived_unique,
            "duplicate_jobs": duplicate_records,
            "apel_staged_messages": len(staged_paths),
            "apel_staged_records": staged_manifest_records,
            "apel_manifest_messages_written": staged_manifest_messages,
            "apel_manifest_total_bytes": staged_manifest_bytes,
            "apel_pushed_messages": len(sent_entries),
            "apel_sent_ledger_entries": len(sent_entries),
            "apel_resend_events": len(resend_entries),
        },
        "identity_quality": {
            "missing_resolved_vo": missing_vo,
            "missing_resolved_fqan": missing_fqan,
            "missing_accounting_group": missing_accounting_group,
            "auth_method_counts": auth_method_counts,
            "unresolved_jobs": unresolved,
        },
        "apel": {
            "manifest_exists": bool(apel_manifests),
            "staged_files_present": len(staged_paths),
            "sent_entries": len(sent_entries),
            "resend_events": len(resend_entries),
            "missing_staged_for_sent": missing_staged_for_sent,
        },
        "warnings": warnings,
        "errors": errors,
    }

    if derived_summary is not None and schedd_name is None:
        payload["counts"]["derived_summary_unique_records"] = int(derived_summary.get("unique_records") or 0)
        payload["counts"]["derived_summary_duplicate_records"] = int(derived_summary.get("duplicate_records") or 0)

    return ValidationResult(payload=payload)
