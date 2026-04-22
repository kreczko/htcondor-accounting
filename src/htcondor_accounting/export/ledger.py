from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from htcondor_accounting.export.dirq import StagedMessageInfo
from htcondor_accounting.store.layout import (
    apel_ledger_resends_dir,
    apel_ledger_resend_marker_path,
    apel_ledger_sent_dir,
    apel_ledger_sent_marker_path,
    ensure_parent_dir,
)


@dataclass(frozen=True)
class LedgerPushResult:
    staged_path: Path
    outgoing_path: Path
    message_md5: str
    bytes: int
    records: int
    skipped_as_sent: bool
    resent: bool
    sent_marker_path: Path | None
    resend_marker_path: Path | None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sent_marker_path(output_root: Path, message_md5: str) -> Path:
    return apel_ledger_sent_marker_path(output_root, message_md5)


def resend_marker_path(output_root: Path, when: datetime, message_md5: str) -> Path:
    return apel_ledger_resend_marker_path(output_root, when, message_md5)


def sent_marker_exists(output_root: Path, message_md5: str) -> bool:
    return sent_marker_path(output_root, message_md5).exists()


def parse_run_stamp_from_staged_path(staged_path: Path) -> str | None:
    stem = staged_path.stem
    parts = stem.rsplit("-", 1)
    if len(parts) != 2:
        return None
    return parts[0] or None


def build_sent_marker(
    *,
    day: str,
    info: StagedMessageInfo,
    outgoing_path: Path,
    run_stamp: str | None,
    first_pushed_at: datetime,
    manifest_path: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "record_type": "apel_sent_marker",
        "message_md5": info.message_md5,
        "day": day,
        "staged_path": str(info.path),
        "outgoing_path": str(outgoing_path),
        "records": info.records,
        "bytes": info.bytes,
        "first_pushed_at": first_pushed_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_stamp": run_stamp,
    }
    if manifest_path is not None:
        payload["manifest_path"] = manifest_path
    return payload


def build_resend_marker(
    *,
    day: str,
    info: StagedMessageInfo,
    outgoing_path: Path,
    run_stamp: str | None,
    resent_at: datetime,
    reason: str | None = None,
    manifest_path: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "record_type": "apel_resend_event",
        "message_md5": info.message_md5,
        "day": day,
        "staged_path": str(info.path),
        "outgoing_path": str(outgoing_path),
        "records": info.records,
        "bytes": info.bytes,
        "resent_at": resent_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_stamp": run_stamp,
    }
    if reason:
        payload["reason"] = reason
    if manifest_path is not None:
        payload["manifest_path"] = manifest_path
    return payload


def write_sent_marker(
    output_root: Path,
    *,
    day: str,
    info: StagedMessageInfo,
    outgoing_path: Path,
    run_stamp: str | None,
    pushed_at: datetime | None = None,
    manifest_path: str | None = None,
) -> Path:
    when = pushed_at or utc_now()
    path = sent_marker_path(output_root, info.message_md5)
    if path.exists():
        return path
    _write_json(
        path,
        build_sent_marker(
            day=day,
            info=info,
            outgoing_path=outgoing_path,
            run_stamp=run_stamp,
            first_pushed_at=when,
            manifest_path=manifest_path,
        ),
    )
    return path


def write_resend_marker(
    output_root: Path,
    *,
    day: str,
    info: StagedMessageInfo,
    outgoing_path: Path,
    run_stamp: str | None,
    resent_at: datetime | None = None,
    reason: str | None = None,
    manifest_path: str | None = None,
) -> Path:
    when = resent_at or utc_now()
    path = resend_marker_path(output_root, when, info.message_md5)
    _write_json(
        path,
        build_resend_marker(
            day=day,
            info=info,
            outgoing_path=outgoing_path,
            run_stamp=run_stamp,
            resent_at=when,
            reason=reason,
            manifest_path=manifest_path,
        ),
    )
    return path


def load_ledger_entries(output_root: Path, include_resends: bool = False) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    sent_root = apel_ledger_sent_dir(output_root)
    if sent_root.exists():
        for path in sorted(sent_root.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["_ledger_path"] = str(path)
            entries.append(payload)

    if include_resends:
        resend_root = apel_ledger_resends_dir(output_root)
        if resend_root.exists():
            for path in sorted(resend_root.glob("*.json")):
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["_ledger_path"] = str(path)
                entries.append(payload)

    return entries
