from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class RunStamp:
    """A timestamp used for naming extracted accounting batches."""

    timestamp: datetime


    @classmethod
    def now(cls) -> "RunStamp":
        return cls(datetime.now(timezone.utc))


    def as_filename_component(self) -> str:
        return self.timestamp.strftime("%Y%m%dT%H%M%SZ")


def canonical_day_dir(root: Path, when: datetime) -> Path:
    """
    Return the canonical daily directory path.


    Example:
        archive/canonical/2026/04/17
    """
    return root / "canonical" / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")


def derived_daily_dir(root: Path, when: datetime) -> Path:
    """
    Return the derived daily directory path.

    Example:
        archive/derived/daily/2026/04/17
    """
    return root / "derived" / "daily" / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")


def derived_daily_jobs_file(root: Path, when: datetime) -> Path:
    """Return the derived daily per-job file path."""
    return derived_daily_dir(root, when) / "jobs.jsonl.zst"


def derived_daily_summary_path(root: Path, when: datetime) -> Path:
    """Return the derived daily summary JSON path."""
    return derived_daily_dir(root, when) / "summary.json"


def derived_daily_duplicates_path(root: Path, when: datetime) -> Path:
    """Return the derived daily duplicates JSON path."""
    return derived_daily_dir(root, when) / "duplicates.json"


def derived_weekly_dir(root: Path, year: int, week: int) -> Path:
    """Return the derived weekly directory path."""
    return root / "derived" / "weekly" / f"{year:04d}" / f"week-{week:02d}"


def derived_weekly_summary_path(root: Path, year: int, week: int) -> Path:
    """Return the derived weekly summary JSON path."""
    return derived_weekly_dir(root, year, week) / "summary.json"


def derived_monthly_dir(root: Path, year: int, month: int) -> Path:
    """Return the derived monthly directory path."""
    return root / "derived" / "monthly" / f"{year:04d}" / f"{month:02d}"


def derived_monthly_summary_path(root: Path, year: int, month: int) -> Path:
    """Return the derived monthly summary JSON path."""
    return derived_monthly_dir(root, year, month) / "summary.json"


def derived_yearly_dir(root: Path, year: int) -> Path:
    """Return the derived yearly directory path."""
    return root / "derived" / "yearly" / f"{year:04d}"


def derived_yearly_summary_path(root: Path, year: int) -> Path:
    """Return the derived yearly summary JSON path."""
    return derived_yearly_dir(root, year) / "summary.json"


def derived_all_time_dir(root: Path) -> Path:
    """Return the derived all-time directory path."""
    return root / "derived" / "all-time"


def derived_all_time_summary_path(root: Path) -> Path:
    """Return the derived all-time summary JSON path."""
    return derived_all_time_dir(root) / "summary.json"


def reports_monthly_dir(root: Path, year: int, month: int) -> Path:
    """Return the internal monthly reports directory path."""
    return root / "reports" / "monthly" / f"{year:04d}" / f"{month:02d}"


def reports_monthly_users_csv_path(root: Path, year: int, month: int) -> Path:
    """Return the monthly user usage CSV path."""
    return reports_monthly_dir(root, year, month) / "users.csv"


def reports_monthly_vos_csv_path(root: Path, year: int, month: int) -> Path:
    """Return the monthly VO usage CSV path."""
    return reports_monthly_dir(root, year, month) / "vos.csv"


def reports_monthly_accounting_groups_csv_path(root: Path, year: int, month: int) -> Path:
    """Return the monthly accounting-group usage CSV path."""
    return reports_monthly_dir(root, year, month) / "accounting_groups.csv"


def reports_monthly_schedds_csv_path(root: Path, year: int, month: int) -> Path:
    """Return the monthly schedd usage CSV path."""
    return reports_monthly_dir(root, year, month) / "schedds.csv"


def reports_monthly_summary_path(root: Path, year: int, month: int) -> Path:
    """Return the monthly reporting summary JSON path."""
    return reports_monthly_dir(root, year, month) / "summary.json"


def reports_monthly_index_path(root: Path, year: int, month: int) -> Path:
    """Return the monthly report HTML index path."""
    return reports_monthly_dir(root, year, month) / "index.html"


def apel_staging_day_dir(root: Path, when: datetime) -> Path:
    """Return the APEL staging daily directory path under the output root."""
    return root / "apel" / "staging" / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")


def apel_staging_message_path(root: Path, when: datetime, run_stamp: RunStamp, index: int) -> Path:
    """Return a staged APEL message file path."""
    return apel_staging_day_dir(root, when) / f"{run_stamp.as_filename_component()}-{index:04d}.msg"


def apel_manifest_day_dir(root: Path, when: datetime) -> Path:
    """Return the APEL export manifest daily directory path."""
    return root / "apel" / "manifests" / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")


def apel_manifest_path(root: Path, when: datetime, run_stamp: RunStamp) -> Path:
    """Return the APEL export manifest JSON path."""
    return apel_manifest_day_dir(root, when) / f"{run_stamp.as_filename_component()}.json"


def apel_ledger_sent_dir(root: Path) -> Path:
    """Return the APEL sent-ledger directory."""
    return root / "apel" / "ledger" / "sent"


def apel_ledger_sent_marker_path(root: Path, message_md5: str) -> Path:
    """Return the sent marker path for a message MD5."""
    return apel_ledger_sent_dir(root) / f"{message_md5}.json"


def apel_ledger_resends_dir(root: Path) -> Path:
    """Return the APEL resend-ledger directory."""
    return root / "apel" / "ledger" / "resends"


def apel_ledger_resend_marker_path(root: Path, timestamp: datetime, message_md5: str) -> Path:
    """Return the resend event marker path for a message MD5 and timestamp."""
    stamp = timestamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return apel_ledger_resends_dir(root) / f"{stamp}-{message_md5}.json"


def raw_history_day_dir(root: Path, when: datetime) -> Path:
    """Return the raw history snapshot daily directory path."""
    return root / "raw-history" / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")


def raw_history_run_file(root: Path, when: datetime, source: str, run_stamp: RunStamp) -> Path:
    """Return the raw history snapshot file path."""
    return raw_history_day_dir(root, when) / f"{source}-{run_stamp.as_filename_component()}.jsonl.zst"


def manifest_day_dir(root: Path, when: datetime) -> Path:
    """
    Return the manifest daily directory path.

    Example:
        archive/manifests/2026/04/21
    """
    return root / "manifests" / when.strftime("%Y") / when.strftime("%m") / when.strftime("%d")


def manifest_file(root: Path, run_stamp: RunStamp) -> Path:
    """Return the manifest JSON path for an extraction run stamp."""
    return manifest_day_dir(root, run_stamp.timestamp) / f"{run_stamp.as_filename_component()}.json"


def canonical_run_file(root: Path, when: datetime, source: str, run_stamp: RunStamp) -> Path:
    """
    Return the path for a canonical run file.


    Example:
        archive/canonical/2026/04/17/ce02-20260417T120000Z.jsonl.zst
    """
    day_dir = canonical_day_dir(root, when)
    filename = f"{source}-{run_stamp.as_filename_component()}.jsonl.zst"
    return day_dir / filename


def ensure_parent_dir(path: Path) -> None:
    """Ensure the parent directory of a file path exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
