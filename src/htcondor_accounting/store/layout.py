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
