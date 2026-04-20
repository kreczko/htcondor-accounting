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
