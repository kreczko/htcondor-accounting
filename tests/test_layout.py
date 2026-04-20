from datetime import datetime, timezone
from pathlib import Path

from htcondor_accounting.store.layout import RunStamp, canonical_day_dir, canonical_run_file

def test_canonical_day_dir() -> None:
    root = Path("archive")
    when = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)


    assert canonical_day_dir(root, when) == Path("archive/canonical/2026/04/17")

def test_canonical_run_file() -> None:
    root = Path("archive")
    when = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)
    run_stamp = RunStamp(datetime(2026, 4, 17, 12, 34, 56, tzinfo=timezone.utc))

    path = canonical_run_file(root, when, "ce02", run_stamp)

    assert path == Path("archive/canonical/2026/04/17/ce02-20260417T123456Z.jsonl.zst")
