from datetime import datetime, timezone
from pathlib import Path

from htcondor_accounting.store.layout import (
    RunStamp,
    apel_manifest_path,
    apel_staging_message_path,
    canonical_day_dir,
    canonical_run_file,
    derived_all_time_summary_path,
    derived_daily_dir,
    derived_daily_duplicates_path,
    derived_daily_jobs_file,
    derived_daily_summary_path,
    derived_monthly_summary_path,
    derived_weekly_summary_path,
    derived_yearly_summary_path,
    manifest_day_dir,
    manifest_file,
)

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


def test_derived_daily_layout_paths() -> None:
    root = Path("archive")
    when = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)

    assert derived_daily_dir(root, when) == Path("archive/derived/daily/2026/04/17")
    assert derived_daily_jobs_file(root, when) == Path("archive/derived/daily/2026/04/17/jobs.jsonl.zst")
    assert derived_daily_summary_path(root, when) == Path("archive/derived/daily/2026/04/17/summary.json")
    assert derived_daily_duplicates_path(root, when) == Path("archive/derived/daily/2026/04/17/duplicates.json")


def test_manifest_layout_paths() -> None:
    root = Path("archive")
    run_stamp = RunStamp(datetime(2026, 4, 21, 12, 30, 38, tzinfo=timezone.utc))

    assert manifest_day_dir(root, run_stamp.timestamp) == Path("archive/manifests/2026/04/21")
    assert manifest_file(root, run_stamp) == Path("archive/manifests/2026/04/21/20260421T123038Z.json")


def test_rollup_layout_paths() -> None:
    root = Path("archive")

    assert derived_weekly_summary_path(root, 2026, 16) == Path("archive/derived/weekly/2026/week-16/summary.json")
    assert derived_monthly_summary_path(root, 2026, 4) == Path("archive/derived/monthly/2026/04/summary.json")
    assert derived_yearly_summary_path(root, 2026) == Path("archive/derived/yearly/2026/summary.json")
    assert derived_all_time_summary_path(root) == Path("archive/derived/all-time/summary.json")


def test_apel_layout_paths() -> None:
    root = Path("archive")
    when = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)
    run_stamp = RunStamp(datetime(2026, 4, 21, 12, 30, 38, tzinfo=timezone.utc))

    assert apel_staging_message_path(root, when, run_stamp, 1) == Path("archive/apel/staging/2026/04/17/20260421T123038Z-0001.msg")
    assert apel_manifest_path(root, when, run_stamp) == Path("archive/apel/manifests/2026/04/17/20260421T123038Z.json")
