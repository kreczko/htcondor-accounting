import json
from pathlib import Path

from htcondor_accounting.report.periods import iso_week_parts, month_period, parse_day, week_period, year_period
from htcondor_accounting.report.rollup import (
    aggregate_rollup_summary,
    derive_all_rollups,
    derive_all_time,
    derive_monthly,
    derive_weekly,
    derive_yearly,
    group_daily_summaries_monthly,
    group_daily_summaries_weekly,
    group_daily_summaries_yearly,
    load_all_daily_summaries,
)


def _write_daily_summary(root: Path, day: str, *, users: int, vos: int, wall: int, cpu_user: int, cpu_sys: int, scaled_wall: float, scaled_cpu: float, input_files: int = 1, input_records: int = 1, unique_records: int = 1, duplicate_records: int = 0) -> Path:
    year, month, day_number = day.split("-")
    path = root / "derived" / "daily" / year / month / day_number / "summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "day": day,
        "input_files": input_files,
        "input_records": input_records,
        "unique_records": unique_records,
        "duplicate_records": duplicate_records,
        "users": users,
        "vos": vos,
        "wall_seconds": wall,
        "cpu_user_seconds": cpu_user,
        "cpu_sys_seconds": cpu_sys,
        "cpu_total_seconds": cpu_user + cpu_sys,
        "scaled_wall_seconds": scaled_wall,
        "scaled_cpu_seconds": scaled_cpu,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_period_helpers() -> None:
    day = parse_day("2026-04-13")
    assert iso_week_parts(day) == (2026, 16)
    assert week_period(day) == "2026-W16"
    assert month_period(day) == "2026-04"
    assert year_period(day) == "2026"


def test_rollup_groupings(tmp_path: Path) -> None:
    _write_daily_summary(tmp_path, "2026-04-13", users=1, vos=1, wall=10, cpu_user=5, cpu_sys=1, scaled_wall=10.0, scaled_cpu=6.0)
    _write_daily_summary(tmp_path, "2026-04-14", users=2, vos=2, wall=20, cpu_user=7, cpu_sys=3, scaled_wall=20.0, scaled_cpu=10.0)
    _write_daily_summary(tmp_path, "2026-05-01", users=3, vos=1, wall=30, cpu_user=8, cpu_sys=2, scaled_wall=30.0, scaled_cpu=10.0)
    _write_daily_summary(tmp_path, "2027-01-01", users=4, vos=2, wall=40, cpu_user=9, cpu_sys=1, scaled_wall=40.0, scaled_cpu=10.0)

    records = load_all_daily_summaries(tmp_path)

    weekly = group_daily_summaries_weekly(records)
    monthly = group_daily_summaries_monthly(records)
    yearly = group_daily_summaries_yearly(records)

    assert list(weekly.keys()) == [(2026, 16), (2026, 18), (2026, 53)]
    assert len(weekly[(2026, 16)]) == 2
    assert list(monthly.keys()) == [(2026, 4), (2026, 5), (2027, 1)]
    assert list(yearly.keys()) == [2026, 2027]


def test_aggregate_rollup_summary_sums_totals_deterministically(tmp_path: Path) -> None:
    second = _write_daily_summary(tmp_path, "2026-04-14", users=2, vos=2, wall=20, cpu_user=7, cpu_sys=3, scaled_wall=22.0, scaled_cpu=11.0, input_files=2, input_records=2, unique_records=2)
    first = _write_daily_summary(tmp_path, "2026-04-13", users=1, vos=1, wall=10, cpu_user=5, cpu_sys=1, scaled_wall=10.0, scaled_cpu=6.0)

    records = load_all_daily_summaries(tmp_path)
    summary = aggregate_rollup_summary("weekly", "2026-W16", records)

    assert summary["days_included"] == 2
    assert summary["source_daily_summaries"] == [str(first), str(second)]
    assert summary["input_files"] == 3
    assert summary["input_records"] == 3
    assert summary["unique_records"] == 3
    assert summary["duplicate_records"] == 0
    assert summary["user_days_total"] == 3
    assert summary["vo_days_total"] == 3
    assert summary["wall_seconds"] == 30
    assert summary["cpu_user_seconds"] == 12
    assert summary["cpu_sys_seconds"] == 4
    assert summary["cpu_total_seconds"] == 16
    assert summary["scaled_wall_seconds"] == 32.0
    assert summary["scaled_cpu_seconds"] == 17.0
    assert summary["first_day"] == "2026-04-13"
    assert summary["last_day"] == "2026-04-14"


def test_rollup_output_paths_and_generation(tmp_path: Path) -> None:
    _write_daily_summary(tmp_path, "2026-04-13", users=1, vos=1, wall=10, cpu_user=5, cpu_sys=1, scaled_wall=10.0, scaled_cpu=6.0)
    _write_daily_summary(tmp_path, "2026-04-14", users=2, vos=2, wall=20, cpu_user=7, cpu_sys=3, scaled_wall=20.0, scaled_cpu=10.0)
    _write_daily_summary(tmp_path, "2026-05-01", users=3, vos=1, wall=30, cpu_user=8, cpu_sys=2, scaled_wall=30.0, scaled_cpu=10.0)
    _write_daily_summary(tmp_path, "2027-01-01", users=4, vos=2, wall=40, cpu_user=9, cpu_sys=1, scaled_wall=40.0, scaled_cpu=10.0)

    weekly = derive_weekly(tmp_path, year=2026, week=16)
    monthly = derive_monthly(tmp_path, year=2026, month=4)
    yearly = derive_yearly(tmp_path, year=2026)
    all_time = derive_all_time(tmp_path)

    assert weekly.output_path == tmp_path / "derived" / "weekly" / "2026" / "week-16" / "summary.json"
    assert monthly.output_path == tmp_path / "derived" / "monthly" / "2026" / "04" / "summary.json"
    assert yearly.output_path == tmp_path / "derived" / "yearly" / "2026" / "summary.json"
    assert all_time.output_path == tmp_path / "derived" / "all-time" / "summary.json"
    assert weekly.summary["period"] == "2026-W16"
    assert weekly.summary["unique_records"] == 2
    assert monthly.summary["unique_records"] == 2
    assert yearly.summary["unique_records"] == 3
    assert all_time.summary["unique_records"] == 4


def test_derive_all_rollups_regenerates_all_levels(tmp_path: Path) -> None:
    _write_daily_summary(tmp_path, "2026-04-13", users=1, vos=1, wall=10, cpu_user=5, cpu_sys=1, scaled_wall=10.0, scaled_cpu=6.0)
    _write_daily_summary(tmp_path, "2026-04-14", users=2, vos=2, wall=20, cpu_user=7, cpu_sys=3, scaled_wall=20.0, scaled_cpu=10.0)
    _write_daily_summary(tmp_path, "2026-05-01", users=3, vos=1, wall=30, cpu_user=8, cpu_sys=2, scaled_wall=30.0, scaled_cpu=10.0)

    results = derive_all_rollups(tmp_path)
    result_keys = {(result.period_type, result.period) for result in results}

    assert ("weekly", "2026-W16") in result_keys
    assert ("weekly", "2026-W18") in result_keys
    assert ("monthly", "2026-04") in result_keys
    assert ("monthly", "2026-05") in result_keys
    assert ("yearly", "2026") in result_keys
    assert ("all-time", "all-time") in result_keys
    assert (tmp_path / "derived" / "all-time" / "summary.json").exists()
