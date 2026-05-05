import json
from datetime import datetime, timezone
from pathlib import Path

from htcondor_accounting.export.csv import write_csv_rows
from htcondor_accounting.models.reporting import UsageGroupRow
from htcondor_accounting.render.html import (
    build_monthly_report_context,
    format_gb,
    format_hours,
    format_scaled_pair,
    render_monthly_report_html,
    render_report_html,
)
from htcondor_accounting.render.plots import (
    bucket_wall_hours_by_accounting_group,
    write_wall_hours_by_accounting_group_plot,
)
from htcondor_accounting.report.builder import write_report_set
from htcondor_accounting.report.jobs import (
    filter_jobs_by_schedd,
    group_jobs_by_accounting_group,
    group_jobs_by_user,
    group_jobs_by_vo,
    iter_monthly_job_paths,
    load_monthly_jobs,
    monthly_schedd_names,
)
from htcondor_accounting.report.summary import build_monthly_report_summary
from htcondor_accounting.store.jsonl import write_jsonl_zst


def _job(
    global_job_id: str,
    *,
    day: str,
    user: str,
    vo: str,
    wall_seconds: int,
    cpu_user_seconds: int,
    cpu_sys_seconds: int,
    processors: int,
    memory_real_kb: int,
    memory_virtual_kb: int,
    scale_factor: float = 1.0,
    schedd: str = "schedd-a.example",
    acct_group: str | None = None,
    acct_group_user: str | None = None,
    accounting_group: str | None = None,
    route_name: str | None = None,
    end_time: int = 2,
) -> dict:
    return {
        "schema_version": 1,
        "record_type": "report_job",
        "site_name": "TEST-SITE",
        "global_job_id": global_job_id,
        "owner": user,
        "local_user": user,
        "vo": vo,
        "vo_group": f"/{vo}",
        "vo_role": None,
        "auth_method": "scitoken",
        "start_time": 1,
        "end_time": end_time,
        "wall_seconds": wall_seconds,
        "cpu_user_seconds": cpu_user_seconds,
        "cpu_sys_seconds": cpu_sys_seconds,
        "cpu_total_seconds": cpu_user_seconds + cpu_sys_seconds,
        "processors": processors,
        "memory_real_kb": memory_real_kb,
        "memory_virtual_kb": memory_virtual_kb,
        "scale_factor": scale_factor,
        "benchmark_type": "hepscore23",
        "source_schedd": schedd,
        "acct_group": acct_group,
        "acct_group_user": acct_group_user,
        "accounting_group": accounting_group,
        "route_name": route_name,
        "day": day,
    }


def _write_daily_jobs(root: Path, day: str, jobs: list[dict]) -> None:
    year, month, day_number = day.split("-")
    path = root / "derived" / "daily" / year / month / day_number / "jobs.jsonl.zst"
    write_jsonl_zst(path, jobs)


def _timestamp(year: int, month: int, day: int, hour: int) -> int:
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp())


def test_monthly_grouping_by_user_and_vo(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-2", day="2026-04-17", user="alice", vo="atlas", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=200, memory_virtual_kb=400),
            _job("job-3", day="2026-04-17", user="bob", vo="cms", wall_seconds=5, cpu_user_seconds=2, cpu_sys_seconds=1, processors=4, memory_real_kb=150, memory_virtual_kb=250),
        ],
    )
    _write_daily_jobs(
        tmp_path,
        "2026-04-18",
        [
            _job("job-4", day="2026-04-18", user="alice", vo="atlas", wall_seconds=7, cpu_user_seconds=3, cpu_sys_seconds=1, processors=1, memory_real_kb=120, memory_virtual_kb=320, scale_factor=2.0),
            _job("job-5", day="2026-04-18", user="charlie", vo="cms", wall_seconds=8, cpu_user_seconds=3, cpu_sys_seconds=1, processors=2, memory_real_kb=180, memory_virtual_kb=260),
        ],
    )

    jobs = load_monthly_jobs(tmp_path, 2026, 4)
    user_rows = group_jobs_by_user(jobs)
    vo_rows = group_jobs_by_vo(jobs)

    assert len(iter_monthly_job_paths(tmp_path, 2026, 4)) == 2
    assert len(user_rows) == 3
    assert len(vo_rows) == 2

    alice = next(row for row in user_rows if row.group_key == "alice")
    assert alice.jobs == 3
    assert alice.vo == "atlas"
    assert alice.wall_seconds == 37
    assert alice.cpu_total_seconds == 17
    assert alice.scaled_wall_seconds == 44.0
    assert alice.scaled_cpu_seconds == 21.0
    assert alice.avg_processors == 4 / 3
    assert alice.max_processors == 2
    assert alice.memory_real_kb_max == 200
    assert alice.memory_virtual_kb_max == 400

    atlas = next(row for row in vo_rows if row.group_key == "atlas")
    assert atlas.jobs == 3
    assert atlas.wall_seconds == 37
    assert atlas.users == 1

    cms = next(row for row in vo_rows if row.group_key == "cms")
    assert cms.users == 2


def test_user_projection_vo_marker_handles_multiple_and_missing_vo(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-2", day="2026-04-17", user="alice", vo="cms", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-3", day="2026-04-17", user="bob", vo="-", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
        ],
    )
    rows = group_jobs_by_user(load_monthly_jobs(tmp_path, 2026, 4))
    alice = next(row for row in rows if row.group_key == "alice")
    bob = next(row for row in rows if row.group_key == "bob")
    assert alice.vo == "MULTIPLE"
    assert bob.vo == "-"


def test_accounting_group_projection_is_deterministic(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300, acct_group="group-a"),
            _job("job-2", day="2026-04-17", user="bob", vo="atlas", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=200, memory_virtual_kb=400, accounting_group="group-b"),
            _job("job-3", day="2026-04-17", user="charlie", vo="cms", wall_seconds=5, cpu_user_seconds=2, cpu_sys_seconds=1, processors=4, memory_real_kb=150, memory_virtual_kb=250, acct_group_user="group-c"),
            _job("job-4", day="2026-04-17", user="dana", vo="cms", wall_seconds=7, cpu_user_seconds=3, cpu_sys_seconds=1, processors=8, memory_real_kb=500, memory_virtual_kb=700, route_name="route-d"),
        ],
    )
    rows = group_jobs_by_accounting_group(load_monthly_jobs(tmp_path, 2026, 4))
    assert [row.group_key for row in rows] == ["group-a", "group-b", "group-c", "route-d"]
    group_d = next(row for row in rows if row.group_key == "route-d")
    assert group_d.vo == "cms"
    assert group_d.users == 1
    assert group_d.avg_processors == 8.0
    assert group_d.max_processors == 8
    assert group_d.memory_real_kb_max == 500


def test_monthly_jobs_can_be_filtered_by_schedd(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300, schedd="schedd-a.example"),
            _job("job-2", day="2026-04-17", user="bob", vo="cms", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=200, memory_virtual_kb=400, schedd="schedd-b.example"),
        ],
    )
    jobs = load_monthly_jobs(tmp_path, 2026, 4)

    assert monthly_schedd_names(jobs) == ["schedd-a.example", "schedd-b.example"]
    filtered = filter_jobs_by_schedd(jobs, "schedd-b.example")
    assert len(filtered) == 1
    assert filtered[0]["source_schedd"] == "schedd-b.example"


def test_monthly_summary_aggregates_totals_and_max_memory(tmp_path: Path) -> None:
    _write_daily_jobs(
        tmp_path,
        "2026-04-17",
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
            _job("job-2", day="2026-04-17", user="bob", vo="cms", wall_seconds=20, cpu_user_seconds=6, cpu_sys_seconds=2, processors=2, memory_real_kb=250, memory_virtual_kb=450, scale_factor=2.0),
        ],
    )
    jobs = load_monthly_jobs(tmp_path, 2026, 4)
    summary = build_monthly_report_summary(2026, 4, jobs)

    assert summary.jobs_total == 2
    assert summary.days_included == 1
    assert summary.wall_seconds == 30
    assert summary.cpu_total_seconds == 13
    assert summary.scaled_wall_seconds == 50.0
    assert summary.scaled_cpu_seconds == 21.0
    assert summary.avg_processors == 1.5
    assert summary.max_processors == 2
    assert summary.memory_real_kb_max == 250
    assert summary.memory_virtual_kb_max == 450


def test_write_csv_rows_uses_stable_headers(tmp_path: Path) -> None:
    path = tmp_path / "users.csv"
    write_csv_rows(
        path,
        [
            UsageGroupRow(
                group_type="user",
                group_key="alice",
                jobs=1,
                users=None,
                vo="atlas",
                wall_seconds=10,
                cpu_user_seconds=4,
                cpu_sys_seconds=1,
                cpu_total_seconds=5,
                scaled_wall_seconds=10.0,
                scaled_cpu_seconds=5.0,
                avg_processors=1.0,
                max_processors=1,
                memory_real_kb_max=100,
                memory_virtual_kb_max=200,
            )
        ],
        ["group_type", "group_key", "vo", "jobs", "avg_processors", "max_processors"],
    )

    assert path.read_text(encoding="utf-8") == "group_type,group_key,vo,jobs,avg_processors,max_processors\nuser,alice,atlas,1,1.0,1\n"


def test_html_render_includes_summary_and_tables(tmp_path: Path) -> None:
    summary = build_monthly_report_summary(
        2026,
        4,
        [
            _job("job-1", day="2026-04-17", user="alice", vo="atlas", wall_seconds=10, cpu_user_seconds=4, cpu_sys_seconds=1, processors=1, memory_real_kb=100, memory_virtual_kb=300),
        ],
    )
    user_rows = [
        UsageGroupRow(
            group_type="user",
            group_key="alice",
            jobs=1,
            users=None,
            vo="atlas",
            wall_seconds=3600,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=7200,
            scaled_wall_seconds=5400.0,
            scaled_cpu_seconds=10800.0,
            avg_processors=1.0,
            max_processors=1,
            memory_real_kb_max=2097152,
            memory_virtual_kb_max=300,
        )
    ]
    vo_rows = [
        UsageGroupRow(
            group_type="vo",
            group_key="atlas",
            jobs=1,
            users=1,
            vo=None,
            wall_seconds=10,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=5,
            scaled_wall_seconds=10.0,
            scaled_cpu_seconds=5.0,
            avg_processors=1.0,
            max_processors=1,
            memory_real_kb_max=100,
            memory_virtual_kb_max=300,
        )
    ]
    accounting_group_rows = [
        UsageGroupRow(
            group_type="accounting_group",
            group_key="group-a",
            jobs=1,
            users=1,
            vo="atlas",
            wall_seconds=10,
            cpu_user_seconds=4,
            cpu_sys_seconds=1,
            cpu_total_seconds=5,
            scaled_wall_seconds=10.0,
            scaled_cpu_seconds=5.0,
            avg_processors=1.0,
            max_processors=1,
            memory_real_kb_max=100,
            memory_virtual_kb_max=300,
        )
    ]

    context = build_monthly_report_context(
        summary,
        user_rows,
        vo_rows,
        accounting_group_rows,
        benchmark_type="hepscore23",
        benchmark_baseline=20.0,
    )
    html = render_monthly_report_html(context)

    assert "HTCondor Accounting Monthly Report 2026-04" in html
    assert "Days Included" in html
    assert "Total Wall Hours" in html
    assert "Users" in html
    assert "VOs" in html
    assert "Accounting Groups" in html
    assert "alice" in html
    assert "atlas" in html
    assert "group-a" in html
    assert 'href="users.csv"' in html or "href='users.csv'" in html
    assert 'href="vos.csv"' in html or "href='vos.csv'" in html
    assert 'href="accounting_groups.csv"' in html or "href='accounting_groups.csv'" in html
    assert "configured hepscore23 baseline of 20" in html
    assert "1.0 (1.5)" in html
    assert "2.0 (3.0)" in html
    assert "2.0" in html


def test_jinja_context_builds_sections_and_relative_links() -> None:
    summary = build_monthly_report_summary(2026, 4, [])
    context = build_monthly_report_context(
        summary,
        [],
        [],
        [],
        benchmark_type="hepscore23",
        benchmark_baseline=20.0,
        schedd_links=[{"label": "schedd-a.example", "href": "schedds/schedd-a.example/index.html", "jobs": "2"}],
    )

    assert context["title"] == "HTCondor Accounting Monthly Report 2026-04"
    assert context["sections"][0]["csv_href"] == "users.csv"
    assert context["sections"][1]["csv_href"] == "vos.csv"
    assert context["sections"][2]["csv_href"] == "accounting_groups.csv"
    assert context["schedd_links"][0]["href"] == "schedds/schedd-a.example/index.html"
    assert "configured hepscore23 baseline of 20" in context["scaling_note"]


def test_html_includes_relative_plot_link_when_present() -> None:
    summary = build_monthly_report_summary(2026, 4, [])
    context = build_monthly_report_context(
        summary,
        [],
        [],
        [],
        benchmark_type="hepscore23",
        benchmark_baseline=20.0,
        plot_href="wall_hours_by_accounting_group.png",
    )

    html = render_monthly_report_html(context)

    assert 'src="wall_hours_by_accounting_group.png"' in html
    assert 'alt="Wall hours by accounting group"' in html


def test_html_omits_plot_section_when_absent() -> None:
    summary = build_monthly_report_summary(2026, 4, [])
    context = build_monthly_report_context(
        summary,
        [],
        [],
        [],
        benchmark_type="hepscore23",
        benchmark_baseline=20.0,
    )

    html = render_report_html(context)

    assert "Wall hours by accounting group</h2>" not in html
    assert "wall_hours_by_accounting_group.png" not in html


def test_daily_report_output_paths_and_files(tmp_path: Path) -> None:
    jobs = [
        _job(
            "job-1",
            day="2026-04-21",
            user="alice",
            vo="atlas",
            wall_seconds=3600,
            cpu_user_seconds=10,
            cpu_sys_seconds=5,
            processors=1,
            memory_real_kb=100,
            memory_virtual_kb=200,
            acct_group="group-a",
            end_time=_timestamp(2026, 4, 21, 8),
        )
    ]
    report_dir = tmp_path / "reports" / "daily" / "2026" / "04" / "21"

    result = write_report_set(
        period_type="daily",
        period_label="2026-04-21",
        jobs=jobs,
        output_dir=report_dir,
        benchmark_type="hepscore23",
        benchmark_baseline=20.0,
        users_csv_path=report_dir / "users.csv",
        vos_csv_path=report_dir / "vos.csv",
        accounting_groups_csv_path=report_dir / "accounting_groups.csv",
        summary_path=report_dir / "summary.json",
        index_path=report_dir / "index.html",
        plot_path=report_dir / "wall_hours_by_accounting_group.png",
    )

    assert result["index_path"] == report_dir / "index.html"
    assert (report_dir / "users.csv").exists()
    assert (report_dir / "vos.csv").exists()
    assert (report_dir / "accounting_groups.csv").exists()
    assert (report_dir / "summary.json").exists()
    assert (report_dir / "index.html").exists()
    assert (report_dir / "wall_hours_by_accounting_group.png").stat().st_size > 0
    payload = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["record_type"] == "daily_report_summary"
    assert payload["day"] == "2026-04-21"
    html = (report_dir / "index.html").read_text(encoding="utf-8")
    assert "HTCondor Accounting Daily Report 2026-04-21" in html
    assert 'src="wall_hours_by_accounting_group.png"' in html


def test_daily_plot_buckets_wall_hours_by_completion_hour(tmp_path: Path) -> None:
    jobs = [
        _job("job-1", day="2026-04-21", user="alice", vo="atlas", wall_seconds=3600, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, acct_group="group-a", end_time=_timestamp(2026, 4, 21, 8)),
        _job("job-2", day="2026-04-21", user="bob", vo="atlas", wall_seconds=1800, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, acct_group="group-a", end_time=_timestamp(2026, 4, 21, 8)),
        _job("job-3", day="2026-04-21", user="carol", vo="cms", wall_seconds=7200, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, accounting_group="group-b", end_time=_timestamp(2026, 4, 21, 9)),
        _job("job-4", day="2026-04-21", user="dana", vo="cms", wall_seconds=3600, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, end_time=_timestamp(2026, 4, 21, 10)),
    ]

    bucketed = bucket_wall_hours_by_accounting_group(jobs, period_type="daily")
    plot_path = write_wall_hours_by_accounting_group_plot(
        jobs,
        tmp_path / "wall_hours_by_accounting_group.png",
        period_type="daily",
        title="Daily",
    )

    assert bucketed["group-a"][8] == 1.5
    assert bucketed["group-b"][9] == 2.0
    assert bucketed["-"][10] == 1.0
    assert plot_path.stat().st_size > 0


def test_monthly_plot_buckets_wall_hours_by_completion_day(tmp_path: Path) -> None:
    jobs = [
        _job("job-1", day="2026-04-01", user="alice", vo="atlas", wall_seconds=3600, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, acct_group="group-a", end_time=_timestamp(2026, 4, 1, 23)),
        _job("job-2", day="2026-04-02", user="bob", vo="atlas", wall_seconds=7200, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, acct_group="group-a", end_time=_timestamp(2026, 4, 2, 0)),
        _job("job-3", day="2026-04-02", user="carol", vo="cms", wall_seconds=1800, cpu_user_seconds=1, cpu_sys_seconds=1, processors=1, memory_real_kb=1, memory_virtual_kb=1, route_name="route-c", end_time=_timestamp(2026, 4, 2, 13)),
    ]

    bucketed = bucket_wall_hours_by_accounting_group(jobs, period_type="monthly")
    plot_path = write_wall_hours_by_accounting_group_plot(
        jobs,
        tmp_path / "wall_hours_by_accounting_group.png",
        period_type="monthly",
        title="Monthly",
    )

    assert bucketed["group-a"][1] == 1.0
    assert bucketed["group-a"][2] == 2.0
    assert bucketed["route-c"][2] == 0.5
    assert plot_path.stat().st_size > 0


def test_html_helpers_format_human_units_and_scaled_pairs() -> None:
    assert format_hours(7200) == "2.0"
    assert format_gb(2097152) == "2.0"
    assert format_scaled_pair(3600, 5400) == "1.0 (1.5)"
