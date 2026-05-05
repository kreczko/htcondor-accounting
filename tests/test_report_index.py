import json
import shutil
from pathlib import Path

from htcondor_accounting.report.index import discover_reports, generate_report_indexes


def _write_report(
    reports_root: Path,
    *,
    period: str,
    label: str,
    jobs: int,
    wall_seconds: int,
    cpu_seconds: int,
    users: list[str],
    vos: list[str],
    accounting_groups: list[str],
) -> Path:
    if period == "daily":
        year, month, day = label.split("-")
        report_dir = reports_root / "daily" / year / month / day
        summary = {"record_type": "daily_report_summary", "day": label}
    else:
        year, month = label.split("-")
        report_dir = reports_root / "monthly" / year / month
        summary = {"record_type": "monthly_report_summary", "period": label}

    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "index.html").write_text(f"<h1>{label}</h1>", encoding="utf-8")
    summary.update(
        {
            "jobs_total": jobs,
            "wall_seconds": wall_seconds,
            "cpu_total_seconds": cpu_seconds,
        }
    )
    (report_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (report_dir / "users.csv").write_text(
        "user,jobs\n" + "".join(f"{user},1\n" for user in users),
        encoding="utf-8",
    )
    (report_dir / "vos.csv").write_text(
        "vo,jobs\n" + "".join(f"{vo},1\n" for vo in vos),
        encoding="utf-8",
    )
    (report_dir / "accounting_groups.csv").write_text(
        "accounting_group,jobs\n" + "".join(f"{group},1\n" for group in accounting_groups),
        encoding="utf-8",
    )
    return report_dir


def test_root_landing_page_generation_and_aggregation(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    _write_report(
        reports_root,
        period="daily",
        label="2026-04-21",
        jobs=2,
        wall_seconds=7200,
        cpu_seconds=3600,
        users=["alice", "bob"],
        vos=["cms", "-"],
        accounting_groups=["group_cms", "-"],
    )
    _write_report(
        reports_root,
        period="daily",
        label="2026-04-22",
        jobs=3,
        wall_seconds=3600,
        cpu_seconds=1800,
        users=["alice", "carol"],
        vos=["lhcb"],
        accounting_groups=["group_lhcb"],
    )
    _write_report(
        reports_root,
        period="monthly",
        label="2026-04",
        jobs=99,
        wall_seconds=99,
        cpu_seconds=99,
        users=["ignored"],
        vos=["dune"],
        accounting_groups=["group_physics"],
    )

    generate_report_indexes(reports_root)

    html = (reports_root / "index.html").read_text(encoding="utf-8")
    assert "HTCondor Accounting Reports" in html
    assert "Earliest report" in html
    assert "2026-04-21" in html
    assert "2026-04-22" in html
    assert "Total jobs" in html
    assert "5" in html
    assert "Total wall hours" in html
    assert "3.0" in html
    assert "Total CPU hours" in html
    assert "1.5" in html
    assert "Total users" in html
    assert "3" in html
    assert "group_cms" in html
    assert "group_lhcb" in html
    assert "group_physics" in html
    assert "cms" in html
    assert "lhcb" in html
    assert "dune" in html
    assert 'href="daily/2026/04/22/index.html"' in html
    assert 'href="monthly/2026/04/index.html"' in html
    assert "http://" not in html
    assert "https://" not in html
    assert "<script" not in html.lower()


def test_daily_calendar_generation_and_relative_links(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    _write_report(
        reports_root,
        period="daily",
        label="2026-04-21",
        jobs=1,
        wall_seconds=1,
        cpu_seconds=1,
        users=["alice"],
        vos=["cms"],
        accounting_groups=["group_cms"],
    )
    _write_report(
        reports_root,
        period="daily",
        label="2026-05-04",
        jobs=1,
        wall_seconds=1,
        cpu_seconds=1,
        users=["bob"],
        vos=["lhcb"],
        accounting_groups=["group_lhcb"],
    )

    generate_report_indexes(reports_root)

    html = (reports_root / "daily" / "index.html").read_text(encoding="utf-8")
    assert "2026-05" in html
    assert "2026-04" in html
    assert 'href="2026/04/21/index.html"' in html
    assert 'href="2026/05/04/index.html"' in html
    assert "missing" in html
    assert "../index.html" in html
    assert "<script" not in html.lower()


def test_monthly_index_generation_and_relative_links(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    _write_report(
        reports_root,
        period="monthly",
        label="2025-12",
        jobs=1,
        wall_seconds=1,
        cpu_seconds=1,
        users=["alice"],
        vos=["cms"],
        accounting_groups=["group_cms"],
    )
    _write_report(
        reports_root,
        period="monthly",
        label="2026-04",
        jobs=1,
        wall_seconds=1,
        cpu_seconds=1,
        users=["bob"],
        vos=["lhcb"],
        accounting_groups=["group_lhcb"],
    )

    generate_report_indexes(reports_root)

    html = (reports_root / "monthly" / "index.html").read_text(encoding="utf-8")
    assert "2026" in html
    assert "2025" in html
    assert 'href="2026/04/index.html"' in html
    assert 'href="2025/12/index.html"' in html
    assert "../index.html" in html
    assert "<script" not in html.lower()


def test_yearly_placeholder_generation(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"

    generate_report_indexes(reports_root)

    html = (reports_root / "yearly" / "index.html").read_text(encoding="utf-8")
    assert "Yearly Reports" in html
    assert "not implemented yet" in html
    assert "../index.html" in html


def test_discovery_handles_partial_report_trees_and_missing_reports_dir(tmp_path: Path) -> None:
    missing = discover_reports(tmp_path / "missing" / "reports")
    assert missing.daily_reports == []
    assert missing.monthly_reports == []

    reports_root = tmp_path / "reports"
    partial = reports_root / "daily" / "2026" / "04" / "21"
    partial.mkdir(parents=True)
    (partial / "index.html").write_text("<h1>partial</h1>", encoding="utf-8")

    discovery = generate_report_indexes(reports_root)

    assert len(discovery.daily_reports) == 1
    assert (reports_root / "index.html").exists()
    assert (reports_root / "daily" / "index.html").exists()
    assert (reports_root / "monthly" / "index.html").exists()
    assert (reports_root / "yearly" / "index.html").exists()


def test_indexes_regenerate_from_filesystem_state(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    first = _write_report(
        reports_root,
        period="daily",
        label="2026-04-21",
        jobs=1,
        wall_seconds=1,
        cpu_seconds=1,
        users=["alice"],
        vos=["cms"],
        accounting_groups=["group_cms"],
    )
    _write_report(
        reports_root,
        period="daily",
        label="2026-04-22",
        jobs=1,
        wall_seconds=1,
        cpu_seconds=1,
        users=["bob"],
        vos=["lhcb"],
        accounting_groups=["group_lhcb"],
    )
    generate_report_indexes(reports_root)
    assert "2026/04/21" in (reports_root / "daily" / "index.html").read_text(encoding="utf-8")

    shutil.rmtree(first)
    generate_report_indexes(reports_root)

    daily_html = (reports_root / "daily" / "index.html").read_text(encoding="utf-8")
    root_html = (reports_root / "index.html").read_text(encoding="utf-8")
    assert "2026/04/21" not in daily_html
    assert 'href="daily/2026/04/22/index.html"' in root_html
