from pathlib import Path
from types import ModuleType
import json

from typer.testing import CliRunner

from htcondor_accounting.cli import app
from htcondor_accounting.models.canonical import (
    BenchmarkInfo,
    CanonicalJobRecord,
    IdentityInfo,
    JobInfo,
    SourceInfo,
    TimingInfo,
    UsageInfo,
)
from htcondor_accounting.store.jsonl import write_jsonl_zst


runner = CliRunner()


def _record(job_id: str, owner: str, vo: str | None, scale_factor: float | None) -> CanonicalJobRecord:
    return CanonicalJobRecord(
        site_name="UKI-SOUTHGRID-BRIS-HEP",
        source=SourceInfo(
            schedd="lcgce02.phy.bris.ac.uk",
            collected_at="2026-04-17T12:00:00Z",
        ),
        job=JobInfo(
            global_job_id=job_id,
            owner=owner,
            local_user=owner,
        ),
        usage=UsageInfo(
            wall_seconds=42601,
            cpu_user_seconds=40029,
            cpu_sys_seconds=51,
            processors=1,
        ),
        timing=TimingInfo(
            start_time=1776386139,
            end_time=1776428989,
            status_time=1776428989,
        ),
        identity=IdentityInfo(vo=vo),
        resolved_identity={"vo": vo, "vo_group": f"/{vo}" if vo else None, "vo_role": None, "fqan": f"/{vo}" if vo else None, "resolution_method": "test"},
        benchmark=BenchmarkInfo(scale_factor=scale_factor),
    )


def test_extract_uses_config_defaults(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakeHistoryQuery:
        def __init__(self, schedd_name, match, constraint):
            self.schedd_name = schedd_name
            self.match = match
            self.constraint = constraint

    fake_module = ModuleType("htcondor_accounting.extract.htcondor")

    def fake_extract_many_canonical_records(site_name, schedd_names, base_query):
        captured["site_name"] = site_name
        captured["schedd_names"] = schedd_names
        captured["match"] = base_query.match
        captured["constraint"] = base_query.constraint
        return {}

    fake_module.HistoryQuery = FakeHistoryQuery
    fake_module.extract_many_canonical_records = fake_extract_many_canonical_records
    monkeypatch.setitem(__import__("sys").modules, "htcondor_accounting.extract.htcondor", fake_module)

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[site]",
                'name = "CONFIG-SITE"',
                "",
                "[storage]",
                'root = "/tmp/config-archive"',
                "",
                "[extract]",
                "default_match = 250",
                'default_schedds = ["schedd-a.example", "schedd-b.example"]',
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2026-04-17",
            "--end",
            "2026-04-17",
            "--config",
            str(config_path),
        ],
        terminal_width=160,
    )

    assert result.exit_code == 0
    assert captured["site_name"] == "CONFIG-SITE"
    assert captured["schedd_names"] == ["schedd-a.example", "schedd-b.example"]
    assert captured["match"] == 250
    assert "site       = CONFIG-SITE" in result.stdout
    assert "output     = /tmp/config-archive" in result.stdout
    assert "match      = 250" in result.stdout
    assert "manifest   = /tmp/config-archive/manifests/" in result.stdout



def test_extract_cli_arguments_override_config(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakeHistoryQuery:
        def __init__(self, schedd_name, match, constraint):
            self.schedd_name = schedd_name
            self.match = match
            self.constraint = constraint

    fake_module = ModuleType("htcondor_accounting.extract.htcondor")

    def fake_extract_many_canonical_records(site_name, schedd_names, base_query):
        captured["site_name"] = site_name
        captured["schedd_names"] = schedd_names
        captured["match"] = base_query.match
        return {}

    fake_module.HistoryQuery = FakeHistoryQuery
    fake_module.extract_many_canonical_records = fake_extract_many_canonical_records
    monkeypatch.setitem(__import__("sys").modules, "htcondor_accounting.extract.htcondor", fake_module)

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[site]",
                'name = "CONFIG-SITE"',
                "",
                "[storage]",
                'root = "/tmp/config-archive"',
                "",
                "[extract]",
                "default_match = 250",
                'default_schedds = ["schedd-a.example"]',
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2026-04-17",
            "--end",
            "2026-04-17",
            "--config",
            str(config_path),
            "--site-name",
            "CLI-SITE",
            "--output-root",
            str(tmp_path / "override-archive"),
            "--match",
            "10",
            "--schedd",
            "override-schedd.example",
        ],
        terminal_width=160,
    )

    assert result.exit_code == 0
    assert captured["site_name"] == "CLI-SITE"
    assert captured["schedd_names"] == ["override-schedd.example"]
    assert captured["match"] == 10
    assert "site       = CLI-SITE" in result.stdout
    assert "output     =" in result.stdout
    assert str(tmp_path / "override-archive") in result.stdout
    assert "match      = 10" in result.stdout


def test_extract_buckets_records_by_record_day(monkeypatch, tmp_path: Path) -> None:
    class FakeHistoryQuery:
        def __init__(self, schedd_name, match, constraint):
            self.schedd_name = schedd_name
            self.match = match
            self.constraint = constraint

    fake_module = ModuleType("htcondor_accounting.extract.htcondor")

    def fake_extract_many_canonical_records(site_name, schedd_names, base_query):
        del site_name, schedd_names, base_query
        first = _record("job-001", "alice", "atlas", 1.0)
        first.timing.end_time = 1776470399  # 2026-04-17 23:59:59 UTC
        second = _record("job-002", "bob", "cms", 1.0)
        second.timing.end_time = 1776470400  # 2026-04-18 00:00:00 UTC
        return {"lcgce02.phy.bris.ac.uk": [first, second]}

    fake_module.HistoryQuery = FakeHistoryQuery
    fake_module.extract_many_canonical_records = fake_extract_many_canonical_records
    monkeypatch.setitem(__import__("sys").modules, "htcondor_accounting.extract.htcondor", fake_module)

    output_root = tmp_path / "archive"
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-01",
            "--end",
            "2026-04-30",
            "--output-root",
            str(output_root),
            "--site-name",
            "TEST-SITE",
        ],
        terminal_width=200,
    )

    first_path = output_root / "canonical" / "2026" / "04" / "17"
    second_path = output_root / "canonical" / "2026" / "04" / "18"

    assert result.exit_code == 0
    assert first_path.exists()
    assert second_path.exists()
    assert len(list(first_path.glob("*.jsonl.zst"))) == 1
    assert len(list(second_path.glob("*.jsonl.zst"))) == 1
    assert "files      = 2" in result.stdout
    assert "2026-04-17" in result.stdout
    assert "2026-04-18" in result.stdout


def test_extract_writes_manifest_with_correct_total_records(monkeypatch, tmp_path: Path) -> None:
    class FakeHistoryQuery:
        def __init__(self, schedd_name, match, constraint):
            self.schedd_name = schedd_name
            self.match = match
            self.constraint = constraint

    fake_module = ModuleType("htcondor_accounting.extract.htcondor")

    def fake_extract_many_canonical_records(site_name, schedd_names, base_query):
        del site_name, schedd_names, base_query
        first = _record("job-001", "alice", "atlas", 1.0)
        first.timing.end_time = 1776470399
        second = _record("job-002", "bob", "cms", 1.0)
        second.timing.end_time = 1776470400
        return {"lcgce02.phy.bris.ac.uk": [first, second]}

    fake_module.HistoryQuery = FakeHistoryQuery
    fake_module.extract_many_canonical_records = fake_extract_many_canonical_records
    monkeypatch.setitem(__import__("sys").modules, "htcondor_accounting.extract.htcondor", fake_module)

    output_root = tmp_path / "archive"
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2026-04-17",
            "--end",
            "2026-04-18",
            "--output-root",
            str(output_root),
            "--site-name",
            "TEST-SITE",
        ],
        terminal_width=200,
    )

    manifest_paths = list((output_root / "manifests").rglob("*.json"))

    assert result.exit_code == 0
    assert len(manifest_paths) == 1

    manifest = json.loads(manifest_paths[0].read_text(encoding="utf-8"))
    assert manifest["record_type"] == "extract_manifest"
    assert manifest["tool_version"] == "0.1.0"
    assert manifest["site_name"] == "TEST-SITE"
    assert manifest["total_records"] == 2
    assert manifest["files_written_count"] == 2
    assert manifest["schedds"] == ["lcgce02.phy.bris.ac.uk"]
    assert len(manifest["files_written"]) == 2
    assert manifest["files_written"][0]["day"] == "2026-04-17"
    assert manifest["files_written"][1]["day"] == "2026-04-18"
    assert manifest["files_written"][0]["records"] == 1
    assert manifest["files_written"][1]["records"] == 1
    assert "manifest   =" in result.stdout


def test_derive_daily_command_creates_outputs(tmp_path: Path) -> None:
    day_dir = tmp_path / "archive" / "canonical" / "2026" / "04" / "17"
    write_jsonl_zst(
        day_dir / "ce01.jsonl.zst",
        [
            {
                "schema_version": 1,
                "record_type": "job",
                "site_name": "UKI-SOUTHGRID-BRIS-HEP",
                "source": {
                    "system": "htcondor",
                    "schedd": "lcgce02.phy.bris.ac.uk",
                    "collector_host": None,
                    "collected_at": "2026-04-17T12:00:00Z",
                },
                "job": {
                    "global_job_id": "job-001",
                    "routed_from_job_id": None,
                    "owner": "alice",
                    "local_user": "alice",
                },
                "usage": {
                    "wall_seconds": 100,
                    "cpu_user_seconds": 50,
                    "cpu_sys_seconds": 10,
                    "processors": 1,
                    "memory_real_kb": 1000,
                    "memory_virtual_kb": 2000,
                },
                "timing": {
                    "queue_time": None,
                    "start_time": 1776386139,
                    "end_time": 1776428989,
                    "status_time": 1776428989,
                },
                "identity": {
                    "dn": "/C=UK/O=eScience/CN=alice",
                    "fqan": "/atlas",
                    "vo": "atlas",
                    "vo_group": "/atlas",
                    "vo_role": None,
                    "auth_method": "scitoken",
                    "token_issuer": "https://issuer.example",
                    "token_subject": "subject",
                    "token_groups": ["/atlas"],
                },
                "benchmark": {
                    "benchmark_type": "hepscore23",
                    "site_baseline_per_core": None,
                    "node_per_core": None,
                    "scale_factor": 2.0,
                },
                "execution": {
                    "ce_host": "lcgce02.phy.bris.ac.uk",
                    "ce_id": None,
                    "execute_node": "slot1@node",
                    "slot_name": "slot1@node",
                },
            }
        ],
    )

    result = runner.invoke(
        app,
        ["derive-daily", "--day", "2026-04-17", "--output-root", str(tmp_path / "archive")],
        terminal_width=160,
    )

    assert result.exit_code == 0
    assert "Derive Daily" in result.stdout
    assert "Unique records" in result.stdout
    assert (tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "17" / "jobs.jsonl.zst").exists()
    assert (tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "17" / "summary.json").exists()
    assert (tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "17" / "duplicates.json").exists()


def test_derive_rollups_command_creates_higher_level_summaries(tmp_path: Path) -> None:
    first = tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "13" / "summary.json"
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_text(
        json.dumps(
            {
                "day": "2026-04-13",
                "input_files": 1,
                "input_records": 2,
                "unique_records": 2,
                "duplicate_records": 0,
                "users": 1,
                "vos": 1,
                "wall_seconds": 10,
                "cpu_user_seconds": 5,
                "cpu_sys_seconds": 1,
                "cpu_total_seconds": 6,
                "scaled_wall_seconds": 10.0,
                "scaled_cpu_seconds": 6.0,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    second = tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "14" / "summary.json"
    second.parent.mkdir(parents=True, exist_ok=True)
    second.write_text(
        json.dumps(
            {
                "day": "2026-04-14",
                "input_files": 1,
                "input_records": 3,
                "unique_records": 3,
                "duplicate_records": 0,
                "users": 2,
                "vos": 2,
                "wall_seconds": 20,
                "cpu_user_seconds": 7,
                "cpu_sys_seconds": 3,
                "cpu_total_seconds": 10,
                "scaled_wall_seconds": 20.0,
                "scaled_cpu_seconds": 10.0,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["derive-rollups", "--output-root", str(tmp_path / "archive")],
        terminal_width=200,
    )

    assert result.exit_code == 0
    assert "Derive Rollups" in result.stdout
    assert (tmp_path / "archive" / "derived" / "weekly" / "2026" / "week-16" / "summary.json").exists()
    assert (tmp_path / "archive" / "derived" / "monthly" / "2026" / "04" / "summary.json").exists()
    assert (tmp_path / "archive" / "derived" / "yearly" / "2026" / "summary.json").exists()
    assert (tmp_path / "archive" / "derived" / "all-time" / "summary.json").exists()


def test_export_apel_daily_command_writes_staged_messages(tmp_path: Path) -> None:
    jobs_path = tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "17" / "jobs.jsonl.zst"
    write_jsonl_zst(
        jobs_path,
        [
            {
                "schema_version": 1,
                "record_type": "report_job",
                "site_name": "UKI-SOUTHGRID-BRIS-HEP",
                "global_job_id": "host#1.0#999",
                "owner": "alice",
                "local_user": "alice",
                "vo": "atlas",
                "vo_group": "/atlas",
                "vo_role": None,
                "auth_method": "scitoken",
                "start_time": 1776386139,
                "end_time": 1776428989,
                "wall_seconds": 10,
                "cpu_user_seconds": 6,
                "cpu_sys_seconds": 1,
                "cpu_total_seconds": 7,
                "processors": 1,
                "memory_real_kb": 1000,
                "memory_virtual_kb": 2000,
                "scale_factor": 2.0,
                "benchmark_type": "hepscore23",
                "source_schedd": "lcgce02.phy.bris.ac.uk",
                "acct_group": "group-a",
                "acct_group_user": "alice",
                "accounting_group": "group-a.main",
                "route_name": "route-a",
                "day": "2026-04-17",
            }
        ],
    )

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path / "archive"}"',
                "",
                "[apel]",
                "enabled = true",
                'ce_id = "ce.example:9619/condor"',
                'submit_host = "submit.example"',
                'machine_name = "worker.example"',
                'queue_name = "condor"',
                'infrastructure_description = "APEL-HTCondor"',
                'infrastructure_type = "grid"',
                'service_level_type = "hepscore23"',
                "service_level_value = 20.0",
                'staging_dir = "apel/staging"',
                f'outgoing_dir = "{tmp_path / "outgoing"}"',
                "message_soft_limit_bytes = 800000",
                "message_hard_limit_bytes = 1000000",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["export-apel-daily", "--day", "2026-04-17", "--config", str(config_path)],
        terminal_width=200,
    )

    assert result.exit_code == 0
    assert "Export APEL Daily" in result.stdout
    assert (tmp_path / "archive" / "apel" / "staging" / "2026" / "04" / "17").exists()
    assert len(list((tmp_path / "archive" / "apel" / "staging" / "2026" / "04" / "17").glob("*.msg"))) == 1
    assert len(list((tmp_path / "archive" / "apel" / "manifests" / "2026" / "04" / "17").glob("*.json"))) == 1
    assert not (tmp_path / "outgoing").exists()


def test_push_apel_daily_promotes_messages_into_dirq_queue(tmp_path: Path) -> None:
    staged_dir = tmp_path / "archive" / "apel" / "staging" / "2026" / "04" / "17"
    staged_dir.mkdir(parents=True, exist_ok=True)
    (staged_dir / "20260421T132304Z-0001.msg").write_text("%%\nSite: TEST\n", encoding="utf-8")

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path / "archive"}"',
                "",
                "[apel]",
                "enabled = true",
                'submit_host = "submit.example"',
                'machine_name = "worker.example"',
                'queue_name = "condor"',
                'infrastructure_description = "APEL-HTCondor"',
                'infrastructure_type = "grid"',
                'service_level_type = "hepscore23"',
                "service_level_value = 20.0",
                'staging_dir = "apel/staging"',
                'outgoing_dir = "apel/outgoing"',
                "message_soft_limit_bytes = 800000",
                "message_hard_limit_bytes = 1000000",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["push-apel-daily", "--day", "2026-04-17", "--config", str(config_path)],
        terminal_width=200,
    )

    queue_files = [path for path in (tmp_path / "archive" / "apel" / "outgoing").rglob("*") if path.is_file()]
    sent_markers = list((tmp_path / "archive" / "apel" / "ledger" / "sent").glob("*.json"))

    assert result.exit_code == 0
    assert "Push APEL Daily" in result.stdout
    assert "Staged files" in result.stdout
    assert "Already-sent skipped" in result.stdout
    assert "Newly pushed" in result.stdout
    assert len(queue_files) == 1
    assert len(sent_markers) == 1
    assert len(queue_files[0].parent.name) == 8
    assert len(queue_files[0].name) == 14


def test_render_monthly_command_writes_csv_html_and_summary(tmp_path: Path) -> None:
    jobs_path = tmp_path / "archive" / "derived" / "daily" / "2026" / "04" / "17" / "jobs.jsonl.zst"
    write_jsonl_zst(
        jobs_path,
        [
            {
                "schema_version": 1,
                "record_type": "report_job",
                "site_name": "UKI-SOUTHGRID-BRIS-HEP",
                "global_job_id": "host#1.0#999",
                "owner": "alice",
                "local_user": "alice",
                "vo": "atlas",
                "vo_group": "/atlas",
                "vo_role": None,
                "auth_method": "scitoken",
                "start_time": 1776386139,
                "end_time": 1776428989,
                "wall_seconds": 10,
                "cpu_user_seconds": 6,
                "cpu_sys_seconds": 1,
                "cpu_total_seconds": 7,
                "processors": 1,
                "memory_real_kb": 1000,
                "memory_virtual_kb": 2000,
                "scale_factor": 2.0,
                "benchmark_type": "hepscore23",
                "source_schedd": "lcgce02.phy.bris.ac.uk",
                "day": "2026-04-17",
            }
        ],
    )

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path / "archive"}"',
                "",
                "[reporting]",
                'output_dir = "reports"',
                "publish_html = true",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["render-monthly", "--year", "2026", "--month", "4", "--config", str(config_path)],
        terminal_width=200,
    )

    report_dir = tmp_path / "archive" / "reports" / "monthly" / "2026" / "04"
    assert result.exit_code == 0
    assert "Render Monthly" in result.stdout
    assert (report_dir / "users.csv").exists()
    assert (report_dir / "vos.csv").exists()
    assert (report_dir / "accounting_groups.csv").exists()
    assert (report_dir / "summary.json").exists()
    assert (report_dir / "index.html").exists()
    summary = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["period"] == "2026-04"
    assert summary["jobs_total"] == 1
    assert (report_dir / "users.csv").read_text(encoding="utf-8").splitlines()[0] == (
        "user,vo,jobs,wall_seconds,cpu_user_seconds,cpu_sys_seconds,cpu_total_seconds,"
        "scaled_wall_seconds,scaled_cpu_seconds,avg_processors,max_processors,memory_real_kb_max,memory_virtual_kb_max"
    )
    assert (report_dir / "vos.csv").read_text(encoding="utf-8").splitlines()[0] == (
        "vo,users,jobs,wall_seconds,cpu_user_seconds,cpu_sys_seconds,cpu_total_seconds,"
        "scaled_wall_seconds,scaled_cpu_seconds,avg_processors,max_processors,memory_real_kb_max,memory_virtual_kb_max"
    )
    assert (report_dir / "accounting_groups.csv").read_text(encoding="utf-8").splitlines()[0] == (
        "accounting_group,vo,users,jobs,wall_seconds,cpu_user_seconds,cpu_sys_seconds,cpu_total_seconds,"
        "scaled_wall_seconds,scaled_cpu_seconds,avg_processors,max_processors,memory_real_kb_max,memory_virtual_kb_max"
    )
    html = (report_dir / "index.html").read_text(encoding="utf-8")
    assert "Accounting Groups" in html
    assert "href='users.csv'" in html
    assert "href='vos.csv'" in html
    assert "href='accounting_groups.csv'" in html
    assert "Wall h (scaled)" in html


def test_push_apel_daily_skips_when_sent_marker_exists(tmp_path: Path) -> None:
    staged_dir = tmp_path / "archive" / "apel" / "staging" / "2026" / "04" / "17"
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_file = staged_dir / "20260421T132304Z-0001.msg"
    staged_file.write_text("%%\nSite: TEST\n", encoding="utf-8")

    sent_dir = tmp_path / "archive" / "apel" / "ledger" / "sent"
    sent_dir.mkdir(parents=True, exist_ok=True)
    sent_marker = sent_dir / "59c8392ae7071bffae97d3624d0d9ff0.json"
    sent_marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_type": "apel_sent_marker",
                "message_md5": "59c8392ae7071bffae97d3624d0d9ff0",
                "day": "2026-04-17",
                "staged_path": str(staged_file),
                "outgoing_path": str(tmp_path / "archive" / "apel" / "outgoing" / "59c8392a" / "e7071bffae97d3"),
                "records": 1,
                "bytes": 14,
                "first_pushed_at": "2026-04-21T13:23:04Z",
                "run_stamp": "20260421T132304Z",
            }
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path / "archive"}"',
                "",
                "[apel]",
                "enabled = true",
                'submit_host = "submit.example"',
                'machine_name = "worker.example"',
                'queue_name = "condor"',
                'infrastructure_description = "APEL-HTCondor"',
                'infrastructure_type = "grid"',
                'service_level_type = "hepscore23"',
                "service_level_value = 20.0",
                'staging_dir = "apel/staging"',
                'outgoing_dir = "apel/outgoing"',
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["push-apel-daily", "--day", "2026-04-17", "--config", str(config_path)],
        terminal_width=200,
    )

    queue_root = tmp_path / "archive" / "apel" / "outgoing"
    queue_files = [path for path in queue_root.rglob("*") if path.is_file()] if queue_root.exists() else []

    assert result.exit_code == 0
    assert "1" in result.stdout
    assert len(queue_files) == 0


def test_push_apel_daily_force_resend_writes_resend_event(tmp_path: Path) -> None:
    staged_dir = tmp_path / "archive" / "apel" / "staging" / "2026" / "04" / "17"
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_file = staged_dir / "20260421T132304Z-0001.msg"
    staged_file.write_text("%%\nSite: TEST\n", encoding="utf-8")

    sent_dir = tmp_path / "archive" / "apel" / "ledger" / "sent"
    sent_dir.mkdir(parents=True, exist_ok=True)
    sent_dir.joinpath("59c8392ae7071bffae97d3624d0d9ff0.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_type": "apel_sent_marker",
                "message_md5": "59c8392ae7071bffae97d3624d0d9ff0",
                "day": "2026-04-17",
                "staged_path": str(staged_file),
                "outgoing_path": str(tmp_path / "archive" / "apel" / "outgoing" / "59c8392a" / "e7071bffae97d3"),
                "records": 1,
                "bytes": 14,
                "first_pushed_at": "2026-04-21T13:23:04Z",
                "run_stamp": "20260421T132304Z",
            }
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path / "archive"}"',
                "",
                "[apel]",
                "enabled = true",
                'submit_host = "submit.example"',
                'machine_name = "worker.example"',
                'queue_name = "condor"',
                'infrastructure_description = "APEL-HTCondor"',
                'infrastructure_type = "grid"',
                'service_level_type = "hepscore23"',
                "service_level_value = 20.0",
                'staging_dir = "apel/staging"',
                'outgoing_dir = "apel/outgoing"',
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "push-apel-daily",
            "--day",
            "2026-04-17",
            "--config",
            str(config_path),
            "--force-resend",
            "--reason",
            "operator retry",
        ],
        terminal_width=200,
    )

    resend_dir = tmp_path / "archive" / "apel" / "ledger" / "resends"
    resend_files = list(resend_dir.glob("*.json"))

    assert result.exit_code == 0
    assert len(resend_files) == 1
    payload = json.loads(resend_files[0].read_text(encoding="utf-8"))
    assert payload["record_type"] == "apel_resend_event"
    assert payload["reason"] == "operator retry"


def test_inspect_apel_ledger_outputs_json(tmp_path: Path) -> None:
    sent_dir = tmp_path / "archive" / "apel" / "ledger" / "sent"
    sent_dir.mkdir(parents=True, exist_ok=True)
    sent_dir.joinpath("abc123.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_type": "apel_sent_marker",
                "message_md5": "abc123",
                "day": "2026-04-17",
                "staged_path": "archive/apel/staging/2026/04/17/sample.msg",
                "outgoing_path": "archive/apel/outgoing/abc12345/6789abcdef0123",
                "records": 2,
                "bytes": 100,
                "first_pushed_at": "2026-04-21T13:23:04Z",
                "run_stamp": "20260421T132304Z",
            }
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{tmp_path / "archive"}"',
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["inspect-apel-ledger", "--day", "2026-04-17", "--format", "json", "--config", str(config_path)],
        terminal_width=200,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 1
    assert payload[0]["message_md5"] == "abc123"
    assert payload[0]["records"] == 2


def test_show_config_with_explicit_file(tmp_path: Path) -> None:
    config_path = tmp_path / "site.toml"
    config_path.write_text(
        "\n".join(
            [
                "[site]",
                'name = "TEST-SITE"',
                'timezone = "Europe/London"',
                "",
                "[storage]",
                'root = "/tmp/archive"',
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["show-config", "--config", str(config_path)], terminal_width=160)

    assert result.exit_code == 0
    assert "Show Config" in result.stdout
    assert "source =" in result.stdout
    assert str(config_path) in result.stdout
    assert '"name": "TEST-SITE"' in result.stdout
    assert '"timezone": "Europe/London"' in result.stdout
    assert '"root": "/tmp/archive"' in result.stdout


def test_show_config_uses_defaults_when_no_file_found(tmp_path: Path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(app, ["show-config"], terminal_width=160, catch_exceptions=False)

    assert result.exit_code == 0
    assert "source = <defaults>" in result.stdout
    assert '"name": "UNKNOWN"' in result.stdout
    assert '"timezone": "UTC"' in result.stdout


def test_inspect_least_verbosity_shows_compact_columns(tmp_path: Path) -> None:
    output_path = tmp_path / "sample.jsonl.zst"
    write_jsonl_zst(
        output_path,
        [
            _record("lcgce02.phy.bris.ac.uk#684860.0#1776420670", "alice", "atlas", 1.23456),
            _record("lcgce02.phy.bris.ac.uk#684861.0#1776420671", "bob", None, None),
        ],
    )

    result = runner.invoke(app, ["inspect", str(output_path)], terminal_width=160)

    assert result.exit_code == 0
    assert "verbosity    = least" in result.stdout
    assert "total jobs   = 2" in result.stdout
    assert "Schedd" in result.stdout
    assert "Start Date" in result.stdout
    assert "End Date" in result.stdout
    assert "alice" in result.stdout
    assert "atlas" in result.stdout
    assert "1.235" in result.stdout
    assert "2026-04-17" in result.stdout
    assert "00:35:39" in result.stdout
    assert "12:29:49" in result.stdout
    assert "Wallclock" not in result.stdout


def test_inspect_json_format_outputs_clean_json_array(tmp_path: Path) -> None:
    output_path = tmp_path / "sample.jsonl.zst"
    write_jsonl_zst(output_path, [_record("lcgce02.phy.bris.ac.uk#684860.0#1776420670", "alice", "atlas", 1.23456)])

    result = runner.invoke(
        app,
        ["inspect", str(output_path), "--format", "json", "--verbosity", "full"],
        terminal_width=160,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    assert payload[0]["job"]["global_job_id"] == "lcgce02.phy.bris.ac.uk#684860.0#1776420670"
    assert payload[0]["inspect"]["global_job_id"]["job_id"] == "684860.0"


def test_inspect_ndjson_format_outputs_one_record_per_line(tmp_path: Path) -> None:
    output_path = tmp_path / "sample.jsonl.zst"
    write_jsonl_zst(
        output_path,
        [
            _record("lcgce02.phy.bris.ac.uk#684860.0#1776420670", "alice", "atlas", 1.23456),
            _record("lcgce02.phy.bris.ac.uk#684861.0#1776420671", "bob", "cms", 1.0),
        ],
    )

    result = runner.invoke(
        app,
        ["inspect", str(output_path), "--format", "ndjson", "--verbosity", "medium"],
        terminal_width=160,
    )

    assert result.exit_code == 0
    lines = [json.loads(line) for line in result.stdout.strip().splitlines()]
    assert len(lines) == 2
    assert lines[0]["user"] == "alice"
    assert lines[1]["vo"] == "cms"


def test_inspect_medium_verbosity_adds_wallclock_and_identity(tmp_path: Path) -> None:
    output_path = tmp_path / "sample.jsonl.zst"
    record = _record("lcgce02.phy.bris.ac.uk#684860.0#1776420670", "alice", "atlas", 1.23456)
    record.identity.dn = "/C=UK/O=eScience/CN=alice"
    write_jsonl_zst(output_path, [record])

    result = runner.invoke(app, ["inspect", str(output_path), "--verbosity", "medium"], terminal_width=160)

    assert result.exit_code == 0
    assert "verbosity    = medium" in result.stdout
    assert "Identity" in result.stdout
    assert "Wallcl" in result.stdout
    assert "11:50:" in result.stdout
    assert "/C=UK/O" in result.stdout


def test_inspect_full_verbosity_shows_all_fields_and_parsed_job_id(tmp_path: Path) -> None:
    output_path = tmp_path / "sample.jsonl.zst"
    record = _record("lcgce02.phy.bris.ac.uk#684860.0#1776420670", "alice", "atlas", 1.23456)
    record.identity.token_issuer = "https://issuer.example"
    record.identity.token_subject = "alice-subject"
    write_jsonl_zst(output_path, [record])

    result = runner.invoke(app, ["inspect", str(output_path), "--verbosity", "full"], terminal_width=160)

    assert result.exit_code == 0
    assert "verbosity    = full" in result.stdout
    assert "\"site_name\": \"UKI-SOUTHGRID-BRIS-HEP\"" in result.stdout
    assert "\"job_id\": \"684860.0\"" in result.stdout
    assert "\"schedd\": \"lcgce02.phy.bris.ac.uk\"" in result.stdout
    assert "\"date\": \"2026-04-17 10:11:10\"" in result.stdout
    assert "\"identity_display\": \"issuer=https://issuer.example subject=alice-subject\"" in result.stdout
    assert "\"resolved_identity\"" in result.stdout


def test_inspect_directory_respects_limit(tmp_path: Path) -> None:
    day_dir = tmp_path / "archive" / "canonical" / "2026" / "04" / "17"
    write_jsonl_zst(
        day_dir / "ce01.jsonl.zst",
        [_record("lcgce02.phy.bris.ac.uk#684860.0#1776420670", "alice", "atlas", 1.0)],
    )
    write_jsonl_zst(
        day_dir / "ce02.jsonl.zst",
        [_record("lcgce02.phy.bris.ac.uk#684861.0#1776420671", "bob", "cms", 2.0)],
    )

    result = runner.invoke(app, ["inspect", str(tmp_path / "archive"), "--limit", "1"], terminal_width=160)

    assert result.exit_code == 0
    assert "files        = 2" in result.stdout
    assert "total jobs   = 2" in result.stdout
    assert "showing jobs = 1" in result.stdout


def test_snapshot_history_writes_raw_ads_partitioned_by_day(monkeypatch, tmp_path: Path) -> None:
    class FakeHistoryQuery:
        def __init__(self, schedd_name, match, constraint):
            self.schedd_name = schedd_name
            self.match = match
            self.constraint = constraint

    fake_module = ModuleType("htcondor_accounting.extract.htcondor")

    def fake_fetch_history_ads(query):
        del query
        return [
            {
                "GlobalJobId": "job-001",
                "CompletionDate": 1776470399,
                "EnteredCurrentStatus": 1776470399,
                "JobStartDate": 1776386139,
            },
            {
                "GlobalJobId": "job-002",
                "CompletionDate": 1776470400,
                "EnteredCurrentStatus": 1776470400,
                "JobStartDate": 1776386139,
            },
        ]

    fake_module.HistoryQuery = FakeHistoryQuery
    fake_module.fetch_history_ads = fake_fetch_history_ads
    monkeypatch.setitem(__import__("sys").modules, "htcondor_accounting.extract.htcondor", fake_module)

    output_root = tmp_path / "archive"
    result = runner.invoke(
        app,
        [
            "snapshot-history",
            "--start",
            "2026-04-17",
            "--end",
            "2026-04-18",
            "--output-root",
            str(output_root),
            "--schedd",
            "lcgce02.phy.bris.ac.uk",
        ],
        terminal_width=200,
    )

    assert result.exit_code == 0
    assert (output_root / "raw-history" / "2026" / "04" / "17").exists()
    assert (output_root / "raw-history" / "2026" / "04" / "18").exists()
    assert len(list((output_root / "raw-history" / "2026" / "04" / "17").glob("*.jsonl.zst"))) == 1
    assert len(list((output_root / "raw-history" / "2026" / "04" / "18").glob("*.jsonl.zst"))) == 1
    assert "Snapshot History" in result.stdout
