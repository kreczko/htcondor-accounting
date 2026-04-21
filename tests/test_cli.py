from pathlib import Path
from types import ModuleType

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
