from pathlib import Path

from htcondor_accounting.config.load import resolve_reports_root
from htcondor_accounting.config.models import AppConfig


def test_reports_root_defaults_to_reports_under_storage_root() -> None:
    config = AppConfig()
    config.storage.root = Path("/var/lib/condor/accounting")

    assert resolve_reports_root(config) == Path("/var/lib/condor/accounting/reports")


def test_relative_reports_root_resolves_under_storage_root() -> None:
    config = AppConfig()
    config.storage.root = Path("/var/lib/condor/accounting")
    config.reporting.output_dir = Path("reports-public")

    assert resolve_reports_root(config) == Path("/var/lib/condor/accounting/reports-public")


def test_absolute_reports_root_is_used_directly() -> None:
    config = AppConfig()
    config.storage.root = Path("/var/lib/condor/accounting")
    config.reporting.output_dir = Path("/mnt/shared/condor/accounting/reports")

    assert resolve_reports_root(config) == Path("/mnt/shared/condor/accounting/reports")


def test_reports_root_uses_output_root_override_for_relative_reporting_dir() -> None:
    config = AppConfig()
    config.storage.root = Path("/var/lib/condor/accounting")
    config.reporting.output_dir = Path("reports-public")

    assert resolve_reports_root(config, output_root_override=Path("/tmp/accounting")) == Path(
        "/tmp/accounting/reports-public"
    )
