import sys
from types import SimpleNamespace

sys.modules.setdefault("htcondor2", SimpleNamespace())

from htcondor_accounting.extract.htcondor import canonical_from_ad
from htcondor_accounting.extract.identity import (
    construct_fqan,
    detect_auth_method,
    extract_raw_identity,
    resolve_reporting_identity,
)
from htcondor_accounting.models.canonical import AccountingInfo


def test_detect_auth_method_prefers_scitoken_over_x509() -> None:
    ad = {
        "x509UserProxySubject": "/C=UK/O=eScience/CN=alice",
        "orig_AuthTokenIssuer": "https://issuer.example",
    }

    assert detect_auth_method(ad) == "scitoken"


def test_detect_auth_method_uses_x509_when_only_dn_present() -> None:
    assert detect_auth_method({"x509UserProxySubject": "/C=UK/O=eScience/CN=alice"}) == "x509"


def test_detect_auth_method_falls_back_to_local() -> None:
    assert detect_auth_method({}) == "local"


def test_canonical_from_ad_preserves_accounting_fields() -> None:
    record = canonical_from_ad(
        {
            "GlobalJobId": "host#123.0#999",
            "Owner": "alice",
            "RemoteWallClockTime": 10,
            "RemoteUserCpu": 6,
            "RemoteSysCpu": 1,
            "RequestCpus": 1,
            "CompletionDate": 1776428989,
            "EnteredCurrentStatus": 1776428989,
            "AcctGroup": "group-a",
            "AcctGroupUser": "alice",
            "AccountingGroup": "accounting.group",
            "RouteName": "route-a",
            "LastMatchName": "slot1@example",
            "LastJobRouterName": "router-a",
        },
        site_name="TEST-SITE",
        schedd_name="schedd.example",
    )

    assert record.accounting.acct_group == "group-a"
    assert record.accounting.acct_group_user == "alice"
    assert record.accounting.accounting_group == "accounting.group"
    assert record.accounting.route_name == "route-a"
    assert record.accounting.last_match_name == "slot1@example"
    assert record.accounting.last_job_router_name == "router-a"


def test_token_group_resolution_uses_first_group() -> None:
    raw_identity = extract_raw_identity({"orig_AuthTokenGroups": "/dune,/dune/pilot"})
    resolved = resolve_reporting_identity(raw_identity, AccountingInfo(), owner="alice")

    assert resolved.vo == "dune"
    assert resolved.vo_group == "/dune"
    assert resolved.resolution_method == "token_groups"


def test_owner_heuristic_resolves_cms_pilot() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity({}),
        AccountingInfo(acct_group_user="cmspil000"),
        owner="cmspil000",
    )

    assert resolved.vo == "cms"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/cms/Role=pilot/Capability=NULL"


def test_construct_fqan_for_resolved_role() -> None:
    assert construct_fqan("eucliduk.net", "Role=pilot") == "/eucliduk.net/Role=pilot/Capability=NULL"


def test_unresolved_identity_falls_back_cleanly() -> None:
    resolved = resolve_reporting_identity(extract_raw_identity({}), AccountingInfo(), owner=None)

    assert resolved.vo is None
    assert resolved.fqan is None
    assert resolved.resolution_method == "unresolved"
