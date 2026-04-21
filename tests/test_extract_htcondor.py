import sys
from types import SimpleNamespace

sys.modules.setdefault("htcondor2", SimpleNamespace())

from htcondor_accounting.extract.htcondor import canonical_from_ad, detect_auth_method


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
