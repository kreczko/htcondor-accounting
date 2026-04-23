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


def test_owner_heuristic_resolves_cms_admin_with_sgm_precedence() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity({}),
        AccountingInfo(acct_group_user="cmssgm", acct_group="cmspil"),
        owner="cmssgm",
    )

    assert resolved.vo == "cms"
    assert resolved.vo_role == "Role=admin"
    assert resolved.fqan == "/cms/Role=admin/Capability=NULL"


def test_construct_fqan_for_resolved_role() -> None:
    assert construct_fqan("eucliduk.net", "Role=pilot") == "/eucliduk.net/Role=pilot/Capability=NULL"


def test_scitoken_identity_plus_pilot_role_constructs_full_fqan() -> None:
    raw_identity = extract_raw_identity(
        {
            "orig_AuthTokenIssuer": "https://issuer.example",
            "orig_AuthTokenSubject": "subject",
            "orig_AuthTokenGroups": "/cms",
        }
    )
    resolved = resolve_reporting_identity(
        raw_identity,
        AccountingInfo(acct_group_user="cmspil000"),
        owner="cmspil000",
    )

    assert resolved.vo == "cms"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/cms/Role=pilot/Capability=NULL"


def test_scitoken_identity_plus_admin_role_constructs_full_fqan() -> None:
    raw_identity = extract_raw_identity(
        {
            "orig_AuthTokenIssuer": "https://issuer.example",
            "orig_AuthTokenSubject": "subject",
            "orig_AuthTokenGroups": "/cms",
        }
    )
    resolved = resolve_reporting_identity(
        raw_identity,
        AccountingInfo(acct_group_user="cmssgm"),
        owner="cmssgm",
    )

    assert resolved.vo == "cms"
    assert resolved.vo_role == "Role=admin"
    assert resolved.fqan == "/cms/Role=admin/Capability=NULL"


def test_x509_first_fqan_takes_precedence_over_owner_heuristics() -> None:
    raw_identity = extract_raw_identity(
        {
            "x509UserProxyFirstFQAN": "/lhcb/Role=pilot/Capability=NULL",
            "x509UserProxyVOName": "lhcb",
        }
    )
    resolved = resolve_reporting_identity(
        raw_identity,
        AccountingInfo(acct_group_user="lhcb031"),
        owner="lhcb031",
    )

    assert resolved.vo == "lhcb"
    assert resolved.vo_group == "/lhcb"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/lhcb/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "x509_first_fqan"


def test_orig_x509_first_fqan_takes_precedence_over_owner_heuristics() -> None:
    raw_identity = extract_raw_identity(
        {
            "orig_x509UserProxyFirstFQAN": "/lhcb/Role=pilot/Capability=NULL",
            "orig_x509UserProxyVOName": "lhcb",
            "x509UserProxyFirstFQAN": "/lhcb",
        }
    )
    resolved = resolve_reporting_identity(
        raw_identity,
        AccountingInfo(acct_group_user="lhcb031"),
        owner="lhcb031",
    )

    assert resolved.vo == "lhcb"
    assert resolved.vo_group == "/lhcb"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/lhcb/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "orig_x509_first_fqan"


def test_orig_x509_vo_name_resolves_vo_and_vogroup() -> None:
    raw_identity = extract_raw_identity(
        {
            "orig_x509UserProxyVOName": "dune",
        }
    )
    resolved = resolve_reporting_identity(raw_identity, AccountingInfo(), owner="alice")

    assert resolved.vo == "dune"
    assert resolved.vo_group == "/dune"
    assert resolved.vo_role is None
    assert resolved.fqan == "/dune"
    assert resolved.resolution_method == "orig_x509_vo_name"


def test_x509_email_resolves_lhcb_pilot_for_older_jobs() -> None:
    raw_identity = extract_raw_identity({"x509UserProxyEmail": "lb.pilot@cern.ch"})
    resolved = resolve_reporting_identity(raw_identity, AccountingInfo(), owner="lhcb031")

    assert resolved.vo == "lhcb"
    assert resolved.vo_group == "/lhcb"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/lhcb/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "x509_email"


def test_x509_subject_resolves_lhcb_pilot_for_older_jobs() -> None:
    raw_identity = extract_raw_identity({"x509userproxysubject": "/DC=ch/CN=lbpilot"})
    resolved = resolve_reporting_identity(raw_identity, AccountingInfo(), owner="lhcb031")

    assert resolved.vo == "lhcb"
    assert resolved.vo_group == "/lhcb"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/lhcb/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "x509_subject"


def test_route_name_gridpp_resolves_with_existing_pilot_role() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity(
            {
                "orig_AuthTokenIssuer": "https://issuer.example",
                "orig_AuthTokenSubject": "subject",
                "x509userproxysubject": "/C=UK/O=eScience/CN=dirac-pilot.grid.hep.ph.ic.ac.uk",
            }
        ),
        AccountingInfo(route_name="gridpp"),
        owner="gridpp001",
    )

    assert resolved.vo == "gridpp"
    assert resolved.vo_group == "/gridpp"
    assert resolved.vo_role == "Role=pilot"
    assert resolved.fqan == "/gridpp/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "route_name"
    assert resolved.resolution_evidence == "gridpp"


def test_route_name_gridpp_resolves_without_role() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity(
            {
                "orig_AuthTokenIssuer": "https://issuer.example",
                "orig_AuthTokenSubject": "subject",
            }
        ),
        AccountingInfo(route_name="gridpp"),
        owner="alice",
    )

    assert resolved.vo == "gridpp"
    assert resolved.vo_group == "/gridpp"
    assert resolved.vo_role is None
    assert resolved.fqan == "/gridpp"
    assert resolved.resolution_method == "route_name"
    assert resolved.resolution_evidence == "gridpp"


def test_route_name_gridpp_does_not_override_token_group_vo() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity(
            {
                "orig_AuthTokenIssuer": "https://issuer.example",
                "orig_AuthTokenSubject": "subject",
                "orig_AuthTokenGroups": "/cms",
            }
        ),
        AccountingInfo(route_name="gridpp"),
        owner="cmspil000",
    )

    assert resolved.vo == "cms"
    assert resolved.fqan == "/cms/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "token_groups"


def test_route_name_gridpp_does_not_override_explicit_fqan() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity(
            {
                "x509UserProxyFirstFQAN": "/lhcb/Role=pilot/Capability=NULL",
                "orig_AuthTokenIssuer": "https://issuer.example",
                "orig_AuthTokenSubject": "subject",
            }
        ),
        AccountingInfo(route_name="gridpp"),
        owner="gridpp001",
    )

    assert resolved.vo == "lhcb"
    assert resolved.fqan == "/lhcb/Role=pilot/Capability=NULL"
    assert resolved.resolution_method == "x509_first_fqan"


def test_unrelated_route_names_are_not_auto_mapped() -> None:
    resolved = resolve_reporting_identity(
        extract_raw_identity(
            {
                "orig_AuthTokenIssuer": "https://issuer.example",
                "orig_AuthTokenSubject": "subject",
            }
        ),
        AccountingInfo(route_name="mystery-route"),
        owner="user123",
    )

    assert resolved.vo is None
    assert resolved.fqan is None
    assert resolved.resolution_method == "unresolved"


def test_canonical_from_ad_preserves_forwarded_identity_fields() -> None:
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
            "x509UserProxyEmail": "alice@example.org",
            "orig_x509userproxysubject": "/C=UK/O=eScience/CN=alice",
            "orig_x509UserProxyFirstFQAN": "/dune/Role=pilot/Capability=NULL",
            "orig_x509UserProxyVOName": "dune",
            "orig_x509UserProxyFQAN": "/dune,/dune/Role=pilot/Capability=NULL",
        },
        site_name="TEST-SITE",
        schedd_name="schedd.example",
    )

    assert record.identity.x509_email == "alice@example.org"
    assert record.identity.orig_dn == "/C=UK/O=eScience/CN=alice"
    assert record.identity.orig_fqan == "/dune/Role=pilot/Capability=NULL"
    assert record.identity.orig_vo_name == "dune"
    assert record.identity.orig_fqan_list == "/dune,/dune/Role=pilot/Capability=NULL"


def test_unresolved_identity_falls_back_cleanly() -> None:
    resolved = resolve_reporting_identity(extract_raw_identity({}), AccountingInfo(), owner=None)

    assert resolved.vo is None
    assert resolved.fqan is None
    assert resolved.resolution_method == "unresolved"
