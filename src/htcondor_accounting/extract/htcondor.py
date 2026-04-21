from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import htcondor2 as htcondor

from htcondor_accounting.models.canonical import (
    AccountingInfo,
    BenchmarkInfo,
    CanonicalJobRecord,
    ExecutionInfo,
    IdentityInfo,
    JobInfo,
    SourceInfo,
    TimingInfo,
    UsageInfo,
)


@dataclass(frozen=True)
class HistoryQuery:
    schedd_name: str | None = None
    since: str | int | None = None
    match: int = 100
    constraint: str = "JobStatus == 4"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ad_str(ad: dict[str, Any], key: str) -> str | None:
    value = ad.get(key)
    if value is None:
        return None
    return str(value)


def ad_int(ad: dict[str, Any], key: str) -> int | None:
    value = ad.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def ad_float(ad: dict[str, Any], key: str) -> float | None:
    value = ad.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def ad_token_groups(ad: dict[str, Any]) -> list[str]:
    value = ad.get("orig_AuthTokenGroups")
    if value is None:
        return []

    if isinstance(value, str):
        return [group.strip() for group in value.split(",") if group.strip()]

    return []


def detect_auth_method(ad: dict[str, Any]) -> str:
    if ad.get("orig_AuthTokenIssuer") or ad.get("orig_AuthTokenSubject"):
        return "scitoken"
    if ad.get("x509UserProxySubject") or ad.get("x509userproxysubject"):
        return "x509"
    return "local"


def resolve_identity(ad: dict[str, Any]) -> IdentityInfo:
    token_groups = ad_token_groups(ad)
    fqan = ad_str(ad, "x509UserProxyFirstFQAN")
    vo = None
    vo_group = None
    vo_role = None

    if fqan:
        parts = [part for part in fqan.split("/") if part]
        if parts:
            vo = parts[0]
            if len(parts) > 1 and not parts[1].startswith("Role="):
                vo_group = "/" + "/".join(parts[:2])
            if "Role=" in fqan:
                vo_role = fqan.split("Role=", 1)[1].split("/", 1)[0]
    elif token_groups:
        first_group = token_groups[0]
        fqan = first_group
        parts = [part for part in first_group.split("/") if part]
        if parts:
            vo = parts[0]
            if len(parts) > 1:
                vo_group = "/" + "/".join(parts[:2])

    return IdentityInfo(
        dn=ad_str(ad, "x509UserProxySubject") or ad_str(ad, "x509userproxysubject"),
        fqan=fqan,
        vo=vo,
        vo_group=vo_group,
        vo_role=vo_role,
        auth_method=detect_auth_method(ad),
        token_issuer=ad_str(ad, "orig_AuthTokenIssuer"),
        token_subject=ad_str(ad, "orig_AuthTokenSubject"),
        token_groups=token_groups,
    )


def canonical_from_ad(
    ad: dict[str, Any],
    site_name: str,
    schedd_name: str,
) -> CanonicalJobRecord:
    global_job_id = ad_str(ad, "GlobalJobId") or "<missing-global-job-id>"
    owner = ad_str(ad, "Owner") or "<unknown-owner>"

    return CanonicalJobRecord(
        site_name=site_name,
        source=SourceInfo(
            schedd=schedd_name,
            collected_at=utc_now_iso(),
        ),
        job=JobInfo(
            global_job_id=global_job_id,
            routed_from_job_id=ad_str(ad, "RoutedFromJobId"),
            owner=owner,
            local_user=owner,
        ),
        usage=UsageInfo(
            wall_seconds=ad_int(ad, "RemoteWallClockTime") or 0,
            cpu_user_seconds=ad_int(ad, "RemoteUserCpu") or 0,
            cpu_sys_seconds=ad_int(ad, "RemoteSysCpu") or 0,
            processors=ad_int(ad, "RequestCpus") or 1,
            memory_real_kb=ad_int(ad, "ResidentSetSize_RAW"),
            memory_virtual_kb=ad_int(ad, "ImageSize_RAW"),
        ),
        timing=TimingInfo(
            queue_time=ad_int(ad, "QDate"),
            start_time=ad_int(ad, "JobStartDate"),
            end_time=ad_int(ad, "CompletionDate") or ad_int(ad, "EnteredCurrentStatus"),
            status_time=ad_int(ad, "EnteredCurrentStatus"),
        ),
        identity=resolve_identity(ad),
        accounting=AccountingInfo(
            acct_group=ad_str(ad, "AcctGroup"),
            acct_group_user=ad_str(ad, "AcctGroupUser"),
            accounting_group=ad_str(ad, "AccountingGroup"),
            route_name=ad_str(ad, "RouteName"),
            last_match_name=ad_str(ad, "LastMatchName"),
            last_job_router_name=ad_str(ad, "LastJobRouterName"),
        ),
        benchmark=BenchmarkInfo(
            benchmark_type="hepscore23"
            if ad.get("MachineAttrACCOUNTING_SCALE_FACTOR0") is not None
            else None,
            scale_factor=ad_float(ad, "MachineAttrACCOUNTING_SCALE_FACTOR0"),
        ),
        execution=ExecutionInfo(
            ce_host=schedd_name,
            ce_id=None,
            execute_node=ad_str(ad, "LastRemoteHost"),
            slot_name=ad_str(ad, "RemoteHost"),
        ),
    )


def get_schedd(schedd_name: str | None = None) -> tuple[htcondor.Schedd, str]:
    if schedd_name:
        collector = htcondor.Collector()
        schedd_ad = collector.locate(htcondor.DaemonType.Schedd, schedd_name)
        return htcondor.Schedd(schedd_ad), schedd_name

    schedd = htcondor.Schedd()
    return schedd, "local"


def fetch_history_ads(query: HistoryQuery) -> list[dict[str, Any]]:
    schedd, resolved_name = get_schedd(query.schedd_name)

    projection = [
        "GlobalJobId",
        "Owner",
        "RoutedFromJobId",
        "RemoteWallClockTime",
        "RemoteUserCpu",
        "RemoteSysCpu",
        "JobStartDate",
        "CompletionDate",
        "EnteredCurrentStatus",
        "QDate",
        "ResidentSetSize_RAW",
        "ImageSize_RAW",
        "RequestCpus",
        "LastRemoteHost",
        "RemoteHost",
        "MachineAttrACCOUNTING_SCALE_FACTOR0",
        "x509UserProxySubject",
        "x509userproxysubject",
        "x509UserProxyFirstFQAN",
        "orig_AuthTokenIssuer",
        "orig_AuthTokenSubject",
        "orig_AuthTokenGroups",
        "AcctGroup",
        "AcctGroupUser",
        "AccountingGroup",
        "RouteName",
        "LastMatchName",
        "LastJobRouterName",
    ]

    ads = schedd.history(
        constraint=query.constraint,
        projection=projection,
        match=query.match,
        since=query.since,
    )

    result: list[dict[str, Any]] = []
    for ad in ads:
        row = {key: ad.get(key) for key in projection}
        row["_schedd_name"] = resolved_name
        result.append(row)

    return result


def extract_canonical_records(
    site_name: str,
    query: HistoryQuery,
) -> list[CanonicalJobRecord]:
    ads = fetch_history_ads(query)
    records: list[CanonicalJobRecord] = []

    for ad in ads:
        schedd_name = str(ad.pop("_schedd_name", query.schedd_name or "local"))
        records.append(canonical_from_ad(ad, site_name=site_name, schedd_name=schedd_name))

    return records


def extract_many_canonical_records(
    site_name: str,
    schedd_names: list[str] | None,
    base_query: HistoryQuery,
) -> dict[str, list[CanonicalJobRecord]]:
    """
    Extract canonical records from one or more schedds.

    Returns a mapping:
        schedd_name -> list[CanonicalJobRecord]
    """
    results: dict[str, list[CanonicalJobRecord]] = {}

    if not schedd_names:
        local_query = HistoryQuery(
            schedd_name=None,
            since=base_query.since,
            match=base_query.match,
            constraint=base_query.constraint,
        )
        results["local"] = extract_canonical_records(site_name=site_name, query=local_query)
        return results

    for schedd_name in schedd_names:
        query = HistoryQuery(
            schedd_name=schedd_name,
            since=base_query.since,
            match=base_query.match,
            constraint=base_query.constraint,
        )
        results[schedd_name] = extract_canonical_records(site_name=site_name, query=query)

    return results
