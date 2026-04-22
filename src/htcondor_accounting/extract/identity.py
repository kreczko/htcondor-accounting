from __future__ import annotations

from typing import Any

from htcondor_accounting.models.canonical import AccountingInfo, IdentityInfo, ResolvedIdentityInfo


def _ad_str(ad: dict[str, Any], key: str) -> str | None:
    value = ad.get(key)
    if value is None:
        return None
    return str(value)


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


def _parse_fqan(value: str | None) -> tuple[str | None, str | None, str | None]:
    if not value:
        return None, None, None

    parts = [part for part in value.split("/") if part]
    if not parts:
        return None, None, None

    vo = parts[0]
    role = next((part for part in parts if part.startswith("Role=")), None)
    return vo, f"/{vo}", role


def construct_fqan(vo: str | None, role: str | None) -> str | None:
    if not vo:
        return None
    if role:
        return f"/{vo}/{role}/Capability=NULL"
    return f"/{vo}"


def extract_raw_identity(ad: dict[str, Any]) -> IdentityInfo:
    raw_fqan = _ad_str(ad, "x509UserProxyFirstFQAN")
    raw_vo, raw_vo_group, raw_vo_role = _parse_fqan(raw_fqan)

    return IdentityInfo(
        dn=_ad_str(ad, "x509UserProxySubject") or _ad_str(ad, "x509userproxysubject"),
        fqan=raw_fqan,
        vo=raw_vo,
        vo_group=raw_vo_group,
        vo_role=raw_vo_role,
        auth_method=detect_auth_method(ad),
        token_issuer=_ad_str(ad, "orig_AuthTokenIssuer"),
        token_subject=_ad_str(ad, "orig_AuthTokenSubject"),
        token_groups=ad_token_groups(ad),
    )


def _pilot_role_from_strings(*values: str | None) -> str | None:
    for value in values:
        if value and ("pilot" in value.lower() or "pil" in value.lower()):
            return "Role=pilot"
    return None


def _resolve_from_token_groups(raw_identity: IdentityInfo) -> tuple[str | None, str | None]:
    if not raw_identity.token_groups:
        return None, None
    first_group = raw_identity.token_groups[0]
    parts = [part for part in first_group.split("/") if part]
    if not parts:
        return None, None
    return parts[0], first_group


def _infer_vo_from_text(value: str) -> str | None:
    lowered = value.lower()
    mappings = [
        ("cmspil", "cms"),
        ("cms", "cms"),
        ("lhcb", "lhcb"),
        ("dune", "dune"),
        ("na62", "na62.vo.gridpp.ac.uk"),
        ("euclid", "eucliduk.net"),
        ("mu3e", "mu3e.org"),
        ("alice", "alice"),
        ("atlas", "atlas"),
    ]
    for needle, vo in mappings:
        if needle in lowered:
            return vo
    return None


def _resolve_from_accounting(accounting: AccountingInfo, owner: str | None) -> tuple[str | None, str | None, str | None]:
    candidates = [
        ("acct_group", accounting.acct_group),
        ("acct_group_user", accounting.acct_group_user),
        ("accounting_group", accounting.accounting_group),
        ("route_name", accounting.route_name),
        ("owner_heuristic", owner),
    ]
    for method, value in candidates:
        if not value:
            continue
        vo = _infer_vo_from_text(value)
        if vo:
            return vo, method, value
    return None, None, None


def resolve_reporting_identity(
    raw_identity: IdentityInfo,
    accounting: AccountingInfo,
    owner: str | None = None,
) -> ResolvedIdentityInfo:
    role = raw_identity.vo_role
    if role is None:
        role = _pilot_role_from_strings(
            owner,
            accounting.acct_group_user,
            accounting.acct_group,
            accounting.route_name,
        )

    token_vo, token_evidence = _resolve_from_token_groups(raw_identity)
    if token_vo:
        return ResolvedIdentityInfo(
            vo=token_vo,
            vo_group=f"/{token_vo}",
            vo_role=role,
            fqan=construct_fqan(token_vo, role),
            resolution_method="token_groups",
            resolution_evidence=token_evidence,
            is_pilot=role == "Role=pilot",
        )

    accounting_vo, accounting_method, accounting_evidence = _resolve_from_accounting(accounting, owner)
    if accounting_vo:
        return ResolvedIdentityInfo(
            vo=accounting_vo,
            vo_group=f"/{accounting_vo}",
            vo_role=role,
            fqan=construct_fqan(accounting_vo, role),
            resolution_method=accounting_method or "owner_heuristic",
            resolution_evidence=accounting_evidence,
            is_pilot=role == "Role=pilot",
        )

    raw_fqan_vo, _, raw_fqan_role = _parse_fqan(raw_identity.fqan)
    if raw_fqan_vo:
        resolved_role = raw_fqan_role or role
        return ResolvedIdentityInfo(
            vo=raw_fqan_vo,
            vo_group=f"/{raw_fqan_vo}",
            vo_role=resolved_role,
            fqan=construct_fqan(raw_fqan_vo, resolved_role),
            resolution_method="raw_fqan",
            resolution_evidence=raw_identity.fqan,
            is_pilot=resolved_role == "Role=pilot",
        )

    return ResolvedIdentityInfo(
        vo=None,
        vo_group=None,
        vo_role=role,
        fqan=None,
        resolution_method="unresolved",
        resolution_evidence=None,
        is_pilot=role == "Role=pilot" if role else None,
    )
