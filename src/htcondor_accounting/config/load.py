from __future__ import annotations

from pathlib import Path
from typing import Any

import tomllib

from htcondor_accounting.config.models import AppConfig


DEFAULT_CONFIG_PATHS = [
    Path("./site.toml"),
    Path("./examples/site-config.toml"),
    Path("/etc/htcondor-accounting/site.toml"),
]


def resolve_config_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path

    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.exists():
            return candidate

    return None


def load_config(path: Path | None = None) -> AppConfig:
    resolved_path = resolve_config_path(path)
    if resolved_path is not None:
        with resolved_path.open("rb") as stream:
            data: dict[str, Any] = tomllib.load(stream)
        return AppConfig.model_validate(data)

    return AppConfig()
