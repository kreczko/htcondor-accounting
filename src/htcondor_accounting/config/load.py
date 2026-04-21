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


def load_config(path: Path | None = None) -> AppConfig:
    if path is not None:
        with path.open("rb") as stream:
            data: dict[str, Any] = tomllib.load(stream)
        return AppConfig.model_validate(data)

    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.exists():
            with candidate.open("rb") as stream:
                data = tomllib.load(stream)
            return AppConfig.model_validate(data)

    return AppConfig()
