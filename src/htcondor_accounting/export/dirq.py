from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DirqPromotionResult:
    staged_path: Path
    queue_path: Path
    written: bool
    bytes: int


def message_md5_hex(message_bytes: bytes) -> str:
    return hashlib.md5(message_bytes).hexdigest()


def dirq_components_from_bytes(message_bytes: bytes) -> tuple[str, str]:
    digest = message_md5_hex(message_bytes)
    return digest[:8], digest[8:22]


def dirq_path_for_bytes(outgoing_root: Path, message_bytes: bytes) -> Path:
    subdir, filename = dirq_components_from_bytes(message_bytes)
    return outgoing_root / subdir / filename


def promote_staged_message(staged_path: Path, outgoing_root: Path) -> DirqPromotionResult:
    message_bytes = staged_path.read_bytes()
    queue_path = dirq_path_for_bytes(outgoing_root, message_bytes)
    queue_path.parent.mkdir(parents=True, exist_ok=True)

    if queue_path.exists():
        return DirqPromotionResult(
            staged_path=staged_path,
            queue_path=queue_path,
            written=False,
            bytes=len(message_bytes),
        )

    tmp_path = queue_path.parent / f".{queue_path.name}.tmp.{os.getpid()}"
    tmp_path.write_bytes(message_bytes)
    os.replace(tmp_path, queue_path)

    return DirqPromotionResult(
        staged_path=staged_path,
        queue_path=queue_path,
        written=True,
        bytes=len(message_bytes),
    )
