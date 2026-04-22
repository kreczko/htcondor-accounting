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
    message_md5: str


@dataclass(frozen=True)
class StagedMessageInfo:
    path: Path
    body: bytes
    bytes: int
    records: int
    message_md5: str


def message_md5_hex(message_bytes: bytes) -> str:
    return hashlib.md5(message_bytes).hexdigest()


def record_count_from_bytes(message_bytes: bytes) -> int:
    return message_bytes.decode("utf-8").count("%%\n")


def read_staged_message_info(staged_path: Path) -> StagedMessageInfo:
    body = staged_path.read_bytes()
    return StagedMessageInfo(
        path=staged_path,
        body=body,
        bytes=len(body),
        records=record_count_from_bytes(body),
        message_md5=message_md5_hex(body),
    )


def dirq_components_from_bytes(message_bytes: bytes) -> tuple[str, str]:
    digest = message_md5_hex(message_bytes)
    return digest[:8], digest[8:22]


def dirq_path_for_md5(outgoing_root: Path, digest: str) -> Path:
    return outgoing_root / digest[:8] / digest[8:22]


def dirq_path_for_bytes(outgoing_root: Path, message_bytes: bytes) -> Path:
    subdir, filename = dirq_components_from_bytes(message_bytes)
    return outgoing_root / subdir / filename


def promote_staged_message(staged_path: Path, outgoing_root: Path) -> DirqPromotionResult:
    info = read_staged_message_info(staged_path)
    queue_path = dirq_path_for_md5(outgoing_root, info.message_md5)
    queue_path.parent.mkdir(parents=True, exist_ok=True)

    if queue_path.exists():
        return DirqPromotionResult(
            staged_path=staged_path,
            queue_path=queue_path,
            written=False,
            bytes=info.bytes,
            message_md5=info.message_md5,
        )

    tmp_path = queue_path.parent / f".{queue_path.name}.tmp.{os.getpid()}"
    tmp_path.write_bytes(info.body)
    os.replace(tmp_path, queue_path)

    return DirqPromotionResult(
        staged_path=staged_path,
        queue_path=queue_path,
        written=True,
        bytes=info.bytes,
        message_md5=info.message_md5,
    )
