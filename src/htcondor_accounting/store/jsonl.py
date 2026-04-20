from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Any

import zstandard as zstd
from pydantic import BaseModel

from htcondor_accounting.store.layout import ensure_parent_dir

def _to_jsonable(record: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
    """Convert a supported record object into a JSON-serializable dictionary."""
    if isinstance(record, BaseModel):
        return record.model_dump(mode="json")
    return dict(record)

def write_jsonl_zst(path: Path, records: Iterable[BaseModel | Mapping[str, Any]]) -> int:
    """
    Write records to a compressed JSONL (.jsonl.zst) file.

    Returns the number of records written.
    """
    ensure_parent_dir(path)

    compressor = zstd.ZstdCompressor(level=3)
    count = 0

    with path.open("wb") as raw_stream:
        with compressor.stream_writer(raw_stream) as compressed_stream:
            for record in records:
                payload = _to_jsonable(record)
                line = json.dumps(payload, sort_keys=True) + "\n"
                compressed_stream.write(line.encode("utf-8"))
                count += 1

    return count


def read_jsonl_zst(path: Path) -> Iterator[dict[str, Any]]:
    """Read records from a compressed JSONL (.jsonl.zst) file."""
    decompressor = zstd.ZstdDecompressor()


    with path.open("rb") as raw_stream:
        with decompressor.stream_reader(raw_stream) as compressed_stream:
            for raw_line in compressed_stream:
                line = raw_line.decode("utf-8").strip()
                if not line:
                    continue
                yield json.loads(line)
