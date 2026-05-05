#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


APEL_HEADER = "APEL-individual-job-message: v0.3"
DEFAULT_SOURCE_DIR = Path("/software/dice/accounting/apel/sent")
DEFAULT_DEST_DIR = Path("/software/dice/accounting/apel/outgoing")


@dataclass
class PatchStats:
    files_scanned: int = 0
    files_patched: int = 0
    skipped_already_patched: int = 0
    skipped_destination_exists: int = 0
    errors: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Patch missing APEL individual-job message headers and requeue messages."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"Directory to recursively scan for sent APEL messages (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--dest-dir",
        type=Path,
        default=DEFAULT_DEST_DIR,
        help=f"Outgoing queue directory to write patched messages into (default: {DEFAULT_DEST_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report actions without writing files.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-file actions.",
    )
    return parser.parse_args()


def log(message: str, *, verbose: bool, dry_run: bool = False, always: bool = False) -> None:
    if not always and not verbose:
        return
    prefix = "[dry-run] " if dry_run else ""
    print(prefix + message)


def iter_regular_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for path in sorted(root.rglob("*")):
        if path.is_file():
            yield path


def remove_one_leading_separator(text: str) -> tuple[str, bool]:
    if text.startswith("%%\r\n"):
        return text[4:], True
    if text.startswith("%%\n"):
        return text[3:], True
    if text == "%%" or text.startswith("%%\r"):
        return text[2:].lstrip("\r"), True
    return text, False


def remove_header(text: str) -> tuple[str, bool]:
    if not text.startswith(APEL_HEADER):
        return text, False

    body = text[len(APEL_HEADER) :]
    if body.startswith("\r\n"):
        body = body[2:]
    elif body.startswith("\n"):
        body = body[1:]
    return body, True


def trim_trailing_separator_blocks(body: str) -> str:
    text = body.rstrip()
    while text == "%%" or text.endswith("\n%%"):
        if text == "%%":
            text = ""
        else:
            text = text[: -len("\n%%")].rstrip()
    return text


def patch_message_text(text: str) -> str:
    body, had_header = remove_header(text)
    if not had_header:
        body, _ = remove_one_leading_separator(body)

    body = trim_trailing_separator_blocks(body)
    if body:
        patched = f"{APEL_HEADER}\n{body}\n%%\n"
    else:
        patched = f"{APEL_HEADER}\n%%\n"

    return patched


def md5_path(root: Path, message_bytes: bytes) -> Path:
    digest = hashlib.md5(message_bytes).hexdigest()
    return root / digest[:8] / digest[8:22]


def write_message(path: Path, message_bytes: bytes, *, dry_run: bool) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(message_bytes)


def process_file(
    source_path: Path,
    *,
    dest_dir: Path,
    dry_run: bool,
    verbose: bool,
    stats: PatchStats,
) -> None:
    stats.files_scanned += 1

    try:
        text = source_path.read_text(encoding="utf-8")
        patched_text = patch_message_text(text)
        if patched_text == text:
            stats.skipped_already_patched += 1
            log(f"already patched, skip write: {source_path}", verbose=verbose, dry_run=dry_run)
            return

        patched_bytes = patched_text.encode("utf-8")
        dest_path = md5_path(dest_dir, patched_bytes)

        if dest_path.exists():
            stats.skipped_destination_exists += 1
            log(
                f"destination exists, skip write: {source_path} -> {dest_path}",
                verbose=verbose,
                dry_run=dry_run,
            )
            return

        log(f"write patched message: {source_path} -> {dest_path}", verbose=verbose, dry_run=dry_run)
        write_message(dest_path, patched_bytes, dry_run=dry_run)
        stats.files_patched += 1
    except Exception as exc:
        stats.errors.append(f"{source_path}: {exc}")
        log(f"error processing {source_path}: {exc}", verbose=verbose, dry_run=dry_run, always=True)


def print_summary(stats: PatchStats) -> None:
    print("Summary:")
    print(f"  files scanned: {stats.files_scanned}")
    print(f"  files patched: {stats.files_patched}")
    print(f"  files skipped because already patched: {stats.skipped_already_patched}")
    print(f"  files skipped because destination exists: {stats.skipped_destination_exists}")
    print(f"  errors: {len(stats.errors)}")
    for error in stats.errors:
        print(f"    {error}")


def main() -> int:
    args = parse_args()
    stats = PatchStats()

    for source_path in iter_regular_files(args.source_dir):
        process_file(
            source_path,
            dest_dir=args.dest_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
            stats=stats,
        )

    print_summary(stats)
    return 1 if stats.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
