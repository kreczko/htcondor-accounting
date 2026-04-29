#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


DEFAULT_SHARED_ROOT = Path("/dice/admin/condor/accounting/apel")
DEFAULT_SPOOL_ROOT = Path("/var/spool/apel/outgoing")


@dataclass
class HandoffStats:
    outgoing_scanned: int = 0
    copied_to_spool: int = 0
    moved_to_retrieved: int = 0
    empty_outgoing_dirs_removed: int = 0
    retrieved_scanned: int = 0
    moved_to_sent: int = 0
    empty_retrieved_dirs_removed: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage shared-storage APEL handoff state for a CE host."
    )
    parser.add_argument(
        "--shared-root",
        type=Path,
        default=DEFAULT_SHARED_ROOT,
        help=f"Shared APEL root (default: {DEFAULT_SHARED_ROOT})",
    )
    parser.add_argument(
        "--spool-root",
        type=Path,
        default=DEFAULT_SPOOL_ROOT,
        help=f"Local CE spool root (default: {DEFAULT_SPOOL_ROOT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report actions without modifying files.",
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


def relative_under(root: Path, path: Path) -> Path:
    return path.relative_to(root)


def ensure_parent_dir(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file(source: Path, destination: Path, *, dry_run: bool) -> None:
    ensure_parent_dir(destination, dry_run=dry_run)
    if dry_run:
        return
    shutil.copy2(source, destination)


def move_file(source: Path, destination: Path, *, dry_run: bool) -> None:
    ensure_parent_dir(destination, dry_run=dry_run)
    if dry_run:
        return
    source.rename(destination)


def prune_empty_dirs(root: Path, *, dry_run: bool, verbose: bool) -> int:
    if not root.exists():
        return 0

    removed = 0
    directories = sorted((path for path in root.rglob("*") if path.is_dir()), reverse=True)
    for directory in directories:
        if directory == root:
            continue
        try:
            if any(directory.iterdir()):
                continue
        except FileNotFoundError:
            continue

        log(f"remove empty directory {directory}", verbose=verbose, dry_run=dry_run)
        if not dry_run:
            try:
                directory.rmdir()
            except FileNotFoundError:
                continue
            except OSError:
                continue
        removed += 1

    return removed


def copy_outgoing_to_spool(
    outgoing_file: Path,
    *,
    outgoing_root: Path,
    retrieved_root: Path,
    spool_root: Path,
    dry_run: bool,
    verbose: bool,
    stats: HandoffStats,
) -> None:
    relative_path = relative_under(outgoing_root, outgoing_file)
    spool_path = spool_root / relative_path
    retrieved_path = retrieved_root / relative_path

    if spool_path.exists():
        log(
            f"spool already has {relative_path}; treating as already copied",
            verbose=verbose,
            dry_run=dry_run,
        )
        stats.skipped += 1
    else:
        log(f"copy {outgoing_file} -> {spool_path}", verbose=verbose, dry_run=dry_run)
        copy_file(outgoing_file, spool_path, dry_run=dry_run)
        stats.copied_to_spool += 1

    if not dry_run and not spool_path.exists():
        raise RuntimeError(f"spool copy missing after copy attempt: {spool_path}")

    log(f"move {outgoing_file} -> {retrieved_path}", verbose=verbose, dry_run=dry_run)
    move_file(outgoing_file, retrieved_path, dry_run=dry_run)
    stats.moved_to_retrieved += 1


def move_retrieved_to_sent_if_consumed(
    retrieved_file: Path,
    *,
    retrieved_root: Path,
    sent_root: Path,
    spool_root: Path,
    dry_run: bool,
    verbose: bool,
    stats: HandoffStats,
) -> None:
    relative_path = relative_under(retrieved_root, retrieved_file)
    spool_path = spool_root / relative_path
    sent_path = sent_root / relative_path

    if spool_path.exists():
        log(
            f"spool still has {relative_path}; leaving in retrieved",
            verbose=verbose,
            dry_run=dry_run,
        )
        stats.skipped += 1
        return

    log(f"move {retrieved_file} -> {sent_path}", verbose=verbose, dry_run=dry_run)
    move_file(retrieved_file, sent_path, dry_run=dry_run)
    stats.moved_to_sent += 1


def process_outgoing(
    *,
    outgoing_root: Path,
    retrieved_root: Path,
    spool_root: Path,
    dry_run: bool,
    verbose: bool,
    stats: HandoffStats,
) -> None:
    for outgoing_file in iter_regular_files(outgoing_root):
        stats.outgoing_scanned += 1
        try:
            copy_outgoing_to_spool(
                outgoing_file,
                outgoing_root=outgoing_root,
                retrieved_root=retrieved_root,
                spool_root=spool_root,
                dry_run=dry_run,
                verbose=verbose,
                stats=stats,
            )
        except Exception as exc:  # noqa: BLE001
            stats.errors.append(f"outgoing {outgoing_file}: {exc}")
            log(
                f"error processing outgoing file {outgoing_file}: {exc}",
                verbose=True,
                dry_run=dry_run,
                always=True,
            )


def process_retrieved(
    *,
    retrieved_root: Path,
    sent_root: Path,
    spool_root: Path,
    dry_run: bool,
    verbose: bool,
    stats: HandoffStats,
) -> None:
    for retrieved_file in iter_regular_files(retrieved_root):
        stats.retrieved_scanned += 1
        try:
            move_retrieved_to_sent_if_consumed(
                retrieved_file,
                retrieved_root=retrieved_root,
                sent_root=sent_root,
                spool_root=spool_root,
                dry_run=dry_run,
                verbose=verbose,
                stats=stats,
            )
        except Exception as exc:  # noqa: BLE001
            stats.errors.append(f"retrieved {retrieved_file}: {exc}")
            log(
                f"error processing retrieved file {retrieved_file}: {exc}",
                verbose=True,
                dry_run=dry_run,
                always=True,
            )


def print_summary(stats: HandoffStats, *, dry_run: bool, shared_root: Path, spool_root: Path) -> None:
    title = "APEL CE handoff summary"
    prefix = "[dry-run] " if dry_run else ""
    print(prefix + title)
    print(f"  shared root                 = {shared_root}")
    print(f"  spool root                  = {spool_root}")
    print(f"  outgoing files scanned      = {stats.outgoing_scanned}")
    print(f"  files copied to local spool = {stats.copied_to_spool}")
    print(f"  outgoing moved to retrieved = {stats.moved_to_retrieved}")
    print(f"  empty outgoing dirs removed = {stats.empty_outgoing_dirs_removed}")
    print(f"  retrieved files scanned     = {stats.retrieved_scanned}")
    print(f"  retrieved moved to sent     = {stats.moved_to_sent}")
    print(f"  empty retrieved dirs removed = {stats.empty_retrieved_dirs_removed}")
    print(f"  files skipped               = {stats.skipped}")
    print(f"  errors                      = {len(stats.errors)}")
    if stats.errors:
        print("  error details:")
        for error in stats.errors:
            print(f"    - {error}")


def main() -> int:
    args = parse_args()
    shared_root = args.shared_root
    spool_root = args.spool_root
    outgoing_root = shared_root / "outgoing"
    retrieved_root = shared_root / "retrieved"
    sent_root = shared_root / "sent"

    stats = HandoffStats()

    process_outgoing(
        outgoing_root=outgoing_root,
        retrieved_root=retrieved_root,
        spool_root=spool_root,
        dry_run=args.dry_run,
        verbose=args.verbose,
        stats=stats,
    )
    stats.empty_outgoing_dirs_removed = prune_empty_dirs(
        outgoing_root,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    process_retrieved(
        retrieved_root=retrieved_root,
        sent_root=sent_root,
        spool_root=spool_root,
        dry_run=args.dry_run,
        verbose=args.verbose,
        stats=stats,
    )
    stats.empty_retrieved_dirs_removed = prune_empty_dirs(
        retrieved_root,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    print_summary(stats, dry_run=args.dry_run, shared_root=shared_root, spool_root=spool_root)
    return 1 if stats.errors else 0


if __name__ == "__main__":
    sys.exit(main())
