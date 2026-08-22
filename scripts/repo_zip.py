#!/usr/bin/env python3
"""Create a clean ZIP snapshot of the current Git working tree.

The archive contains tracked files plus untracked files that are not ignored,
using the files as they exist in the working tree rather than the contents of
HEAD. Git metadata, Cargo build output, and SQLite databases are excluded.
Untracked files matched by the repository ignore rules are omitted using normal
Git semantics, except that versioned Replicant Space reference snapshots are
included as complete contract trees. Crawler-local HTML/cache artifacts remain
excluded. Tracked repository files remain included.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
from datetime import datetime
import zipfile


REFERENCE_SNAPSHOT_EXCLUDED_DIRS = frozenset({".source-html"})
REFERENCE_SNAPSHOT_EXCLUDED_FILES = frozenset({".crawl-cache.json"})


SQLITE_SUFFIXES = (
    ".db",
    ".db-shm",
    ".db-wal",
    ".sqlite",
    ".sqlite-shm",
    ".sqlite-wal",
    ".sqlite3",
)


def git(root: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode not in (0, 1):
        message = result.stderr.decode(errors="replace").strip()
        raise RuntimeError(message or f"git {' '.join(args)} failed")
    return result.stdout


def nul_paths(data: bytes) -> list[str]:
    return [os.fsdecode(item) for item in data.split(b"\0") if item]


def reference_snapshot_files(root: Path) -> set[str]:
    """Return complete versioned reference trees, excluding crawler-local caches."""
    reference = root / "reference"
    if not reference.is_dir():
        return set()

    paths: set[str] = set()
    for snapshot in reference.glob("replicant-space-*"):
        if not snapshot.is_dir():
            continue
        for source in snapshot.rglob("*"):
            if not source.is_file():
                continue
            relative_snapshot = source.relative_to(snapshot)
            if any(part in REFERENCE_SNAPSHOT_EXCLUDED_DIRS for part in relative_snapshot.parts):
                continue
            if source.name in REFERENCE_SNAPSHOT_EXCLUDED_FILES:
                continue
            paths.add(source.relative_to(root).as_posix())
    return paths


def explicitly_excluded(path: str) -> bool:
    posix = PurePosixPath(path)
    if ".git" in posix.parts or "target" in posix.parts:
        return True
    if any(part in REFERENCE_SNAPSHOT_EXCLUDED_DIRS for part in posix.parts):
        return True
    if posix.name in REFERENCE_SNAPSHOT_EXCLUDED_FILES:
        return True
    lower = path.lower()
    return lower.endswith(SQLITE_SUFFIXES)


def default_output(root: Path) -> Path:
    stamp = datetime.now().astimezone().strftime("%m%d%y-%H%M")
    # Match the user's handoff naming style by dropping the leading zero from
    # January through September: 081126 -> 81126.
    stamp = stamp[1:] if stamp.startswith("0") else stamp
    return root / f"{root.name}-{stamp}-4z.zip"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a clean ZIP snapshot of the current Git working tree."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="output ZIP path (default: <repo>-<MDDYY-HHMM>-4z.zip in repo root)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        top_level = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"error: run this command from inside a Git repository: {error}", file=sys.stderr)
        return 2

    root = Path(top_level).resolve()
    output = args.output or default_output(root)
    if not output.is_absolute():
        output = root / output
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    try:
        candidates = set(
            nul_paths(
                git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
            )
        )
        candidates.update(reference_snapshot_files(root))
    except (OSError, RuntimeError) as error:
        print(f"error: unable to enumerate repository files: {error}", file=sys.stderr)
        return 2

    files: list[tuple[str, Path]] = []
    for relative in sorted(candidates):
        if explicitly_excluded(relative):
            continue
        source = root / relative
        if not source.is_file():
            continue
        if source.resolve() == output:
            continue
        files.append((relative, source))

    if output.exists():
        output.unlink()

    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
        allowZip64=True,
    ) as archive:
        for relative, source in files:
            archive.write(source, arcname=PurePosixPath(relative).as_posix())

    size_mib = output.stat().st_size / (1024 * 1024)
    print(f"Created {output}")
    print(f"Included {len(files)} files ({size_mib:.2f} MiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
