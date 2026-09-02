#!/usr/bin/env python3
"""Create compressed ZIP snapshots of the repository and optional local data.

The repository archive contains tracked files plus untracked files that are not
ignored, using the files as they exist in the working tree rather than the
contents of HEAD. Git metadata, Cargo build output, and SQLite databases are
excluded. Untracked files matched by the repository ignore rules are omitted
using normal Git semantics, except that versioned Replicant Space reference
snapshots are included as complete contract trees. Crawler-local HTML/cache
artifacts remain excluded. Tracked repository files remain included.

With ``--include-local-data``, separate archives are also created for every
``.log`` file below ``~/.local/share/replicant/logs`` and every ``.sqlite`` file
below ``~/.local/share/replicant`` except ``replicant-history.sqlite``.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import zipfile


REFERENCE_SNAPSHOT_EXCLUDED_DIRS = frozenset({".source-html"})
REFERENCE_SNAPSHOT_EXCLUDED_FILES = frozenset({".crawl-cache.json"})
LOCAL_DATA_DIR = Path.home() / ".local" / "share" / "replicant"
LOGS_DIR_NAME = "logs"
TELEMETRY_DIR_NAME = "telemetry"
EXCLUDED_DATABASE_NAME = "replicant-history.sqlite"
ZIP_COMPRESSION = zipfile.ZIP_LZMA

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

def companion_output(output: Path, kind: str) -> Path:
    """Return a sibling output path for a local-data archive."""
    suffix = "-4z"
    stem = output.stem
    if stem.endswith(suffix):
        stem = f"{stem[:-len(suffix)]}-{kind}{suffix}"
    else:
        stem = f"{stem}-{kind}"
    return output.with_name(f"{stem}{output.suffix}")


def matching_files(root: Path, pattern: str) -> list[tuple[str, Path]]:
    """Return matching files below root with portable relative archive names."""
    return [
        (source.relative_to(root).as_posix(), source)
        for source in sorted(root.rglob(pattern))
        if source.is_file()
    ]


def write_archive(output: Path, files: list[tuple[str, Path]]) -> None:
    """Write files using ZIP's strongest standard-library compression method."""
    if output.exists():
        output.unlink()

    with zipfile.ZipFile(
        output,
        mode="w",
        compression=ZIP_COMPRESSION,
        allowZip64=True,
    ) as archive:
        for relative, source in files:
            archive.write(source, arcname=PurePosixPath(relative).as_posix())


def print_archive_result(output: Path, file_count: int) -> None:
    size_mib = output.stat().st_size / (1024 * 1024)
    print(f"Created {output}")
    print(f"Included {file_count} files ({size_mib:.2f} MiB)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a clean ZIP snapshot of the current Git working tree."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="output repository ZIP path (default: <repo>-<MDDYY-HHMM>-4z.zip)",
    )
    parser.add_argument(
        "--include-local-data",
        action="store_true",
        help="also archive local Replicant logs and databases into separate ZIPs",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=LOCAL_DATA_DIR,
        help=argparse.SUPPRESS,
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
    outputs = {output}
    log_files: list[tuple[str, Path]] = []
    database_files: list[tuple[str, Path]] = []
    logs_output: Path | None = None
    databases_output: Path | None = None
    if args.include_local_data:
        data_dir = args.data_dir.expanduser().resolve()
        logs_dir = data_dir / LOGS_DIR_NAME
        if not logs_dir.is_dir():
            print(f"error: Replicant logs directory does not exist: {logs_dir}", file=sys.stderr)
            return 2

        log_files = [
            (f"{LOGS_DIR_NAME}/{relative}", source)
            for relative, source in matching_files(logs_dir, "*.log")
        ]
        database_files = [
            (relative, source)
            for relative, source in matching_files(data_dir, "*.sqlite")
            if source.name != EXCLUDED_DATABASE_NAME
            and TELEMETRY_DIR_NAME not in PurePosixPath(relative).parts
        ]
        logs_output = companion_output(output, "logs")
        databases_output = companion_output(output, "databases")
        outputs.update((logs_output, databases_output))


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
        if source.resolve() in outputs:
            continue
        files.append((relative, source))

    write_archive(output, files)
    print_archive_result(output, len(files))

    if logs_output is not None and databases_output is not None:
        write_archive(logs_output, log_files)
        print_archive_result(logs_output, len(log_files))
        write_archive(databases_output, database_files)
        print_archive_result(databases_output, len(database_files))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
