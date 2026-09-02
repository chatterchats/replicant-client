#!/usr/bin/env python3
"""Fixture test for repository, log, and database ZIP creation."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "repo_zip.py"


def write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def assert_archive(path: Path, expected_names: list[str]) -> None:
    with zipfile.ZipFile(path) as archive:
        assert archive.namelist() == expected_names, archive.namelist()
        assert all(item.compress_type == zipfile.ZIP_LZMA for item in archive.infolist())


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="replicant-repo-zip-") as directory:
        fixture = Path(directory)
        repository = fixture / "repository"
        data = fixture / "data"
        tracked = repository / "src" / "tracked.txt"
        write(tracked, b"repository content\n" * 100)
        write(data / "logs" / "app.log", b"app event\n" * 100)
        write(data / "logs" / "nested" / "worker.log", b"worker event\n" * 100)
        write(data / "logs" / "ignored.txt", b"not a log")
        write(data / "main.sqlite", b"SQLite format 3\0" + b"A" * 4096)
        write(data / "nested" / "cache.sqlite", b"SQLite format 3\0" + b"B" * 4096)
        write(data / "replicant-history.sqlite", b"excluded history")

        subprocess.run(["git", "init", "-q", str(repository)], check=True)
        subprocess.run(["git", "-C", str(repository), "add", str(tracked)], check=True)

        repository_zip = fixture / "snapshot-4z.zip"
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--include-local-data",
                "--data-dir",
                str(data),
                "--output",
                str(repository_zip),
            ],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr or result.stdout)

        assert_archive(repository_zip, ["src/tracked.txt"])
        assert_archive(
            fixture / "snapshot-logs-4z.zip",
            ["logs/app.log", "logs/nested/worker.log"],
        )
        assert_archive(
            fixture / "snapshot-databases-4z.zip",
            ["main.sqlite", "nested/cache.sqlite"],
        )

    print("repo ZIP fixture test passed: repository, logs, databases, exclusion, compression")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
