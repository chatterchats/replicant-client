#!/usr/bin/env python3
"""Reject private tooling and contract-source artifacts from the crate package."""

from __future__ import annotations

import subprocess
import sys


result = subprocess.run(
    ["cargo", "package", "--list", "--allow-dirty"], text=True, capture_output=True, check=False
)
if result.returncode:
    sys.stderr.write(result.stderr)
    raise SystemExit(result.returncode)

files = set(result.stdout.splitlines())
forbidden_prefixes = (".claude/", ".tokensave/", "reference/", "docs/", "policy/", "scripts/")
forbidden_suffixes = (".db", ".db-shm", ".db-wal", ".sqlite", ".sqlite3")
forbidden = sorted(
    path
    for path in files
    if path.startswith(forbidden_prefixes) or path.endswith(forbidden_suffixes)
)
required = {"Cargo.toml", "README.md", "LICENSE", "src/lib.rs"}
missing = sorted(required - files)

if forbidden or missing:
    if forbidden:
        print("forbidden package files: " + ", ".join(forbidden), file=sys.stderr)
    if missing:
        print("required package files missing: " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)

print(f"package contents check passed: {len(files)} files; no local tooling or reference corpus")
