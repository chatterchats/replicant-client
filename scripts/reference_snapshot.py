#!/usr/bin/env python3
"""Locate versioned Replicant Space reference snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

SNAPSHOT_RE = re.compile(r"^replicant-space-(\d+)-(\d+)-(\d+)$")


@dataclass(frozen=True, order=True)
class ReferenceSnapshot:
    """One checked-in Replicant Space contract snapshot."""

    version_tuple: tuple[int, int, int]
    path: Path

    @property
    def version(self) -> str:
        return ".".join(str(part) for part in self.version_tuple)


def reference_snapshots(root: Path) -> list[ReferenceSnapshot]:
    """Return all valid versioned reference snapshots, oldest first."""
    reference = root / "reference"
    snapshots: list[ReferenceSnapshot] = []
    if not reference.is_dir():
        return snapshots

    for path in reference.iterdir():
        if not path.is_dir():
            continue
        match = SNAPSHOT_RE.fullmatch(path.name)
        if match is None:
            continue
        snapshots.append(
            ReferenceSnapshot(
                tuple(int(part) for part in match.groups()),
                path,
            )
        )
    snapshots.sort()
    return snapshots


def latest_reference_snapshot(root: Path) -> ReferenceSnapshot:
    """Return the highest semantic-version reference snapshot."""
    snapshots = reference_snapshots(root)
    if not snapshots:
        raise FileNotFoundError(
            f"no reference/replicant-space-X-Y-Z snapshots found under {root}"
        )
    return snapshots[-1]
