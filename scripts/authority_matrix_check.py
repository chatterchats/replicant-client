#!/usr/bin/env python3
"""Ensure every supported operation has one precise Phase 3 authority rule."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
inventory = json.loads((ROOT / "policy/operations.json").read_text())
matrix = json.loads((ROOT / "policy/authority-matrix.json").read_text())
expected = {(entry["method"], entry["path"]) for entry in inventory["operations"] if entry["classification"] == "supported"}
actual = {(entry["method"], entry["path"]) for entry in matrix["operations"]}
errors = []
if expected != actual:
    errors.append(f"matrix mismatch: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}")
for entry in matrix["operations"]:
    if not all(entry.get(key) for key in ("authority", "completeness", "tombstone")):
        errors.append(f"incomplete authority rule: {entry['method']} {entry['path']}")
if errors:
    print("authority matrix check FAILED:", *errors, sep="\n  - ", file=sys.stderr)
    sys.exit(1)
print(f"authority matrix check passed: {len(actual)} supported operations covered")
