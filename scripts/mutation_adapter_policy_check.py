#!/usr/bin/env python3
"""Verify Phase 11.6's managed/raw unsafe-operation partition."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
operations = json.loads((ROOT / "policy/operations.json").read_text())["operations"]
inventory = json.loads((ROOT / "policy/mutation-adapters.json").read_text())
supported = {
    f'{operation["method"]} {operation["path"]}'
    for operation in operations
    if operation["classification"] == "supported" and operation["safety"] == "mutating"
}
managed = set(inventory["managed"])
raw_only = set(inventory["raw_only"])

if managed & raw_only or managed | raw_only != supported:
    raise SystemExit("mutation adapter inventory must partition supported unsafe operations exactly")
if any(not adapter for adapter in inventory["managed"].values()):
    raise SystemExit("every managed unsafe operation requires a typed adapter")

print(f"mutation adapter policy: {len(managed)} managed, {len(raw_only)} raw-only")
