#!/usr/bin/env python3
"""Checks the checked-in fresh-schema policy against migration 0001."""
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
policy = json.loads((root / "policy/persistence-schema.json").read_text())
schema = (root / policy["migration"]).read_text()
missing = [name for name in policy["required_tables"] + policy["required_indexes"] if name not in schema]
if missing:
    raise SystemExit(f"schema policy missing: {', '.join(missing)}")
if "FOREIGN KEY" not in schema or "PRIMARY KEY (realm," not in schema:
    raise SystemExit("schema policy requires foreign and realm-qualified composite keys")
print("schema policy ok")
