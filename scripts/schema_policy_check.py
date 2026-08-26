#!/usr/bin/env python3
"""Checks the checked-in persistence policy against all primary migrations."""
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
policy = json.loads((root / "policy/persistence-schema.json").read_text())
schema = "\n".join(
    (root / migration).read_text()
    for migration in policy.get("primary_migrations", [policy["migration"]])
)
missing = [name for name in policy["required_tables"] + policy["required_indexes"] if name not in schema]
if missing:
    raise SystemExit(f"schema policy missing: {', '.join(missing)}")
if "FOREIGN KEY" not in schema or "PRIMARY KEY (realm," not in schema:
    raise SystemExit("schema policy requires foreign and realm-qualified composite keys")

history_schema = "\n".join(
    (root / migration).read_text() for migration in policy.get("history_migrations", [])
)
history_required = policy.get("history_required_tables", []) + policy.get(
    "history_required_indexes", []
)
history_missing = [name for name in history_required if name not in history_schema]
if history_missing:
    raise SystemExit(f"history schema policy missing: {', '.join(history_missing)}")
history_columns = policy.get("history_required_columns", [])
column_missing = [name for name in history_columns if name not in history_schema]
if column_missing:
    raise SystemExit(
        f"history schema policy columns missing: {', '.join(column_missing)}"
    )
print("schema policy ok")
