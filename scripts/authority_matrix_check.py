#!/usr/bin/env python3
"""Validate operation authority and executable durable-refresh policy metadata."""

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
inventory = json.loads((ROOT / "policy/operations.json").read_text())
matrix = json.loads((ROOT / "policy/authority-matrix.json").read_text())
sync_bytes = (ROOT / "policy/sync-domains.json").read_bytes()
sync = json.loads(sync_bytes)
expected = {
    (entry["method"], entry["path"])
    for entry in inventory["operations"]
    if entry["classification"] == "supported"
}
deltas = json.loads((ROOT / "policy/documented-operation-deltas.json").read_text())
expected.update(
    (entry["method"], entry["path"])
    for entry in deltas["operations"]
    if entry["classification"] == "supported"
)
actual = {(entry["method"], entry["path"]) for entry in matrix["operations"]}
errors = []
if expected != actual:
    errors.append(
        f"matrix mismatch: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
    )
for entry in matrix["operations"]:
    if not all(entry.get(key) for key in ("authority", "completeness", "tombstone")):
        errors.append(f"incomplete authority rule: {entry['method']} {entry['path']}")

if sync.get("version") != 2:
    errors.append("sync policy version must be 2")
expected_digest = hashlib.sha256(sync_bytes).hexdigest()
if matrix.get("sync_domain_policy_sha256") != expected_digest:
    errors.append("authority matrix sync policy digest is stale")

rest_plan = ["account", "devices", "replicants", "locations", "inventory", "simulations"]
full_plan = [
    "account", "devices", "replicants", "stars", "systems", "bodies",
    "events", "messages", "locations", "inventory", "simulations",
]
if sync.get("rest_plan") != rest_plan:
    errors.append("rest_plan order does not match the bounded managed sync contract")
if sync.get("full_plan") != full_plan:
    errors.append("full_plan order does not match the durable refresh contract")

domains = sync.get("domains", {})
if set(domains) != set(full_plan):
    errors.append(
        f"domain names mismatch: missing={sorted(set(full_plan) - set(domains))}, "
        f"extra={sorted(set(domains) - set(full_plan))}"
    )
if len(full_plan) != len(set(full_plan)) or len(rest_plan) != len(set(rest_plan)):
    errors.append("sync plans contain duplicate phase names")

allowed_reconciliation = {
    "never",
    "terminal_unfiltered_cursor_with_guards",
    "complete_single_response_with_total_and_guards",
}
supported = expected
for name, rule in domains.items():
    required = {"depends_on", "endpoints", "bound", "checkpoint", "absence_reconciliation"}
    if set(rule) != required:
        errors.append(f"{name} has unexpected keys: {sorted(set(rule) ^ required)}")
    dependencies = rule.get("depends_on", [])
    if len(dependencies) != len(set(dependencies)):
        errors.append(f"{name} has duplicate dependencies")
    for dependency in dependencies:
        if dependency not in domains:
            errors.append(f"{name} depends on unknown phase {dependency}")
    if rule.get("absence_reconciliation") not in allowed_reconciliation:
        errors.append(f"{name} has unsupported absence reconciliation")
    endpoints = rule.get("endpoints", [])
    if not endpoints or len(endpoints) != len(set(endpoints)):
        errors.append(f"{name} endpoints must be a non-empty unique array")
    for endpoint in endpoints:
        try:
            method, path = endpoint.split(" ", 1)
        except ValueError:
            errors.append(f"{name} has malformed endpoint {endpoint!r}")
            continue
        if (method, path) not in supported:
            errors.append(f"{name} references unsupported endpoint {endpoint}")

visiting = set()
visited = set()
def visit(name):
    if name in visiting:
        errors.append(f"sync dependency cycle includes {name}")
        return
    if name in visited or name not in domains:
        return
    visiting.add(name)
    for dependency in domains[name].get("depends_on", []):
        visit(dependency)
    visiting.remove(name)
    visited.add(name)
for name in domains:
    visit(name)

if sync.get("deletion_safety") != {
    "empty_nonempty_abort": True,
    "shrink_approval_percent": 20,
    "newer_observation_wins": True,
    "tombstone_evidence_prefix": "full-refresh",
}:
    errors.append("deletion_safety must match the guarded refresh contract")
if sync.get("rate_budget") != {
    "default_gets_per_minute": 60,
    "maximum_gets_per_minute": 60,
    "yield_to_foreground": True,
}:
    errors.append("rate_budget must match the hard refresh sub-budget")
if set(sync.get("readiness", {})) != {
    "complete", "rest_baseline", "unavailable", "event_continuity"
}:
    errors.append("readiness vocabulary is incomplete")

if errors:
    print("authority matrix check FAILED:", *errors, sep="\n  - ", file=sys.stderr)
    sys.exit(1)
print(
    f"authority matrix check passed: {len(actual)} operations and {len(domains)} refresh phases covered"
)
