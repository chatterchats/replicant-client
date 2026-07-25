#!/usr/bin/env python3
"""Verify that Phase 2 exposes exactly the 77 supported callable methods."""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

EXPECTED = {
    "src/raw/client.rs": {"health"},
    "src/raw/accounts.rs": {"register", "recover", "verify", "me", "update", "request_destructive_wipe", "achievements", "events", "reputation", "simulations"},
    "src/raw/achievements.rs": {"list", "get"},
    "src/raw/blueprints.rs": {"list"},
    "src/raw/bobnet.rs": {"channels", "messages"},
    "src/raw/devices.rs": {"list", "list_by_tag", "get", "configure", "command", "audit", "logs", "network", "list_permissions", "grant_permission", "revoke_permission"},
    "src/raw/feedback.rs": {"submit"},
    "src/raw/galaxy.rs": {"stars_near", "catalogue"},
    "src/raw/inventory.rs": {"list", "for_replicant"},
    "src/raw/leaderboards.rs": {"index", "distance", "fleet", "megastructure", "reputation", "simulations", "simulation_scenario", "trades", "xp"},
    "src/raw/location_events.rs": {"list", "resolve"},
    "src/raw/locations.rs": {"system_map", "get", "contribute"},
    "src/raw/messages.rs": {"list", "mark_read"},
    "src/raw/replicants.rs": {"list", "get", "update", "devices", "message", "stop_mining", "mine", "print", "scan", "scan_devices", "stars", "star", "teleport", "transfer", "cancel_travel", "travel"},
    "src/raw/reputation.rs": {"for_replicant"},
    "src/raw/simulations.rs": {"scenarios", "enter", "active", "cancel"},
    "src/raw/species.rs": {"list"},
    "src/raw/trading.rs": {"list", "create", "delete", "fulfill", "visible_to_replicant"},
    "src/events.rs": {"list", "stream"},
}

errors = []
actual_total = 0
for relative, expected in EXPECTED.items():
    text = (ROOT / relative).read_text()
    actual = set(re.findall(r"pub async fn\s+(\w+)", text))
    # Ignore rate-limit/pagination helpers by checking only operation-bearing files.
    if actual != expected:
        errors.append(f"{relative}: expected {sorted(expected)}, got {sorted(actual)}")
    actual_total += len(actual)

inventory = json.loads((ROOT / "policy/operations.json").read_text())
supported = inventory["totals"]["supported"]
if actual_total != supported:
    errors.append(f"callable total is {actual_total}, policy requires {supported}")

source = "\n".join(path.read_text() for path in (ROOT / "src").rglob("*.rs"))
for operation in inventory["operations"]:
    if operation["classification"] == "supported":
        continue
    literal = operation["path"].replace("{designation}", "").replace("{replicant_code}", "")
    if f'"{literal}"' in source:
        errors.append(f"excluded route appears as executable string: {operation['method']} {operation['path']}")

if errors:
    print("raw transport policy check FAILED:", file=sys.stderr)
    for error in errors:
        print(f"  - {error}", file=sys.stderr)
    sys.exit(1)

print("raw transport policy check passed: 77 callable methods; 7 excluded operations absent")
