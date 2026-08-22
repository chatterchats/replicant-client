#!/usr/bin/env python3
"""Verify the OpenAPI-backed raw surface."""

import json
import re
import sys
from pathlib import Path

from generate_operation_inventory import build_inventory

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from reference_snapshot import latest_reference_snapshot  # noqa: E402

REFERENCE = latest_reference_snapshot(ROOT).path

EXPECTED = {
    "src/raw/client.rs": {"health"},
    "src/raw/accounts.rs": {"register", "recover", "verify", "me", "update", "request_destructive_wipe", "achievements", "events", "reputation", "simulations"},
    "src/raw/achievements.rs": {"list", "get"},
    "src/raw/blueprints.rs": {"list"},
    "src/raw/bobnet.rs": {"channels", "messages"},
    "src/raw/devices.rs": {"list", "list_by_tag", "get", "configure", "retrieve", "command", "audit", "logs", "network", "list_permissions", "grant_permission", "revoke_permission"},
    "src/raw/feedback.rs": {"submit"},
    "src/raw/galaxy.rs": {"stars_near", "catalogue"},
    "src/raw/inventory.rs": {"list", "for_replicant"},
    "src/raw/leaderboards.rs": {"index", "colony_moon", "colony_planet", "distance", "fleet", "megastructure", "reputation", "simulations", "simulation_scenario", "trades", "xp"},
    "src/raw/location_events.rs": {"list", "resolve"},
    "src/raw/locations.rs": {"system_map", "get", "contribute"},
    "src/raw/messages.rs": {"list", "mark_read"},
    "src/raw/replicants.rs": {"list", "get", "update", "devices", "message", "stop_mining", "mine", "print", "scan", "scan_devices", "stars", "star", "teleport", "transfer", "cancel_travel", "travel"},
    "src/raw/reputation.rs": {"for_replicant"},
    "src/raw/simulations.rs": {"scenarios", "enter", "active", "cancel"},
    "src/raw/species.rs": {"list"},
    "src/raw/trading.rs": {"list", "create", "delete", "fulfill", "visible_to_replicant"},
    "src/raw/tutorials.rs": {"list", "get"},
    "src/events.rs": {"list", "stream"},
}

OPAQUE_SUCCESS_RESPONSES = {
    ("GET", "/v1/health"),
    ("DELETE", "/v1/replicants/{replicant_code}/mine"),
    ("GET", "/v1/replicants/{replicant_code}/scan/devices"),
    ("DELETE", "/v1/replicants/{replicant_code}/travel"),
    ("POST", "/v1/locations/{location_code}/events/{designation}"),
    ("DELETE", "/v1/devices/{device_code}/simulate/{sim_id}"),
    ("GET", "/v1/devices/{device_code}/audit"),
    ("GET", "/v1/devices/{device_code}/permissions"),
    ("POST", "/v1/devices/{device_code}/permissions"),
    ("DELETE", "/v1/devices/{device_code}/permissions"),
    ("GET", "/v1/devices/{device_code}/trades"),
    ("POST", "/v1/devices/{device_code}/trades"),
    ("DELETE", "/v1/devices/{device_code}/trades/{trade_code}"),
    ("POST", "/v1/devices/{device_code}/trades/{trade_code}"),
    ("GET", "/v1/replicants/{replicant_code}/traders"),
    ("POST", "/v1/locations/{designation}/contribute"),
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
expected_inventory = build_inventory(
    json.loads((REFERENCE / "openapi.json").read_text())
)
for actual, expected in zip(inventory["operations"], expected_inventory["operations"], strict=True):
    descriptor = (
        "operation_id",
        "method",
        "path",
        "auth_required",
        "safety",
        "request_type",
        "response_type",
    )
    if any(actual.get(field) != expected.get(field) for field in descriptor):
        errors.append(
            "route descriptor mismatch for "
            f"{expected['method']} {expected['path']}"
        )
supported = inventory["totals"]["supported"]
expected_total = supported
if actual_total != expected_total:
    errors.append(
        f"callable total is {actual_total}, policy requires {supported} OpenAPI operations"
    )

by_route = {(item["method"], item["path"]): item for item in inventory["operations"]}
for route in OPAQUE_SUCCESS_RESPONSES:
    if by_route.get(route, {}).get("response_type") != "none":
        errors.append(f"opaque response is schema-backed: {route[0]} {route[1]}")

source = "\n".join(path.read_text() for path in (ROOT / "src").rglob("*.rs"))
opaque_count = source.count("RawResponse<serde_json::Value>")
if opaque_count != len(OPAQUE_SUCCESS_RESPONSES):
    errors.append(
        f"found {opaque_count} opaque raw success responses; policy records "
        f"{len(OPAQUE_SUCCESS_RESPONSES)} schema-less exceptions"
    )
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

print(
    f"raw transport policy check passed: {supported} OpenAPI-backed methods; "
    "route descriptors match the corpus"
)
