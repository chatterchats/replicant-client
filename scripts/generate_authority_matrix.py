#!/usr/bin/env python3
"""Generate Phase 3's checked-in authority matrix from the operation inventory."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
inventory = json.loads((ROOT / "policy/operations.json").read_text())
documented_deltas = json.loads(
    (ROOT / "policy/documented-operation-deltas.json").read_text()
)

OVERRIDES = {
    ("GET", "/v1/accounts/me"): ("entity_snapshot", "complete_entity", "never"),
    ("PATCH", "/v1/accounts/me"): ("operation_result", "complete_entity", "never"),
    ("GET", "/v1/accounts/achievements"): ("complete_collection", "complete_collection", "additive"),
    ("GET", "/v1/accounts/reputation"): ("entity_snapshot", "complete_entity", "never"),
    ("GET", "/v1/accounts/simulations"): ("entity_snapshot", "complete_history", "never"),
    ("GET", "/v1/devices"): ("collection_member", "unfiltered_traversal_only", "full_unfiltered_only"),
    ("GET", "/v1/devices/tags/{tag}"): ("collection_member", "filtered", "never"),
    ("GET", "/v1/devices/{device_code}"): ("entity_snapshot", "complete_entity", "never"),
    ("GET", "/v1/replicants"): ("public_profile", "public_directory", "never"),
    ("GET", "/v1/replicants/{replicant_code}"): ("authorization_dependent", "owned_or_public_entity", "never"),
    ("GET", "/v1/replicants/{replicant_code}/devices"): ("collection_member", "range_scoped", "never"),
    ("GET", "/v1/replicants/{replicant_code}/stars"): ("discovery", "known_stars", "never"),
    ("GET", "/v1/locations"): ("discovery", "visibility_scoped", "never"),
    ("GET", "/v1/locations/{designation}"): ("entity_snapshot", "discoverable_fields", "never"),
    ("GET", "/v1/inventory"): ("entity_snapshot", "account_visible", "never"),
    ("GET", "/v1/stars"): ("complete_collection", "complete_catalogue", "atomic_replace"),
    ("GET", "/v1/events"): ("event_delta", "append_only_history", "never"),
    ("GET", "/v1/events/stream"): ("event_delta", "filtered_low_latency", "never"),
}

VOLATILE = ("/leaderboards", "/health", "/network", "/channels", "/messages", "/audit", "/logs")

def classify(method, path):
    if (method, path) in OVERRIDES:
        return OVERRIDES[(method, path)]
    if method != "GET":
        tombstone = "explicit_response_only" if method == "DELETE" or path.endswith("/decommission") else "never"
        return ("operation_result", "mutation_response", tombstone)
    if any(part in path for part in VOLATILE):
        return ("state_neutral", "volatile_or_history", "never")
    if path in {"/v1/achievements", "/v1/blueprints", "/v1/species"}:
        return ("complete_collection", "reference_collection", "atomic_replace")
    return ("collection_member", "endpoint_scoped", "never")

operations = []
for operation in [*inventory["operations"], *documented_deltas["operations"]]:
    if operation["classification"] != "supported":
        continue
    method, path = operation["method"], operation["path"]
    authority, completeness, tombstone = classify(method, path)
    operations.append({"method": method, "path": path, "authority": authority, "completeness": completeness, "tombstone": tombstone})

operations.sort(key=lambda entry: (entry["path"], entry["method"]))

(ROOT / "policy/authority-matrix.json").write_text(json.dumps({
    "version": 1,
    "sync_domain_policy": "policy/sync-domains.json",
    "contract": "Verified Replicant Space 2.4.0 OpenAPI corpus",
    "operations": operations,
}, indent=2) + "\n")
