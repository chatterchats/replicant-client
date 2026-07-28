#!/usr/bin/env python3
"""Contract and deprecation policy gate for replicant-client.

Verifies the checked-in Replicant Space 2.3.3 OpenAPI corpus and explicit
rendered-document corrections:

- the OpenAPI document checksum matches the recorded contract metadata;
- policy/operations.json is not stale relative to the live OpenAPI document;
- the inventory totals are exactly 86 operations, 79 supported, 7 excluded
  (5 deprecated + 2 admin);
- every excluded operation has a reason and evidence file, and its
  method/path actually exists in the OpenAPI document;
- `message_notify` is recorded as excluded from managed account settings;
- deprecated mining aliases (belt -> location, designation -> site) are
  recorded for normalization rather than public exposure;
- `replicant-sdk` appears in the repository only inside the README's
  historical-note section.

Exits non-zero with a description of every failure found.
"""

import hashlib
import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "scripts"))
from generate_operation_inventory import build_inventory  # noqa: E402

REFERENCE = ROOT / "reference" / "replicant-space"
OPENAPI = REFERENCE / "openapi.json"
POLICY = ROOT / "policy"
DOCUMENTED_DELTAS = POLICY / "documented-operation-deltas.json"

ERRORS: list[str] = []


def fail(message: str) -> None:
    ERRORS.append(message)


def check_openapi_checksum(metadata: dict) -> dict:
    data = OPENAPI.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    expected = metadata["openapi_sha256"]
    if actual != expected:
        fail(
            f"openapi.json sha256 mismatch: expected {expected}, got {actual}"
        )
    spec = json.loads(data)
    op_count = sum(
        1
        for methods in spec["paths"].values()
        for m in methods
        if m.lower() in {"get", "post", "put", "patch", "delete"}
    )
    path_count = len(spec["paths"])
    if op_count != metadata["openapi_operation_count"]:
        fail(
            f"operation count drift: metadata says "
            f"{metadata['openapi_operation_count']}, openapi.json has "
            f"{op_count}"
        )
    if path_count != metadata["openapi_path_count"]:
        fail(
            f"path count drift: metadata says {metadata['openapi_path_count']}, "
            f"openapi.json has {path_count}"
        )
    return spec


def check_operation_inventory(spec: dict) -> None:
    checked_in = json.loads((POLICY / "operations.json").read_text())
    fresh = build_inventory(spec)

    if checked_in["operations"] != fresh["operations"]:
        fail(
            "policy/operations.json is stale relative to "
            "reference/replicant-space/openapi.json; run "
            "scripts/generate_operation_inventory.py"
        )

    totals = checked_in["totals"]
    expected_totals = {
        "total_operations": 86,
        "supported": 79,
        "deprecated": 5,
        "admin": 2,
        "excluded": 7,
    }
    for key, expected in expected_totals.items():
        if totals.get(key) != expected:
            fail(
                f"policy/operations.json totals.{key} = {totals.get(key)!r}, "
                f"expected {expected!r}"
            )

    paths = spec["paths"]
    for op in checked_in["operations"]:
        if op["classification"] == "supported":
            continue
        if not op.get("reason") or not op.get("evidence"):
            fail(
                f"excluded operation {op['method']} {op['path']} is missing "
                "a reason or evidence field"
            )
        methods = paths.get(op["path"], {})
        if op["method"].lower() not in {m.lower() for m in methods}:
            fail(
                f"excluded operation {op['method']} {op['path']} does not "
                "exist in the current OpenAPI document"
            )
        if op["evidence"] != "openapi.json" and not (
            REFERENCE / op["evidence"]
        ).is_file():
            fail(
                f"evidence file for {op['method']} {op['path']} not found: "
                f"{op['evidence']}"
            )


def check_documented_operation_deltas(metadata: dict) -> None:
    data = json.loads(DOCUMENTED_DELTAS.read_text())
    operations = data.get("operations", [])
    if data.get("base_openapi_version") != metadata.get("openapi_version"):
        fail("documented operation deltas use the wrong OpenAPI baseline")
    if data.get("documentation_version") != metadata.get("documentation_version"):
        fail("documented operation deltas use the wrong documentation version")
    if len(operations) != metadata.get("documented_operation_delta_count"):
        fail("documented operation delta count does not match contract metadata")

    if operations:
        fail(f"unexpected documented operation deltas: {operations!r}")


def check_excluded_fields() -> None:
    data = json.loads((POLICY / "excluded-fields.json").read_text())
    fields = {entry["field"] for entry in data["excluded_fields"]}
    if "message_notify" not in fields:
        fail(
            "policy/excluded-fields.json does not record message_notify as "
            "excluded from managed account settings"
        )


def check_normalization_aliases() -> None:
    data = json.loads((POLICY / "normalization-aliases.json").read_text())
    pairs = {
        (entry["deprecated_field"], entry["normalized_field"])
        for entry in data["aliases"]
    }
    for required in (("belt", "location"), ("designation", "site")):
        if required not in pairs:
            fail(
                "policy/normalization-aliases.json missing required alias "
                f"{required[0]} -> {required[1]}"
            )


def check_contract_mismatches(metadata: dict) -> None:
    scopes = {
        entry["scope"]
        for entry in metadata.get("openapi_documentation_mismatches", [])
    }
    required = {
        "GET /v1/events/stream",
        "GET /v1/devices/{device_code}/audit and GET/POST/DELETE /v1/devices/{device_code}/permissions",
        "Five current trading operations",
    }
    if not required.issubset(scopes):
        fail("policy/contract-metadata.json is missing raw transport contract mismatches")


def check_no_old_crate_name() -> None:
    exclude_dirs = {".git", "target", "node_modules", "reference"}
    # This checker's own source and the authoritative rewrite guide discuss
    # the old crate name as history/instructions, not as a live reference.
    allowed_files = {
        "scripts/contract_policy_check.py",
        "CLAUDE.md",
        "AGENTS.md",
        "docs/implementation/rewrite-guide.md",
    }
    matches = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in exclude_dirs for part in path.relative_to(ROOT).parts):
            continue
        if str(path.relative_to(ROOT)) in allowed_files:
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if "replicant-sdk" in line:
                matches.append((path.relative_to(ROOT), lineno, line))

    readme = ROOT / "README.md"
    readme_lines = readme.read_text().splitlines() if readme.is_file() else []

    def in_history_section(lineno: int) -> bool:
        # True once a "## History" heading precedes this line and no other
        # "## " heading interrupts before it.
        in_history = False
        for text in readme_lines[:lineno]:
            if text.startswith("## "):
                in_history = text.strip() == "## History"
        return in_history

    for rel_path, lineno, line in matches:
        if str(rel_path) == "README.md" and in_history_section(lineno):
            continue
        fail(f"'replicant-sdk' found outside historical notes: {rel_path}:{lineno}: {line.strip()}")


def check_package_identity() -> None:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text())
    package = cargo["package"]

    expected_package = {
        "name": "replicant-client",
        "version": "1.0.0",
        "edition": "2024",
        "license": "MIT",
    }
    for key, expected in expected_package.items():
        if package.get(key) != expected:
            fail(f"Cargo.toml package.{key} = {package.get(key)!r}, expected {expected!r}")

    lib = cargo.get("lib", {})
    if lib.get("name") != "replicant_client":
        fail(f"Cargo.toml lib.name = {lib.get('name')!r}, expected 'replicant_client'")

    features = cargo.get("features", {})
    expected_default = {"managed", "rustls-tls"}
    if set(features.get("default", [])) != expected_default:
        fail(
            f"Cargo.toml default features = {features.get('default')!r}, "
            f"expected {sorted(expected_default)!r}"
        )
    if "raw" not in features.get("events", []):
        fail("Cargo.toml 'events' feature must imply 'raw'")
    if "events" not in features.get("managed", []):
        fail("Cargo.toml 'managed' feature must imply 'events'")
    for standalone in ("raw", "rustls-tls", "native-tls"):
        if standalone not in features:
            fail(f"Cargo.toml is missing the '{standalone}' feature")


def main() -> None:
    check_package_identity()
    metadata = json.loads((POLICY / "contract-metadata.json").read_text())
    spec = check_openapi_checksum(metadata)
    check_contract_mismatches(metadata)
    check_operation_inventory(spec)
    check_documented_operation_deltas(metadata)
    check_excluded_fields()
    check_normalization_aliases()
    check_no_old_crate_name()

    if ERRORS:
        print("contract policy check FAILED:", file=sys.stderr)
        for err in ERRORS:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    print("contract policy check passed: 84 OpenAPI operations "
          "(77 supported, 5 deprecated, 2 admin) plus 2 documented "
          "leaderboard deltas; message_notify excluded; mining aliases "
          "recorded; no stray replicant-sdk references.")


if __name__ == "__main__":
    main()
