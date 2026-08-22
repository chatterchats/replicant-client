#!/usr/bin/env python3
"""Contract and deprecation policy gate for replicant-client.

Verifies the newest checked-in Replicant Space OpenAPI and rendered-document
corpus:

- the OpenAPI and documentation-manifest checksums match recorded provenance;
- policy/operations.json is not stale relative to the live OpenAPI document;
- the inventory totals are exactly 89 operations, 82 supported, 7 excluded
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
from reference_snapshot import latest_reference_snapshot  # noqa: E402

SNAPSHOT = latest_reference_snapshot(ROOT)
REFERENCE = SNAPSHOT.path
OPENAPI = REFERENCE / "openapi.json"
MANIFEST = REFERENCE / "manifest.json"
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
    schema_count = len(spec.get("components", {}).get("schemas", {}))
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
    if schema_count != metadata["openapi_schema_count"]:
        fail(
            f"schema count drift: metadata says {metadata['openapi_schema_count']}, "
            f"openapi.json has {schema_count}"
        )
    return spec


def check_documentation_manifest(metadata: dict) -> None:
    data = MANIFEST.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    expected = metadata["documentation_manifest_sha256"]
    if actual != expected:
        fail(f"manifest.json sha256 mismatch: expected {expected}, got {actual}")
        return

    manifest = json.loads(data)
    pages = manifest.get("pages", [])
    expected_pages = metadata["documentation_page_count"]
    if manifest.get("page_count") != expected_pages or len(pages) != expected_pages:
        fail(
            "documentation page count drift: metadata/manifest/list are "
            f"{expected_pages}/{manifest.get('page_count')}/{len(pages)}"
        )

    for page in pages:
        local_path = page.get("local_path")
        expected_hash = page.get("markdown_sha256")
        if not local_path or not expected_hash:
            fail(f"manifest page is missing local_path or markdown_sha256: {page!r}")
            continue
        path = REFERENCE / local_path
        if not path.is_file():
            fail(f"manifest page is missing from the corpus: {local_path}")
            continue
        actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            fail(
                f"documentation page sha256 mismatch for {local_path}: "
                f"expected {expected_hash}, got {actual_hash}"
            )


def check_operation_inventory(spec: dict) -> None:
    checked_in = json.loads((POLICY / "operations.json").read_text())
    fresh = build_inventory(spec)

    if checked_in["operations"] != fresh["operations"]:
        fail(
            "policy/operations.json is stale relative to "
            f"{REFERENCE.relative_to(ROOT)}/openapi.json; run "
            "scripts/generate_operation_inventory.py"
        )

    totals = checked_in["totals"]
    expected_totals = {
        "total_operations": 89,
        "supported": 82,
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
        "GET /v1/tutorials and GET /v1/tutorials/{slug}",
        "POST /v1/replicants/{replicant_code}/teleport",
        "POST /v1/devices/{device_code} system ward activate/deactivate responses",
        "POST /v1/devices/{device_code}/retrieve",
        "Quickstart GET /v1/replicants/{replicant_code}/inventory example",
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


def check_package_identity(metadata: dict) -> None:
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

    contract = package.get("metadata", {}).get("replicant-space", {})
    expected_contract = {
        "version": metadata["replicant_space_version"],
        "documentation-version": metadata["documentation_version"],
        "documentation-manifest-sha256": metadata["documentation_manifest_sha256"],
        "openapi-version": metadata["openapi_version"],
        "openapi-sha256": metadata["openapi_sha256"],
    }
    for key, expected in expected_contract.items():
        if contract.get(key) != expected:
            fail(
                f"Cargo.toml package.metadata.replicant-space.{key} = "
                f"{contract.get(key)!r}, expected {expected!r}"
            )


def main() -> None:
    metadata = json.loads((POLICY / "contract-metadata.json").read_text())
    if metadata.get("replicant_space_version") != SNAPSHOT.version:
        fail(
            "policy/contract-metadata.json targets Replicant Space "
            f"{metadata.get('replicant_space_version')!r}, but newest reference snapshot "
            f"is {SNAPSHOT.version!r}"
        )
    check_package_identity(metadata)
    spec = check_openapi_checksum(metadata)
    check_documentation_manifest(metadata)
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

    print(
        f"contract policy check passed: Replicant Space {SNAPSHOT.version} corpus, "
        f"{metadata['openapi_path_count']} paths, "
        f"{metadata['openapi_operation_count']} OpenAPI operations "
        f"({json.loads((POLICY / 'operations.json').read_text())['totals']['supported']} supported, "
        f"{json.loads((POLICY / 'operations.json').read_text())['totals']['deprecated']} deprecated, "
        f"{json.loads((POLICY / 'operations.json').read_text())['totals']['admin']} admin), "
        f"{metadata['openapi_schema_count']} schemas, "
        f"{metadata['documentation_page_count']} rendered documentation pages; "
        "message_notify excluded; mining aliases recorded; no stray "
        "replicant-sdk references."
    )


if __name__ == "__main__":
    main()
