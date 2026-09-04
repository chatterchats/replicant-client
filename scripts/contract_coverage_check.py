#!/usr/bin/env python3
"""Reject unclassified or stale event and OpenAPI field coverage policy."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "scripts"))

from generate_contract_coverage import build_contract_inventory, render_coverage  # noqa: E402

EVENT_KEYS = {
    "name", "payload_status", "payload", "payload_symbol",
    "persistence_status", "treatment", "replay", "projection_symbol",
    "reason", "evidence", "backlog_group",
}
FIELD_KEYS = {
    "pointer", "schema", "field", "raw", "raw_symbol", "domain",
    "domain_symbol", "durability", "persistence_symbol", "reason",
    "evidence", "backlog_group",
}
EVENT_ENUMS = {
    "payload_status": {"implemented", "backlog"},
    "payload": {"typed", "opaque"},
    "persistence_status": {"implemented", "backlog"},
    "treatment": {"projection_upsert", "projection_delete", "reconciliation_only", "history_only"},
    "replay": {"rebuild", "reconcile", "forward_only", "not_applicable"},
    "backlog_group": {"automation_primitives", "device_movement", "world_lifecycle", "account_content", "operational_lifecycle", "none"},
}
FIELD_ENUMS = {
    "raw": {"typed", "opaque_object", "opaque_value", "passthrough", "contract_drift", "excluded", "missing"},
    "domain": {"normalized", "passthrough", "server_only", "excluded", "missing", "not_applicable"},
    "durability": {"projected", "history_only", "server_authoritative", "ephemeral", "missing", "not_applicable"},
    "backlog_group": {"location_projection", "device_fields", "world_fields", "raw_opaque", "none"},
}
EVIDENCE_RE = re.compile(r"^[^:\n]+(?:/[^:\n]+)*:\d+(?:-\d+)?$")


def _rows(path: Path, key: str, errors: list[str]) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"{path}: {error}")
        return []
    rows = data.get(key)
    if not isinstance(rows, list):
        errors.append(f"{path}: expected {key!r} array")
        return []
    return [row for row in rows if isinstance(row, dict)]


def _block(source: str, marker: str) -> str | None:
    start = source.find(marker)
    if start < 0:
        return None
    brace = source.find("{", start)
    if brace < 0:
        return None
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : index]
    return None


def _event_vocab(path: Path) -> set[str]:
    body = _block(path.read_text(), "EventName")
    return set(re.findall(r'=>\s*"([a-z][a-z0-9_.]+)"', body or ""))


def _string_array(source: str, name: str) -> set[str] | None:
    match = re.search(
        rf"const\s+{re.escape(name)}\s*:\s*&\[&str\]\s*=\s*&\[(.*?)\];",
        source,
        re.DOTALL,
    )
    return None if match is None else set(re.findall(r'"([a-z][a-z0-9_]*)"', match.group(1)))

def _struct_fields(source: str, name: str) -> set[str] | None:
    body = _block(source, f"pub struct {name} {{")
    if body is None:
        return None
    return set(re.findall(r"^\s*pub\s+([a-z][a-z0-9_]*)\s*:", body, re.MULTILINE))


def _check_row_shape(
    rows: list[dict[str, Any]],
    keys: set[str],
    enums: dict[str, set[str]],
    identity: str,
    errors: list[str],
) -> None:
    seen: set[Any] = set()
    for row in rows:
        value = row.get(identity)
        if value in seen:
            errors.append(f"duplicate {identity}: {value}")
        seen.add(value)
        if set(row) != keys:
            errors.append(
                f"{identity} {value}: row keys differ; "
                f"missing={sorted(keys - set(row))}, extra={sorted(set(row) - keys)}"
            )
        for field, allowed in enums.items():
            if row.get(field) not in allowed:
                errors.append(f"{identity} {value}: invalid {field}={row.get(field)!r}")
        if not isinstance(row.get("reason"), str) or not row["reason"].strip():
            errors.append(f"{identity} {value}: reason is empty")
        evidence = row.get("evidence")
        if not isinstance(evidence, str) or EVIDENCE_RE.fullmatch(evidence) is None:
            errors.append(f"{identity} {value}: evidence must be an exact file:line or file:line-line anchor")


def _check_event_rows(root: Path, inventory: dict[str, Any], rows: list[dict[str, Any]], errors: list[str]) -> None:
    expected = set(inventory["events"])
    actual = {row.get("name") for row in rows}
    if actual != expected:
        errors.append(f"event policy mismatch: unclassified={sorted(expected - actual)}, unknown={sorted(actual - expected)}")
    for row in rows:
        name = row.get("name")
        if row.get("payload_status") != "implemented":
            errors.append(f"event {name}: payload remains backlog")
        if row.get("persistence_status") != "implemented":
            errors.append(f"event {name}: persistence remains backlog")
        if row.get("payload") == "typed" and not row.get("payload_symbol"):
            errors.append(f"event {name}: typed payload has no payload_symbol")
        if row.get("persistence_status") == "implemented" and not row.get("projection_symbol"):
            errors.append(f"event {name}: implemented persistence has no projection_symbol")
        if row.get("backlog_group") != "none":
            errors.append(f"event {name}: implemented row retains a backlog group")

    raw_names = _event_vocab(root / "src/raw/vocab.rs")
    domain_names = _event_vocab(root / "src/domain/vocab.rs")
    if raw_names != expected:
        errors.append(f"raw EventName mismatch: missing={sorted(expected - raw_names)}, extra={sorted(raw_names - expected)}")
    if domain_names != expected:
        errors.append(f"domain EventName mismatch: missing={sorted(expected - domain_names)}, extra={sorted(domain_names - expected)}")

    events_source = (root / "src/events.rs").read_text()
    payload_source = (root / "src/events/payloads.rs").read_text()
    payload_sources = f"{events_source}\n{payload_source}"
    decoder_body = _block(events_source, "event_payload_decoders!")
    decoder_names = set(re.findall(r'=>\s*"([a-z][a-z0-9_.]+)"\s*=>', decoder_body or ""))
    if decoder_names != expected:
        errors.append(f"event decoder registry mismatch: missing={sorted(expected - decoder_names)}, extra={sorted(decoder_names - expected)}")
    fixture = json.loads((root / "tests/fixtures/events-3.0.0.json").read_text())
    fixture_rows = {row["name"]: row for row in fixture["events"]}
    if set(fixture_rows) != expected:
        errors.append(
            f"event fixture mismatch: missing={sorted(expected - set(fixture_rows))}, "
            f"extra={sorted(set(fixture_rows) - expected)}"
        )
    for row in rows:
        name = row.get("name")
        symbol = row.get("payload_symbol")
        type_name = symbol.rsplit("::", 1)[-1] if symbol else ""
        fields = _struct_fields(payload_sources, type_name)
        if fields is None:
            errors.append(f"event {name}: stale payload symbol {symbol}")
            continue
        fixture_row = fixture_rows.get(name)
        if fixture_row is None or not isinstance(fixture_row.get("payload"), dict):
            errors.append(f"event {name}: fixture payload is missing or not an object")
            continue
        undocumented = set(fixture_row["payload"]) - (fields - {"extra"})
        if undocumented:
            errors.append(
                f"event {name}: fixture fields are not typed by {type_name}: "
                f"{sorted(undocumented)}"
            )

    managed_source = (root / "src/managed/events.rs").read_text()
    treatment_body = _block(managed_source, "event_treatments!")
    treatment_names = set(re.findall(r'"([a-z][a-z0-9_.]+)"\s*=>', treatment_body or ""))
    if treatment_names != expected:
        errors.append(f"event treatment registry mismatch: missing={sorted(expected - treatment_names)}, extra={sorted(treatment_names - expected)}")
    for row in rows:
        symbol = row.get("projection_symbol")
        if symbol and not re.search(rf"\b{re.escape(symbol.rsplit('::', 1)[-1])}\b", managed_source):
            errors.append(f"event {row.get('name')}: stale projection symbol {symbol}")
    version_match = re.search(
        r"pub\(crate\) const EVENT_PROJECTION_VERSION:\s*i64\s*=\s*(\d+);",
        managed_source,
    )
    configured_version = json.loads(
        (root / "policy/persistence-schema.json").read_text()
    ).get("event_projection_version")
    if version_match is None:
        errors.append("EVENT_PROJECTION_VERSION is missing")
    elif int(version_match.group(1)) != configured_version:
        errors.append(
            "EVENT_PROJECTION_VERSION differs from policy/persistence-schema.json"
        )


def _check_field_rows(root: Path, inventory: dict[str, Any], rows: list[dict[str, Any]], errors: list[str]) -> None:
    expected = {field["pointer"] for schema in inventory["schemas"] for field in schema["fields"]}
    actual = {row.get("pointer") for row in rows}
    if actual != expected:
        errors.append(f"schema-field policy mismatch: unclassified={sorted(expected - actual)}, unknown={sorted(actual - expected)}")
    for row in rows:
        pointer = row.get("pointer")
        if row.get("raw") == "missing":
            errors.append(f"schema field {pointer}: raw representation remains missing")
        if row.get("domain") == "missing":
            errors.append(f"schema field {pointer}: domain representation remains missing")
        if row.get("durability") == "missing":
            errors.append(f"schema field {pointer}: durability remains missing")
        if "missing" not in {row.get("raw"), row.get("domain"), row.get("durability")} and row.get("backlog_group") != "none":
            errors.append(f"schema field {pointer}: closed row retains a backlog group")
        if row.get("raw") in {"typed", "contract_drift"} and not row.get("raw_symbol"):
            errors.append(f"schema field {pointer}: {row.get('raw')} has no raw_symbol")
        if row.get("domain") in {"normalized", "passthrough"} and not row.get("domain_symbol"):
            errors.append(f"schema field {pointer}: {row.get('domain')} has no domain_symbol")
        if row.get("durability") == "projected" and not row.get("persistence_symbol"):
            errors.append(f"schema field {pointer}: projected field has no persistence_symbol")

    adapters_source = (root / "src/domain/adapters.rs").read_text()
    promoted = _string_array(adapters_source, "LOCATION_PROMOTED_FIELDS")
    passthrough = _string_array(adapters_source, "LOCATION_PASSTHROUGH_FIELDS")
    if promoted is None or passthrough is None:
        errors.append("location field registries are missing")
        return
    location_body = _block(
        (root / "src/raw/locations.rs").read_text(), "pub struct Location {"
    )
    raw_fields = set(
        re.findall(
            r"^\s*pub\s+([a-z][a-z0-9_]*)\s*:",
            location_body or "",
            re.MULTILINE,
        )
    )
    raw_fields.discard("unknown")
    if promoted & passthrough:
        errors.append(f"location field registries overlap: {sorted(promoted & passthrough)}")
    if promoted | passthrough != raw_fields:
        errors.append(
            f"location field registry mismatch: unclassified={sorted(raw_fields - promoted - passthrough)}, "
            f"unknown={sorted((promoted | passthrough) - raw_fields)}"
        )


def check(root: Path, policy_dir: Path) -> list[str]:
    """Return every contract coverage error without mutating the repository."""
    errors: list[str] = []
    try:
        inventory = build_contract_inventory(root)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return [f"contract inventory: {error}"]
    event_rows = _rows(policy_dir / "event-persistence.json", "events", errors)
    field_rows = _rows(policy_dir / "schema-field-coverage.json", "fields", errors)
    _check_row_shape(event_rows, EVENT_KEYS, EVENT_ENUMS, "name", errors)
    _check_row_shape(field_rows, FIELD_KEYS, FIELD_ENUMS, "pointer", errors)
    _check_event_rows(root, inventory, event_rows, errors)
    _check_field_rows(root, inventory, field_rows, errors)
    try:
        expected = render_coverage(root, policy_dir)
        actual = (policy_dir / "contract-coverage.json").read_text()
        if actual != expected:
            errors.append("policy/contract-coverage.json is stale; regenerate it")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"coverage matrix: {error}")
    return errors


def main() -> int:
    errors = check(ROOT, ROOT / "policy")
    if errors:
        print("contract coverage check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    inventory = build_contract_inventory(ROOT)
    totals = inventory["totals"]
    print(f"contract coverage check passed: {totals['events']}/{totals['events']} events, {totals['schemas']} schemas, {totals['fields']} fields")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
