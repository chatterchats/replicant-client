#!/usr/bin/env python3
"""Generate the reviewed Replicant Space contract coverage matrix."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "scripts"))

from reference_snapshot import latest_reference_snapshot  # noqa: E402

EVENT_HEADING_RE = re.compile(r"^### \*([^*]+)\*$", re.MULTILINE)
DIGEST_ROW_RE = re.compile(r"^\| \*(ami\.[a-z_]+\.digest)\* \|", re.MULTILINE)
SHAPE_KEYS = (
    "type",
    "format",
    "nullable",
    "$ref",
    "enum",
    "oneOf",
    "anyOf",
    "allOf",
    "items",
    "additionalProperties",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _pointer_part(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _shape(value: Any) -> Any:
    """Retain contract-shape facets relevant to a declared property."""
    if isinstance(value, list):
        return [_shape(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {key: _shape(value[key]) for key in SHAPE_KEYS if key in value}


def build_contract_inventory(root: Path) -> dict[str, Any]:
    """Build contract-derived event, schema, and property identities."""
    snapshot = latest_reference_snapshot(root)
    reference = snapshot.path
    catalogue_text = (reference / "api/events/catalogue/index.md").read_text()
    digest_text = (reference / "api/events/ami-digests/index.md").read_text()
    catalogue_events = EVENT_HEADING_RE.findall(catalogue_text)
    digest_events = DIGEST_ROW_RE.findall(digest_text)
    if len(catalogue_events) != len(set(catalogue_events)):
        raise ValueError("event catalogue contains duplicate headings")
    if len(digest_events) != len(set(digest_events)):
        raise ValueError("AMI digest table contains duplicate event names")
    overlap = set(catalogue_events) & set(digest_events)
    if overlap:
        raise ValueError(f"catalogue and AMI digest event sets overlap: {sorted(overlap)}")

    openapi = json.loads((reference / "openapi.json").read_text())
    declarations = openapi.get("components", {}).get("schemas", {})
    schemas: list[dict[str, Any]] = []
    field_count = 0
    for schema_name in sorted(declarations):
        declaration = declarations[schema_name]
        required = set(declaration.get("required", []))
        fields: list[dict[str, Any]] = []
        for field_name in sorted(declaration.get("properties", {})):
            field = declaration["properties"][field_name]
            fields.append(
                {
                    "pointer": (
                        f"/components/schemas/{_pointer_part(schema_name)}"
                        f"/properties/{_pointer_part(field_name)}"
                    ),
                    "schema": schema_name,
                    "field": field_name,
                    "required": field_name in required,
                    "shape": _shape(field),
                }
            )
        field_count += len(fields)
        schemas.append(
            {
                "pointer": f"/components/schemas/{_pointer_part(schema_name)}",
                "name": schema_name,
                "fields": fields,
            }
        )

    events = sorted(catalogue_events + digest_events)
    return {
        "contract_version": snapshot.version,
        "manifest_sha256": _sha256(reference / "manifest.json"),
        "openapi_sha256": _sha256(reference / "openapi.json"),
        "totals": {
            "catalogue_events": len(catalogue_events),
            "digest_events": len(digest_events),
            "events": len(events),
            "schemas": len(schemas),
            "fields": field_count,
        },
        "catalogue_events": sorted(catalogue_events),
        "digest_events": sorted(digest_events),
        "events": events,
        "schemas": schemas,
    }


def _load_rows(path: Path, key: str) -> list[dict[str, Any]]:
    data = json.loads(path.read_text())
    rows = data.get(key)
    if not isinstance(rows, list):
        raise ValueError(f"{path}: expected a {key!r} array")
    return rows


def build_contract_coverage(root: Path, policy_dir: Path) -> dict[str, Any]:
    """Join reviewed decisions to the current contract inventory."""
    inventory = build_contract_inventory(root)
    event_rows = _load_rows(policy_dir / "event-persistence.json", "events")
    field_rows = _load_rows(policy_dir / "schema-field-coverage.json", "fields")
    event_policy: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        name = row.get("name")
        if name in event_policy:
            raise ValueError(f"duplicate event policy row: {name}")
        event_policy[name] = row
    field_policy: dict[str, dict[str, Any]] = {}
    for row in field_rows:
        pointer = row.get("pointer")
        if pointer in field_policy:
            raise ValueError(f"duplicate schema-field policy row: {pointer}")
        field_policy[pointer] = row

    contract_event_names = set(inventory["events"])
    if set(event_policy) != contract_event_names:
        missing = sorted(contract_event_names - set(event_policy))
        extra = sorted(set(event_policy) - contract_event_names)
        raise ValueError(f"event policy key mismatch; missing={missing}, extra={extra}")

    contract_field_pointers = {
        field["pointer"]
        for schema in inventory["schemas"]
        for field in schema["fields"]
    }
    if set(field_policy) != contract_field_pointers:
        missing = sorted(contract_field_pointers - set(field_policy))
        extra = sorted(set(field_policy) - contract_field_pointers)
        raise ValueError(f"field policy key mismatch; missing={missing}, extra={extra}")

    catalogue = set(inventory["catalogue_events"])
    events = []
    for name in inventory["events"]:
        decision = {key: value for key, value in event_policy[name].items() if key != "name"}
        events.append(
            {
                "name": name,
                "source": "catalogue" if name in catalogue else "ami_digest",
                **decision,
            }
        )

    schemas = []
    for schema in inventory["schemas"]:
        fields = []
        for contract_field in schema["fields"]:
            decision = {
                key: value
                for key, value in field_policy[contract_field["pointer"]].items()
                if key not in {"pointer", "schema", "field"}
            }
            fields.append({**contract_field, **decision})
        schemas.append({**schema, "fields": fields})

    return {
        "contract_version": inventory["contract_version"],
        "manifest_sha256": inventory["manifest_sha256"],
        "openapi_sha256": inventory["openapi_sha256"],
        "totals": inventory["totals"],
        "events": events,
        "schemas": schemas,
    }


def render_coverage(root: Path, policy_dir: Path) -> str:
    """Render the canonical checked-in coverage matrix."""
    return json.dumps(build_contract_coverage(root, policy_dir), indent=2) + "\n"


def main() -> int:
    output = ROOT / "policy/contract-coverage.json"
    output.write_text(render_coverage(ROOT, ROOT / "policy"))
    inventory = build_contract_inventory(ROOT)
    totals = inventory["totals"]
    print(
        f"wrote {output} ({totals['events']} events, {totals['schemas']} schemas, "
        f"{totals['fields']} fields)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
