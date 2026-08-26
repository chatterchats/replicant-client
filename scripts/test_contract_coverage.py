#!/usr/bin/env python3
"""Fixture tests for unclassified and stale contract coverage failures."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "scripts"))

from contract_coverage_check import check  # noqa: E402
from generate_contract_coverage import render_coverage  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def event_row(name: str, payload: str, projection: str, evidence: str) -> dict:
    return {
        "name": name,
        "payload_status": "implemented",
        "payload": "typed",
        "payload_symbol": f"crate::events::{payload}",
        "persistence_status": "implemented",
        "treatment": "history_only",
        "replay": "not_applicable",
        "projection_symbol": f"crate::managed::events::{projection}",
        "reason": "Fixture event is deliberately retained as typed history only.",
        "evidence": evidence,
        "backlog_group": "none",
    }


def build_fixture(root: Path) -> None:
    reference = root / "reference/replicant-space-1-0-0"
    write(
        reference / "api/events/catalogue/index.md",
        "# Events\n\n### *base.event*\n\n```\n{\"value\": 1}\n```\n",
    )
    write(
        reference / "api/events/ami-digests/index.md",
        "# Digests\n\n| Event | Controller | Report |\n| --- | --- | --- |\n"
        "| *ami.base.digest* | Base | Base report. |\n",
    )
    write(reference / "manifest.json", '{"page_count": 2}\n')
    write(
        reference / "openapi.json",
        json.dumps(
            {
                "openapi": "3.0.3",
                "components": {
                    "schemas": {
                        "BaseSchema": {
                            "type": "object",
                            "properties": {"known": {"type": "string"}},
                        }
                    }
                },
            },
            indent=2,
        )
        + "\n",
    )
    events = [
        event_row(
            "ami.base.digest",
            "AmiBaseDigestPayload",
            "projection_ami_base_digest",
            "api/events/ami-digests/index.md:5",
        ),
        event_row(
            "base.event",
            "BaseEventPayload",
            "projection_base_event",
            "api/events/catalogue/index.md:3",
        ),
    ]
    write(
        root / "policy/event-persistence.json",
        json.dumps({"contract_version": "1.0.0", "events": events}, indent=2) + "\n",
    )
    pointer = "/components/schemas/BaseSchema/properties/known"
    fields = [
        {
            "pointer": pointer,
            "schema": "BaseSchema",
            "field": "known",
            "raw": "typed",
            "raw_symbol": "crate::raw::Base::known",
            "domain": "server_only",
            "domain_symbol": None,
            "durability": "server_authoritative",
            "persistence_symbol": None,
            "reason": "Fixture response remains server authoritative.",
            "evidence": "src/raw/base.rs:1",
            "backlog_group": "none",
        }
    ]
    write(
        root / "policy/schema-field-coverage.json",
        json.dumps({"contract_version": "1.0.0", "fields": fields}, indent=2) + "\n",
    )
    write(
        root / "policy/persistence-schema.json",
        json.dumps({"event_projection_version": 1}, indent=2) + "\n",
    )
    vocab = (
        'open_vocab! { EventName { AmiBaseDigest => "ami.base.digest", '
        'BaseEvent => "base.event" } }\n'
    )
    write(root / "src/raw/vocab.rs", vocab)
    write(root / "src/domain/vocab.rs", vocab)
    write(
        root / "src/events.rs",
        "pub struct AmiBaseDigestPayload { pub report: serde_json::Value, pub extra: JsonObject }\n"
        "pub struct BaseEventPayload { pub value: Option<i64>, pub extra: JsonObject }\n"
        "event_payload_decoders! {\n"
        ' ami_base_digest => "ami.base.digest" => AmiBaseDigestPayload,\n'
        ' base_event => "base.event" => BaseEventPayload,\n'
        "}\n",
    )
    write(root / "src/events/payloads.rs", "")
    write(
        root / "src/managed/events.rs",
        "pub(crate) const EVENT_PROJECTION_VERSION: i64 = 1;\n"
        "fn projection_ami_base_digest() {}\n"
        "fn projection_base_event() {}\n"
        "event_treatments! {\n"
        ' "ami.base.digest" => (HistoryOnly, NotApplicable, projection_ami_base_digest),\n'
        ' "base.event" => (HistoryOnly, NotApplicable, projection_base_event),\n'
        "}\n",
    )
    write(
        root / "src/raw/locations.rs",
        "pub struct Location { pub location: Option<String>, pub unknown: JsonObject }\n",
    )
    write(
        root / "src/domain/adapters.rs",
        'const LOCATION_PROMOTED_FIELDS: &[&str] = &["location"];\n'
        "const LOCATION_PASSTHROUGH_FIELDS: &[&str] = &[];\n",
    )
    write(
        root / "tests/fixtures/events-2.5.2.json",
        json.dumps(
            {
                "contract_version": "1.0.0",
                "events": [
                    {"name": "ami.base.digest", "payload": {"report": {}}, "evidence": "digest:5"},
                    {"name": "base.event", "payload": {"value": 1}, "evidence": "catalogue:3"},
                ],
            },
            indent=2,
        )
        + "\n",
    )
    write(
        root / "policy/contract-coverage.json",
        render_coverage(root, root / "policy"),
    )


def assert_error(errors: list[str], fragment: str) -> None:
    if not any(fragment in error for error in errors):
        raise AssertionError(f"expected {fragment!r} in errors: {errors}")


def scenario(mutator, expected: str) -> None:
    with tempfile.TemporaryDirectory(prefix="replicant-coverage-") as directory:
        root = Path(directory)
        build_fixture(root)
        mutator(root)
        assert_error(check(root, root / "policy"), expected)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="replicant-coverage-base-") as directory:
        root = Path(directory)
        build_fixture(root)
        errors = check(root, root / "policy")
        if errors:
            raise AssertionError(f"base fixture failed: {errors}")

    scenario(
        lambda root: (root / "reference/replicant-space-1-0-0/api/events/catalogue/index.md").write_text(
            (root / "reference/replicant-space-1-0-0/api/events/catalogue/index.md").read_text()
            + "\n### *new.event*\n\n```\n{}\n```\n"
        ),
        "new.event",
    )

    def add_field(root: Path) -> None:
        path = root / "reference/replicant-space-1-0-0/openapi.json"
        spec = json.loads(path.read_text())
        spec["components"]["schemas"]["BaseSchema"]["properties"]["new_field"] = {
            "type": "boolean"
        }
        path.write_text(json.dumps(spec, indent=2) + "\n")

    scenario(add_field, "/components/schemas/BaseSchema/properties/new_field")

    def duplicate_event(root: Path) -> None:
        path = root / "policy/event-persistence.json"
        data = json.loads(path.read_text())
        data["events"].append(dict(data["events"][0]))
        path.write_text(json.dumps(data, indent=2) + "\n")

    scenario(duplicate_event, "duplicate name: ami.base.digest")

    def stale_decoder(root: Path) -> None:
        path = root / "policy/event-persistence.json"
        data = json.loads(path.read_text())
        data["events"][1]["payload_symbol"] = "crate::events::MissingPayload"
        path.write_text(json.dumps(data, indent=2) + "\n")
        (root / "policy/contract-coverage.json").write_text(
            render_coverage(root, root / "policy")
        )

    scenario(stale_decoder, "stale payload symbol crate::events::MissingPayload")

    def unclassified_location(root: Path) -> None:
        path = root / "src/raw/locations.rs"
        path.write_text(
            "pub struct Location {\n"
            "    pub location: Option<String>,\n"
            "    pub new_location_field: Option<String>,\n"
            "    pub unknown: JsonObject,\n"
            "}\n"
        )

    scenario(unclassified_location, "new_location_field")
    print("contract coverage fixture tests passed: unclassified event, field, duplicate, stale decoder, location field")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
