#!/usr/bin/env python3
"""Deterministic Replicant Space 2.5.2 coverage audit and validator."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = ROOT / "reference" / "replicant-space-2-5-2"
OPENAPI = SNAPSHOT / "openapi.json"
MANIFEST = SNAPSHOT / "manifest.json"
CATALOGUE = SNAPSHOT / "api" / "events" / "catalogue" / "index.md"
METADATA = ROOT / "policy" / "contract-metadata.json"
AUDIT = ROOT / "audit" / "2.5.2"
WORKLIST = AUDIT / "worklist.jsonl"
DOC_PAGES = AUDIT / "doc-pages.jsonl"
MERGED = AUDIT / "merged.jsonl"
REPORT = ROOT / "AUDIT-2.5.2.md"
METHODS = {"get", "post", "put", "patch", "delete"}
KINDS = ("operation", "schema", "event")
VERDICTS = ("covered", "partial", "missing", "drift", "n/a")
DOC_ONLY_FIELDS = {
    "schema:app_schemas_locations_LocationResponseSchema": {
        "/properties/atmosphere":
            "reference/replicant-space-2-5-2/api/locations/index.md:78",
        "/properties/has_atmosphere":
            "reference/replicant-space-2-5-2/changelog/index.md:66",
        "/properties/code": "reference/replicant-space-2-5-2/api/locations/index.md:73",
        "/properties/parent": "reference/replicant-space-2-5-2/api/locations/index.md:76",
        "/properties/surveyed": "reference/replicant-space-2-5-2/api/locations/index.md:77",
        "/properties/system": "reference/replicant-space-2-5-2/api/locations/index.md:75",
        "/properties/your_devices":
            "reference/replicant-space-2-5-2/api/locations/index.md:83",
        "/properties/your_resources":
            "reference/replicant-space-2-5-2/api/locations/index.md:84",
    },
    "schema:flask_smorest_error_handler_ErrorSchema": {
        "/properties/error": "reference/replicant-space-2-5-2/errors/index.md:19",
    },
}


SLICES = (
    (1, "01-changelog-2.5.2"),
    (2, "02-accounts-achievements"),
    (3, "03-replicants-travel-mining-printing"),
    (4, "04-devices"),
    (5, "05-device-commands-blueprints"),
    (6, "06-locations-stars-scanning"),
    (7, "07-inventory-trades-species"),
    (8, "08-events-messages-location-events"),
    (9, "09-leaderboards-simulations"),
    (10, "10-admin-feedback-health-tutorials"),
    (11, "11-catalogue-ami-through-mining"),
    (12, "12-catalogue-print-through-ward"),
)
SLICE_COUNTS = (20, 33, 33, 36, 26, 26, 21, 18, 27, 14, 36, 38)
SLICE_BY_NUMBER = dict(SLICES)

EXPLICIT_UNITS = frozenset(
    {
        "operation:GET:/v1/devices",
        "operation:GET:/v1/devices/{device_code}",
        "operation:POST:/v1/devices/{device_code}",
        "operation:POST:/v1/replicants/{replicant_code}/print",
        "operation:GET:/v1/accounts/achievements",
        "operation:GET:/v1/achievements",
        "operation:GET:/v1/achievements/{achievement_key}",
        "schema:app_schemas_achievements_AchievementSchema",
        "schema:app_schemas_device_commands_TravelSchema",
        "schema:app_schemas_devices_DeviceListItemSchema",
        "schema:app_schemas_devices_DeviceStatusSchema",
        "schema:app_schemas_locations_LocationResponseSchema",
        "schema:app_schemas_printing_PrintRequestSchema",
        "schema:app_schemas_stars_CatalogueStarSchema",
        "schema:app_schemas_stars_StarItemSchema",
        "event:device.stowed",
        "event:hub.maintained",
        "event:hub.warning",
        "event:multiplayer.replicant_entered",
        "event:multiplayer.replicant_left",
    }
)

OPERATION_SLICES = {
    "accounts": 2,
    "achievements_public": 2,
    "replicants": 3,
    "travel": 3,
    "mining": 3,
    "printing": 3,
    "devices": 4,
    "blueprints": 5,
    "locations": 6,
    "stars": 6,
    "scanning": 6,
    "inventory": 7,
    "trades": 7,
    "species": 7,
    "events": 8,
    "messages": 8,
    "location_events": 8,
    "leaderboards": 9,
    "admin": 10,
    "feedback": 10,
    "health": 10,
    "tutorials": 10,
}
SCHEMA_SLICES = {
    "accounts": 2,
    "achievements": 2,
    "replicants": 3,
    "travel": 3,
    "mining": 3,
    "printing": 3,
    "devices": 4,
    "device": 5,
    "blueprints": 5,
    "locations": 6,
    "stars": 6,
    "scanning": 6,
    "common": 6,
    "inventory": 7,
    "species": 7,
    "events": 8,
    "messages": 8,
    "location": 8,
    "leaderboards": 9,
    "simulations": 9,
    "admin": 10,
    "feedback": 10,
    "flask": 10,
    "validation": 10,
}

EVENT_HEADING = re.compile(r"^### \*([^*]+)\*$")
EVIDENCE = re.compile(r"^(.+):([1-9][0-9]*)$")
SENTENCE_END = re.compile(r"[.!?](?=\s|$)")


class AuditError(Exception):
    pass


def fail(message: str) -> None:
    raise AuditError(message)


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def jsonl_bytes(rows: list[Any]) -> bytes:
    return b"".join(json_bytes(row) for row in rows)


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        Path(name).replace(path)
    except BaseException:
        try:
            Path(name).unlink()
        except FileNotFoundError:
            pass
        raise


def pointer_part(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")
def schema_field_pointers(schema: Any, path: str = "") -> set[str]:
    pointers: set[str] = set()
    if isinstance(schema, dict):
        properties = schema.get("properties")
        if isinstance(properties, dict):
            for name, value in properties.items():
                pointer = f"{path}/properties/{pointer_part(name)}"
                pointers.add(pointer)
                pointers.update(schema_field_pointers(value, pointer))
        for key, value in schema.items():
            if key != "properties":
                pointers.update(schema_field_pointers(value, f"{path}/{pointer_part(key)}"))
    elif isinstance(schema, list):
        for index, value in enumerate(schema):
            pointers.update(schema_field_pointers(value, f"{path}/{index}"))
    return pointers






def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"cannot read JSON {path.relative_to(ROOT)}: {exc}")


def schema_group(name: str) -> str:
    if name in {"HTTPValidationError", "ValidationError"}:
        return "validation"
    return name.removeprefix("app_schemas_").split("_", 1)[0]


def event_rows() -> list[dict[str, Any]]:
    rows = []
    try:
        lines = CATALOGUE.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read event catalogue: {exc}")
    for line_number, line in enumerate(lines, 1):
        match = EVENT_HEADING.match(line)
        if match:
            name = match.group(1)
            rows.append(
                {
                    "unit": f"event:{name}",
                    "kind": "event",
                    "slice": 0,
                    "source": f"{CATALOGUE.relative_to(ROOT).as_posix()}:{line_number}",
                    "group": name.split(".", 1)[0],
                }
            )
    return rows


def validate_sources() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[str], str]:
    if not SNAPSHOT.is_dir():
        fail(f"missing fixed source snapshot: {SNAPSHOT.relative_to(ROOT)}")
    spec = read_json(OPENAPI)
    metadata = read_json(METADATA)
    manifest = read_json(MANIFEST)
    if not isinstance(spec.get("paths"), dict) or not isinstance(spec.get("components", {}).get("schemas"), dict):
        fail("openapi.json lacks paths/components.schemas objects")
    if not isinstance(manifest.get("pages"), list):
        fail("manifest.json lacks pages list")
    if manifest.get("page_count") != 87:
        fail(f"manifest page_count is {manifest.get('page_count')!r}, expected 87")
    digest = hashlib.sha256(OPENAPI.read_bytes()).hexdigest()
    expected_digest = "df5f74046e95678f54161b930af6d8b1abbe4b07b1718e485b5a4d46f6757639"
    if metadata.get("openapi_sha256") != expected_digest or digest != expected_digest:
        fail(
            "openapi SHA-256 mismatch: "
            f"metadata {metadata.get('openapi_sha256')}, expected {expected_digest}, got {digest}"
        )
    expected_metadata = {
        "replicant_space_version": "2.5.2",
        "documentation_version": "2.5.2",
        "openapi_version": "2.5.2",
        "openapi_path_count": 75,
        "openapi_operation_count": 89,
        "openapi_schema_count": 160,
        "documentation_page_count": 87,
    }
    for key, value in expected_metadata.items():
        if metadata.get(key) != value:
            fail(f"contract metadata {key} is {metadata.get(key)!r}, expected {value!r}")

    pages = []
    seen_pages = set()
    for page in manifest["pages"]:
        if not isinstance(page, dict) or not isinstance(page.get("local_path"), str):
            fail("manifest page has no local_path")
        local = Path(page["local_path"])
        if local.is_absolute() or ".." in local.parts:
            fail(f"unsafe manifest path: {page['local_path']!r}")
        normalized = local.as_posix()
        if normalized in seen_pages:
            fail(f"duplicate manifest page: {normalized}")
        seen_pages.add(normalized)
        if not (SNAPSHOT / local).is_file():
            fail(f"manifest page does not exist: {normalized}")
        pages.append(f"{(SNAPSHOT / local).relative_to(ROOT).as_posix()}")
    pages.sort()

    paths = spec["paths"]
    operations = []
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if method.lower() not in METHODS:
                continue
            if not isinstance(operation, dict):
                fail(f"operation {method.upper()} {path} is not an object")
            tags = operation.get("tags")
            if not isinstance(tags, list) or len(tags) != 1 or not isinstance(tags[0], str) or not tags[0]:
                fail(f"operation {method.upper()} {path} must declare exactly one tag")
            operations.append(
                {
                    "unit": f"operation:{method.upper()}:{path}",
                    "kind": "operation",
                    "slice": 0,
                    "source": f"/paths/{pointer_part(path)}/{pointer_part(method.lower())}",
                    "group": tags[0],
                }
            )
    schemas = [
        {
            "unit": f"schema:{name}",
            "kind": "schema",
            "slice": 0,
            "source": f"/components/schemas/{pointer_part(name)}",
            "group": schema_group(name),
        }
        for name in spec["components"]["schemas"]
    ]
    events = event_rows()
    if len(paths) != 75 or len(operations) != 89 or len(schemas) != 160 or len(events) != 79 or len(pages) != 87:
        fail(
            "source counts changed: "
            f"{len(paths)} paths, {len(operations)} operations, {len(schemas)} schemas, "
            f"{len(events)} events, {len(pages)} pages"
        )
    all_units = [row["unit"] for row in operations + schemas + events]
    if len(set(all_units)) != 328:
        fail(f"worklist unit IDs are not unique ({len(set(all_units))}/328)")
    return spec, metadata, operations + schemas + events, pages, digest


def assign_slices(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assigned = []
    for row in rows:
        unit = row["unit"]
        if unit in EXPLICIT_UNITS:
            number = 1
        elif row["kind"] == "operation":
            try:
                number = OPERATION_SLICES[row["group"]]
            except KeyError:
                fail(f"operation {unit} has unmapped tag {row['group']!r}")
        elif row["kind"] == "schema":
            try:
                number = SCHEMA_SLICES[row["group"]]
            except KeyError:
                fail(f"schema {unit} has unmapped group {row['group']!r}")
        else:
            prefix = row["group"]
            if not ("ami" <= prefix <= "ward"):
                fail(f"event {unit} has out-of-range prefix {prefix!r}")
            number = 11 if prefix <= "mining" else 12
        row = dict(row)
        row["slice"] = number
        assigned.append(row)
    order = {kind: index for index, kind in enumerate(KINDS)}
    assigned.sort(key=lambda row: (row["slice"], order[row["kind"]], row["unit"]))
    if len(assigned) != 328 or len({row["unit"] for row in assigned}) != 328:
        fail("assigned worklist does not contain exactly 328 unique units")
    actual = [sum(row["slice"] == number for row in assigned) for number, _ in SLICES]
    if tuple(actual) != SLICE_COUNTS:
        fail(f"slice counts changed: {actual!r}, expected {list(SLICE_COUNTS)!r}")
    return assigned


def generated() -> tuple[list[dict[str, Any]], dict[Path, bytes], bytes, str]:
    _spec, _metadata, source_rows, pages, digest = validate_sources()
    worklist = assign_slices(source_rows)
    outputs: dict[Path, bytes] = {WORKLIST: jsonl_bytes(worklist), DOC_PAGES: jsonl_bytes(pages)}
    for number, stem in SLICES:
        outputs[AUDIT / "inputs" / f"{stem}.jsonl"] = jsonl_bytes(
            [row for row in worklist if row["slice"] == number]
        )
    return worklist, outputs, jsonl_bytes(pages), digest


def prepare() -> None:
    worklist, outputs, _pages, _digest = generated()
    AUDIT.joinpath("results").mkdir(parents=True, exist_ok=True)
    for path, data in outputs.items():
        atomic_write(path, data)
    counts = [sum(row["slice"] == number for row in worklist) for number, _ in SLICES]
    kinds = Counter(row["kind"] for row in worklist)
    print(
        f"{len(worklist)} units: {kinds['operation']} operations, {kinds['schema']} schemas, "
        f"{kinds['event']} events; 12 slices"
    )
    print("slice counts: " + ",".join(str(count) for count in counts))


def parse_jsonl(path: Path) -> list[tuple[int, Any]]:
    if not path.is_file():
        fail(f"missing result file: {path.relative_to(ROOT)}")
    rows = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read {path.relative_to(ROOT)}: {exc}")
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            rows.append((line_number, json.loads(line)))
        except json.JSONDecodeError as exc:
            fail(f"{path.relative_to(ROOT)}:{line_number}: invalid JSON: {exc.msg}")
    return rows


def evidence_path(value: Any) -> tuple[Path, int]:
    if not isinstance(value, str) or "\n" in value or "\r" in value:
        fail("evidence must be a single path:line string")
    match = EVIDENCE.fullmatch(value)
    if not match:
        fail(f"invalid evidence locator: {value!r}")
    raw_path, raw_line = match.groups()
    path = Path(raw_path)
    if path.is_absolute() or ".." in path.parts:
        fail(f"evidence is not repository-relative: {value!r}")
    normalized = path.as_posix()
    if normalized.startswith("AUDIT-") or normalized.startswith("audit/"):
        fail(f"evidence points into generated audit output: {value!r}")
    target = ROOT / path
    if not target.is_file():
        fail(f"evidence file does not exist: {value!r}")
    line_number = int(raw_line)
    if line_number > len(target.read_bytes().splitlines()):
        fail(f"evidence line is out of range: {value!r}")
    return target, line_number
def validate_result_row(
    row: Any,
    expected_unit: str,
    expected_fields: set[str] | None,
    location: str,
) -> dict[str, Any]:
    base_keys = {"unit", "verdict", "client_symbol", "evidence", "notes"}
    expected_keys = base_keys | ({"field_verdicts"} if expected_fields is not None else set())
    if not isinstance(row, dict) or set(row) != expected_keys:
        fail(f"{location}: result row keys differ from the required contract")
    if row["unit"] != expected_unit:
        fail(f"{location}: expected unit {expected_unit!r}, got {row['unit']!r}")
    verdict = row["verdict"]
    if not isinstance(verdict, str) or verdict not in VERDICTS:
        fail(f"{location}: invalid verdict {verdict!r}")
    symbol = row["client_symbol"]
    if verdict in {"covered", "partial"} and (not isinstance(symbol, str) or not symbol.strip()):
        fail(f"{location}: {verdict} requires a nonempty client_symbol")
    if verdict in {"missing", "n/a"} and symbol is not None:
        fail(f"{location}: {verdict} requires client_symbol null")
    if verdict == "drift" and symbol is not None and (not isinstance(symbol, str) or not symbol.strip()):
        fail(f"{location}: drift client_symbol must be null or nonempty string")
    notes = row["notes"]
    if not isinstance(notes, str) or "\n" in notes or "\r" in notes:
        fail(f"{location}: notes must be a newline-free string")
    if verdict != "covered" and not notes.strip():
        fail(f"{location}: {verdict} requires nonempty notes")
    if len(SENTENCE_END.findall(notes)) > 2:
        fail(f"{location}: notes contain more than two sentence terminators")
    if notes.startswith("Source disagreement:") and not re.search(
        r"(?:^|[\s(`\[])(?:reference|src|crates|policy|openapi\.json)[^ \t,;)]*:[1-9][0-9]*",
        notes,
    ):
        fail(f"{location}: source disagreement notes must cite a repository path:line")
    row_evidence = row["evidence"]
    if isinstance(row_evidence, list):
        if not row_evidence:
            fail(f"{location}: row has no evidence")
        for locator in row_evidence:
            evidence_path(locator)
    else:
        evidence_path(row_evidence)
    if expected_fields is not None:
        field_verdicts = row["field_verdicts"]
        if not isinstance(field_verdicts, dict) or set(field_verdicts) != expected_fields:
            fail(f"{location}: field_verdicts must exhaustively match the schema properties")
        for pointer, field_result in field_verdicts.items():
            if not isinstance(field_result, dict) or set(field_result) != {"verdict", "evidence"}:
                fail(f"{location}: {pointer} must have verdict and evidence")
            if field_result["verdict"] not in VERDICTS:
                fail(f"{location}: {pointer} has invalid verdict {field_result['verdict']!r}")
            evidence = field_result["evidence"]
            if isinstance(evidence, list):
                if not evidence:
                    fail(f"{location}: {pointer} has no evidence")
                for locator in evidence:
                    evidence_path(locator)
            else:
                evidence_path(evidence)
    return row


def validate_results(worklist: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_slice = {number: [row["unit"] for row in worklist if row["slice"] == number] for number, _ in SLICES}
    schemas = read_json(OPENAPI)["components"]["schemas"]
    fields_by_unit = {
        f"schema:{name}": schema_field_pointers(schema)
        for name, schema in schemas.items()
    }
    for unit, fields in DOC_ONLY_FIELDS.items():
        if unit not in fields_by_unit:
            fail(f"docs-only fields reference unknown schema unit {unit!r}")
        fields_by_unit[unit].update(fields)
        for evidence in fields.values():
            evidence_path(evidence)
    all_rows = []
    seen: set[str] = set()
    for number, stem in SLICES:
        result_path = AUDIT / "results" / f"{stem}.jsonl"
        parsed = parse_jsonl(result_path)
        expected = by_slice[number]
        if len(parsed) != len(expected):
            missing = next((unit for unit in expected if unit not in {row.get("unit") for _, row in parsed if isinstance(row, dict)}), None)
            if missing:
                fail(f"{result_path.relative_to(ROOT)}: missing unit {missing}")
            fail(f"{result_path.relative_to(ROOT)}: expected {len(expected)} rows, got {len(parsed)}")
        for index, ((line_number, raw), unit) in enumerate(zip(parsed, expected), 1):
            location = f"{result_path.relative_to(ROOT)}:{line_number}"
            row = validate_result_row(raw, unit, fields_by_unit.get(unit), location)
            if unit in seen:
                fail(f"duplicate unit {unit}")
            seen.add(unit)
            all_rows.append(row)
    expected_units = [row["unit"] for row in worklist]
    if len(seen) != len(expected_units):
        missing = next(unit for unit in expected_units if unit not in seen)
        fail(f"missing unit {missing}")
    if seen != set(expected_units):
        unexpected = next(iter(seen - set(expected_units)))
        fail(f"unexpected unit {unexpected}")
    validate_calibration(all_rows)
    return all_rows


def validate_calibration(rows: list[dict[str, Any]]) -> None:
    by_unit = {row["unit"]: row for row in rows}
    event_evidence = {
        "event:hub.maintained": "src/events.rs:532",
        "event:hub.warning": "src/events.rs:533",
        "event:multiplayer.replicant_entered": "src/events.rs:539",
        "event:multiplayer.replicant_left": "src/events.rs:540",
    }
    for unit, evidence in event_evidence.items():
        row = by_unit[unit]
        if row["verdict"] != "covered" or row["evidence"] != evidence:
            fail(f"calibration mismatch for {unit}: expected covered at {evidence}")
    location = by_unit["schema:app_schemas_locations_LocationResponseSchema"]
    location_note = location["notes"].lower()
    if (
        location["verdict"] != "drift"
        or location["client_symbol"] != "replicant_client::raw::locations::Location"
        or location["evidence"]
        != ["src/raw/locations.rs:80", "reference/replicant-space-2-5-2/api/locations/index.md:78"]
        or "boolean" not in location_note
        or "atmosphere" not in location_note
        or "option<string>" not in location_note
    ):
        fail("calibration mismatch for schema:app_schemas_locations_LocationResponseSchema")


def md(value: Any) -> str:
    if isinstance(value, list):
        value = "<br>".join(value)
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_report(worklist: list[dict[str, Any]], rows: list[dict[str, Any]], digest: str) -> bytes:
    row_by_unit = {row["unit"]: row for row in rows}
    counts = Counter(row["verdict"] for row in rows)
    by_kind = {
        kind: Counter(
            row_by_unit[item["unit"]]["verdict"]
            for item in worklist
            if item["kind"] == kind
        )
        for kind in KINDS
    }
    ordered = [row_by_unit[item["unit"]] for item in worklist]
    findings = {
        verdict: [row for row in ordered if row["verdict"] == verdict]
        for verdict in ("covered", "missing", "drift", "partial", "n/a")
    }
    disagreements = [row for row in ordered if row["notes"].startswith("Source disagreement:")]
    lines = [
        "# Replicant Space 2.5.2 Contract Unit Audit",
        "",
        "## Verdict summary",
        "",
        "| Scope | covered | partial | missing | drift | n/a | total |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| Total | {counts['covered']} | {counts['partial']} | {counts['missing']} | {counts['drift']} | {counts['n/a']} | {len(rows)} |",
    ]
    for kind in KINDS:
        c = by_kind[kind]
        lines.append(
            f"| {kind} | {c['covered']} | {c['partial']} | {c['missing']} | "
            f"{c['drift']} | {c['n/a']} | {sum(c.values())} |"
        )

    def finding_section(title: str, selected: list[dict[str, Any]]) -> None:
        lines.extend(["", f"## {title}", "", "| Unit | Verdict | Client symbol | Evidence | Notes |", "|---|---|---|---|---|"])
        for row in selected:
            symbol = md(row["client_symbol"]) if row["client_symbol"] is not None else ""
            lines.append(
                f"| `{md(row['unit'])}` | {row['verdict']} | `{symbol}` | "
                f"`{md(row['evidence'])}` | {md(row['notes'])} |"
            )

    finding_section("Missing and drift findings", findings["missing"] + findings["drift"])
    finding_section("Partial findings", findings["partial"])
    finding_section("n/a rows", findings["n/a"])
    lines.extend(["", "## Source disagreements", "", "| Unit | Evidence | Notes |", "|---|---|---|"])
    for row in disagreements:
        lines.append(f"| `{md(row['unit'])}` | `{md(row['evidence'])}` | {md(row['notes'])} |")
    lines.extend(
        [
            "",
            "## Source snapshot",
            "",
            "- Snapshot: `reference/replicant-space-2-5-2/`",
            f"- OpenAPI SHA-256: `{digest}`",
            "- Counts: 75 paths, 89 operations, 160 schemas, 79 catalogue events, 87 rendered pages, 328 worklist units.",
            "",
            "## Methodology",
            "",
            "OpenAPI is authoritative, followed by the 87-page rendered markdown mirror, then the v2.5.2 changelog. Source disagreements remain `drift` findings. `covered` requires the complete public transport or representation; `partial` records an incomplete symbol; `missing` records no public implementation; `n/a` is reserved for a concrete player-facing exclusion.",
            "",
            "## Fixed slices",
            "",
            "| # | Slice | Units |",
            "|---:|---|---:|",
        ]
    )
    for number, stem in SLICES:
        lines.append(f"| {number} | `{stem}` | {sum(item['slice'] == number for item in worklist)} |")
    lines.extend(["", "## Calibration findings", ""])
    for unit in sorted(EXPLICIT_UNITS):
        if unit.startswith("event:") or unit == "schema:app_schemas_locations_LocationResponseSchema":
            row = row_by_unit[unit]
            lines.append(f"- `{unit}`: **{row['verdict']}**, `{row['evidence']}` — {row['notes']}")
    lines.extend(
        [
            "",
            "## Changelog delta adjudication",
            "",
            "The v2.5.2 changelog documents the event-catalogue field additions (`reference/replicant-space-2-5-2/changelog/index.md:26`) and separately lists reactive AMI Mining Controller re-evaluation, account-wipe and webhook behavior, compacted-device capacity, both event payload fields, notification deduplication, and BobNet chatter changes (`reference/replicant-space-2-5-2/changelog/index.md:30-37`). The wire-visible webhook and event payload deltas are adjudicated in their schema and event rows; the remaining server-behavior changes introduce no operation, schema, or catalogue-event unit and are therefore excluded from the generated worklist.",
            "",
            "## Artifacts",
            "",
            "- [`audit/2.5.2/doc-pages.jsonl`](audit/2.5.2/doc-pages.jsonl)",
            "- [`audit/2.5.2/worklist.jsonl`](audit/2.5.2/worklist.jsonl)",
            "- [`audit/2.5.2/merged.jsonl`](audit/2.5.2/merged.jsonl)",
        ]
    )
    for _number, stem in SLICES:
        lines.append(f"- [`audit/2.5.2/results/{stem}.jsonl`](audit/2.5.2/results/{stem}.jsonl)")
    lines.extend(["", "## Appendix: covered rows", "", "| Unit | Client symbol | Evidence |", "|---|---|---|"])
    for row in findings.get("covered", []):
        lines.append(f"| `{md(row['unit'])}` | `{md(row['client_symbol'])}` | `{md(row['evidence'])}` |")
    lines.append("")
    return ("\n".join(lines)).encode("utf-8")


def finalize(check_only: bool = False) -> tuple[bytes, bytes]:
    worklist, generated_outputs, _pages, digest = generated()
    for path, expected in generated_outputs.items():
        if not path.is_file() or path.read_bytes() != expected:
            fail(f"checked-in generated bytes differ: {path.relative_to(ROOT)}")
    rows = validate_results(worklist)
    merged_data = jsonl_bytes(rows)
    report_data = render_report(worklist, rows, digest)
    if check_only:
        for path, expected in ((MERGED, merged_data), (REPORT, report_data)):
            if not path.is_file() or path.read_bytes() != expected:
                fail(f"checked-in generated bytes differ: {path.relative_to(ROOT)}")
    else:
        atomic_write(MERGED, merged_data)
        atomic_write(REPORT, report_data)
    return merged_data, report_data


def check() -> None:
    finalize(check_only=True)
    worklist, _outputs, _pages, _digest = generated()
    print(f"check ok: {len(worklist)}/{len(worklist)} units adjudicated")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "finalize", "check"))
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            prepare()
        elif args.command == "finalize":
            finalize()
            print("328/328 units adjudicated")
        else:
            check()
    except AuditError as exc:
        print(f"coverage_audit: error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
