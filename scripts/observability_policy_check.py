#!/usr/bin/env python3
"""Static observability policy for replicant-client.

The check intentionally avoids depending on a Rust toolchain so it can catch
accidental regressions in logging dependencies, targets, and secret handling
before compilation.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
CARGO = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
SOURCE_PATHS = [ROOT / "src", ROOT / "examples"]


def rust_sources() -> list[Path]:
    return sorted(
        path
        for base in SOURCE_PATHS
        for path in base.rglob("*.rs")
        if path.is_file()
    )


def fail(message: str) -> None:
    print(f"observability policy check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


if re.search(r"(?m)^\s*log\s*=", CARGO):
    fail("Cargo.toml still declares the legacy `log` crate")
if not re.search(r"(?m)^\s*tracing\s*=", CARGO):
    fail("Cargo.toml does not declare `tracing`")
if "tracing-subscriber" not in CARGO:
    fail("examples/tests do not have tracing-subscriber available")

combined = "\n".join(path.read_text(encoding="utf-8") for path in rust_sources())
for forbidden in ("log::", "use log", "extern crate log"):
    if forbidden in combined:
        fail(f"legacy logging reference remains: {forbidden!r}")

library = "\n".join(
    path.read_text(encoding="utf-8") for path in (ROOT / "src").rglob("*.rs")
)
for forbidden in (
    "tracing_subscriber::fmt().init()",
    "tracing_subscriber::registry().init()",
    "set_global_default(",
):
    if forbidden in library:
        fail("the library must emit tracing events but never install a global subscriber")

required_targets = {
    "replicant_client::raw::http",
    "replicant_client::raw::rate_limit",
    "replicant_client::sync",
    "replicant_client::galaxy",
    "replicant_client::locations",
    "replicant_client::events",
    "replicant_client::ops",
    "replicant_client::store",
    "replicant_client::state",
    "replicant_client::query::devices",
    "replicant_client::query::locations",
}
missing_targets = sorted(target for target in required_targets if target not in combined)
if missing_targets:
    fail(f"required tracing target(s) missing: {', '.join(missing_targets)}")

required_events = {
    "http.request_started",
    "http.response_decoded",
    "sync.started",
    "sync.domain_completed",
    "galaxy.replicant_stars_page_completed",
    "locations.hydration_location_completed",
    "store.command_completed",
    "state.snapshot_published",
    "events.apply_completed",
    "operation.submission_accepted",
    "query.devices_evaluated",
    "location_query.evaluated",
}
missing_events = sorted(event for event in required_events if event not in combined)
if missing_events:
    fail(f"required timing event(s) missing: {', '.join(missing_events)}")

initializer = (ROOT / "examples" / "initialize_colony_database.rs").read_text(
    encoding="utf-8"
)
for required in (
    "EnvFilter",
    "FmtSpan::CLOSE",
    "time::SystemTime",
    "RUST_LOG",
    "full_sync_ms",
    "star_sync_ms",
    "hydration_ms",
):
    if required not in initializer:
        fail(f"initializer tracing setup/summary is missing {required!r}")

# Guard the most obvious secret regressions. Structured paths and IDs are
# permitted, but token values and Authorization headers must not be fields.
for pattern in (
    r"(?i)(debug|info|warn|error|trace)!\([^;]*(token\s*=|authorization\s*=)",
    r"(?i)(debug|info|warn|error|trace)!\([^;]*expose_secret",
):
    if re.search(pattern, combined, flags=re.DOTALL):
        fail("a tracing event appears to record token/authorization material")

if not (ROOT / "docs" / "observability.md").is_file():
    fail("docs/observability.md is missing")

print(
    "observability policy check passed: tracing-only logging, required targets/events, "
    "initializer subscriber, and secret guards are present"
)
