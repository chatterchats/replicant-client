# Replicant Client Tracing Instrumentation Report

## Scope

This instrumentation pass replaces the repository's direct `log` usage with
structured `tracing` events and adds duration-aware observability across the
paths used by `examples/initialize_colony_database.rs`.

The library emits events but does not install a global subscriber. Applications
remain responsible for selecting a subscriber, output format, timestamp source,
filters, and telemetry exporter.

The initializer example installs a human-readable `tracing-subscriber`
formatter with:

- wall-clock timestamps;
- target names;
- thread IDs and names;
- span close events;
- `RUST_LOG` filtering;
- millisecond duration fields.

## Important trace targets

| Target | Scope |
| --- | --- |
| `replicant_client::initializer` | Overall initialization phases and per-system progress |
| `replicant_client::raw::http` | HTTP attempts, response timing, retries, status, and decoding |
| `replicant_client::raw::rate_limit` | Local permit waits and server rate-limit observations |
| `replicant_client::sync` | Overall synchronization, domain timing, pages, persistence, and reconciliation |
| `replicant_client::galaxy` | Global catalogue and paginated replicant-star synchronization |
| `replicant_client::locations` | Recursive system/location hydration |
| `replicant_client::events` | Event catch-up, event application, SSE connection, and startup continuity |
| `replicant_client::ops` | Durable operation registration, submission, and outcome classification |
| `replicant_client::store` | SQLite worker queue wait and execution durations |
| `replicant_client::state` | Restoration and immutable snapshot publication |
| `replicant_client::gateway::*` | Managed request, normalization, persistence, and total timing |
| `replicant_client::query::devices` | Device-query stage counts and local evaluation duration |
| `replicant_client::query::locations` | Location predicate outcomes and local evaluation duration |

## HTTP timing fields

Each raw request can now expose:

- `rate_limit_wait_ms`
- `request_prepare_ms`
- `time_to_headers_ms`
- `metadata_ms`
- `body_read_ms`
- `decode_ms`
- `attempt_elapsed_ms`
- `elapsed_ms`
- `response_bytes`
- `attempt`
- `status`
- local and server request IDs

This allows a slow request to be classified as:

1. local/server throttling;
2. request setup;
3. network/server latency before headers;
4. response transfer;
5. JSON decoding;
6. retries.

No request bodies, bearer tokens, Authorization headers, or secret values are
intentionally logged.

## Sync and initializer timing

The initializer emits a final `initializer.summary` with:

- `full_sync_ms`
- `star_sync_ms`
- `hydration_ms`
- total `elapsed_ms`
- systems and locations processed
- failures
- eligible candidate count

The sync engine separately emits:

- `sync.started`
- `sync.domain_started`
- `sync.domain_completed`
- `sync.domain_failed`
- page-level device and inventory timing
- request, normalization, persistence, and reconciliation durations where available

The galaxy and location subsystems add their own page/entity timings, allowing
the initializer summary to be drilled down into individual API requests and
SQLite operations.

## SQLite timing

The SQLite store worker now records:

- command ID;
- command/closure type;
- `queue_wait_ms`;
- `execute_ms`;
- total `elapsed_ms`.

A high `queue_wait_ms` indicates store contention or backpressure. A high
`execute_ms` indicates the SQLite command or transaction itself is slow.

## Identified likely slowdown

The current location hydrator accepts a configured concurrency value but still
performs the traversal serially.

The instrumentation reports:

- `configured_concurrency`
- `effective_concurrency = 1`

and emits:

```text
locations.hydration_concurrency_not_applied
```

This is likely to dominate initialization time when many explored systems and
location details must be fetched. The instrumentation makes the limitation
visible; it does not silently introduce concurrent commits or change hydration
correctness in this pass.

## Running the initializer

Detailed default output:

```sh
cargo run --example initialize_colony_database
```

Explicit performance filter:

```sh
RUST_LOG='replicant_client=info,replicant_client::initializer=debug,replicant_client::raw::http=debug,replicant_client::raw::rate_limit=debug,replicant_client::sync=debug,replicant_client::galaxy=debug,replicant_client::locations=debug,replicant_client::state=debug,replicant_client::store=debug' \
  cargo run --example initialize_colony_database
```

Maximum detail and capture:

```sh
RUST_LOG='replicant_client=trace' \
  cargo run --example initialize_colony_database 2>&1 | tee initializer-trace.log
```

A quieter phase summary:

```sh
RUST_LOG='replicant_client::initializer=info,replicant_client::sync=info,replicant_client::galaxy=info,replicant_client::locations=info' \
  cargo run --example initialize_colony_database
```

## Files added

- `docs/observability.md`
- `scripts/observability_policy_check.py`

## Major files modified

- `Cargo.toml`
- `.github/workflows/ci.yml`
- `Makefile`
- `README.md`
- `examples/initialize_colony_database.rs`
- `src/raw/client.rs`
- `src/raw/rate_limit.rs`
- `src/managed/client.rs`
- `src/managed/sync.rs`
- `src/managed/galaxy.rs`
- `src/managed/operation.rs`
- `src/managed/events.rs`
- `src/managed/store.rs`
- `src/managed/state.rs`
- `src/managed/gateways.rs`
- `src/domain/adapters.rs`
- `src/domain/merge.rs`
- `src/lib.rs`

## Static validation performed

Passed:

```text
observability_policy_check.py
forward_compatibility_policy_check.py
raw_transport_policy_check.py
schema_policy_check.py
authority_matrix_check.py
mutation_adapter_policy_check.py
phase_11_5_remediation_check.py
phase_11_5_remediation_check.py --self-test
Cargo.toml parsing
legacy log-reference scan
patch whitespace check
```

The existing `contract_policy_check.py` still fails because historical
`replicant-sdk` references are present in the checked-in rewrite guide and
post-Phase 11 review. Those references existed before this observability
change and are unrelated to tracing.

## Rust-toolchain validation limitation

This environment did not contain `cargo`, `rustc`, or `rustfmt`, and external
toolchain installation was unavailable. Therefore I could not independently
run:

- `cargo fmt`;
- `cargo check`;
- Clippy;
- tests;
- Rustdoc;
- examples;
- packaging.

`Cargo.lock` was not regenerated. Because `tracing-subscriber` is newly used by
the initializer example, run Cargo once without `--locked` and commit the
updated lockfile.

Recommended first validation sequence:

```sh
cargo check --all-features
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- --deny warnings
cargo test --all-features
cargo check --all-features --examples
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
python3 scripts/observability_policy_check.py
```

Then run the project's complete feature, MSRV, policy, and package matrix.
