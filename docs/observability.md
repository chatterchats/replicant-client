# Observability and performance tracing

`replicant-client` emits structured `tracing` events. The library does not
install a global subscriber; the application owns formatting, timestamps,
filtering, and export.

The `initialize_colony_database` example installs `tracing-subscriber` with:

- wall-clock timestamps;
- thread IDs and names;
- target names;
- span-close events;
- millisecond duration fields;
- `RUST_LOG` filtering.

## Run the initializer

```sh
RUST_LOG='replicant_client=info,replicant_client::raw::http=debug,replicant_client::raw::rate_limit=debug,replicant_client::sync=debug,replicant_client::galaxy=debug,replicant_client::locations=debug,replicant_client::state=debug,replicant_client::store=debug' \
  cargo run --example initialize_colony_database
```

For maximum detail:

```sh
RUST_LOG='replicant_client=trace' \
  cargo run --example initialize_colony_database
```

Capture a complete run while keeping it visible in the terminal:

```sh
RUST_LOG='replicant_client=trace' \
  cargo run --example initialize_colony_database 2>&1 | tee initializer-trace.log
```

For a quieter phase-only view:

```sh
RUST_LOG='replicant_client::initializer=info,replicant_client::sync=info,replicant_client::galaxy=info,replicant_client::locations=info' \
  cargo run --example initialize_colony_database
```

## Targets

| Target | What it measures |
| --- | --- |
| `replicant_client::initializer` | Whole initializer phases and per-system progress |
| `replicant_client::raw::http` | Rate-limit wait, time to response headers, body read, decoding, attempts, status |
| `replicant_client::raw::rate_limit` | Local permit delays and server schedule updates |
| `replicant_client::sync` | Overall sync, each domain, pages, normalization, persistence, reconciliation |
| `replicant_client::galaxy` | Catalogue refresh and each replicant-star page |
| `replicant_client::locations` | Each recursively hydrated system object |
| `replicant_client::events` | Catch-up pages, event application, SSE connection, startup continuity |
| `replicant_client::ops` | Durable registration, claim, HTTP submission, outcome classification |
| `replicant_client::store` | SQLite worker queue wait and execution time |
| `replicant_client::state` | Restoration and immutable snapshot publication |
| `replicant_client::domain` | Trace-level normalization and merge decisions |
| `replicant_client::gateway::account` | Managed account request, normalization, persistence, and total time |
| `replicant_client::gateway::devices` | Managed device detail/list phases |
| `replicant_client::gateway::replicants` | Managed owned-replicant detail phases |
| `replicant_client::query::devices` | Local device filter stage counts and elapsed time |
| `replicant_client::query::locations` | Local location predicate matched/rejected/unknown counts |

## Important duration fields

- `elapsed_ms`: total duration of the event's operation.
- `rate_limit_wait_ms`: time spent waiting for a local/server rate-limit permit.
- `delay_enforced`: whether the observed server countdown actually changed the
  local schedule. Replicant Space may return `Retry-After` and
  `X-RateLimit-Reset` on successful responses as informational window
  countdowns; the client enforces them only for HTTP 429 or when
  `X-RateLimit-Remaining` is zero.
- `time_to_headers_ms`: request send through receipt of response headers.
- `body_read_ms`: time spent receiving the response body.
- `decode_ms`: JSON decoding time.
- `normalize_ms`: raw DTO to normalized domain observation time.
- `persist_ms`: durable commit time from the caller's perspective.
- `queue_wait_ms`: time a SQLite command waited in the store worker queue.
- `execute_ms`: time the SQLite worker spent executing the command.
- `apply_ms`: time spent applying a page of events.

All durations are integer milliseconds.

## Finding a slow initializer phase

Start with `initializer.summary`:

- `full_sync_ms`
- `star_sync_ms`
- `hydration_ms`

Then drill down.

### Full sync is slow

Inspect `sync.domain_completed` and compare `elapsed_ms` by domain.

For devices and inventory, inspect page events:

- `sync.devices_page_completed`
- `sync.inventory_page_completed`

For account/replicant/location detail reads, compare their elapsed values with
matching `raw::http` request events.

### HTTP is slow

Inspect `http.response_decoded`:

- high `rate_limit_wait_ms` means deliberate throttling;
- `rate_limit.delay_informational` means the server supplied a window
  countdown on a successful response while quota remained, so no extra wait
  was imposed;
- high `time_to_headers_ms` means network/server latency;
- high `body_read_ms` means response transfer time;
- high `decode_ms` means JSON decoding cost.

The same `local_request_id` appears on request, retry, response, and failure
events.


### Managed gateway or local query is slow

Managed gateway events separate the raw request from normalization and durable
commit:

- `account.get_completed`
- `device.get_completed`
- `devices.list_completed`
- `replicant.get_owned_completed`

Compare `request_ms`, `normalize_ms`, and `persist_ms`. The raw request has a
matching `raw::http` event with more detailed network phases.

Local query events never represent network work:

- `query.devices_evaluated` includes counts after each filter stage, including
  `adopted_relationships` and the `without_adopted_devices` flag.
- `location_query.evaluated` includes matched, rejected, and unknown predicate
  totals.

A slow local query with no corresponding HTTP event is CPU/snapshot work, not
API latency.

### SQLite is slow

Inspect `store.command_completed`:

- high `queue_wait_ms` means store contention/backpressure;
- high `execute_ms` means the SQLite transaction itself is slow;
- `operation_type` identifies the closure/caller type when available.

Then correlate it with `persist_ms` in sync, galaxy, location, or operation
events.

### System hydration is slow

Inspect:

- `initializer.system_hydration_completed`
- `locations.hydration_location_completed`
- matching `http.response_decoded` events.

The current hydration implementation records both `configured_concurrency` and
`effective_concurrency`. If configured concurrency is greater than one while
effective concurrency remains one, the traversal is still using the serial
commit-before-next pipeline. The log emits
`locations.hydration_concurrency_not_applied` to make that limitation explicit.

### Star census is slow

Inspect:

- `galaxy.replicant_stars_page_completed`
- `request_ms`
- `normalize_and_persist_ms`
- rate-limit waits on the corresponding raw HTTP requests.

## Security

Instrumentation must not emit:

- bearer tokens;
- authorization headers;
- request bodies;
- recovery or verification secrets;
- full error bodies before redaction.

The raw transport logs method, relative path, request identifiers, timing,
status, byte counts, and rate-limit metadata. The initializer never logs its
token or the complete `Config` value.

## Application setup

A normal application may configure its own subscriber:

```rust
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .with_target(true)
    .with_span_events(FmtSpan::CLOSE)
    .init();
```

Install the subscriber once, near process startup, before constructing the
client.

## Response-body limits

Ordinary endpoints retain the raw client's conservative default response cap
(1 MiB). The complete unpaginated `GET /v1/stars` catalogue has a separate
bounded default of 32 MiB because its legitimate payload is substantially
larger. HTTP trace events include `response_body_limit_bytes`, so a cap failure
can be distinguished from network, server, or decoding latency.

Applications may tune only the catalogue cap through
`ClientBuilder::max_star_catalogue_response_body_bytes`. The initializer also
accepts `REPLICANT_INIT_STAR_CATALOGUE_LIMIT_BYTES`.
