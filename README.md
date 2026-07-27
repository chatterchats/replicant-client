# replicant-client

## Colony database initializer

`cargo run --example initialize_colony_database` performs only managed safe
reads and populates the durable survey database from knowledge the account has
already discovered. It cannot discover unsurveyed worlds; use `REPLICANT_DB`
to choose the SQLite path and `REPLICANT_INIT_*` bounds to cap the traversal.

A durable, stateful Rust client for building [Replicant Space](https://replicant.space) applications.

`replicant-client` targets the Replicant Space `2.3.1` contract. It is
client-centered: the normal entry point is `replicant_client::Client`, which
fetches, validates, normalizes, persists, publishes, watches, reconciles, and
performs game operations, without requiring the application to assemble a
transport client, runtime, state actor, or persistence layer by hand.

```rust
use replicant_client::{Client, SecretString};

let client = Client::builder()
    .authentication_token(SecretString::from(token))
    .sqlite("replicant-client.sqlite")
    .start()
    .await?;

client.ready().await?;

let miners = client
    .devices()
    .miners()
    .idle()
    .at("SOL")
    .collect()
    .await?;
```

**Status:** this repository is at the Phase 1 bootstrap stage. The package,
feature graph, and checked-in Replicant Space 2.3.1 contract corpus exist;
the client itself does not yet. See
[`docs/implementation/rewrite-guide.md`](docs/implementation/rewrite-guide.md)
for the full implementation plan.

## Features

| Feature | Implies | Provides |
| --- | --- | --- |
| `raw` | — | Typed raw HTTP transport for the current, non-deprecated, non-admin contract. |
| `events` | `raw` | Raw SSE parsing and event streaming. |
| `managed` (default) | `events` | SQLite-backed durable state, synchronization, durable operations, and the managed `Client`. |
| `rustls-tls` (default) | — | reqwest's rustls TLS backend. |
| `native-tls` | — | reqwest's native-tls TLS backend. |

## Contract

The corrected Replicant Space 2.3.1 documentation and OpenAPI spec are
checked in under [`reference/replicant-space/`](reference/replicant-space/).
[`policy/`](policy/) records a machine-readable inventory of all 84 contract
operations: 77 supported, 5 deprecated, and 2 admin-only (7 excluded).
`scripts/contract_policy_check.py` verifies this inventory against the
checked-in OpenAPI document on every run; see the Makefile's
`contract-policy-check` target.

## Development

```sh
cargo fmt --all -- --check
cargo check
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
cargo check --all-features
make contract-policy-check
```

## License

MIT. See [LICENSE](LICENSE).

## Observability

The client emits structured `tracing` events for HTTP requests, rate-limit
waits, managed synchronization, event catch-up, durable operations, SQLite
work, state publication, galaxy hydration, and location traversal. The library
does not install a subscriber; applications remain in control of formatting
and export. The `initialize_colony_database` example includes a timestamped
`tracing-subscriber` setup and useful default filters.

See `docs/observability.md` for targets, duration fields, and a workflow for
locating initializer and synchronization bottlenecks.

The colony database initializer accepts
`REPLICANT_INIT_STAR_CATALOGUE_LIMIT_BYTES` to override the dedicated bounded
`GET /v1/stars` response cap (32 MiB by default). Ordinary API endpoints keep
their smaller default response limit.
