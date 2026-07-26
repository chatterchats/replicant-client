# replicant-client

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