# replicant-client

`replicant-client` is a durable, stateful Rust client for the Replicant Space
API. It combines typed HTTP and SSE access with a managed client that
normalizes remote observations, stores them in SQLite, publishes consistent
snapshots, reconciles state, and journals mutations before sending them.

The crate targets the Replicant Space documentation through **2.3.5**. Its
machine-readable baseline is the checked-in, verified 2.3.3 OpenAPI document,
with later rendered-document changes recorded explicitly under
[`docs/contract`](docs/contract).

## Requirements

- Rust 1.94 or newer
- Tokio when using the asynchronous client
- A Replicant Space API token for authenticated requests

```toml
[dependencies]
replicant-client = "1.0.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The default features enable the managed client and rustls TLS.

## Managed client

`replicant_client::Client` is the normal entry point. A file-backed client
restores its last committed snapshot before applying its startup policy.

```rust,no_run
use replicant_client::{Client, SecretString, StartupPolicy};

#[tokio::main]
async fn main() -> replicant_client::Result<()> {
    let token = std::env::var("REPLICANT_API_KEY")
        .map_err(|_| replicant_client::Error::Configuration {
            message: "set REPLICANT_API_KEY".into(),
        })?;

    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .sqlite("replicant-client.sqlite")
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;

    client.ready().await?;
    println!("account: {:?}", client.account().get().await?);
    client.close().await
}
```

Use `ClientBuilder::in_memory()` for tests or temporary applications. The
builder also configures the base URL, request timeout, rate-limit policies,
event streaming, reconciliation, and startup behavior.

### Startup policies

| Policy | Behavior |
| --- | --- |
| `RestoreOnly` | Restore SQLite state without network activity. |
| `Essential` | Bind the account, establish the essential baseline, catch up events, and connect the live event stream. This is the default. |
| `Full` | Require all bounded account-domain baselines before startup is ready. |

`status()`, `readiness()`, and `watch_status()` expose lifecycle health.
`ready()` waits for the configured policy. `close()` is idempotent across all
clones and flushes the store after stopping background work.

## The consistency model

The managed client follows four rules that matter to callers:

1. A successful managed remote read has already been normalized, committed to
   SQLite, and published to the local snapshot before it returns.
2. Fluent queries and cached lookups are local-only. They never hide network
   requests.
3. Unsafe mutations are durably registered before transmission and are never
   blindly retried after an ambiguous transport failure.
4. Live and simulation data are isolated by `Realm`; public directory data
   cannot erase richer account-owned data.

Use explicit methods to choose where data comes from:

- `get()` or `refresh()` performs a targeted remote read.
- `find()`, `cached()`, and `state()` read committed local state.
- `sync()` performs bounded REST reconciliation.
- `events()` combines durable event-log catch-up with low-latency SSE.
- `raw()` bypasses managed persistence and publication entirely.

## Gateways and handles

The client groups behavior by game domain. Gateways cover accounts, devices,
owned and directory replicants, galaxy data, inventories, messages, BobNet,
location events, trading, simulations, synchronization, events, and durable
operations. Entity handles keep an ID and realm attached to targeted reads and
mutations.

```rust,no_run
use replicant_client::{Client, DeviceStatus, DeviceType};

async fn idle_miners(client: &Client) -> replicant_client::Result<()> {
    let miners = client
        .devices()
        .find()
        .of_type(DeviceType::MiningDrone)
        .with_status(DeviceStatus::Idle)
        .collect()
        .await?;

    for miner in miners {
        println!("{}", miner.key.id);
    }
    Ok(())
}
```

The query runs against the latest committed snapshot. Call
`client.devices().get(code).await?` or `client.sync().essential().await?`
first when freshness is required.

## Synchronization and events

REST synchronization is the correctness mechanism; SSE is an observation
channel. On startup the managed client restores its durable cursor, catches up
through the unfiltered event log, and then connects SSE. If continuity cannot
be proven, it schedules REST reconciliation.

```rust,no_run
use replicant_client::Client;

async fn reconcile(client: &Client) -> replicant_client::Result<()> {
    let report = client.sync().full().await?;
    println!("completed: {:?}", report.completed);
    Ok(())
}
```

Managed event watches are deduplicated and only publish events after their
effects and cursor are durable. Use `client.raw().events()` when an unmanaged
history page or SSE stream is intentionally preferred.

## Durable operations

Managed mutations return an `Operation`. The operation journal records intent
before sending, then classifies the result as accepted, rejected, ambiguous,
or resolved by later evidence. Inspect previous operations through
`client.operations()` after a restart. Do not repeat an ambiguous mutation by
hand unless application-specific evidence proves that it is safe.

## Raw client

Enable only `raw` when SQLite, managed state, and background workers are not
needed:

```toml
replicant-client = { version = "1.0.0", default-features = false, features = ["raw", "rustls-tls"] }
```

```rust,no_run
use replicant_client::raw::{Client, SecretString};

async fn account(token: String) -> replicant_client::Result<()> {
    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .build()?;
    let response = client.accounts().me().await?;
    println!("status: {}", response.metadata.status);
    println!("account: {:?}", response.value);
    Ok(())
}
```

Raw responses contain transport DTOs plus status, request ID, and rate-limit
metadata. Safe reads may use bounded retries; mutating requests are never
automatically retried. Raw calls do not update managed state, even when the raw
client came from `Client::raw()`.

## Features

| Feature | Includes |
| --- | --- |
| `raw` | Typed HTTP transport, authentication, DTOs, pagination, and rate-limit metadata. |
| `events` | `raw` plus SSE framing and raw event streaming. |
| `managed` | `events` plus SQLite, normalized state, synchronization, durable operations, and `Client`. Enabled by default. |
| `rustls-tls` | rustls with native root certificates. Enabled by default. |
| `native-tls` | The platform native-TLS backend. |

Examples:

```sh
cargo run --example raw_read --no-default-features --features raw,rustls-tls
cargo run --example raw_events --no-default-features --features events,rustls-tls
cargo run --example managed_sync
cargo run --example fluent_queries
```

## Persistence and security

SQLite stores account binding, normalized projections, simulation realms,
event history and cursor state, reconciliation work, and the operation journal.
Tokens are held through secrecy-aware wrappers and are never persisted.
Debug output redacts credentials; request instrumentation must not include
authorization headers or private message bodies.

Use a separate database per account. The managed store rejects an authenticated
account that does not match its durable binding. Back up or remove the SQLite
database only while the client is closed.

## Contract boundaries

The public surface includes current, non-deprecated, non-administrative
operations from the verified OpenAPI baseline plus explicit rendered-document
deltas. Deprecated and admin-only endpoints are intentionally unavailable,
including through `raw`. Unknown JSON fields are ignored, and open server
vocabularies preserve unknown values for forward compatibility.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
python3 scripts/contract_policy_check.py
```

Run `make ci` for the complete formatting, lint, test, feature, documentation,
package, and contract-policy suite. See [`CONTRIBUTING.md`](CONTRIBUTING.md),
[`SECURITY.md`](SECURITY.md), and
[`docs/observability.md`](docs/observability.md) for repository-specific
guidance.

## License

MIT
