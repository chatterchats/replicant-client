# replicant-client

`replicant-client` is a local Rust workspace for building and running
Replicant Space automation. The root package is a durable, stateful client;
the packages under [`crates/`](crates) add reusable planners, transport and
printing helpers, and one consolidated command-line interface.

This project is not published to crates.io. Build it from this checkout or use
local path dependencies.

## Requirements

- Rust 1.94 or newer
- A Replicant Space API token for authenticated examples and CLI commands
- SQLite storage for the managed client (bundled through `rusqlite`)

From the repository root:

```sh
cargo check --workspace
cargo test --workspace --all-features
cargo run -p replicant-cli -- --help
```

## Use the client locally

From another project next to this checkout:

```toml
[dependencies]
replicant-client = { path = "../replicant-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Workspace crates use the root package with:

```toml
replicant-client = { path = "../.." }
```

### Managed client

`replicant_client::Client` is the normal application entry point. It owns the
HTTP transport, SQLite store, normalized state, event processing,
synchronization, and durable operation journal.

```rust,no_run
use replicant_client::{Client, SecretString, StartupPolicy};

#[tokio::main]
async fn main() -> replicant_client::Result<()> {
    let token = std::env::var("RS_API_TOKEN")
        .map_err(|_| replicant_client::Error::Configuration {
            message: "set RS_API_TOKEN".into(),
        })?;

    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .sqlite("replicant-client.sqlite")
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;

    client.ready().await?;
    let account = client.account().get().await?;
    println!("{account:?}");
    client.close().await
}
```

Use `in_memory()` instead of `sqlite(...)` for tests and disposable programs.

### Consistency rules

- Managed remote reads normalize, commit, and publish before returning.
- `find()`, `cached()`, and state snapshots are local-only and never hide
  network requests.
- `get()`, `refresh()`, and `sync()` make network work explicit.
- Managed mutations are journaled before their single submission attempt.
- Ambiguous mutation outcomes are reconciled from later evidence, not blindly
  retried.
- Live and simulation entities are isolated by realm.
- Public directory observations cannot erase richer account-owned data.
- `client.raw()` bypasses managed persistence and publication.

### Local queries

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
        println!("{}", miner.id().as_str());
    }
    Ok(())
}
```

Refresh a target or run synchronization before the query when current server
state is required.

### Raw client

For transport DTOs and response metadata without SQLite or managed state:

```toml
replicant-client = {
  path = "../replicant-client",
  default-features = false,
  features = ["raw", "rustls-tls"]
}
```

```rust,no_run
use replicant_client::raw::{Client, SecretString};

async fn read_account(token: String) -> replicant_client::Result<()> {
    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .build()?;
    let response = client.accounts().me().await?;
    println!("status: {}", response.metadata.status);
    println!("{:?}", response.value);
    Ok(())
}
```

Safe reads may use bounded retries. Mutations are never automatically retried.

## Feature tiers

| Feature | Provides |
| --- | --- |
| `raw` | HTTP transport, authentication, DTOs, pagination, and rate-limit metadata. |
| `events` | `raw` plus SSE framing and raw event streaming. |
| `managed` | `events` plus SQLite, normalized state, synchronization, operations, and `Client`. Enabled by default. |
| `rustls-tls` | rustls and native root certificates. Enabled by default. |
| `native-tls` | Platform native TLS. |

## Workspace packages

| Package | Purpose |
| --- | --- |
| [`replicant-cli`](crates/replicant-cli) | Unified CLI for printing, transport, survey, relay, mining, events, bootstrap, and Riker reports. |
| [`replicant-bootstrap-planner`](crates/replicant-bootstrap-planner) | Pure regional-bootstrap sizing and belt-selection rules. |
| [`replicant-event-planner`](crates/replicant-event-planner) | Pure civilisation-event logistics planning. |
| [`replicant-mining-planner`](crates/replicant-mining-planner) | Pure mining-network bills of materials and resource expansion. |
| [`replicant-printing`](crates/replicant-printing) | Pure print scheduling plus optional managed Autofactory workflows. |
| [`replicant-route-planner`](crates/replicant-route-planner) | Pure survey-route and FTL relay-network algorithms. |
| [`replicant-transport`](crates/replicant-transport) | Managed point-to-point resource and device delivery. |

Run the consolidated CLI locally:

```sh
export RS_API_TOKEN='your-token'
cargo run -p replicant-cli -- help print
cargo run -p replicant-cli -- print status --system SCEPTURUM
```

## Examples

The root [`examples/`](examples) directory contains runnable tools and
compile-checked API sketches. See [`examples/README.md`](examples/README.md)
for prerequisites, safety notes, and a command for every example.

```sh
cargo run --example raw_read
cargo run --example nearby_belt_report -- SCEPTURUM 25
cargo check --example fluent_queries
```

## Persistence and security

The managed SQLite database stores account binding, normalized projections,
simulation realms, event cursor/history, reconciliation work, and durable
operation outcomes. API tokens are never persisted. Use a separate database
per account and close the client before copying or removing the database.

Do not log tokens, authorization headers, private message bodies, or databases
containing user data.

## Contract boundary

The client follows the checked-in Replicant Space 2.5.0 documentation and
OpenAPI corpus. Deprecated and administrative operations are intentionally
absent, including from the raw client. Unknown fields and open vocabularies
remain forward compatible.

The 2.5.0 surface includes tutorial progress, one-time equipment-locker
retrieval, FTL slingshot device linking, and typed System Ward responses/events.
Slingshot firing intentionally reuses the normal teleport operation rather than
introducing a parallel transport API:

```rust
let tutorials = client.tutorials().list().await?;
let slingshot = client.devices().get("SLINGSHOT-CODE").await?;
slingshot.link_device("EMPTY-MATRIX-CODE").await?;

let replicant = client.replicants().get_owned("REPLICANT-CODE").await?;
replicant.teleport_via_slingshot("SLINGSHOT-CODE").await?;

// Existing accounts can claim the one-time equipment-locker device through
// the durable operation journal. A successful claim refreshes owned devices.
let retrieval = client.devices().retrieve("LOCKER-CODE").await?;
```

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
python3 scripts/contract_policy_check.py
```

`make ci` runs the complete repository gate.

## License

MIT
