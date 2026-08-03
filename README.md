# replicant-client

## Colony database initializer

`cargo run --example initialize_colony_database` performs only managed safe
reads and populates the durable survey database from knowledge the account has
already discovered. It cannot discover unsurveyed worlds; use `REPLICANT_DB`
to choose the SQLite path and `REPLICANT_INIT_*` bounds to cap the traversal.

A durable, stateful Rust client for building [Replicant Space](https://replicant.space) applications.

`replicant-client` targets Replicant Space documentation through `2.3.5`,
using the checked-in verified `2.3.3` OpenAPI corpus plus explicit
rendered-document corrections where the specification remains incomplete. It is
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

## Event logistics CLI

The workspace includes a pure `replicant-event-planner` crate and the
`replicant-events` binary. It performs live event discovery, interactive event
and criterion selection, achievement-aware comparison, progress and
location-stock subtraction, balanced manufacturing, AMI-free Cargo Freighter
selection, repeated-trip logistics, event resolution, reward recovery, fleet
return, and the best-effort persistent FTL-beacon objective.

```sh
cargo run --quiet -p replicant-event-cli -- list
cargo run --quiet -p replicant-event-cli -- plan WIXUKHHU-4-EVT-002
cargo run --quiet -p replicant-event-cli -- run --execute
cargo run --quiet -p replicant-event-cli -- resume --execute
cargo run --quiet -p replicant-event-cli -- status
```

`Chats-1` and `SCEPTURUM-BELT-1` are the defaults. Use `--replicant` and
`--home` to override them. Plans are saved atomically to `event-mission.json`;
only one nonterminal mission is allowed in the first release. Cargo Freighters
with an AMI controller relationship are never selected. Claimed mission devices
receive bounded tags, remain assigned to the selected replicant, and have only
the mission-added tags removed after they are safely returned. The CLI is quiet
by default; use `--verbose` and/or `--log-file PATH` for diagnostics.

## Mining expansion CLI

The `replicant-mining-planner` crate and `replicant-mining` binary audit a
list of known systems, repair or create one nine-device mining setup at the
densest discovered belt in each system, and establish one Cargo Freighter
`ferry` route per belt back to the manufacturing hub. The executor reuses idle
hub stock, balances shortages across Autofactories, deploys as many complete
sets concurrently as there are available Surge Carriers, retroactively tags
existing automation, and persists every stage to `mining-expansion.json`.

```sh
cargo run --quiet -p replicant-mining-cli -- plan \
  --hub SCEPTURUM-BELT-1 \
  --systems-file examples/mining-expansion-systems.txt

cargo run --quiet -p replicant-mining-cli -- run
cargo run --quiet -p replicant-mining-cli -- status
```

`plan` performs reads only. `run` always reconciles the persisted mission and
continues its first incomplete stage, so there is no separate resume command
or execution-confirmation flag. The binary is quiet by default; use
`--verbose` and/or `--log-file PATH` when diagnostics are needed.

## Distributed printing CLI

The reusable `replicant-printing` crate balances work across every eligible
Autofactory at a hub. Its CLI accepts repeated print requests and, on Replicant
Space 2.3.5, can request compacted output for devices with the `modular`
blueprint feature.

```sh
cargo run --quiet -p replicant-printing-cli -- \
  --hub SCEPTURUM-BELT-1 \
  --flatpack \
  --print 6 autofactory \
  --print 6 cargo_freighter
```

## FTL relay expansion CLI

The workspace includes a pure `replicant-route-planner` crate and a
restart-safe managed CLI that plans an exact minimum-new-relay network,
reuses or activates account-owned relays, manufactures any shortfall, deploys
and verifies the network, and returns the selected replicant to its hub.

```sh
cargo run --quiet -p replicant-relay-cli -- plan \
  --replicant Chats-1 \
  --hub SCEPTURUM-BELT-1 \
  WIHAX ILPHARD KRAKHUX XHAKKWUKKXHU XIHAKHXA XHAKHKHU

cargo run --quiet -p replicant-relay-cli -- run
cargo run --quiet -p replicant-relay-cli -- status
```

`plan` is read-only. `run` reconciles and continues the persisted
`ftl-relay-expansion.json` mission, so it replaces both the former execute and
resume invocations and requires no `--execute` flag.

## Survey route CLI

`replicant-survey` plans a bounded route around a centre system, prepares an
AMI survey fleet, scans and surveys each system, and checkpoints every phase
to `explore-survey-route.json`.

```sh
cargo run --quiet -p replicant-survey-cli -- plan \
  --replicant B6BA399E \
  --vessel FD5EA802 \
  --center SCEPTURUM \
  --radius 30

cargo run --quiet -p replicant-survey-cli -- run
cargo run --quiet -p replicant-survey-cli -- status
```

As with mining and relay expansion, rerunning `run` reconciles live state and
continues the incomplete mission. Use `--verbose` or `--log-file PATH` for
diagnostics.

## Riker colony candidates CLI

The read-only `replicant-rikers` command synchronizes known survey data,
prints staged local-query diagnostics, and ranks explainable colony candidates.
It never sends the proposed message to BobNet.

```sh
cargo run --quiet -p replicant-rikers-cli -- --limit 10
```

**Status:** this repository is at the Phase 1 bootstrap stage. The package,
feature graph, and checked-in Replicant Space contract corpus exist;
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

The verified Replicant Space 2.3.3 OpenAPI corpus is checked in under
[`reference/replicant-space/`](reference/replicant-space/). Its inventory
contains 86 operations: 79 supported, 5 deprecated, and 2 admin-only.
Replicant Space 2.3.5 rendered-document corrections remain explicit under [`docs/contract/`](docs/contract/)
and [`policy/contract-metadata.json`](policy/contract-metadata.json); the
documented-operation delta list is currently empty because both colony routes
are now present in OpenAPI. `scripts/contract_policy_check.py` verifies the
checksum, inventory, exclusions, and correction metadata.

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
