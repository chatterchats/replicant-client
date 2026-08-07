# Examples

These examples use the local root `replicant-client` package. Run commands from
the repository root; nothing needs to be installed or fetched from crates.io.

```sh
cargo check --examples --all-features
```

Some examples are live tools. Read the safety note for an example before
running it against an account.

## Authentication and storage

The examples use two historical token names:

- `RS_API_TOKEN` — managed operational examples.
- `REPLICANT_API_KEY` — minimal raw examples.
- `REPLICANT_TOKEN` — accepted as a fallback by the initializer and belt
  report.

Managed examples usually use `REPLICANT_DB`, defaulting to
`replicant-client.sqlite`. Use a separate database per account.

## Runnable examples

### `raw_read`

Performs one authenticated raw account read and prints HTTP status, request ID,
and rate-limit metadata. It does not write managed state.

```sh
REPLICANT_API_KEY='your-token' cargo run --example raw_read
```

### `raw_events`

Reads filtered event history and then follows the raw SSE stream from the
returned cursor. Stop it with Ctrl-C. It does not update managed state.

```sh
REPLICANT_API_KEY='your-token' cargo run --example raw_events
```

### `bobnet_messages`

Displays recent BobNet relay history and can follow `bobnet.new` account
events. History needs a relay code; live-only following does not.

```sh
export RS_API_TOKEN='your-token'

cargo run --example bobnet_messages -- 008A353D
cargo run --example bobnet_messages -- 008A353D --limit 100 --channel general --follow
cargo run --example bobnet_messages -- --follow
```

`RS_BOBNET_RELAY` and `RS_BOBNET_CHANNEL` provide defaults. The example only
reads and displays messages.

### `initialize_colony_database`

Hydrates the durable database used by local reports. It performs safe reads
only: catalogue refresh, owned-replicant star synchronization, and bounded
known-system/location hydration. It does not scan, survey, submit candidates,
or send messages.

```sh
export RS_API_TOKEN='your-token'
export REPLICANT_DB='replicant-client.sqlite'
cargo run --example initialize_colony_database
```

Optional bounds include `REPLICANT_INIT_SYSTEM_LIMIT` and
`REPLICANT_INIT_OBJECT_LIMIT`. The database can only contain facts the account
has already discovered.

### `nearby_belt_report`

Refreshes owned-replicant star knowledge, selects explored systems within a
catalogue radius, refreshes their locations, and prints asteroid belts from
dense to sparse. It performs safe reads only.

```sh
RS_API_TOKEN='your-token' \
  cargo run --example nearby_belt_report -- SCEPTURUM 25
```

`REPLICANT_DB` selects the database. `RS_BELT_REPORT_CONCURRENCY` defaults to
4 and is capped at 16.

### `tag_twaffy_ring_injectors`

Adds `twaffy-ring-001` to every owned `exotic_matter_injector` that lacks it,
preserving existing tags.

This example **performs durable gameplay mutations**.

```sh
export RS_API_TOKEN='your-token'
export REPLICANT_DB='replicant-client.sqlite'
cargo run --example tag_twaffy_ring_injectors
```

The operation journal prevents blind retries after ambiguous transport
failures. Review the fixed device type and tag constants in the source before
running it.

## Compile-checked API sketches

These examples primarily demonstrate API composition. They intentionally do
not provide a complete authenticated application.

### `fluent_queries`

Shows local-only device query builders for types, status, location, controller
relationships, and adopted-device predicates.

```sh
cargo check --example fluent_queries
```

Copy `preferred_queries` into a program that has already started and populated
a managed `Client`.

### `game_concepts`

Shows the high-level travel, AMI controller, BobNet, trading, and simulation
APIs. Its `main` is intentionally empty.

```sh
cargo check --example game_concepts
```

The functions include mutations and must only be called intentionally from an
authenticated managed application.

### `managed_sync`

Shows the minimal call shape for an explicit essential REST synchronization
sweep and clean shutdown using an in-memory client.

```sh
cargo check --example managed_sync
```

Before using it live, add token authentication to the builder as shown in the
root README. As checked in, it is an API sketch rather than a credential-aware
tool.

## Verify all examples

```sh
cargo check --examples --all-features
cargo clippy --examples --all-features -- -D warnings
```
