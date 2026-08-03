# replicant-printing

Reusable distributed Autofactory scheduling and queue submission for
Replicant Space. The default crate is a pure planner; the optional `managed`
feature discovers live factories and submits durable print operations through
`replicant_client::Client`.

The crate is private to this repository (`publish = false`).

## Pure scheduling

```toml
[dependencies]
replicant-printing = { path = "../replicant-printing" }
```

```rust
use replicant_printing::{Blueprint, FactoryWorkload, normalize_requests, schedule_prints};

let requests = vec![replicant_printing::PrintRequest {
    device_type: "Mining Drone".into(),
    quantity: 3,
}];
let factories = vec![FactoryWorkload {
    code: "FACTORY-1".into(),
    remaining_seconds: 0.0,
}];
let blueprints = [("Mining Drone".into(), Blueprint {
    device_type: "Mining Drone".into(),
    print_time_seconds: 60.0,
    features: vec![],
})].into_iter().collect();

let required = normalize_requests(&requests)?;
let schedule = schedule_prints(&required, &blueprints, &factories)?;
# Ok::<(), replicant_printing::ScheduleError>(())
```

`normalize_requests` merges repeated device requests and rejects invalid
quantities. `schedule_prints` assigns individual units to factories using live
queue capacity and projected workload. `PrintTime` allows callers to use their
own blueprint type.

## Managed queueing

Enable the adapter:

```toml
replicant-printing = { path = "../replicant-printing", features = ["managed"] }
```

The `managed` module provides:

- `fetch_blueprints` for the account's unlocked catalogue;
- `discover_factories` and `inspect_factory` for live Autofactory state;
- `factory_queue_slots` for current capacity;
- `enqueue_print` and `enqueue_print_flatpacked` for one durable submission;
- `queue_prints` for scheduling, waiting for capacity, and submitting a batch;
- `ensure_submission_accepted` for interpreting managed operation state.

```rust,ignore
use replicant_printing::{PrintRequest, managed::{QueueOptions, queue_prints}};

let requests = vec![PrintRequest {
    device_type: "Mining Drone".into(),
    quantity: 3,
}];
let report = queue_prints(&client, &requests, &QueueOptions::default()).await?;
println!("queued: {:?}", report.queued);
```

`QueueOptions` selects the hub, tags, flatpack behavior, polling interval, and
wait timeout. `QueueReport` records accepted quantities by factory and the
durable operation IDs. Queueing returns after all work is accepted; it does not
wait for physical printing to finish.

## Errors and guarantees

`ScheduleError` covers pure input and capacity problems. `PrintingError` adds
managed-client, timeout, factory-state, and operation-outcome failures. Every
managed print request is journaled before transmission. A transport ambiguity
is not automatically resubmitted.

## Verify

```sh
cargo test -p replicant-printing --all-features
cargo clippy -p replicant-printing --all-targets --all-features -- -D warnings
```
