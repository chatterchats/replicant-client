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
    components: Default::default(),
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
- `queue_prints` for direct scheduling and submission when the caller owns
  dependency handling;
- `queue_prints_with_components` for recursive component expansion,
  dependency-wave waiting, scheduling, and submission;
- `printing_status_in_system` for read-only system inventory, live Autofactory
  work, and recursive missing-output/component calculations;
- `clear_factories_in_system` for clearing queued work and stopping any active
  print on every account-owned Autofactory in a star system;
- `ensure_submission_accepted` for interpreting managed operation state.

```rust,ignore
use replicant_printing::{PrintRequest, managed::{QueueOptions, queue_prints}};

let requests = vec![PrintRequest {
    device_type: "Mining Drone".into(),
    quantity: 3,
}];
let report = queue_prints(&client, &requests, &QueueOptions::at("SCEPTURUM-BELT-1")).await?;
println!("queued: {:?}", report.queued);
```

`QueueOptions` selects the hub, tags, flatpack behavior, polling interval, and
wait timeout. `QueueReport` separates requested outputs, printed prerequisite
components, and reused local component stock. Prerequisite waves physically
finish before dependent devices are queued; the final requested output still
returns after submission rather than physical completion.

Blueprint components are expanded recursively with cycle detection. Existing
free devices at the exact hub satisfy component demand before printing. Before
a new dependency plan is built, active or queued work for those prerequisite
types is allowed to finish and the completed devices are rescanned. Restarting
an interrupted component wave therefore does not blindly enqueue it again.
This also allows a locked or event-supplied component to be consumed when local
stock is present, while a local shortage without an unlocked component
blueprint fails before the parent is queued. `--flatpack` semantics apply only
to final requested devices; component prints remain assembled for consumption.

Factory discovery requires the live `enqueue_print` command, so flatpacked or
inactive Autofactory outputs at the same hub are not mistaken for usable
printers.

`printing_status_in_system` can be called with no requests to inventory a whole
system, or with desired `PrintRequest`s to reconstruct an interrupted batch. It
subtracts completed, active, and queued parent outputs before expanding
prerequisites. Gap lines clamp missing quantities at zero and report any
surplus separately. Optional tags scope the gap calculation without hiding
unrelated factory work from the report.

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
