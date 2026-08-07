# replicant-printing

Reusable distributed Autofactory planning and queueing for Replicant Space.
The default crate is pure. Its optional `managed` feature discovers live
factories and submits durable print operations through `replicant-client`.

This is an unpublished workspace crate.

## Pure scheduling

```toml
[dependencies]
replicant-printing = { path = "../replicant-printing" }
```

```rust
use replicant_printing::{
    Blueprint, FactoryWorkload, PrintRequest, normalize_requests, schedule_prints,
};

let requests = vec![PrintRequest {
    device_type: "mining_drone".into(),
    quantity: 3,
}];
let required = normalize_requests(&requests)?;
let blueprints = [("mining_drone".into(), Blueprint {
    device_type: "mining_drone".into(),
    print_time_seconds: 60.0,
    features: vec![],
    components: Default::default(),
})].into_iter().collect();
let factories = vec![FactoryWorkload {
    code: "FACTORY-1".into(),
    remaining_seconds: 0.0,
}];

let schedule = schedule_prints(&required, &blueprints, &factories)?;
println!("{:?}", schedule.batches);
# Ok::<(), replicant_printing::ScheduleError>(())
```

`plan_print_dependencies` recursively expands component requirements with
cycle detection. `PrintTime` lets callers use their own blueprint type.

## Managed workflows

```toml
[dependencies]
replicant-printing = { path = "../replicant-printing", features = ["managed"] }
replicant-client = { path = "../.." }
```

The `managed` module provides:

- blueprint and Autofactory discovery;
- live queue inspection and capacity checks;
- assembled and flatpacked durable submissions;
- direct queueing with `queue_prints`;
- component-aware dependency waves with `queue_prints_with_components`;
- system-wide manufacturing status with `printing_status_in_system`; and
- queue/active-work cleanup with `clear_factories_in_system`.

```rust,ignore
use replicant_printing::{PrintRequest, managed::{QueueOptions, queue_prints_with_components}};

let requests = vec![PrintRequest {
    device_type: "mining_drone".into(),
    quantity: 3,
}];
let options = QueueOptions::at("SCEPTURUM-BELT-1");
let report = queue_prints_with_components(&client, &requests, &options).await?;
println!("queued: {:?}", report.queued);
```

Managed queueing returns when final output submissions are accepted, not when
the devices finish printing. Required component waves do finish before their
dependent parent jobs are queued. Existing eligible local components are
reused first.

Every mutation is journaled by the managed client. Ambiguous submissions are
not blindly repeated.

## Verify

```sh
cargo test -p replicant-printing --all-features
cargo clippy -p replicant-printing --all-targets --all-features -- -D warnings
```
