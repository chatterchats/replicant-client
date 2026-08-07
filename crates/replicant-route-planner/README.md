# replicant-route-planner

Pure route and FTL relay-network algorithms for Replicant Space. Callers supply
a star catalogue and current relay facts; the crate performs no HTTP,
persistence, managed-state access, or gameplay mutations.

This is an unpublished workspace crate.

## Use locally

```toml
[dependencies]
replicant-route-planner = { path = "../replicant-route-planner" }
```

```rust,ignore
use std::collections::BTreeSet;
use replicant_route_planner::{RelayNetworkRequest, plan_relay_network};

let request = RelayNetworkRequest {
    start: "SCEPTURUM".into(),
    targets: vec!["ILPHARD".into(), "KRAKHUX".into()],
    active_relay_systems: BTreeSet::new(),
    inactive_relay_systems: BTreeSet::new(),
    max_hop_ly: 7.499,
};
let plan = plan_relay_network(stars, request)?;
println!("new relays: {:?}", plan.new_relay_systems);
```

## Model

- `Position` and `Star` represent the catalogue.
- `StarGraph` validates unique designations and reachable edges.
- `RelayNetworkRequest` separates the anchored start, targets, active relays,
  inactive reusable relays, and hop range.
- `RelayNetworkPlan` reports selected nodes/edges, new and activated sites,
  dependency-safe execution order, hop counts, and distances.
- `NetworkNode`, `NetworkEdge`, and `RelayAvailability` expose the result
  without application-specific state.
- `PlannerError` reports invalid ranges, duplicate/unknown stars, unreachable
  targets, and solver limits.

The minimum-new-relay network solver accepts at most 20 terminals. Execution
order is exact through 16 stops; larger valid networks use a deterministic,
dependency-safe heuristic and say that the order is not proven optimal.

Use `replicant-cli relay` for live discovery and durable execution.

## Verify

```sh
cargo test -p replicant-route-planner
cargo clippy -p replicant-route-planner --all-targets -- -D warnings
```
