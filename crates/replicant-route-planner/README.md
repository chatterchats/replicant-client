# replicant-route-planner

A pure relay-network planner for Replicant Space. Given a star catalogue,
target systems, existing account-owned relays, and a hop limit, it computes a
connected network that minimizes newly manufactured relay sites first and
graph hops second.

The crate knows nothing about HTTP, SQLite, devices, or managed-client state.
It is private to this repository (`publish = false`).

## Use

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

- `Position` and `Star` describe the catalogue.
- `StarGraph` validates unique designations and builds reachable edges.
- `RelayNetworkRequest` distinguishes active and inactive owned relays from
  targets and the anchored start system.
- `RelayNetworkPlan` contains selected nodes and edges, new/activated relay
  sites, dependency-safe execution order, hop counts, distances, and whether
  the execution order is proven optimal.
- `NetworkNode`, `NetworkEdge`, and `RelayAvailability` make the result
  inspectable without application-specific types.

The exact solver is bounded: network optimization accepts at most 20 terminals.
Execution-order optimization is exact through 16 stops; larger valid networks
use a deterministic, dependency-safe ordering heuristic and report that the
order is not proven optimal.

`PlannerError` covers duplicate or unknown stars, invalid hop ranges,
unreachable targets, and other inconsistent inputs.

## Verify

```sh
cargo test -p replicant-route-planner
cargo clippy -p replicant-route-planner --all-targets -- -D warnings
```
