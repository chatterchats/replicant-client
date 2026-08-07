# replicant-mining-planner

Pure planning helpers for repeatable Replicant Space mining-network expansion.
The crate owns quantity arithmetic, the canonical mining-site bill of
materials, recursive blueprint costing, and stable site/role tags.

It has no HTTP, SQLite, Tokio, or managed-state behavior. This is an
unpublished workspace crate.

## Use locally

```toml
[dependencies]
replicant-mining-planner = { path = "../replicant-mining-planner" }
```

```rust
use replicant_mining_planner::{mining_site_requirements, shortages};

let required = mining_site_requirements();
let reusable = [("mining_drone".to_owned(), 2)].into_iter().collect();
let missing = shortages(&required, &reusable);
assert!(missing.values().all(|quantity| *quantity >= 0));
```

## API

- `mining_site_requirements` returns the canonical nine-device site.
- `shortages` subtracts reusable stock without negative results.
- `multiply` and `add_quantities` combine requirements across sites.
- `blueprint_resource_cost` recursively expands components into raw-resource
  requirements.
- `site_tag` and `role_tag` create stable API-safe tags.
- `BlueprintSpec`, `QuantityMap`, and `PlannerError` are the core types.

Live belt selection, inventory, printing, deployment, and device configuration
belong to `replicant-cli mining`.

## Verify

```sh
cargo test -p replicant-mining-planner
cargo clippy -p replicant-mining-planner --all-targets -- -D warnings
```
