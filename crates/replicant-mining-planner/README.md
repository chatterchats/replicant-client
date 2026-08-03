# replicant-mining-planner

Pure, deterministic planning helpers for repeatable Replicant Space mining
network expansion. It contains the arithmetic and naming rules shared by the
mining CLI without depending on HTTP, SQLite, Tokio, or managed state.

The crate is private to this repository (`publish = false`).

## Use

```toml
[dependencies]
replicant-mining-planner = { path = "../replicant-mining-planner" }
```

```rust
use replicant_mining_planner::{mining_site_requirements, shortages};

let required = mining_site_requirements();
let reusable = [("Mining Drone".to_owned(), 2)].into_iter().collect();
let missing = shortages(&required, &reusable);
assert!(missing.values().all(|quantity| *quantity >= 0));
```

## Public API

- `mining_site_requirements()` returns the canonical nine-device site bill of
  materials.
- `shortages(required, reusable)` subtracts reusable device stock without
  producing negative quantities.
- `multiply` and `add_quantities` combine quantity maps for multiple sites.
- `blueprint_resource_cost` recursively expands device and component
  blueprints into raw resource requirements.
- `site_tag` and `role_tag` produce stable tags within the upstream length
  limit.
- `BlueprintSpec`, `QuantityMap`, and `PlannerError` are the core data and
  error types.

The planner does not decide which belt is best, inspect live stock, schedule
Autofactories, transport equipment, or configure devices. Those orchestration
steps belong to `replicant-mining-cli` and `replicant-printing`.

## Verify

```sh
cargo test -p replicant-mining-planner
cargo clippy -p replicant-mining-planner --all-targets -- -D warnings
```
