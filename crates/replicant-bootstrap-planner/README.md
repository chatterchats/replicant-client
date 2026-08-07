# replicant-bootstrap-planner

Pure planning primitives for an autonomous Replicant Space regional bootstrap.
The crate sizes an ark payload, validates supported mission profiles, calculates
carrier capacity, ranks dense asteroid belts, and creates stable mission tags.

It performs no HTTP requests, persistence, or gameplay mutations. This is an
unpublished workspace crate.

## Use locally

From another crate under `crates/`:

```toml
[dependencies]
replicant-bootstrap-planner = { path = "../replicant-bootstrap-planner" }
```

From the repository root:

```sh
cargo test -p replicant-bootstrap-planner
```

## API

- `BootstrapProfile` describes mining setups, Autofactories, freighters,
  controllers, maintenance and survey drones, relays, beacons, and dedicated
  Surge Carriers.
- `validate_profile` enforces the supported operational ranges.
- `ark_device_requirements` expands a profile into device quantities.
- `attachment_slots`, `missing_carriers`, and `carrier_provisioning` calculate
  the transport capacity needed for the payload.
- `BeltCandidate` and `select_dense_belts` rank known belts for the first
  regional mining sites.
- `mission_tag` and `role_tag` produce stable API-safe tags.
- `PlannerError` reports invalid profiles and insufficient candidate data.

```rust
use replicant_bootstrap_planner::{
    BootstrapProfile, ark_device_requirements, validate_profile,
};

let profile = BootstrapProfile {
    mining_setups: 8,
    autofactories: 6,
    cargo_freighters: 6,
    transport_controllers: 3,
    hub_maintenance_drones: 4,
    exploration_survey_drones: 3,
    root_relays: 1,
    expansion_relays: 18,
    ftl_beacons: 9,
    dedicated_surge_carriers: 3,
};
validate_profile(&profile)?;
let payload = ark_device_requirements(&profile);
println!("ark device types: {}", payload.len());
# Ok::<(), replicant_bootstrap_planner::PlannerError>(())
```

The planner does not inspect live inventory or execute a mission. Use
`replicant-cli bootstrap` for orchestration.

## Verify

```sh
cargo test -p replicant-bootstrap-planner
cargo clippy -p replicant-bootstrap-planner --all-targets -- -D warnings
```
