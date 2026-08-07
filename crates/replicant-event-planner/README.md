# replicant-event-planner

Pure planning primitives for Replicant Space civilisation events. This crate
turns an event definition plus local inventory, progress, blueprint, factory,
and transport facts into an actionable `EventPlan`.

It performs no HTTP requests, persistence, or gameplay mutations. The crate is
private to this repository (`publish = false`) and is used by
`replicant-cli event`.

## What it plans

The planner:

- normalizes open-shaped criteria and rewards;
- subtracts confirmed event progress and stock already at the destination;
- recursively expands device blueprints into resource costs;
- balances print units across available Autofactories;
- selects transport capacity and divides cargo into repeated trips;
- determines whether a beacon must be manufactured, deployed, activated, or
  reused; and
- produces stable mission and role tags that fit API limits.

Planning is deterministic for the same inputs. The server remains authoritative
for event availability, inventory, and mutation acceptance.

## Use

Add a path dependency from another workspace crate:

```toml
[dependencies]
replicant-event-planner = { path = "../replicant-event-planner" }
```

Construct an `EventDefinition` and `PlanningContext`, then call `plan_event`:

```rust,ignore
use replicant_event_planner::{plan_event, EventDefinition, PlanningContext};

fn build_plan(event: EventDefinition, context: &PlanningContext) {
    let plan = plan_event(event, context).expect("event inputs should be complete");
    for criterion in plan.criteria {
        println!("{criterion:?}");
    }
}
```

`PlanningContext` deliberately requires callers to supply observations. The
CLI adapter obtains those through `replicant-client`; tests and offline tools
can construct them directly.

## Public API

- Event input: `EventDefinition`, `EventCriterion`, `EventRewards`, and
  `OpenEventFields`.
- Manufacturing input: `BlueprintSpec`, `DeviceStock`, and `FactoryWorkload`.
- Transport input: `SelectedTransport` and the corresponding context fields.
- Output: `EventPlan`, `CriterionAssessment`, `RemainingRequirements`,
  `PrintSchedule`, `TransportPlan`, `BeaconPlan`, and `Recommendation`.
- Helpers: `remaining_requirements`, `schedule_print_units`,
  `blueprint_resource_cost`, `mission_tag`, and `role_tag`.

`PlannerError` reports incomplete or inconsistent inputs such as missing
blueprints, invalid quantities, unavailable factories, or insufficient
transport information. Resolve the input rather than retrying the same plan.

## Verify

```sh
cargo test -p replicant-event-planner
cargo clippy -p replicant-event-planner --all-targets -- -D warnings
```
