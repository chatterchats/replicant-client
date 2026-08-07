# replicant-event-planner

Pure civilisation-event logistics planning for Replicant Space. The planner
turns an event definition and caller-supplied inventory, progress, blueprint,
factory, and transport facts into an `EventPlan`.

It performs no HTTP requests, persistence, or gameplay mutations. This is an
unpublished workspace crate.

## Use locally

```toml
[dependencies]
replicant-event-planner = { path = "../replicant-event-planner" }
```

```rust,ignore
use replicant_event_planner::{EventDefinition, PlanningContext, plan_event};

fn plan(event: EventDefinition, context: &PlanningContext) {
    let plan = plan_event(event, context).expect("complete planning inputs");
    for criterion in plan.criteria {
        println!("{criterion:?}");
    }
}
```

## What it does

- Normalizes open-shaped event criteria and rewards.
- Subtracts confirmed progress and destination stock.
- Expands device blueprints into recursive resource costs.
- Balances print work across Autofactories.
- Selects transport capacity and repeated cargo/device trips.
- Plans beacon reuse, manufacture, deployment, and activation.
- Produces stable mission and role tags.

## API

- Inputs: `EventDefinition`, `EventCriterion`, `EventRewards`,
  `OpenEventFields`, `BlueprintSpec`, `DeviceStock`, `FactoryWorkload`, and
  `PlanningContext`.
- Outputs: `EventPlan`, `CriterionAssessment`, `RemainingRequirements`,
  `PrintSchedule`, `TransportPlan`, `BeaconPlan`, and `Recommendation`.
- Helpers: `remaining_requirements`, `schedule_print_units`,
  `blueprint_resource_cost`, `mission_tag`, and `role_tag`.
- Errors: `PlannerError` reports incomplete or inconsistent planning inputs.

Use `replicant-cli event` when live discovery, persistence, printing,
transport, and durable execution are needed.

## Verify

```sh
cargo test -p replicant-event-planner
cargo clippy -p replicant-event-planner --all-targets -- -D warnings
```
