# replicant-runtime

Application services above the managed `replicant-client`. At ~64k lines this
is the largest crate in the workspace — **do not read it whole.** Use the map
below to reach the module you need.

## Position in the stack

```
replicant-client   game/API truth, durable operations, SSE
        |
   replicant-runtime      <- you are here: reports, actions, planning, Director
        |
  replicant-workflow      durable execution, claims, checkpoints
        |
     replicantd           HTTP + WebSocket to frontends
```

The managed client stays authoritative for API access, durable game state,
operations, and events. This crate coordinates that client and supplies the
application-specific workflow implementations and intents. Frontends call the
runtime and own only presentation.

## Module map

| Path | Owns |
| --- | --- |
| `catalogue.rs` | The registry of Reports, Actions, and Workflows. Frontends and the CLI dispatch through here rather than embedding gameplay logic. |
| `reports.rs`, `actions.rs` | Report and action implementations. |
| `automation.rs`, `orchestration.rs` | The Automation Director: goal reconciliation, regional assignment, campaign creation, operating modes. |
| `workflows.rs` | Intent-native workflow registration and intent types. |
| `bootstrap/` | Regional bootstrap campaigns — `model.rs`, `executor.rs`. |
| `event/` | Civilisation-event logistics — `campaign.rs`, `executor.rs`, `stock.rs`. |
| `mining/`, `mining.rs` | Mining expansion planning and execution. |
| `survey.rs`, `belt_search.rs`, `observatory.rs` | Survey tours, belt discovery, star prospecting. |
| `relay.rs` | FTL relay network work. |
| `trade.rs` | Trading operations. |
| `requirements.rs`, `director_requirements.rs` | Desired-state requirements and fulfilment tracking. |
| `ownership.rs` | Regional worker ownership and assignment. |
| `galaxy_scene.rs`, `system_scene.rs` | Typed projections behind `GET /api/galaxy-scene` and `/api/system-scene/:system`. |
| `intelligence.rs`, `rikers.rs`, `mission_stock.rs` | Reporting and intelligence surfaces. |
| `telemetry.rs`, `empire_telemetry.rs` | Observability samples and rollups. |
| `config.rs`, `failure.rs` | Runtime configuration and error classification. |

## Invariants

Two rules here are easy to break with otherwise reasonable code:

- **The Director never issues game commands directly.** It reconciles standing
  goals and creates or reuses durable campaign workflows. Mechanical work
  belongs in registered workflows and managed client operations.
- **Workforce automation is grow-only.** Nothing deletes, retires,
  decommissions, or scales down Replicants. Idle Replicants are retained
  permanently.

Director modes are `off`, `advisory`, and `automatic`. Director state persists
in the workflow database through the generic `runtime_documents` store.

Read [`../../ARCHITECTURE.md`](../../ARCHITECTURE.md) before changing Director
behaviour, adding a standing goal, or introducing a workflow kind.

## Tests

```sh
cargo test -p replicant-runtime --all-features
```

No live Replicant Space account is required.
