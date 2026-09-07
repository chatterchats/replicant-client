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

| Path                                               | Owns                                                                                                                               |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `catalogue.rs`                                     | The registry of Reports, Actions, and Workflows. Frontends and the CLI dispatch through here rather than embedding gameplay logic. |
| `reports.rs`, `actions.rs`                         | Report and action implementations.                                                                                                 |
| `automation.rs`, `orchestration.rs`                | The Automation Director: goal reconciliation, regional assignment, campaign creation, operating modes.                             |
| `workflows.rs`                                     | Intent-native workflow registration and intent types.                                                                              |
| `bootstrap/`                                       | Regional bootstrap campaigns — `model.rs`, `executor.rs`.                                                                          |
| `event/`                                           | Civilisation-event logistics — `campaign.rs`, `executor.rs`, `stock.rs`.                                                           |
| `mining/`, `mining.rs`                             | Mining site health, transport capacity, maintenance rotation/pool policy, and expansion execution.                                 |
| `survey.rs`, `belt_search.rs`, `observatory.rs`    | Survey tours, belt discovery, star prospecting.                                                                                    |
| `relay.rs`                                         | FTL relay network work.                                                                                                            |
| `trade.rs`                                         | Trading operations.                                                                                                                |
| `requirements.rs`, `director_requirements.rs`      | Desired-state requirements and fulfilment tracking.                                                                                |
| `ownership.rs`                                     | Regional worker ownership and assignment.                                                                                          |
| `galaxy_scene.rs`, `system_scene.rs`               | Typed projections behind `GET /api/galaxy-scene` and `/api/system-scene/:system`.                                                  |
| `intelligence.rs`, `rikers.rs`, `mission_stock.rs` | Reporting and intelligence surfaces.                                                                                               |
| `telemetry.rs`, `empire_telemetry.rs`              | Observability samples and rollups.                                                                                                 |
| `config.rs`, `failure.rs`                          | Runtime configuration and error classification.                                                                                    |

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

### Mining Ops

The user-facing **Mining Ops** overseer retains the persisted Rust goal identity
`ExpandMiningOps` and wire/document key `expand_mining_ops`. It maintains existing
productive sites, AMI route health and freighter capacity, remote maintenance
rotation, the hub repair reserve, and priority-system protection before expansion.
Moderate/sparse density toggles affect **new expansion**, not management of existing
sites. The Director delegates every mechanical change to durable workflows.

The additive `DirectorSnapshot.mining_ops` collection exposes independent observed
health dimensions to frontends. Site and route totals cover the Director-managed
regional footprint, not every owned mining asset: known sites beyond 30 LY are excluded.
In-range managed sites remain included when their expansion-density toggle is disabled.
Sites are counted by exact managed belt; expansion candidates and priority protection
are system counts. Route health requires structural
AMI service and FTL reach. Freighters count distinct usable adopted devices. Backlogged
routes have positive authoritative collection inventory; unknown inventory is explicit,
not zero backlog. Remote maintenance counts healthy patrol drones not due for rotation;
hub readiness counts unclaimed patrol drones at replacement-ready capacity.
Legacy snapshots default to no health rows and retain their original goal progress.
`DirectorMiningPolicySummary` remains density policy, never an observed-health container.

Thresholds in `mining.rs` and `orchestration.rs` are **client policy**, not upstream
contract truth: remote rotation below 30%, replacement readiness at >=95%, healthy hub
minimum 2/target 3, a 500-unit planning load, expansion suppression above 30 loads,
scale-up eligibility above 10 loads, and up to four priority non-hub ward systems.
Capacity may be released to regional stock; Replicants are never scaled down.

Workflow steps distinguish replacement provisioning/delivery, verified replacement
patrol, worn-drone return and repair, route evidence, stock reuse, freighter adoption,
and excess-capacity return. The Director surfaces these as next actions, together with
FTL dependencies and critical-backlog expansion deferral.
Active `mining.campaign` next actions come from its typed mission checkpoint, distinguishing
site equipment/adoption/directive work from AMI route repair. `mining.site` and `mining.route`
are work-item kinds, not standalone workflow identities.
Structured tracing records priority transitions and actual provisioning/capacity decisions,
not per-tick health samples; no separate metrics subsystem is involved.

## Tests

```sh
cargo test -p replicant-runtime --all-features
```

No live Replicant Space account is required.
