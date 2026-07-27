# Phase 11.6.01 — full synchronization and managed-domain coverage

## Durable managed-domain inventory

The bounded, durable managed domains for 1.0 are account, devices, owned
replicants, locations, account inventory keyed by location, and owned
simulation history. They are normalized, committed to SQLite, and published
before their managed read returns. The inventory and simulation projections
already use the existing `inventories` and `simulations` schema tables; no
schema migration was needed.

`policy/managed-api-classification.json` classifies the public managed gateway
families. Public directory reads remain stateful public observations but never
reconcile owned data; trade views, BobNet history, device audit/logs,
leaderboards, simulation scenarios/active views, and the global star catalogue
are volatile/reference or raw-only, so they do not make a false full-sync
promise.

## `SyncPlan::full()` graph

```text
Account ───────► Devices ───────► Replicants ───────► Locations ───────► Inventory
   │
   └──────────────────────────────────────────────────────────────────► Simulations
```

Every cursor traversal is bounded by `SyncClient::max_pages` (default 100).
Inventory uses the account-wide unfiltered cursor endpoint; locations and
replicants are bounded by IDs discovered from the already committed durable
snapshot. The star catalogue is intentionally excluded.

## Authority and reconciliation

| Domain | Endpoint | Absence rule |
| --- | --- | --- |
| Account | `GET /v1/accounts/me` | Never tombstone. |
| Devices | `GET /v1/devices` | Reconcile only after every page of one unfiltered traversal commits. |
| Owned replicants | `GET /v1/replicants/{code}` | Never tombstone by absence. |
| Locations | `GET /v1/locations/{designation}` | Never tombstone by absence. |
| Inventory | `GET /v1/inventory` | Never tombstone by absence. |
| Simulations | `GET /v1/accounts/simulations` | Add/update history only; never delete absent runs. |

The authoritative endpoint details remain in `policy/authority-matrix.json`;
the full-plan contract is machine-readable in `policy/sync-domains.json`.

## Report and readiness semantics

Each `SyncDiagnostic` carries pages, items, committed revisions, collection
completeness, queued-reconciliation state, progress, and a structured,
secret-safe failure category/status/retryability. A failed traversal reports
the work committed before failure. Cancellation produces `Cancelled`, not
`Error::Closed`; actual client closure remains a failure.

`Complete` means every requested plan domain committed. `RestBaseline` means
only the essential account/device baseline committed. REST readiness never
claims event continuity; event catch-up is a separate client lifecycle state.

## Deterministic end-to-end scenario

`full_sync_commits_every_durable_managed_domain` mounts one mock response each
for account, device collection, owned replicant detail, location detail,
account inventory, and simulation history. It verifies the complete report and
the committed device, replicant, location, inventory, and simulation state.
`failed_domain_keeps_prior_commits_and_error_cause` verifies that a replicant
503 retains the device commit and exposes a retryable 503 diagnostic.

## Files changed

- `src/domain/adapters.rs`
- `src/managed/gateways.rs`
- `src/managed/simulations.rs`
- `src/managed/sync.rs`
- `policy/authority-matrix.json`
- `policy/managed-api-classification.json`
- `policy/persistence-schema.json`
- `policy/phase-11.5-remediation.json`
- `policy/sync-domains.json`
- `scripts/phase_11_5_remediation_check.py`
- this report

## Commands and results

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --all-targets --all-features -- --deny warnings` | passed |
| `cargo test --all-features` | passed — 143 tests |
| `cargo test sync` | passed — 8 tests |
| `cargo test full_sync` | passed — 1 test |
| `cargo check --all-features --examples` | passed |
| `python3 scripts/authority_matrix_check.py` | passed |
| `python3 scripts/schema_policy_check.py` | passed |
| `python3 scripts/phase_11_5_remediation_check.py` | passed after this report was registered |
| `python3 scripts/phase_11_5_remediation_check.py --self-test` | passed after the ledger validator was extended for this required report |

## Ledger evidence

This phase resolves B-09, H-09, and H-10 with the source, policy, report, and
regression-test references recorded in `policy/phase-11.5-remediation.json`.
