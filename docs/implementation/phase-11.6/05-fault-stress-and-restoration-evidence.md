# Phase 11.6.05 — fault, stress, and restoration evidence

## Evidence inventory

| Finding | Deterministic regression |
| --- | --- |
| B-11 / E-01 | `managed::sync::tests::full_sync_restores_every_durable_managed_domain_after_restart` |
| H-07 / E-02 | `managed::events::tests::slow_event_subscriber_is_bounded_and_reports_lag` |
| M-08 / E-03 | `managed::store::tests::interrupted_migration_rolls_back_and_retry_succeeds` |
| M-09 / E-04 | `managed::client::tests::shutdown_timeout_aborts_only_the_stuck_task_and_closes_the_store` |
| E-05 | `managed::operation::tests::concurrent_operations_on_one_entity_resolve_only_their_own_evidence` |
| Event duplicate stress | `managed::events::tests::duplicate_event_producers_keep_the_cursor_monotonic` |
| Store pressure / repeated close | `managed::store::tests::bounded_worker_queue_reports_backpressure`; `managed::client::tests::concurrent_close_callers_share_one_completion` |
| Scheduler fairness | `raw::rate_limit::tests::queued_foreground_request_precedes_background_work` |
| Full-sync partial failure | `managed::sync::tests::failed_domain_keeps_prior_commits_and_error_cause` |

## Deterministic scenarios

- Full sync writes account, devices, replicants, locations, inventories, and simulations to one file-backed store. A `RestoreOnly` reopen points at an unavailable address; all restored local projections and the `locations().find().at("SOL-4")` result match without network I/O.
- The migration hook interrupts after schema SQL runs but before the transaction commits. The test verifies the prior marker table/data remains, the new tables are absent, and a clean retry succeeds.
- Shutdown uses a zero-duration test-only deadline with one already-complete worker and one pending worker. The timeout counter records one selective abort, the complete worker's signal arrives, and the store rejects work after closing.
- Event broadcast is deliberately overrun by 257 committed events. The subscriber receives the documented lag error, and the durable cursor remains at `257-0`. The duplicate-producer stress delivers each event through two concurrent producers and observes one notification per ID and cursor `32-0`.
- Same-entity operations record different event/payload evidence before their one permitted submission claim. An unrelated event is ignored; out-of-order travel and two same-kind deployment events resolve only the matching rows.

## Validator and ledger

`scripts/phase_11_5_remediation_check.py` now requires all Phase 11.6 reports, including `01b` and this report, plus a report path and named tests for every evidence gap. Its self-test rejects missing evidence-gap tests and reports. The ledger records E-01 through E-06 as resolved only with the named report/test evidence above.

## Commands and results

| Command | Result |
| --- | --- |
| `cargo fmt --all` | passed |
| `cargo test --all-features full_sync_restores_every_durable_managed_domain_after_restart` | passed |
| `cargo test --all-features slow_event_subscriber_is_bounded_and_reports_lag` | passed |
| `cargo test --all-features duplicate_event_producers_keep_the_cursor_monotonic` | passed |
| `cargo test --all-features shutdown_timeout_aborts_only_the_stuck_task_and_closes_the_store` | passed |
| `cargo test --all-features interrupted_migration_rolls_back_and_retry_succeeds` | passed |
| `cargo test --all-features concurrent_operations_on_one_entity_resolve_only_their_own_evidence` | passed |
| `cargo test --all-features` | passed (133 unit tests plus integration and doctests) |
| `python3 scripts/phase_11_5_remediation_check.py` | passed; no release blockers |
| `python3 scripts/phase_11_5_remediation_check.py --self-test` | passed |
| `cargo clippy --all-targets --all-features -- --deny warnings` | blocked by pre-existing `examples/initialize_colony_database.rs` compilation errors (`hydrate_system` missing; partially moved `config.token`) |
| `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` | not reached because the preceding clippy command failed |

## Files changed by this prompt

- `src/managed/client.rs`
- `src/managed/events.rs`
- `src/managed/operation.rs`
- `src/managed/state.rs`
- `src/managed/store.rs`
- `src/managed/sync.rs`
- `scripts/phase_11_5_remediation_check.py`
- `policy/phase-11.5-remediation.json`
- `docs/implementation/phase-11.6/05-fault-stress-and-restoration-evidence.md`
