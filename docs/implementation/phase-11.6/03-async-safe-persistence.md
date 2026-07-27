# Phase 11.6.03 — Async-safe persistence

## Result

SQLite is isolated behind one crate-private `StoreHandle` worker. The worker
owns the `rusqlite::Connection` for its entire lifetime; callers communicate
through a bounded Tokio MPSC queue (capacity 64) and one-shot responses. No
public store, runtime, repository, or actor API was added.

## Execution, ordering, and backpressure

- The worker is a named dedicated OS thread (`replicant-store`). It opens the
  database, migrates it, executes every command serially, checkpoints, and
  drops the connection.
- `StoreHandle::execute` is the async request future. The compatibility facade
  used by existing state call sites dispatches the same typed operation to that
  worker; it never exposes a SQLite connection or a mutex guard.
- Queue capacity is 64. Async sends await capacity; synchronous compatibility
  sends fail with `StoreError::Backpressure` rather than growing unbounded.
- FIFO worker execution preserves existing transaction boundaries: managed
  projections commit before snapshot publication; event journal/projection/
  cursor updates stay in their single store transaction; operation claims and
  terminal transitions remain atomic; reconciliation transitions remain
  serialized.

## Lifecycle and failures

`close` first rejects new commands, then queues one close command after all
accepted work. The worker completes the active transaction and queued work,
flushes SQLite, sends the close result, and exits. Startup opens/migrates on
the worker, preserving file and in-memory configuration, WAL, busy timeout,
foreign keys, full synchronous durability, and existing commit/migration test
seams. Store failures still prevent state publication because publication
remains after the worker response.

## Regression evidence

- `worker_executes_store_requests_off_the_caller_runtime_thread` proves a
  request runs on a different thread than its Tokio caller.
- `bounded_worker_queue_reports_backpressure` fills the 64-command queue and
  observes a deterministic full result.
- `worker_rejects_requests_once_close_begins` verifies close admission.
- `close_waits_for_the_active_transaction_boundary` proves close waits for the
  active worker command before flushing and exiting.
- Existing event atomicity, operation claim, failed-commit/no-publication,
  reconciliation, file migration, and restart tests continue to pass.

## Files changed

- `src/managed/store.rs`
- `src/managed/state.rs`
- `src/managed/client.rs`
- `policy/phase-11.5-remediation.json`
- `docs/implementation/phase-11.6/03-async-safe-persistence.md`

## Commands and results

- `cargo fmt --all -- --check` — passed.
- `cargo clippy --all-targets --all-features -- --deny warnings` — passed.
- `cargo test --all-features --no-run` — passed.
- `cargo test --all-features` — passed (121 unit tests plus integration and
  doc tests).
- `cargo test --all-features managed::store::tests` — passed (21 store tests).
- `cargo test store` — passed.
- `cargo test persistence` — passed.
- `cargo test event` — passed.
- `cargo test operation` — passed (24 tests).
- `cargo check --all-features --examples` — passed.
- `python3 scripts/schema_policy_check.py` — passed.
- `python3 scripts/phase_11_5_remediation_check.py` — passed.
- `python3 scripts/phase_11_5_remediation_check.py --self-test` — passed.

## Ledger evidence

H-06 is resolved by `StoreHandle` worker isolation and the three worker
regressions above. M-09 retains its existing graceful task shutdown evidence;
the worker now supplies the final ordered flush/close boundary.
