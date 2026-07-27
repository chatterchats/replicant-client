# Phase 11.5.09 final correctness audit

**Decision: NO-GO. Phase 12 must not begin.**

Audit date: 2026-07-26.  This was an independent audit of the dirty
remediation worktree; unrelated existing changes were preserved.  The rewrite
guide, post-Phase 11 review, current source/tests, policy ledgers, migration,
OpenAPI corpus, and rendered contract documentation were inspected.

## Release blockers

1. **B-09 remains open.** `SyncPlan::full()` covers only `Account`, `Devices`,
   `Replicants`, and `Locations`.  The only regression is a plan-membership
   assertion; there is no end-to-end full-reconciliation proof for every
   advertised durable domain.  It cannot substantiate the advertised `full()`
   contract.
2. **B-12 remains open.** `src/managed/operation.rs` still contains the
   hand-written `dispatch_target` mapping and JSON route/body dispatcher.
   Managed mutations do not reuse one shared typed raw mutation adapter.
3. **H-06 remains open.** `StateEngine` synchronously locks `StoreHandle`
   (`std::sync::Mutex`) and executes SQLite calls from async managed gateway,
   event, synchronization, and operation flows.  This violates the required
   Tokio-worker isolation.
4. The remediation checker reports B-09 and B-12 as release blockers, and its
   `--self-test` fails an assertion.  A release-gate validator that cannot
   validate its negative fixture is not release evidence.
5. Required release-gate fault/stress evidence is missing: interrupted
   migration, shutdown timeout, concurrent operations on one entity, slow
   subscribers, foreground/background scheduler priority, and restoration for
   every advertised durable domain.  The scheduler-priority item also matches
   still-open M-12.

The repository contains only the baseline, package-files, and runtime-quality
Phase 11.5 reports.  Reports for remediation prompts 11.5.01--08 were not
present, so their claimed focused evidence could not be read as required.

## Finding ledger audit

`resolved` below means the current ledger has code/test evidence and the
focused audit did not disprove it; it is not a substitute for the outstanding
release blockers above.  `open` is deliberately not rewritten: the required
evidence is absent or insufficient.

| Finding | Audit result | Evidence or required fix |
| --- | --- | --- |
| B-01 | resolved | Typed `RateLimitReset`; `reset_epoch_is_converted_relative_to_observation_time` passed. |
| B-02 | resolved | Transactional submission claim; `failed_submission_claim_never_transmits_the_request` passed. |
| B-03 | resolved | Cursor/name/payload evidence match; focused evidence test passed. |
| B-04 | resolved | Atomic journal/projection and concurrent enqueue test passed. |
| B-05 | resolved | Startup watermark test passed. |
| B-06 | resolved | Confirmed cleanup test passed. |
| B-07 | resolved | Partial-seed reconciliation-work test passed. |
| B-08 | resolved | Explicit simulation realm isolation test passed. |
| B-09 | **open** | Implement and prove genuine full reconciliation for all advertised durable domains. |
| B-10 | resolved | `ObservationTime` normalization test passed. |
| B-11 | resolved with evidence gap | Restore implementation covers stored projections; add restart coverage for every advertised durable domain. |
| B-12 | **open** | Remove `OperationEngine::dispatch_target`/JSON transport duplication; use shared typed raw mutation adapters. |
| H-01 | resolved | Five repetitions of 100 concurrent `close()` callers passed. |
| H-02 | **open** | Add/verify essential-baseline failure cannot yield `Ready`; ledger remains open. |
| H-03 | resolved | Event-store failure retains cursor/notification safety; focused test passed. |
| H-04 | resolved | Cursor monotonicity test passed. |
| H-05 | resolved | Immutable account binding evidence recorded. |
| H-06 | **open** | Move synchronous SQLite work off Tokio workers; current `StateEngine` performs it directly. |
| H-07 | resolved with evidence gap | Bounded/coalescing watches exist; add an explicit slow-subscriber stress test. |
| H-08 | resolved | Watch-based timeout test recorded. |
| H-09 | **open** | Complete the managed API/state coverage or intentionally remove unsupported public surface. |
| H-10 | **open** | Complete `SyncReport` error/readiness semantics and prove them. |
| H-11 | resolved | `message_notify` is absent from raw source; serialization regression passed. |
| H-12 | resolved | Streamed body cap regression passed. |
| H-13 | resolved | SSE metadata/reconnect cursor regression passed. |
| H-14 | resolved | Typed account-wipe success response regression passed. |
| M-01 | resolved | Route/schema-aware raw transport policy check passed. |
| M-02 | resolved | Local page-limit validation regression passed. |
| M-03 | resolved | `RUSTDOCFLAGS=-D warnings cargo doc` passed. |
| M-04 | resolved | Error-source regression is recorded. |
| M-05 | **open** | Replace or bound the O(n²) fluent query filter and add a regression/benchmark. |
| M-06 | resolved | Package allowlist check passed: 70 files, no local tooling/reference corpus. |
| M-07 | resolved | Current toolchain and `+1.94` check/test both passed. |
| M-08 | resolved with evidence gap | Known-version rejection implementation exists; add interrupted-migration fault injection. |
| M-09 | resolved with evidence gap | Graceful close join test exists; add the requested shutdown-timeout fault injection. |
| M-10 | resolved | Unknown device command round-trip regression passed. |
| M-11 | **open** | Align `ready()` naming/behavior with actual degraded-readiness semantics. |
| M-12 | **open** | Implement/prove foreground priority over background synchronization. |
| M-13 | resolved | Operation projection/state commit is atomic; focused test passed. |

## Direct contract, persistence, and behavioral checks

- Parsed `reference/replicant-space/openapi.json`: 84 operations.  It exactly
  matches `policy/operations.json`: 77 supported, five deprecated, and two
  admin operations; no missing or extra policy entries.
- `raw_transport_policy_check.py` verified all 77 callable route descriptors
  against the contract.  Raw source contains no literal callable route for the
  seven excluded operations and no `message_notify` field.
- Rendered documentation was inspected alongside the OpenAPI corpus; the
  policy/deprecation gate passed.
- Migration `0001_initial.sql` and `persistence-schema.json` were inspected:
  the schema declares account binding, normalized projections, simulations,
  event journal/cursor, operation journal, and reconciliation tables.  Current
  state restoration loads account, devices, replicants, locations, inventories,
  and simulations.  The required all-domain restart test is still missing.
- Managed-read commit/publish is covered by
  `targeted_device_read_fetches_once_and_is_visible_before_return`; local-query
  no-network behavior is covered by
  `local_device_queries_filter_relationships_and_never_use_the_network`.
- Realm-aware event, simulation cleanup, atomic event deduplication, monotonic
  cursor, operation submission/evidence, typed timestamp, and terminal
  projection tests all passed.  These do not cure the open full-sync,
  mutation-adapter, or blocking-SQLite findings.

## Commands and results

All commands ran from the repository root unless noted.

| Command | Result |
| --- | --- |
| `git status --short` | Dirty pre-existing remediation worktree preserved. |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --all-targets --all-features -- --deny warnings` | passed |
| `cargo test --all-features` | passed |
| `cargo check --no-default-features --features raw,rustls-tls` | passed |
| `cargo check --no-default-features --features raw,native-tls` | passed |
| `cargo check --no-default-features --features events,rustls-tls` | passed |
| `cargo check --no-default-features --features events,native-tls` | passed |
| `cargo check --features managed,rustls-tls` | passed |
| `cargo check --no-default-features --features managed,native-tls` | passed |
| `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` | passed |
| `python3 scripts/contract_policy_check.py` | passed |
| `python3 scripts/forward_compatibility_policy_check.py` | passed |
| `python3 scripts/raw_transport_policy_check.py` | passed; 77 routes |
| `python3 scripts/schema_policy_check.py` | passed |
| `python3 scripts/authority_matrix_check.py` | passed; 77 supported operations covered |
| `python3 scripts/phase_11_5_remediation_check.py` | exit 0, but reports remaining blockers B-09 and B-12 |
| `python3 scripts/phase_11_5_remediation_check.py --self-test` | **failed**: assertion in `self_test()` |
| `cargo package --list` | passed |
| `cargo package` | passed; warning: manifest lacks documentation, homepage, and repository metadata |
| `cargo +1.94 check --all-targets --features managed,rustls-tls` | passed |
| `cargo +1.94 test --all-features` | passed |
| `python3 scripts/package_contents_check.py` | passed; 70 allowed files |

Focused commands passed for reset epoch, durable claim, operation evidence,
atomic SSE/log dedup, initial watermark, confirmed simulation cleanup,
partial seed handling, realm-aware events, full-plan membership, typed time,
restart restoration, operation dispatch inventory, atomic terminal projection,
event-store failure, cursor monotonicity, 100 concurrent close callers,
graceful close join, local no-network queries, and managed-read commit/publish.

Two ad-hoc JavaScript contract-audit snippets initially failed due to escaping
errors in the audit snippet itself; the corrected third run succeeded.  No
repository state changed.  No other command failed.

## Freeze readiness and next action

Database schema coverage, package allowlisting, public package identity, and
the passing compiler/test matrix are encouraging, but they are not a release
freeze.  The package metadata warning should also be resolved before
publication.

**Phase 12 may not begin.** Resolve B-09, B-12, H-06, every still-open
high/medium finding, the remediation-validator self-test, and the missing
fault/stress/restart evidence above; then rerun this independent audit.
