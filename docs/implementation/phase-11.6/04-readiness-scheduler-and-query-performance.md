# Phase 11.6.04 — readiness, scheduler, and query performance

## Result

`Readiness` is stored as independent component truth rather than inferred from
the last lifecycle-status writer. Public barriers are:

| Question | API |
| --- | --- |
| Can cached durable state be used? | `Readiness::locally_usable()` / `Client::wait_until_usable()` |
| Has startup completed? | `Client::startup_policy_satisfied()` / `Client::ready()` |
| Is every tracked component live? | `Client::is_live()` |
| Is any component degraded? | `Readiness::is_degraded()` / `ClientStatus` |

Components cover restoration, account binding, essential and full REST
baselines, event catch-up, SSE, reconciliation, and store health.
`Client::derived_status` recomputes public status after each component update;
SSE success therefore cannot turn a failed REST baseline into `Ready`.

| Policy | Completion |
| --- | --- |
| `RestoreOnly` | Local restoration and store health; no network work. |
| `Essential` | Account binding, essential baseline, catch-up, and SSE. |
| `Full` | Essential completion plus the bounded full baseline. |

SSE failure before a first live connection is `Degraded(StartupIncomplete)`;
a later disconnect is `Offline`. Event continuity failure remains
`Degraded(EventContinuity)`.

## Scheduler and query

`RateLimitCoordinator::acquire_with_priority` maintains per-bucket foreground
and background queues. Foreground goes first; after eight foreground permits,
queued background work gets one turn. Inactive tickets from cancelled waits are
removed during later scheduling passes, while the same rate-limit bucket still
sets permit spacing.

`DeviceQuery::without_adopted_devices()` materializes the immutable snapshot
once, builds a `BTreeSet` of referenced controller keys in one pass, and
filters by membership. Relationship work is O(n). The regression compares it
with a simple O(n²) reference filter over a 134-device fleet.

The manual benchmark ran successfully:

```
10,000 snapshot rows; 1,000 indexed predicates in 45.348553ms (target: < 1s)
```

## Commands and results

- `cargo fmt --all -- --check` — passed.
- `cargo clippy --all-targets --all-features -- --deny warnings` — passed.
- `cargo test --all-features` — passed (149 tests).
- `cargo test --all-features readiness` — passed.
- `cargo test --all-features queued_foreground_request_precedes_background_work` — passed.
- `cargo test --all-features without_adopted_devices_matches_the_reference_relationship_filter` — passed.
- `cargo bench --no-run` and `cargo bench --bench state_snapshot` — passed.
- `cargo check --all-features --examples` — passed.
- `python3 scripts/phase_11_5_remediation_check.py` and `--self-test` — passed.

## Ledger evidence and files

H-02 uses the independent-readiness and SSE regressions; M-05 uses the
reference-equivalence regression; M-11 uses the startup-policy test; and M-12
uses the priority test. The remediation ledger records these references.

Changed: `src/managed/client.rs`, `src/managed/events.rs`,
`src/managed/sync.rs`, `src/managed/gateways.rs`, `src/raw/rate_limit.rs`,
`policy/phase-11.5-remediation.json`, and this report.
