# Phase 11.6.02 — Typed Mutation Adapters

## Result

`src/managed/operation.rs` no longer has `dispatch_target`, a string-to-route
switch, or a generic managed method/path/body HTTP dispatcher.  Its private
`MutationAdapter` enum is the durable operation contract.  Each variant carries
the exact raw request type and path parameters, serializes as a tagged durable
intent, and replays by calling the corresponding `raw::Client` endpoint method.
`TypedMutationAdapter` supplies the stable operation ID, mutating safety,
rate-limit bucket label, target/reconciliation metadata, durable intent, and
one typed submission method.

The enum is intentionally internal.  Its exhaustive `match` arms make an
unknown persisted kind fail decoding before any HTTP call; there is no fallback
transport path.

## Inventory and raw mapping

`policy/mutation-adapters.json` partitions all 27 supported unsafe 2.3.1
operations exactly:

- 24 managed routes, one typed adapter each;
- 3 explicit raw-only bootstrap routes: account registration, account recovery,
  and feedback submission.

The dynamic device-command form is a typed open-vocabulary adapter carrying a
`JsonObject`; it calls the same `raw.devices().command` endpoint as known
commands.  Unknown command names therefore survive durable serialization and
replay without an arbitrary HTTP escape hatch.

`scripts/mutation_adapter_policy_check.py` compares that partition against
`policy/operations.json` and fails for a missing, duplicate, or wrongly
classified unsafe route.

## Response, evidence, and durability

The managed attempt still claims `Submitted` before its one automatic send and
still classifies transport failures as ambiguous without retry.  Typed raw
decoding happens before the operation projection advances.  A typed simulation
enter response is serialized into the accepted projection so the simulation
gateway can commit its realm observations before completion; acknowledgement
responses remain reconciliation-gated and cannot complete before their
authoritative observation/evidence path runs.

Requests used by durable replay now deserialize from the tagged intent.  The
tri-state account update field preserves omitted/null/value semantics on that
round trip.  Credential-shaped dynamic-command data is still rejected before
the intent is written.

## Regression evidence

- `mutation_adapter_inventory_covers_every_non_bootstrap_unsafe_operation`
  proves the source has no former dispatcher and keeps the 24 managed unsafe
  route count aligned with contract policy.
- `mutation_adapter_dynamic_command_round_trips_without_a_dispatcher` proves
  an unknown command name and arguments survive typed durable intent replay.
- Existing operation regressions remain green for durable claim failure,
  ambiguous no-retry, restart recovery, and response projection ordering.
- Simulation gateway regressions prove a typed success response is retained for
  observation commit before the operation is waited to completion.

## Commands run

All commands were run from the repository root and passed:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- --deny warnings
cargo test --all-features                         # 144 passed
cargo test operation                              # 24 passed
cargo test mutation_adapter                       # 2 passed
cargo check --all-features --examples
python3 scripts/contract_policy_check.py
python3 scripts/raw_transport_policy_check.py
python3 scripts/mutation_adapter_policy_check.py  # 24 managed, 3 raw-only
python3 scripts/phase_11_5_remediation_check.py
python3 scripts/phase_11_5_remediation_check.py --self-test
```

## Files changed by this prompt

- `src/managed/operation.rs`
- `src/raw/accounts.rs`
- `src/raw/common.rs`
- `src/raw/devices.rs`
- `src/raw/locations.rs`
- `src/raw/messages.rs`
- `src/raw/replicants.rs`
- `src/raw/simulations.rs`
- `policy/mutation-adapters.json`
- `scripts/mutation_adapter_policy_check.py`
- `scripts/phase_11_5_remediation_check.py`
- `policy/phase-11.5-remediation.json`
- `docs/implementation/phase-11.6/02-typed-mutation-adapters.md`

## B-12 ledger evidence

B-12 is resolved in `policy/phase-11.5-remediation.json` with references to
the internal typed adapter, the inventory policy/check, the two adapter
regressions, and this report.
