# Phase 11.5.00 baseline

Captured after installing the authoritative review and adding only the Phase
11.5 remediation ledger and its policy gate. No transport, event, operation,
simulation, persistence, sync, lifecycle, or public-API behavior was changed.

## Commands and results

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --all-targets --all-features -- --deny warnings` | passed |
| `cargo test --all-features` | passed; all reported test groups passed |
| `cargo check --all-features --examples` | passed |
| `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` | failed; see blockers below |
| `python3 scripts/contract_policy_check.py` | passed |
| `python3 scripts/forward_compatibility_policy_check.py` | passed |
| `python3 scripts/raw_transport_policy_check.py` | passed |
| `python3 scripts/schema_policy_check.py` | passed |
| `python3 scripts/authority_matrix_check.py` | passed |
| `python3 scripts/phase_11_5_remediation_check.py` | passed; 39 findings, B-01 through B-12 remain release blockers |
| `python3 scripts/phase_11_5_remediation_check.py --self-test` | passed; validates missing IDs, duplicates, and evidence-free resolved items fail |
| `cargo package --list` | failed because the worktree is intentionally dirty |
| `cargo package --list --allow-dirty` | passed; 193 package files recorded in `phase-11.5-package-files.txt` |

The supported feature matrix also passed:

```text
cargo check
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
cargo check --all-features
cargo check --no-default-features --features raw,rustls-tls
cargo check --no-default-features --features raw,native-tls
cargo check --no-default-features --features events,rustls-tls
cargo check --no-default-features --features managed,rustls-tls
cargo check --no-default-features --features managed,native-tls
```

`make policy-checks` passed, including all pre-existing policy gates and the
new remediation gate. `make ci` reached the documentation step and failed
there before policy checks, matching the direct documentation result above.

## Baseline blockers for later remediation

`cargo doc` fails under `-D warnings` on four unresolved intra-doc links:

- `ResponseMetadata`;
- `crate::raw::replicants::ReplicantsClient::send_message`;
- `EventLogQuery::filtered`;
- `adapters`.

`cargo package --list` also warns that the manifest has no documentation,
homepage, or repository metadata. The dirty-worktree refusal is expected for
this checkpoint; the allow-dirty package listing is retained separately.
