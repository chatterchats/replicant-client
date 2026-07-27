# Phase 11.6.00 baseline and validator

## Authoritative inputs

| Path | SHA-256 |
| --- | --- |
| `docs/implementation/rewrite-guide.md` | `12e8bcd835154862b55650104a93dbab035cd49a96e20dae159e2a09c8784a95` |
| `docs/implementation/post-phase-11-review.md` | `d2173947480e315b0b829b52494efec9842b6a9e19bbf04d674fdb75faf2f3d9` |
| `docs/implementation/phase-11.5-final-audit.md` | `9d5829b4835d5189259652ae75f42e12dcfb6cd4eab34404d4b1e534da15abcb` |
| `reference/replicant-space/openapi.json` | `ca018a938541f23c4838e8fe58f78889d9ca4b9ab81b488112f90589dd83c2f4` |

The three required implementation documents were present at those paths before
this change. The 84-operation OpenAPI corpus and the rendered event-log,
event-stream, and running-simulation pages were read directly. This phase does
not change any contract-facing code.

## Worktree and report inventory

Initial `git status --short` contained the pre-existing untracked final audit
and `docs/implementation/replicant-client-electronics-style-mermaid.md`; both
were preserved. The final worktree additionally contains this Phase 11.6
report and changes to the validator, ledger, and CI workflow.

Present Phase 11.5 artifacts:

- `docs/implementation/phase-11.5-baseline.md`
- `docs/implementation/phase-11.5-final-audit.md`
- `docs/implementation/phase-11.5-package-files.txt`
- `docs/implementation/phase-11.5-runtime-quality.md`

There is no prompt-indexed evidence report for any of 11.5.01 through 11.5.08.
The final audit records those missing reports as an evidence gap; the
unindexed runtime-quality report does not replace them.

## Validator repair and regression matrix

Root cause: the old `resolved without evidence` negative fixture changed
`B-01` to `resolved`, but `B-01` was already resolved with complete evidence.
It therefore remained valid and made `--self-test` fail.

The validator now requires and checks the Phase 11.6 report registry and
separate evidence-gap schema. Its deterministic `--self-test` covers:

| Case | Expected result |
| --- | --- |
| checked-in ledger | valid |
| fully evidenced ledger | valid |
| missing `B-01` | rejected as missing |
| duplicate `B-01` | rejected as duplicate |
| unknown finding status | rejected |
| resolved finding with cleared evidence | rejected |
| missing 11.6.00 report entry or file | rejected |
| checked-in open blockers | exactly `B-09`, `B-12` |

`policy/phase-11.5-remediation.json` records this report and the self-test
path. It also keeps fault/stress evidence gaps separate from code defects:
all-domain restoration, slow subscriber, interrupted migration, shutdown
timeout, concurrent same-entity operations, and foreground-priority evidence.

## Baseline commands

All commands were run from the repository root after the validator repair.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --all-targets --all-features -- --deny warnings` | passed |
| `cargo test --all-features` | passed (113 unit tests) |
| `python3 scripts/contract_policy_check.py` | passed: 84 operations; 77 supported, five deprecated, two admin |
| `python3 scripts/forward_compatibility_policy_check.py` | passed |
| `python3 scripts/raw_transport_policy_check.py` | passed: 77 callable methods |
| `python3 scripts/schema_policy_check.py` | passed |
| `python3 scripts/authority_matrix_check.py` | passed: 77 supported operations |
| `python3 scripts/phase_11_5_remediation_check.py` | passed; release blockers `B-09`, `B-12` remain |
| `python3 scripts/phase_11_5_remediation_check.py --self-test` | passed |
| `make remediation-policy-check` | passed: normal validator and self-test |
| `cargo package --list` | passed; non-blocking manifest metadata warning remains |
| `git diff --check` | passed |

## Remaining blockers and changed files

No substantive product finding was resolved. The ledger keeps `B-09`, `B-12`,
`H-02`, `H-06`, `H-09`, `H-10`, `M-05`, `M-11`, and `M-12` open. Phase 12 was
not started.

Files changed by this phase:

- `.github/workflows/ci.yml`
- `policy/phase-11.5-remediation.json`
- `scripts/phase_11_5_remediation_check.py`
- `docs/implementation/phase-11.6/00-baseline-and-validator.md`
