# Scripts

Python tooling for the contract gates. Everything here targets the system
`python3` and takes no third-party dependencies.

## Gates — wired into `make policy-checks`

Every checked-in policy gate has a first-class Make target. All are
non-destructive and safe to run individually.

| Script | Make target | Asserts |
| --- | --- | --- |
| `contract_policy_check.py` | `contract-policy-check` | Contract/deprecation policy and operation inventory consistency. |
| `coverage_audit.py check` | `coverage-audit-check` | Current unit/schema-field coverage. |
| `mutation_adapter_policy_check.py` | `mutation-adapter-policy-check` | Managed/raw unsafe-operation partition. |
| `package_contents_check.py` | `package-contents-check` | Published-package contents remain free of private tooling/contract source. |
| `contract_coverage_check.py` | `contract-coverage-check` | Checked-in contract coverage matches the current policy surface. |
| `forward_compatibility_policy_check.py` | `forward-compatibility-policy-check` | Normalized snapshots stay forward-compatible and independent of raw DTOs. |
| `raw_transport_policy_check.py` | `raw-transport-policy-check` | Raw surface matches the OpenAPI-backed inventory. |
| `schema_policy_check.py` | `schema-policy-check` | `policy/persistence-schema.json` matches `migrations/0001_initial.sql`. |
| `authority_matrix_check.py` | `authority-matrix-check` | Every supported operation has exactly one precise authority rule. |

`make policy-checks` composes all of the above plus the policy unit tests.

## Generators — explicit state-changing maintenance

| Script | Produces |
| --- | --- |
| `generate_operation_inventory.py` | `policy/operations.json` from the newest contract snapshot. |
| `generate_authority_matrix.py` | `policy/authority-matrix.json` from the operation inventory and `policy/sync-domains.json`. |

Run `make policy-generate`, inspect and commit the generated diff, then run
`make policy-checks` whenever a change affects which operations, fields, or
aliases the client exposes.

## Utilities

| Script | Purpose |
| --- | --- |
| `reference_snapshot.py` | Locates versioned snapshots under `reference/replicant-space-*` and resolves the highest semantic version. Imported by the gates; rarely run directly. |
| `repo_zip.py` | Creates maximum-compression ZIPs of the Git working tree and, optionally, local logs and databases. Invoked by `make zip` or `make zip-all`. |
| `manage_token.py` | Creates/rotates `REPLICANTD_TOKEN` in the ignored `.env`; invoked by `make token` / `make token-rotate`. |
| `ci_changed.py` | Resolves the last successful GitHub validation baseline and classifies the cumulative diff into core/policy/Galaxy/web/desktop/docs/Docker CI domains. |

`test_repo_zip.py`, `test_manage_token.py`, and `test_ci_changed.py` are composed
by `make utility-tests` and therefore by `make ci-policy` / full `make ci`.

## Historical one-off tooling

`phase_11_5_remediation_check.py` validates a historical Phase 11.5 remediation
ledger. It is intentionally not wired into the current gate; verify its context
before reusing or extending it.

Do not weaken any gate to make a change pass. Fix the implementation, or amend
the relevant file under [`../policy/`](../policy) with a reason and evidence.
