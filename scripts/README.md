# Scripts

Python tooling for the contract gates. Everything here targets the system
`python3` and takes no third-party dependencies.

## Gates — wired into `make policy-checks`

Run in this order by the Makefile. All are non-destructive and safe to run
individually.

| Script | Asserts |
| --- | --- |
| `contract_policy_check.py` | Contract and deprecation policy: the operation inventory matches the active snapshot, exclusions are justified, aliases resolve, and rendered-doc deprecation asides are honoured. |
| `forward_compatibility_policy_check.py` | Normalized snapshots stay forward-compatible and independent of raw DTOs. |
| `raw_transport_policy_check.py` | The raw surface matches the OpenAPI-backed inventory. |
| `schema_policy_check.py` | `policy/persistence-schema.json` matches `migrations/0001_initial.sql`. |
| `authority_matrix_check.py` | Every supported operation has exactly one precise authority rule. |

`make contract-policy-check` runs only the first of these.

## Generators — run manually, commit the output

| Script | Produces |
| --- | --- |
| `generate_operation_inventory.py` | `policy/operations.json` from the newest contract snapshot. |
| `generate_authority_matrix.py` | `policy/authority-matrix.json` from the operation inventory and `policy/sync-domains.json`. |

Run both, then re-run `contract_policy_check.py`, whenever a change affects
which operations, fields, or aliases the client exposes.

## Utilities

| Script | Purpose |
| --- | --- |
| `reference_snapshot.py` | Locates versioned snapshots under `reference/replicant-space-*` and resolves the highest semantic version. Imported by the gates; rarely run directly. |
| `repo_zip.py` | Creates maximum-compression ZIPs of the Git working tree and, optionally, local logs and databases. Invoked by `make zip` or `make zip-with-data`. |

## Not wired into any gate

These exist but no Makefile target runs them. Verify whether they still reflect
current policy before trusting or extending them.

| Script | Purpose | Status |
| --- | --- | --- |
| `mutation_adapter_policy_check.py` | Verifies the managed/raw unsafe-operation partition against `policy/mutation-adapters.json`. | Plausibly still meaningful; consider adding to `policy-checks`. |
| `package_contents_check.py` | Rejects private tooling and contract-source artifacts from the published crate package. | Only meaningful if the crate is ever published; the repo is currently unpublished. |
| `phase_11_5_remediation_check.py` | Validates a historical Phase 11.5 remediation ledger and its release blockers. | Appears to be a spent one-off. |

Do not weaken any gate to make a change pass. Fix the implementation, or amend
the relevant file under [`../policy/`](../policy) with a reason and evidence.
