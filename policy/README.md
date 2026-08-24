# Policy files

Checked-in JSON declarations of what the client is *allowed* to expose and how
it must behave. The gates under `scripts/` verify the implementation against
these files — they are the expected state, not a record of the actual state.

**Do not edit a policy file to make a failing gate pass.** Fix the
implementation. If the policy genuinely needs to change, update it with an
accurate reason and an evidence citation in the same commit as the code.

## Files

| File | Contents | Read by |
| --- | --- | --- |
| `operations.json` | The operation inventory: every Replicant Space operation the client supports, plus totals. Generated, not hand-written. | `contract_policy_check`, `authority_matrix_check`, `raw_transport_policy_check`, `mutation_adapter_policy_check` |
| `authority-matrix.json` | One precise authority rule per supported operation — which layer owns the resulting state. Generated. | `authority_matrix_check` |
| `contract-metadata.json` | Provenance of the active contract: version, crawl timestamp, OpenAPI source URL, and the deprecation asides that override missing OpenAPI `deprecated` flags. | `contract_policy_check` |
| `documented-operation-deltas.json` | Operations that exist in the rendered documentation but not in `openapi.json`, with the base and documentation versions they bridge. | `contract_policy_check`, `authority_matrix_check`, `generate_authority_matrix` |
| `excluded-fields.json` | Fields deliberately omitted from the typed surface, with reasons. | `contract_policy_check` |
| `normalization-aliases.json` | Wire-name to normalized-name mappings applied during domain normalization. | `contract_policy_check` |
| `mutation-adapters.json` | The managed/raw partition for unsafe operations: which mutations may only be reached through `raw`, and which are managed. | `mutation_adapter_policy_check` |
| `persistence-schema.json` | The expected fresh SQLite schema, checked against `migrations/0001_initial.sql`. Includes durability settings. | `schema_policy_check` |
| `sync-domains.json` | Full-sync plan and readiness definitions per sync domain. | `generate_authority_matrix` |
| `managed-api-classification.json` | Durable managed domains, per-operation classifications, and full-sync exclusions. | Reference declaration; no gate currently reads it. |

## Regenerating

`operations.json` and `authority-matrix.json` are generated. After a contract
or surface change:

```sh
python3 scripts/generate_operation_inventory.py
python3 scripts/generate_authority_matrix.py
python3 scripts/contract_policy_check.py
```

Then run `make policy-checks` for the full set.

See [`../CONTRIBUTING.md`](../CONTRIBUTING.md) for contract authority rules and
[`../scripts/README.md`](../scripts/README.md) for what each gate asserts.
